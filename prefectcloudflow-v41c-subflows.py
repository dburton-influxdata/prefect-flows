from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.task_runners import ConcurrentTaskRunner
#from prefect_dask.task_runners import DaskTaskRunner
from prefect.deployments import Deployment
import asyncio
from prefect.artifacts import create_table_artifact

from flightsql import FlightSQLClient, connect
import os
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


#Pandas 2.0 setting: engine='pyarrow', dtype_backend='pyarrow'

#Documentation Links
# https://docs.influxdata.com/influxdb/cloud-iox/query-data/sql/explore-schema/
# https://docs.influxdata.com/influxdb/cloud-iox/query-data/sql/basic-query/
# https://docs.influxdata.com/influxdb/cloud-iox/query-data/sql/aggregate-select/


#InfuxData Variables


# Prefect Blocks
from prefect.blocks.system import Secret
secret_block = Secret.load("iox-token")
# Access the stored secret
token  = secret_block.get()

from prefect_github import GitHubCredentials
github_credentials_block = GitHubCredentials.load("github-prefect")


#from prefect_github.repository import query_repository
#from prefect.filesystems import GitHub
#github_block = GitHub.load("github-influx-downsample")

from prefect_github.repository import GitHubRepository
github_repository_block = GitHubRepository.load("github-influxdata-repo")


from prefect.infrastructure.docker import DockerContainer
docker_container_block = DockerContainer.load("iox-docker")


######################################


org = 'dtiolab'
bucket = 'dtiolab'
url = "https://us-east-1-1.aws.cloud2.influxdata.com/"

pa_type = pd.ArrowDtype(pa.timestamp("ns"))

# 3. Instantiate the FlightSQL Client
client = FlightSQLClient(
    host="us-east-1-1.aws.cloud2.influxdata.com",
    #token=os.environ["INFLUX_TOKEN"],
    token=token,
    metadata={"bucket-name": "dtiolab"},
    features={'metadata-reflection': 'true'}
)


################################################
#     Functions with Prefect Flow decorators
###############################################

@task()
def hi():
    print("Hi from Prefect! 🤗")


@task()
def gettable():
    #hi()
    t = client.get_tables()
    #s = client.get_db_schemas()
    #c = client.get_catalogs()
    tables = client.do_get(t.endpoints[0].ticket).read_all()

    #print(tables)
    #https://github.com/pandas-dev/pandas/issues/51760
    tables_df = tables.to_pandas(types_mapper=pd.ArrowDtype)
    print(tables_df.dtypes)

    #Convert table_name to Measurment to map to Influxdata terminology
    tables_df.rename({'table_name': 'Measurement'}, axis='columns', inplace=True)
    print("Display just the Measurement Names to execute SQL queries:")

    #Take just the iox catalog items
    iox_df = tables_df[tables_df['db_schema_name'] == 'iox']

    #Remove any tabels with downsampled in the name
    iox_df = iox_df[~iox_df['Measurement'].str.contains("downsampled")]

    #Convet a list to query against
    measurements = iox_df['Measurement'].to_list()

    #display tables_df
    #tables_df[['Measurement']]

    print("The availabile Measurments in the defined IOx connection include:")
    print("Measurements:", measurements)
    return(measurements)

    #Return measurments as could be used as a pick-list

@task()
def write_parquet_table(table, table_name):
    print("Writing Table:", table_name, "to parquet file on disk")
    pq.write_table(table, f'/tmp/iox_data/{table_name}.parquet', compression='GZIP')
    print("Parquet Table Write Complete")


@flow(log_prints=True,
      task_runner=ConcurrentTaskRunner(),
      description="Executes the defined SQL query and returns a Pandas Dataframe.")
def execute_iox_query(measurement):
    #########################################################################################################################################
    #4. Execute a query against InfluxDB's Flight SQL endpoint. Here we are querying for all of our data.
    # The function should ingest the Table Name aka measurment to query.

    #input measurement variable as Table to Query
        
   

    #We should convert this basic query into a more powerful query that takes advantage of downsampling on the IOx side using the bin function:
    """ SELECT
        DATE_BIN('1 minute', time) AS time,
        "host",
        max("Percent_User_Time") as max_user_time,
        max("Percent_Privileged_Time") as max_priviliged_time,
        max("Percent_DPC_Time") as max_dpc_time,
        max("Percent_Interrupt_Time") as max_interrupt_time
        FROM "win_cpu"
        GROUP BY DATE_BIN('1 minute', time), "host"
        ORDER BY time
    """
#IOx Query and generate Table - df

    #Query to Execute
    #flightsql = f'select * from {measurement} limit 100000'

    flightsql = f"WITH times_cte AS ( SELECT host, MAX(time) AS time \
        FROM {measurement} \
            GROUP BY host, date_trunc('minute', time) ) \
            SELECT t.* FROM {measurement} t \
            INNER JOIN times_cte m ON t.time = m.time AND t.host = m.host \
            ORDER BY host, time;"

    print(f"Executing IOx Data Query: {flightsql}")

    query = client.execute(f"{flightsql}")

    #Rewrite Query to use SQL Bins to help with downsampling on the query Side as well

    # 5. Create reader to consume result
    print("Create reader to consume result")
    reader = client.do_get(query.endpoints[0].ticket)

    # 6. Read all data into a pyarrow.Table
    print("Read all data into a pyarrow.Table")
    Table = reader.read_all()

    #Write Table (Query Results to Compresed Parquest Files as Badkup)
    print("Writing Table:", measurement, "to parquet file on disk")

    write_parquet_table(Table, measurement)

    print(measurement, "Parquet Write Complete")



    # 7. Convert to Pandas DataFrame and sort by time
    print("Convert to Pandas DataFrame")
    #df = Table.to_pandas(types_mapper=pd.ArrowDtype)
    df = Table.to_pandas()
    df = df.sort_values(by="time")

    print("The existing IOx query contains:", df.shape[0], "rows")

    #Need to add a datetimeindex for group-by to work properly
    print("converting time to datetime index")
    df = df.set_index(pd.DatetimeIndex(df['time']))
    df.set_index(["time"])

    #Write Dataframe into Prefect Artificat for Analysis
    create_table_artifact(
        key="For Support Analysis",
        table=df,
        description= "#Please reaview with Flow run and discuss with the customer"
    )


    #Return Constructed and formatted dataframe for processing and analysis
    print(df.head(7))
    return(df)


@task(retries=2, retry_delay_seconds=60)
def execute_iox_downsample(df, time_interval, down_sample_aggregation):
#############################################################################################################
# Resample Function - input of time interval and passed dataframe - return downsampled dataframe
# imput time_interval, aggregation function

    #time_interval = '10min'
    #down_sample_aggregation = 'mean'

    print(f"Performing the Downsample every {time_interval} and calculating the {down_sample_aggregation}")
    df_mean = df.groupby(by=["host"]).resample(f'{time_interval}', on='time').mean(numeric_only = True).dropna()

    
    # create a copy of the downsampled data so we can write it back to InfluxDB Cloud powered by IOx. 
    df_write = df_mean.reset_index()
    #print(df_mean.head(5))
    #print(df_mean.dtypes)
    return(df_write)

#############################################################################################################



@flow(log_prints=True,
      task_runner=ConcurrentTaskRunner(),
      description="Writes the downsampled dataframe back to IOX using the InfluxClient.")
def write_sampled_to_iox(df_write, table):
    # write data back to InfluxDB Cloud powered by IOx
    print(df_write.head(5))
    client = InfluxDBClient(url=url, token=token, org=org)
    client.write_api(write_options=SYNCHRONOUS).write(bucket=bucket,
                record=df_write,
                data_frame_measurement_name=f"{table}_mean_downsampled",
                data_frame_timestamp_column='time',
                data_frame_tag_columns=['host'])


def deploy():
    deployment = Deployment.build_from_flow(
        flow=gettable,
        name="prefect-gettable-deployment"
    )
    deployment.apply()



@flow(log_prints=True,
      task_runner=SequentialTaskRunner(),
      description="Influxdata IOx support workflow prototype.")
def iox_prototype_subflows():
    #Set Variables
    measurement = 'cpu'
    time_interval = '10min'
    down_sample_aggregation = 'mean'

    tables = gettable()
    print(tables)

    print("Running IOx Query Flow Loop through the measurments")
    for table in tables:
        print(table)
        df = execute_iox_query(table)

        print("Running Pandas Downsampling Flow")
        
        resampled = execute_iox_downsample(df, time_interval, down_sample_aggregation)

        print("writing down-sampled dataframe to Influx")
        write_sampled_to_iox(resampled, table)

    print("Completed Flow")



if __name__ == "__main__":
    iox_prototype_subflows()

    
    #deploy()

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.task_runners import ConcurrentTaskRunner
#from prefect_dask.task_runners import DaskTaskRunner
from prefect.deployments import Deployment
import asyncio
from prefect.artifacts import create_table_artifact
from prefect.artifacts import create_markdown_artifact

from prefect import variables


from flightsql import FlightSQLClient, connect
import os
import sys
from datetime import datetime
import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
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

#Get Bucket variable from the command line
#bucket = sys.argv[1]

bucket = variables.get('bucket', default='dtiolab')

from prefect.filesystems import GitHub
github_block = GitHub.load("github-influxdata-repo")

#Container Infrasturcture
from prefect.infrastructure.docker import DockerContainer
docker_container_block = DockerContainer.load("iox-docker")

#Pager Duty
from prefect.blocks.notifications import PagerDutyWebHook
pagerduty_webhook_block = PagerDutyWebHook.load("pagerduty")



org = 'dtiolab'
#bucket = 'Windows'
url = "https://us-east-1-1.aws.cloud2.influxdata.com/"

pa_type = pd.ArrowDtype(pa.timestamp("ns"))

# 3. Instantiate the FlightSQL Client Globally
client = FlightSQLClient(
    host="us-east-1-1.aws.cloud2.influxdata.com",
    #token=os.environ["INFLUX_TOKEN"],
    token=token,
    metadata={"bucket-name": f"{bucket}"},
    features={'metadata-reflection': 'true'}
)


################################################
#     Functions with Prefect Flow decorators
###############################################

@task
def markdown_task(measurment, cpfs, bucket, column_names):
    na_revenue = 500000
    cpfs_bytes = f"{cpfs:,}"
    cpfs_bytes_string = f"{cpfs_bytes}"
    #convert measurement and bucket to strip underscores and covert to all lowercase to use as Prefect Artifact Key
    bucket_lc = bucket.lower()
    tablename = measurment.replace("_", "-").lower()
    markdown_report = f"""# {measurment}
    

## Summary

The {bucket} {measurment} table is compresed to a Parquet file size of {cpfs_bytes_string} bytes.
The Table contains the following columns {column_names}.


These IOx Parquet Compression rates are encouraging and these Prefect flows demonstrate some initial scuccess with Data Orchestration.
> However, we still have room for improvement and look forward to increasing effeciency.
"""
    create_markdown_artifact(
        key=f"{bucket_lc}-{tablename}-parquet-filesize-report",
        markdown=markdown_report,
        description="IOX database Report",
    )



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
    iox_df = iox_df[~iox_df['Measurement'].str.contains("downsampled|win_proc")]

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
    pq.write_table(table, f'/tmp/{table_name}.parquet', compression='GZIP')
    print("Parquet Table Write Complete")
    print("Getting Compressed Parquet File Size")
    file_size = os.path.getsize(f'/tmp/{table_name}.parquet')
    print(f"File size: {file_size} bytes")
    return(file_size)
   

@task()
def create_artifact(df):
    create_table_artifact(
    key="For-Support-Analysis",
    table=df,
    description= "#Please reaview with Flow run and discuss with the customer")


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
        FROM \"{measurement}\" \
            GROUP BY host, date_trunc('minute', time) ) \
            SELECT t.* FROM \"{measurement}\" t \
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

    #Based on a Flow Error on the Large table and receiveing the following errors:
    #Finished in state Failed('Flow run encountered an exception. pyarrow._flight.FlightInternalError: 
    #                         Flight returned internal error, with message: Received RST_STREAM with error code 2. 
    #                         gRPC client debug context: UNKNOWN:Error received from peer ipv4:34.196.233.7:443 
    #                         {grpc_message:"Received RST_STREAM with error code 2", grpc_status:13, 
    #                          created_time:"2023-04-27T23:30:25.668318402+00:00"}. Client context: OK\n')

    try:

        Table = reader.read_all()


    except Exception:
        print("an error occured")

    else:
        # Get column names
        column_names = Table.column_names
        print(column_names)

        #Write Table (Query Results to Compresed Parquest Files as Badkup)
        print("Writing Table:", measurement, "to parquet file on disk")

        #return Compressed Parquet File size (cpfs) from write_parquet_table function

        cpfs = write_parquet_table(Table, measurement)

        print(measurement, "Parquet Write Complete", cpfs, "bytes")
        markdown_task(measurement, cpfs, bucket, column_names)

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

        #create_artifact(df)

        #Filter out any edge cases where data is older than 30 days (defult retention period)
        # Want to execute prior to downsampleing.
        print("Drop records that are 30 days old from Dataframe prior to downsampling")
        df = df[df.time > datetime.datetime.now() - pd.to_timedelta("29day")]


        #Return Constructed and formatted dataframe for processing and analysis
        print(df.head(7))
        return(df)


@task(retries=2, retry_delay_seconds=20)
def execute_iox_downsample(df, time_interval, down_sample_aggregation):
#############################################################################################################
# Resample Function - input of time interval and passed dataframe - return downsampled dataframe
# imput time_interval, aggregation function

    #time_interval = '10min'
    #down_sample_aggregation = 'mean'

    print("Checking to see if the data has any numerical columns")
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if len(numeric_cols) > 0:
        print('The following columns contain numerical values:', numeric_cols)

        print(f"Performing the Downsample every {time_interval} and calculating the {down_sample_aggregation}")
        df_mean = df.groupby(by=["host"]).resample(f'{time_interval}', on='time').mean(numeric_only = True).dropna()

         # create a copy of the downsampled data so we can write it back to InfluxDB Cloud powered by IOx. 
        df_write = df_mean.reset_index()
        #print(df_mean.head(5))
        #print(df_mean.dtypes)
        return(df_write)

    
    else:
        print('No columns contain numerical values')
        print("Warning: skipping - no numerical columns in downsample to write back to IOx, you should investigate")
        print(df.head(5))
        return(df)


       
#############################################################################################################



#@flow(log_prints=True,
#      task_runner=ConcurrentTaskRunner(),
#      description="Writes the downsampled dataframe back to IOX using the InfluxClient.")
@task(retries=2, retry_delay_seconds=10)
def write_sampled_to_iox(df_write, table):
    # write data back to InfluxDB Cloud powered by IOx

    if df_write.shape[1] == 0:
         print("Warning: no numerical columns in downsample to write back to IOx")
    
    else:
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
      task_runner=ConcurrentTaskRunner(),
      name="iox_prototype_subflows",
      description="Influxdata IOx support workflow prototype - Main Flow.")
def iox_prototype_subflows(bucket):
    
    #Set Variables
    measurement = 'cpu'
    time_interval = '10min'
    down_sample_aggregation = 'mean'
    #bucket = 'dtiolab'


    print("Getting FlightSQL table for bucket", bucket)
    #Run gettable Task
    tables = gettable.submit().result()
    print(tables)

    print("Running IOx Query Flow Loop through the measurments")
    for table in tables:
        print(table)
        try:
            df = execute_iox_query(table)

        except Exception:
            print("Error encountered, skipping interation")
            print("Continue on to next table")
            continue

        else:

            #Dont need to pass empty dataframes for process and downsampling
            if df.shape[0] >= 1:

                print("Running Pandas Downsampling Task")
                resampled = execute_iox_downsample.submit(df, time_interval, down_sample_aggregation).result()
                print("Completed Pandas Downsample Task")
             
                print("writing down-sampled dataframe to Influx")
                write = write_sampled_to_iox.submit(resampled, table).result()

                print("Completed Flow")        

#########################################################################
#     Main - call the iox_prototype_subflow Flow passing the bucket name
#########################################################################

if __name__ == "__main__":
    bucket = sys.argv[1]
    iox_prototype_subflows(bucket)

    
    #deploy()

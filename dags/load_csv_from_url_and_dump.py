import os
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.presto.hooks.presto import PrestoHook
sys.path.append('/datafuel/')
from datafuel.trino_fuel import (get_engine, add_datetime_suffix, 
    run_SQL, create_table, create_table_by_hook, get_hook_engine, 
    insert_records)
from datafuel.minio_fuel import (get_minio_client, df_to_csv_inMinio,
    df_to_csv_inDatalake, csv_inMinio_to_df)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


default_args = {
    'owner': 'ABO',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['datalake'])
def load_sirene_data(
    landing_bucket: str = "landing-bucket",
    landing_directory: str = "siren",
    datalake_bucket: str = "datalake-bucket",
    url: str = "https://data.cquest.org/geo_sirene/v2019/last/communes/06033.csv",
    table_name: str = "siren_06033",
    schema: str = "stg"
):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    
    # @task()
    # def log_datalake(schema): 
    #     # a = run_SQL(f'SHOW TABLES FROM hive.{schema}')
    #     engine = get_engine()
    #     query_result = run_SQL(command=f'SHOW TABLES FROM hive.{schema}', engine=engine)
    #     logging.info('Tables in Presto : ')
    #     logging.info(query_result.fetchall())

    
    @task(multiple_outputs=True)
    def extract_and_dump_inStagingBucket(
        url: str, 
        table_name: str, 
        landing_bucket: str,
        landing_directory: str 
    ) -> dict:
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        

        df = pd.read_csv(url)
        logging.info('DataFrame successfully loaded')
        logging.info(df.shape)
        logging.info(df.columns)
        
        dump_metadata = df_to_csv_inMinio(
            df=df, 
            bucket=landing_bucket, 
            obj_path=f"{landing_directory}/{table_name}.csv",
        )
        logging.info('Dataframe successfully dumped in datalake')
        return dump_metadata
        # create_table_by_hook(df, table_name)
        
        

    
    @task()
    def extract_fromStagingBucket_and_load_inDatalake(
        datalake_bucket: str,
        table_name: str, 
        schema: str, 
        dump_metadata: dict
    ): 
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
    
        df = csv_inMinio_to_df(
            bucket = dump_metadata['bucket'], 
            obj_path=dump_metadata['obj_path']
        )
        logging.info('DataFrame successfully loaded')
        
        # Dump CSV in DataLake
        df_to_csv_inDatalake(
            df=df, 
            bucket=datalake_bucket, 
            schema=schema, 
            table_name=table_name, 
            file_format="csv"
        )
        # engine = get_engine(user=user, schema="stg")
        # Add table pointing to the csv stored above
        create_table(df=df, table_name=table_name, schema=schema)

        logging.info('Dataframe was successfully saved to table {table_name} in schema {schema}')
    
    dump_metadata = extract_and_dump_inStagingBucket(
        url=url, 
        table_name=table_name, 
        landing_bucket=landing_bucket,
        landing_directory=landing_directory)
    # log_datalake(schema=schema)
    extract_fromStagingBucket_and_load_inDatalake(
        datalake_bucket=datalake_bucket, 
        table_name=table_name, 
        schema=schema,
        dump_metadata=dump_metadata
    )
load_sirene_data_dag = load_sirene_data()

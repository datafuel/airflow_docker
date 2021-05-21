import os
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
sys.path.append('/datafuel/')
from datafuel.minio_fuel import (get_minio_client, df_to_csv_inMinio,
    df_to_csv_inDatalake, csv_inMinio_to_df, url_to_csv_inMinio, generate_timestamp, 
    put_local_file_in_bucket)
# from datafuel.EDA_fuel import generate_report
from datafuel.dremio_fuel import (login_to_dremio, promote_folder, unpromote_folder)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


default_args = {
    'owner': 'ABO',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['datalake'])
def sirene_06_csv_to_datalakehouse(
    datalake_name: str = "minio-datalake",
    landing_bucket: str = "landing-bucket",
    landing_directory: str = "siren",
    datalake_bucket: str = "datalake-bucket",
    static_bucket: str = "static-bucket",
    url: str = "https://data.cquest.org/geo_sirene/v2019/last/dep/geo_siret_06.csv.gz",
    table_name: str = "siren_06",
    schema: str = "stg", 
    dremio_host: str = "http://dremio:9047",
    dremio_username: str = "amirb",
    dremio_password: str = "pass4dremio",
):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """    
    
    @task(multiple_outputs=True)
    def extract_and_dump(
        url: str, 
        table_name: str, 
        datalake_bucket: str,
        landing_directory: str,
        dremio_host: str,
        dremio_username: str,
        dremio_password: str,
        is_clean: bool = None
    ) -> dict:
        """
        #### Extract & Dump in Landing Bucket task
        
        """
        logging.info(f'Starting process for {table_name}')
        
        dump_metadata = url_to_csv_inMinio(
            url=url, 
            bucket=datalake_bucket,
            obj_path=f"{landing_directory}/{table_name}.csv"
        )
        

        logging.info(f'CSV from {url} successfully landed')

        promote_folder(
            dremio_host=dremio_host, 
            path_to_obj=f'{datalake_name}/{datalake_bucket}/{landing_directory}', 
            headers=login_to_dremio(dremio_host, dremio_username, dremio_password)
        )
        msg = f'Dataframe was successfully saved and Dataset was created {table_name} in schema {schema}'
        logging.info(msg)

        return dump_metadata
    
    

    dump_metadata = extract_and_dump(
        url=url, 
        table_name=table_name, 
        datalake_bucket=datalake_bucket,
        landing_directory=landing_directory,
        dremio_host=dremio_host,
        dremio_username=dremio_username,
        dremio_password=dremio_password
    )

sirene_06_csv_to_datalakehouse_dag = sirene_06_csv_to_datalakehouse()

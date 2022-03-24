import os
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
sys.path.append('/datafuel/')
from datafuel.trino_fuel import (get_engine, add_datetime_suffix, 
    run_SQL, create_table, create_table_by_hook, get_hook_engine, 
    insert_records)
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
def import_sirene_06_csv(
    airflow_variable: str = "import_sirene_06_csv"
):
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    

    @task()
    def cleaning_task(
        variable_dict: dict,
    ) -> bool:
        """
        #### Cleaning Task
        - Unpromote datasets
        """
        datalake_name = variable_dict['datalake_name']
        datalake_bucket = variable_dict['datalake_bucket']
        datalake_directory = variable_dict['datalake_directory']
        dremio_host = variable_dict['dremio_host']
        dremio_username = variable_dict['dremio_username']
        dremio_password = variable_dict['dremio_password']
        # Unpromote Folder
        unpromote_folder(
            dremio_host=dremio_host, 
            path_to_obj=f'{datalake_name}/{datalake_bucket}/{datalake_directory}', 
            headers=login_to_dremio(dremio_host, dremio_username, dremio_password)
        )
        return True

    @task(multiple_outputs=True)
    def get_airflow_variable(
        airflow_variable: str
    ) -> dict:
        variable = Variable.get(airflow_variable, deserialize_json=True)
        if variable.get('dag_name'):
            logging.info(f'Variable {airflow_variable} successfully loaded')
        return variable
    
    
    @task(multiple_outputs=True)
    def extract_and_dump(
        variable_dict: dict,
        code: str,
        is_clean: bool = None
    ) -> dict:
        """
        #### Extract & Dump in Landing Bucket task
        
        """
        
        base_url = variable_dict['base_url']
        datalake_bucket = variable_dict['datalake_bucket']
        datalake_name = variable_dict['datalake_name']
        datalake_directory = variable_dict['datalake_directory']
        csv_url = base_url.format(code=code)
        dump_metadata = url_to_csv_inMinio(
            url=csv_url, 
            bucket=datalake_bucket,
            obj_path=f"{datalake_directory}/sirene_{code}.csv"
        )

        promote_folder(
            dremio_host=dremio_host, 
            path_to_obj=f'{datalake_name}/{datalake_bucket}/{datalake_directory}', 
            headers=login_to_dremio(dremio_host, dremio_username, dremio_password)
        )
        msg = f'Dataframe was successfully saved and Dataset {datalake_directory} was created'
        logging.info(msg)

        logging.info(f'CSV from {csv_url} successfully landed')
        return dump_metadata
    
    
    
    variable_dict = get_airflow_variable(airflow_variable=airflow_variable)
    
    is_clean = cleaning_task(
        variable_dict=variable_dict
    )

    for x, code in enumerate(variable_dict['code']):
        # extract_and_dump(
        #     variable_dict=variable_dict,
        #     code=code,
        #     is_clean=is_clean
        # )
        run_this = PythonOperator(
            task_id=f'csv_load_{x}',
            python_callable=extract_and_dump,
            op_kwargs = {
                "variable_dict":variable_dict,
                "code":code,
                "is_clean":is_clean
            }
        )
        is_clean >> run_this

dag_import_sirene_06_csv = import_sirene_06_csv()

import os
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
sys.path.append('/datafuel/')
from datafuel.trino_fuel import (get_engine, add_datetime_suffix, 
    run_SQL, create_table, create_table_by_hook, get_hook_engine, 
    insert_records)
from datafuel.minio_fuel import (get_minio_client, df_to_csv_inMinio,
    df_to_csv_inDatalake, csv_inMinio_to_df, url_to_csv_inMinio)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


default_args = {
    'owner': 'ABO',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['datalake'])
def generate_dbt_source(
    DREMIO_DRIVER: str = "Dremio ODBC Driver 64-bit",
    DREMIO_HOST: str = "dremio",
    DREMIO_PORT: int = 9047,
    DREMIO_ENVIRONMENT: str = "prd",
    DREMIO_DATABASE: str = "datalake-bucket",
    DREMIO_SCHEMA: str = "stg",
    DREMIO_USER: str = "amirb",
    DREMIO_PASSWORD: str = "pass4dremio",
    PROJECT_GITHUB_REPO: str = "https://github.com/datafuel/sirene-06_dbt.git",
    PROJECT_REPO: str = "sirene-06_dbt",
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

    
    
    dbt_clone = BashOperator(
        task_id='dbt_clone',
        bash_command='mkdir /dbt && cd /dbt && git clone https://github.com/datafuel/sirene-06_dbt.git',
        dag=dag
    )

    # dbt_deps = BashOperator(
    #     task_id='dbt_deps',
    #     bash_command=f'cd /dbt/{PROJECT_REPO} && dbt deps',
    #     dag=dag
    # )

    dbt_debug = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /dbt/sirene-06_dbt && ls -l',
        dag=dag
    )    

# dbt_deps.set_upstream(dbt_clone)
        
    dbt_clone >> dbt_debug
    
    
generate_dbt_source_dag = generate_dbt_source()

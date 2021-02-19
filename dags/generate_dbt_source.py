import os
import sys
import json
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
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['dbt'])
def generate_dbt_source(
    # DREMIO_DRIVER: str = "Dremio ODBC Driver 64-bit",
    DREMIO_DRIVER: str = "/opt/dremio-odbc/lib64/libdrillodbc_sb64.so",
    DREMIO_HOST: str = "dremio",
    DREMIO_PORT: str = "31010",
    DREMIO_ENVIRONMENT: str = "prd",
    DREMIO_DATABASE: str = "datalake-bucket",
    DREMIO_SCHEMA: str = "stg",
    DREMIO_USER: str = "amirb",
    DREMIO_PASSWORD: str = "pass4dremio",
    PROJECT_GITHUB_REPO: str = "https://github.com/datafuel/sirene-06_dbt.git",
    PROJECT_REPO: str = "sirene-06_dbt"
):
    """
    ### DBT Test
    """
    

    clean_dbt_project = BashOperator(
        task_id='clean_dbt_project',
        bash_command='rm -rf ~/home/dbt'
    )

    clone_command = """
    mkdir -p ~/home/dbt \
        && cd ~/home/dbt \
        && git clone {{ dag_run.conf['PROJECT_GITHUB_REPO'] }} \
        && pwd \
        && ls
    """
    dbt_clone = BashOperator(
        task_id='dbt_clone',
        bash_command=clone_command
    )


    debug_command = """
    cd ~/home/dbt/sirene-06_dbt \
        && ls \
        && dbt debug --profiles-dir profiles
    """
    # debug_command="cd ~/home/dbt/sirene-06_dbt && ls"
    dbt_debug = BashOperator(
        task_id='dbt_deps',
        bash_command=debug_command,
        env={
            "DREMIO_DRIVER":DREMIO_DRIVER,
            "DREMIO_HOST":DREMIO_HOST,
            "DREMIO_PORT":DREMIO_PORT,
            "DREMIO_ENVIRONMENT":DREMIO_ENVIRONMENT,
            "DREMIO_DATABASE":DREMIO_DATABASE,
            "DREMIO_SCHEMA":DREMIO_SCHEMA,
            "DREMIO_USER":DREMIO_USER,
            "DREMIO_PASSWORD":DREMIO_PASSWORD
        }
    )    

# dbt_deps.set_upstream(dbt_clone)
        
    clean_dbt_project >> dbt_clone >> dbt_debug
    # generate_dbt_vars >> dbt_debug
    
    
generate_dbt_source_dag = generate_dbt_source()

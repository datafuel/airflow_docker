from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from datafuel.minio_fuel import (get_minio_client, df_to_csv_inMinio,
    df_to_csv_inDatalake, csv_inMinio_to_df, url_to_csv_inMinio, generate_timestamp, 
    put_local_file_in_bucket)
# from datafuel.EDA_fuel import generate_report
from datafuel.dremio_fuel import (login_to_dremio, promote_folder, unpromote_folder)

import json


# ----- PYTHON CALLABLES -----
def extract_and_dump(
    variable_dict: dict,
    code: str
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

def cleaning_task(
    airflow_variable,
) :
    """
    #### Cleaning Task
    - Unpromote datasets
    """
    variable_dict = Variable.get(airflow_variable, deserialize_json=True)
    if variable_dict.get('dag_name'):
        logging.info(f'Variable {airflow_variable} successfully loaded')
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

def get_airflow_variable(
    airflow_variable: str
) -> dict:
    variable = Variable.get(airflow_variable, deserialize_json=True)
    if variable.get('dag_name'):
        logging.info(f'Variable {airflow_variable} successfully loaded')
    return variable
    


# ----- DAG FUNCTION -----

def create_dag(
    dag_id,
    schedule,
    default_args
):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        start = DummyOperator(
            task_id='start',
            dag=dag
        )

        end = DummyOperator(
            task_id='end',
            dag=dag
        )

        variable_dict = Variable.get(dag_id, deserialize_json=True)
        for code in variable_dict['codes']:
            run_this = PythonOperator(
                dag=dag,
                task_id=f'csv_load_{code}',
                python_callable=extract_and_dump,
                op_kwargs = {
                    "variable_dict":variable_dict,
                    "code":code
                }
            )
            start >> run_this >> end

        return dag





schedule = None
dag_id = 'import_sirene_06_csv'

args = {
    'owner': 'ABO',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1,
    'max_active_runs': 1
}
globals()[dag_id] = create_dag(dag_id, schedule, args)
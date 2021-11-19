from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
# from airflow.provider.google.cloud.hooks.bigquery import BigQueryHook
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryPandasConnector
import pandas as pd
import sqlalchemy as sql
import logging 
import json
from google.oauth2 import service_account
import os

# import pandas_gbq

# ----- convenience functions -----

# def get_BQ_hook():
#     hook = BigQueryHook(gcp_conn_id="GCP_CONNEXION")
#     return hook 
#     conn = hook.get_conn()

# credentials = service_account.Credentials.from_service_account_file(
#     'path/to/key.json',
# )
# df = pandas_gbq.read_gbq(sql, project_id="YOUR-PROJECT-ID", credentials=credentials)

def get_credentials():
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    credentials = service_account.Credentials.from_service_account_file(
        cred_path,
    )
    return credentials


def get_postgres_connexion(
    username,
    password,
    host,
    port,
    database
):
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}" 
    engine = sql.create_engine(url)
    conn = engine.connect()
    
    return conn
    
    # Connexion à DB postgres

def get_postgres_engine(
    username,
    password,
    host,
    port,
    database
):
    url = f"postgresql://{username}:{password}@{host}:{port}/{database}" 
    engine = sql.create_engine(url)
    
    
    return engine
        
    

# ----- python callables -----

def extract_load_in_dwh(
    year: str,
    month: str,
    file_type: str,
    schema_source : str,
    table_name : str,
    variables_dict: dict
) :
    
    source_username= variables_dict["source_username"]
    source_password = variables_dict["source_password"]
    source_host= variables_dict["source_host"]
    source_port= variables_dict["source_port"]
    source_database= variables_dict["source_database"]
    bucket_name_datalake= variables_dict["bucket_name_datalake"]
    domaine= variables_dict["domaine"]

        
    logging.info(f"schema : {schema_source}")
    logging.info(f"table : {table_name}")
    with get_postgres_connexion(
        username = source_username,
        password = source_password,
        host = source_host,
        port = source_port,
        database = source_database
    ) as source_conn:
        df = pd.read_sql_table(
            table_name = table_name,
            con = source_conn,
            schema = schema_source
        )   
    
    logging.info(f"chargement du Dataframe réussi depuis la db {source_database} de la table {schema_source}.{table_name}")
    logging.info(df.shape)
    logging.info(df.head(1))

    now = datetime.now()
    year =  now.year if year == "" or year is None else year
    month =  now.month if month == "" or month is None else month  
        
    gcs_path = f"gs://{bucket_name_datalake.lower()}/{domaine.lower()}/{table_name.lower()}/year={year}/month={month}"
    
    
    
    if file_type.lower() == 'csv':
        gcs_path += f"/{table_name}.csv"
        logging.info("using ",gcs_path)
        df.to_csv(
            path_or_buf = gcs_path,
            index = False,
        )  
    elif file_type.lower() == 'parquet':
        gcs_path += f"/{table_name}.parquet"
        logging.info("using ",gcs_path)
        df.to_parquet(
            path=gcs_path,
            index=False
        )
        logging.info(f"chargement du Dataframe réussi dans BigQuery dans la repértoire", gcs_path)




# ----- DAG OPERATORS -----

def create_dag(
    dag_id,
    schedule,
    default_args,
    variables_dict
):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        init = DummyOperator(
            task_id='Init',
            dag=dag
        )

        clear = DummyOperator(
            task_id='clear',
            dag=dag
        )
        # operator qui va permettre d'appeler la fonction get_tables_to_import 
        # en utilisant le parametre python callable

        

                
        for table in variables_dict['tables_to_import']:
            
            task = PythonOperator(
                task_id= f'load_{table["table_name"]}',
                python_callable=extract_load_in_dwh,
                op_kwargs={
                    "variables_dict" : variables_dict,
                    "schema_source" : table["schema_source"], 
                    "table_name" : table["table_name"],
                    "year": table["year"],
                    "month":table["month"],
                    "file_type":table["file_type"]
                },
                dag=dag
            )   
            
            init >> task >> clear

        return dag


args = {
    'owner': 'Brocoli',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['bigdataetl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'concurrency': 1,
    'max_active_runs': 1
}

DAG_ID = "dag_load_to_gcs_product"
DAG_SCHEDULE = None
AIRFLOW_VARIABLE_NAME = f"{DAG_ID}_var"
variables_dict = Variable.get(
    AIRFLOW_VARIABLE_NAME,
    deserialize_json=True
)   

globals()[DAG_ID] = create_dag(DAG_ID, DAG_SCHEDULE, args, variables_dict)
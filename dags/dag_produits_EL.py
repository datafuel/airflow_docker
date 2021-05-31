from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.provider.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryPandasConnector
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
    cred_path = os.environ.get("CREDENTIALS_GOOGLE_PATH")
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
    variables_dict : str,
    schema_source : str,
    schema_destination : str,
    table_name : str
) :
    
    source_username= variables_dict["source_username"]
    source_password = variables_dict["source_password"]
    source_host= variables_dict["source_host"]
    source_port= variables_dict["source_port"]
    source_database= variables_dict["source_database"]

    destination_username= variables_dict["destination_username"]
    destination_password= variables_dict["destination_password"]
    destination_host= variables_dict["destination_host"]
    destination_port= variables_dict["destination_port"]
    destination_database= variables_dict["destination_database"]
    

    tables_to_import= variables_dict["tables_to_import"]
    
        
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

    engine = get_postgres_engine(
        username = destination_username,
        password = destination_password,
        host = destination_host,
        port = destination_port,
        database = destination_database
    )  

    # Créer un schema à partir d'un engine (sil n'existe pas)
    
    with engine.connect() as connection:
        result = connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_destination}")

    with  get_postgres_connexion(
        username = destination_username,
        password = destination_password,
        host = destination_host,
        port = destination_port,
        database = destination_database
    ) as destin_conn:

        df.to_sql(
            name = table_name,
            con = destin_conn,
            schema = schema_destination,
            if_exists = "replace"
        )   
    logging.info(f"chargement du Dataframe réussi dans la db {destination_database} de la table {schema_destination}.{table_name}")


from google.oauth2 import service_account
import pandas_gbq

credentials = service_account.Credentials.from_service_account_info(
    {
  "type": "service_account",
  "project_id": "turing-energy-312823",
  "private_key_id": "593425721d27a30deab52c10207f9dfc7a0bd098",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC8ZrZocOWZz03I\nPdnFKRlVruJmdkBtX9U+WGoxJ7HLobD4Pks8OkbbEsZnsxZtn11LV6PPG0HZE3Ou\nyxj4ZkQALHo8ww+/cIpgglR/9tSnr5AXjXeanFIDNwPDcXPbAjdVV9DA1/iina8v\nz8BZizxhcllv9qoh4AydF50VH9EqwsdDSlsIIQhBU8AnEeeArGlQj406Dqptwke2\ns7ouMkZcsfX/VVLJxta2U9TGSbhMvzn7WEAi1SK7aX4VTdMklMVbp08JiSPCyjYE\nnsiA6UirPZmmbVmfQXtpQBjlhfVowe2geU4XvXGSelXCtavqu9d1azMoPS/QVfWo\n6jK2gqt1AgMBAAECggEAHFtoYq11fZJkjnpyw/iE6e7mAE6VsXC+d8EWkLn9Cp2W\ndbLon5LU9eXsS5D60/qr7+pry0E6xsnk/F7w7fh0jUMkje89OE8f4NlVDyKvjOLB\nsS3XngOf24CFFj6GFeWOPw/16YTIVW+e/fcK6F9q8e7kc9QAdRNUSrPSbqrIkKMY\nvn+B5a/rk1/osHPomWwOWumDYDfNx9huRudLX4hGRqZiXpqc4LgbDC5Og/rnw1nK\nZ7Q7+TxT0L9jKYJyYJyevQNt/i8S13OFgkZRgleyeDwQ0Ikw/+wml1b9zpL2iMdN\nTz7OIyZOwwPb99BoPFnX7ZSb6vAGQSVskazmV70eoQKBgQDvomEhF/Mhmd9My+d+\nHtBS3mIE9aUPYsO060H8Wo+Z8TAjjUaBZlKOP2GiVlegj92SYTv9mJu51/XAppuT\n/AVPw3aS+HRbysQn9XeOpieVPE5jUaIiCfFpKrppP+7T6EoESQF4eoitL4BNaS9U\ne5HMUV8LA5r591s1ty6St/kUIQKBgQDJRJrq/80DpEUHETZIkemxRZNk42REF6Wt\nV3snrySGO6xA6W0LcrpDVOTaOXX7hWGN0Yp332lPUtI3FiTp1Q1sOOVI3jTZ3MBX\ncdJmZ2mrSTzfi0l5YeJnKRBiZ5Op1gLt4CfpLuxZexBTeiTAyzwFbus+Kz1wZeVX\nDIk0puds1QKBgDOvKxHPL5JEtMPEisZJ8Raefjehn5sk6x7/SrkZ8zT5ry9LzjQk\nlJO3meBAjR/FR73FmQHGQ3ge2xNVG+McDS059D4YtkgCALK5n7K1pX3rhuCm/7gl\nutq663+92U39INGxyU5p50arIxoPILUw/a46sqwWd/uH3TVFsBbW9TLhAoGALRFa\nqzJ6Ys1e05VMFXXu4U4WEipE4mZ1J9qF7j3urMwh5ysdysl24erZj+/79kVIsevw\n2KTDPQrrnYAs5dtaNRiMZwOyy/H9c9c96MZRSBSPN/NFkZOUkcsFKzPMQeR1JXTj\nGvjx3ZuL7VVVc4JOv1y1+mbOTCPU9egNtSeGfL0CgYA3eaHA6oS8+iI1vKK7s5sc\nu9L2fnXGIZn2LLlARu9OU/s1T9+vdeWXY1VZiFbRewQoOSnwPnGvEOoEeaLdY670\ni+A/rM5hSwQ1z9JIqwy6DdGGAh8fLHI0ekI2BiHGnHc4JvxayZ0qSjSLke3ELwLH\nSXiHjQvjZuIaUHY7/AKONw==\n-----END PRIVATE KEY-----\n",
  "client_email": "serviceacc-test@turing-energy-312823.iam.gserviceaccount.com",
  "client_id": "107768761801530988001",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/serviceacc-test%40turing-energy-312823.iam.gserviceaccount.com"
    },
)
df = pandas_gbq.read_gbq(sql, project_id="turing-energy-312823", credentials=credentials)
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
                    "schema_destination" : table["schema_destination"],
                    "table_name" : table["table_name"]
                },
                dag=dag
            )   
            
            init >> task >> clear

        return dag


args = {
    'owner': 'A_LA',
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

DAG_ID = "dag_produits_EL"
DAG_SCHEDULE = None
AIRFLOW_VARIABLE_NAME = f"{DAG_ID}_var"
variables_dict = Variable.get(
    AIRFLOW_VARIABLE_NAME,
    deserialize_json=True
)   

globals()[DAG_ID] = create_dag(DAG_ID, DAG_SCHEDULE, args, variables_dict)
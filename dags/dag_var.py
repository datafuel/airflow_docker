from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import sqlalchemy as sql
import logging 
import json
import os
from airflow.decorators import dag, task

DEFAULT_ARGS = {"owner": "datafuel"}


# ----- python callables -----
@dag(default_args=DEFAULT_ARGS, schedule_interval=None, start_date=days_ago(2),tags=["Airflow_Variables"])
def dag_export_variables():
    @task()
    def task_export_variables():

        for file_name in os.listdir("/airflow_variables"):
            if file_name.endswith(".json"):
                file_path = f"/airflow_variables/{file_name}"
                with open(file_path) as json_file:
                    data = json.load(json_file)
                variable_value = json.dumps(data)
                variable_name = file_name[:-5]
                variable_dag = Variable.set(variable_name,variable_value)

    extraction = task_export_variables()     
    
dag_extraction = dag_export_variables()

            
            
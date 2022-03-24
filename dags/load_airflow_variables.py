import os
import json
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago

AIRFLOW_VARIABLES_PATH = "/airflow_variables"
default_args = {
    'owner': 'ABO',
}

@dag(
    default_args=default_args, 
    schedule_interval=None, 
    start_date=days_ago(2), 
    tags=['airflow_variables']
)
def load_airflow_variables():
    
    @task()
    def load_variable(file_name):
        file_path = f"{AIRFLOW_VARIABLES_PATH}/{file_name}"
        with open(file_path) as f:
            data = json.load(f)
        
        variable_value = json.dumps(data)
        variable_name = file_name[:-5]

        variable = Variable.set(variable_name, variable_value)

    for var_file in os.listdir(AIRFLOW_VARIABLES_PATH):
        variable_export = load_variable(var_file)


dag_load_airflow_variables = load_airflow_variables()
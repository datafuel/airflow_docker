import os
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.providers.presto.hooks.presto import PrestoHook

SQL_CREATION_TEMPLATE = """{create_table_statement} WITH (FORMAT = '{file_format}', EXTERNAL_LOCATION = '{file_url}')"""


def get_presto_hook():
    presto_hook = PrestoHook('trino')
    return presto_hook

def get_hook_engine():
    presto_hook = get_presto_hook()
    conn = presto_hook.get_conn()
    return conn

def run_hook_SQL(command):
    presto_hook = get_presto_hook()
    return presto_hook.run(command)

def run_SQL(command, engine):
    query_result = engine.execute(command)
    return query_result

# OLD : generate_create_table_sql
    # def generate_create_table_sql(df, table_name):
    #     raw_sql = pd.io.sql.get_schema(df, table_name)
    #     sql = raw_sql \
    #         .replace('\n','') \
    #         .replace('TEXT', 'VARCHAR') \
    #         .replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    #     logging.info(f'SQL script for {table_name} creation')
    #     logging.info(sql)
    #     return sql


def create_table_by_hook(df, table_name):
    sql = generate_create_table_sql(df, table_name)
    run_hook_SQL(sql)


def insert_records(table_name, records, chunksize):
    presto_hook = get_presto_hook()
    presto_hook.insert_rows(
        table=table_name,
        rows=records, 
        commit_every=chunksize
    )
    logging.info(f'Records successfully inserted in table {table_name}')

def get_engine(user=None, schema=None, host="trino-coordinator", port=8080, catalog="hive"):
    if user is None:
        user = "airflow"
    engine_url = f'presto://{user}@{host}:{port}/{catalog}'
    if schema:
        engine_url += f"/{schema}"
    logging.info(f'Using engine URl : {engine_url}')
    engine = create_engine(engine_url)
    return engine


def add_datetime_suffix(table_name):
    now = datetime.now()
    suffix = now.strftime("_%Y_%m_%d_%H_%M_%S")
    table_name += f'_{suffix}'
    return table_name

    
def generate_create_table_sql(df, table_name, schema):
    raw_sql = pd.io.sql.get_schema(df, table_name)
    create_table_statement = raw_sql \
        .replace('\n','') \
        .replace("TEXT", "VARCHAR COMMENT 'TEXT'") \
        .replace("INTEGER", "VARCHAR COMMENT 'INTEGER'") \
        .replace("REAL", "VARCHAR COMMENT 'REAL'") \
        .replace('CREATE TABLE', 'CREATE TABLE IF NOT EXISTS')
    sql = SQL_CREATION_TEMPLATE.format(
        create_table_statement=create_table_statement,
        file_format="CSV",
        file_url=get_s3_url(table_name, schema=schema)
    )
    return sql

def create_table(df, table_name, schema="stg"):
    sql = generate_create_table_sql(df, table_name, schema)
    engine = get_engine(user="amir", schema=schema)
    a = engine.execute(sql)
    print(a.fetchall())

def get_s3_url(table_name, bucket="datalake-bucket", extension="parquet", schema="stg"):
    # table_name_time = add_datetime_suffix(table_name)
    return f's3a://{bucket}/{schema}/{table_name}'
#    return f's3a://{bucket}/{schema}/{table_name_time}.{extension}'
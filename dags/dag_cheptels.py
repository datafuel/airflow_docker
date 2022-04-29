from datetime import datetime
import logging
from numpy import source
from datafuel.minio_fuel import df_to_csv_inDatalake
from datafuel.dremio_fuel import promote_folder
from airflow.decorators import dag, task
import pandas as pd
import requests as rq

@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['hcp'])
def get_cheptels_data(
    source_url: str ="https://data.gov.ma/data/fr/dataset/81c88230-2322-4c72-82f9-e0b369829ead/resource/102aec33-94b8-4161-b451-5b93a762ec52/download/20220114-cheptel-2.xls",
    columns_to_keep: list =["Occurrence", "Filière", "Produit", "Indicateur", "Valeur"],
    destination_bucket: str = "datalake-bucket",
    destination_schema: str = "macro/maroc/agriculture",
    destination_table: str = "cheptel_maroc"
):

    @task()
    def extract(source_url: str):
        xls = pd.ExcelFile(
            source_url,
            engine='openpyxl'
        )
        return pd.read_excel(xls, sheet_name=xls.sheet_names[0])


    @task()
    def transform(df: pd.DataFrame, columns_to_keep: list):
        return (
            df
            [columns_to_keep]
        )


    @task()
    def load(
        df: pd.DataFrame,
        destination_bucket: str,
        destination_schema: str,
        destination_table: str
    ):
        response = df_to_csv_inDatalake(
            df=df,
            bucket=destination_bucket,
            schema=destination_schema,
            table_name=destination_table
        )
        logging.info(f'debug df_to_csv_inDatalake : {response}')

        promote_folder(
            path_to_obj=f"minio-datalake/{destination_bucket}/{destination_schema}/{destination_table}"
        )


    df = extract(
        source_url=source_url
    )

    df = transform(
        df=df,
        columns_to_keep=columns_to_keep
    )

    load(
        df=df,
        destination_bucket=destination_bucket,
        destination_schema=destination_schema,
        destination_table=destination_table
    )


get_cheptels_data_dag = get_cheptels_data()
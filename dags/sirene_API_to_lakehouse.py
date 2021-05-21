import os
import time
import sys
from datetime import datetime
import pandas as pd
import requests as rq
from sqlalchemy import create_engine
import logging 
from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
sys.path.append('/datafuel/')
from datafuel.minio_fuel import (get_minio_client, df_to_csv_inMinio,
    df_to_csv_inDatalake, csv_inMinio_to_df, url_to_csv_inMinio, generate_timestamp, 
    put_local_file_in_bucket, put_json_inMinio, get_json_fromMinio)
# from datafuel.EDA_fuel import generate_report
from datafuel.dremio_fuel import (login_to_dremio, promote_folder, unpromote_folder)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


default_args = {
    'owner': 'ABO',
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['DLH'])
def sirene__api_to_DLH(
    datalake_name: str = "minio-datalake",
    landing_bucket: str = "landing-bucket",
    landing_directory: str = "sirene",
    datalake_bucket: str = "datalake-bucket",
    static_bucket: str = "static-bucket",
    api_base_url: str = "https://public.opendatasoft.com/api/records/1.0/search/",
    api_dataset: str = "sirene_v3",
    api_filter_departementetablissement: str = "ALPES-MARITIMES",
    api_filter_categorieentreprise: str = "GE",
    api_rows: str = "100",
    schema: str = "stg", 
    dremio_host: str = "http://dremio:9047",
    dremio_username: str = "amirb",
    dremio_password: str = "pass4dremio",
):
    """
    ### SIRENE API to DLH
    1. Get data from [Sirene API v3](https://public.opendatasoft.com/explore/dataset/sirene_v3)
    2. Store data in DLH
    """  

    def get_file_name_from_minio_path(minio_path):
        start = minio_path.rfind('/')
        end = minio_path.rfind('.')
        file_name = minio_path[start+1:end]
        return file_name
        
      
    
    def prepare_api_params(
        api_dataset: str,
        api_rows: str = "200",
        api_filter_departementetablissement: str = None,
        api_filter_categorieentreprise: str = None
    ) -> dict:
        dc_params = {"dataset":api_dataset,"rows":api_rows}
        if api_filter_departementetablissement:
            dc_params["refine.departementetablissement"] = api_filter_departementetablissement
        if api_filter_categorieentreprise:
            dc_params["refine.categorieentreprise"] = api_filter_categorieentreprise

        return dc_params


    @task()
    def prepare_and_clean(
        landing_bucket: str,
        datalake_bucket: str,
        landing_directory: str,
    ) -> bool:
        """
        #### Preparation Task
        - Clean data in landing bucket
        """
        minio_client = get_minio_client()
        for zone in [landing_bucket, datalake_bucket]:
            minio_client.remove_object(zone, landing_directory)

        return True



    
    
    # @task(multiple_outputs=True)
    @task()
    def extract_and_dump(
        api_base_url: str,
        api_dataset: str,
        api_filter_departementetablissement: str,
        api_filter_categorieentreprise: str,
        api_rows: str,
        landing_bucket: str,
        landing_directory: str,
        is_clean: bool = None
    ) -> list:
        """
        #### Call Sirene API & Dump json files in Landing Bucket
        
        """

        dc_params = prepare_api_params(
            api_dataset=api_dataset,
            api_filter_departementetablissement=api_filter_departementetablissement,
            api_filter_categorieentreprise=api_filter_categorieentreprise
        )
        logging.info(f'using following params : ')
        logging.info(dc_params)
        test_response = rq.get(api_base_url, params=dc_params)
        test_response.raise_for_status()
        nhits = test_response.json()['nhits']

        ls_files_output = []
        
        for x in range(0, nhits, int(api_rows)):
            time.sleep(1)
            try:
                if x != 0:
                    dc_params['start'] = x+1
                
                response = rq.get(api_base_url, params=dc_params)
                logging.info(f'Response from {response.url} is being saved in Minio..')
                object_name = f'{landing_directory}/sirene_06_{x}.json'
                result = put_json_inMinio(
                    bucket_name=landing_bucket,
                    object_name=object_name,
                    json_dict=response.json()
                )
                logging.info(result)
                ls_files_output.append(object_name)
            except:
                logging.info('error')
        
        logging.info('files uploaded :')
        logging.info(ls_files_output)
        return ls_files_output
        

    @task()
    def raw_to_dataset(
        landing_bucket: str, 
        datalake_bucket: str,
        landing_directory: str,
        dremio_host: str,
        dremio_username: str,
        dremio_password: str,
        dump_metadata: list = None,
        is_clean: bool = None
    ):
        """
        #### Prepare dataset
        1. Extract data from json files in landing zone
        2. Process data and convert it to a dataframe
        3. Convert dataframe to a csv file
        4. Promote folder of CSV files 
        
        """
        # object_list = minio_client.list_objects(landing_bucket, landing_directory)
        for element in dump_metadata:
            dict_from_json = get_json_fromMinio(landing_bucket,element)
            ls_data = [record['fields'] for record in dict_from_json['records']]
            df = pd.DataFrame(ls_data)
            file_name = f'{get_file_name_from_minio_path(element)}.csv'
            object_path=f'{landing_directory}/{file_name}'
            metadata = df_to_csv_inMinio(df, datalake_bucket, object_path)
            logging.info(metadata)
        

        promote_folder(
            dremio_host=dremio_host, 
            path_to_obj=f'{datalake_name}/{datalake_bucket}/{landing_directory}', 
            headers=login_to_dremio(dremio_host, dremio_username, dremio_password)
        )


    
    is_clean = prepare_and_clean(landing_bucket, datalake_bucket, landing_directory)

    dump_metadata = extract_and_dump(
        api_base_url = api_base_url,
        api_dataset = api_dataset,
        api_filter_departementetablissement = api_filter_departementetablissement,
        api_filter_categorieentreprise = api_filter_categorieentreprise,
        api_rows = api_rows,
        landing_bucket = landing_bucket,
        landing_directory = landing_directory,
        is_clean = is_clean
    )

    raw_to_dataset(
        landing_bucket=landing_bucket, 
        datalake_bucket=datalake_bucket,
        landing_directory=landing_directory,
        dremio_host=dremio_host,
        dremio_username=dremio_username,
        dremio_password=dremio_password,
        dump_metadata=dump_metadata,
        is_clean=is_clean
    )
    

sirene__api_to_DLH_dag = sirene__api_to_DLH()

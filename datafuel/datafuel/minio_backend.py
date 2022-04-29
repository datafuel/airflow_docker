import re
from typing import Any
from airflow.models.xcom import BaseXCom
from datafuel.minio_fuel import df_to_csv_inDatalake, csv_inMinio_to_df, delete_objects_in_minio
import pandas as pd
import uuid

class MinioXComBackend(BaseXCom):
    # PREFIX = "xcom_s3://"
    BUCKET_NAME = "landing-bucket"
    SCHEMA_NAME = "xcom"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            key         = "data_" + str(uuid.uuid4())
            filename    = f"{key}.csv"

            value = df_to_csv_inDatalake(
                df=value,
                bucket=MinioXComBackend.BUCKET_NAME,
                schema=MinioXComBackend.SCHEMA_NAME,
                table_name=filename
            )
            value["XCOM_TYPE"] = "dataframe"
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        value = BaseXCom.deserialize_value(result)
        if isinstance(value, dict) and value.get("XCOM_TYPE") == "dataframe":
            result = csv_inMinio_to_df(
                bucket=value["bucket"],
                obj_path=value["obj_path"]
            )
        delete_objects_in_minio(
            bucket=value["bucket"],
            objects=[value["obj_path"]]
        )
        
        return result
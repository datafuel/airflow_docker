from minio import Minio
from io import BytesIO
import pandas as pd
import requests as rq

def df_to_csv_inDatalake(df, bucket, schema, table_name, file_format):
    csv_bytes = df.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    minio_client = get_minio_client()
    obj_path = f'{schema}/{table_name}/{table_name}.{file_format}'
    minio_client.put_object(
        bucket,
        obj_path,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    return {"obj_path":obj_path, "bucket":bucket}

def df_to_csv_inMinio(df, bucket, obj_path):
    csv_bytes = df.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    minio_client = get_minio_client()
    minio_client.put_object(
        bucket,
        obj_path,
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    return {"obj_path":obj_path, "bucket":bucket}

def get_minio_client(
    host="minio", 
    port="9000", 
    access_key="minio", 
    secret_key="minio123", 
    secure=False
):
    minio_client = Minio(
        endpoint=f"{host}:{port}",
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    return minio_client

# def csv_inMinio_to_df(bucket, schema, table_name, file_format):
# obj_path = f'{schema}/{table_name}/{table_name}.{file_format}'
def csv_inMinio_to_df(bucket, obj_path):
    
    minio_client = get_minio_client()
    obj = minio_client.get_object(
        bucket,
        obj_path,
    )
    df = pd.read_csv(obj)
    return df


def url_to_csv_inMinio(url, bucket, obj_path):
    response = rq.get(url)
    data = response.text.encode('utf-8')
    minio_client = get_minio_client()
    minio_client.put_object(
        bucket_name=bucket,
        object_name=obj_path,
        data=BytesIO(data),
        length=len(data),
        content_type='application/csv'
    )

    return {"obj_path":obj_path, "bucket":bucket}
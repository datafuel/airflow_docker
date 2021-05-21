from minio import Minio
from io import BytesIO
import pandas as pd
import requests as rq
from datetime import datetime as dt
import json

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


def put_local_file_in_bucket(output_path, bucket, object_path, content_type=None):

    # Upload data with content-type.
    minio_client = get_minio_client()
    result = minio_client.fput_object(
        bucket, object_path, output_path,
        content_type=content_type,
    )
    print(
        "created {0} object; etag: {1}".format(
            result.object_name, result.etag,
        ),
    )


def generate_timestamp():
    now = dt.now()
    timestamp = now.strftime('%D_%T')
    return timestamp


def put_json_inMinio(bucket_name, object_name, json_dict):
    """
    jsonify a dict and write it as object to the bucket
    """
    # prepare data and corresponding data stream
    data = json.dumps(json_dict).encode("utf-8")
    data_stream = BytesIO(data)
    data_stream.seek(0)

    minio_client = get_minio_client()

    # put data as object into the bucket
    result = minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream, length=len(data),
        content_type="application/json"
    )

    return {"name":result.object_name, "version_id":result.version_id}

def get_json_fromMinio(bucket_name, object_name):
    """
    get stored json object from the bucket
    """
    minio_client = get_minio_client()
    data = minio_client.get_object(bucket_name, object_name)
    return json.load(BytesIO(data.data))


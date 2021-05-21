import json
import requests
from urllib.parse import quote
import logging

LOGIN_HEADERS = {'content-type':'application/json'}
DREMIO_ID_PREFIX = "dremio%3A%2"
DREMIO_ID_SEPARATOR = "%2"
DREMIO_AUTH_ENDPOINT = "apiv2/login"


def login_to_dremio(dremio_host, username, password):
    response = requests.post(
        url=f"{dremio_host}/{DREMIO_AUTH_ENDPOINT}",
        headers=LOGIN_HEADERS,
        data=json.dumps({'userName': username, 'password': password})
    )
    response.raise_for_status()
    data = json.loads(response.text)

    # retrieve the login token
    token = data['token']
    return {'content-type':'application/json', 'authorization':'_dremio{authToken}'.format(authToken=token)}


def getByPath(dremio_host, path_to_obj, headers):
    endpoint = f'{dremio_host}/api/v3/catalog/by-path/{path_to_obj}'
    logging.info(f"Accessing {endpoint} ..")
    response = requests.get(
        url = endpoint, 
        headers=headers
    )
    response.raise_for_status()

    res_text = response.text
    return json.loads(res_text)


def promote_folder(dremio_host, path_to_obj, headers):
    file_obj = getByPath(dremio_host, path_to_obj, headers)
    if file_obj['entityType'] == "folder":
        id_file = str(quote(file_obj['id']))
        response = requests.post(
            url=f'{dremio_host}/api/v3/catalog/{id_file}',
            #url=f'{dremioServer}/api/v3/dremio%3A%2minio-datalake%2landing-bucket%2siren',
            headers=headers,
            data=json.dumps(generate_csv_promotion_body(path_to_obj))
        )
        response.raise_for_status()

def unpromote_folder(dremio_host, path_to_obj, headers):
    file_obj = getByPath(dremio_host, path_to_obj, headers)
    if file_obj['entityType'] == "dataset":
        id_file = str(quote(file_obj['id']))
        response = requests.delete(
            url=f'{dremio_host}/api/v3/catalog/{id_file}',
            headers=headers
        )
        response.raise_for_status()


def generate_csv_promotion_body(
    path_to_obj,
    field_delimiter=",",
    line_delimiter="\n",
    escape = "\"",
    skip_first_line=False,
    extract_header=True,
    trim_header=True,
    auto_generate_column_names=False
):
    dataset_dict = {
        "entityType": "dataset", 
        "id": generate_id_from_path(path_to_obj),
        "type": "PHYSICAL_DATASET", 
        "path": path_to_obj.split('/'), 
        "format": {
            "type": "Text",
            "fieldDelimiter": field_delimiter,
            "lineDelimiter": line_delimiter,
            "escape": escape,
            "skipFirstLine": skip_first_line,
            "extractHeader": extract_header,
            "trimHeader": trim_header,
            "autoGenerateColumnNames": auto_generate_column_names
        }
    }

    return dataset_dict


def generate_id_from_path(path_to_obj):
    path_id = DREMIO_ID_PREFIX + path_to_obj.replace('/', DREMIO_ID_SEPARATOR)
    return path_id
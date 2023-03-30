import os

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd

import datetime
import requests
import json

from msds697_inaturalist import *
from msds697_county import *
from msds697_aggregate_for_mongodb import *
from msds697_insert_to_mongo import *


def retrieve_and_save_inat_data():
    from google.cloud import storage

    # API query from iNaturalist + convert to json
    url = "https://api.inaturalist.org/v1/observations?taxon_id=41638%2C126772&q=California&search_on=place&order=desc&order_by=created_at"
    response = requests.request("GET", url)
    json_object = json.dumps(response.text)

    # credentials to get access google cloud storage
    keyfile = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
    storage_client = storage.Client.from_service_account_json(keyfile)

    # access bucket in gcs
    bucket_name = os.environ.get("GS_BUCKET_NAME")
    BUCKET = storage_client.get_bucket(bucket_name)

    # create filename in gcs
    filepath = 'inaturalist_data/bears_' + datetime.date.today().strftime("%Y%m%d") + '.json'

    # create a blob
    blob = BUCKET.blob(filepath)

    # upload the blob to gcs
    blob.upload_from_string(
        data=json_object,
        content_type='application/json'
        )
    result = filepath + ' upload complete'
    return {'response' : result}


def upload_nlcd():
    from google.cloud import storage

    # dags_folder = os.path.join(os.getcwd(), 'airflow/dags')
    filepath = os.path.join(os.getcwd(), 'nlcd_2019_land_cover_l48_20210604.img')

    # credentials to get access google cloud storage
    keyfile = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
    storage_client = storage.Client.from_service_account_json(keyfile)

    # access bucket in gcs
    bucket_name = os.environ.get("GS_BUCKET_NAME")
    BUCKET = storage_client.get_bucket(bucket_name)

    # create a blob
    destination = 'nlcd/nlcd_2019_land_cover_l48_20210604.img'
    blob = BUCKET.blob(destination)

    # upload the blob to gcs
    blob.upload_from_filename(filepath)

def process_api_insert():
    # Initialize client and bucket_name required in Google Storage.
    from google.cloud import storage
    keyfile = os.environ.get("GS_SERVICE_ACCOUNT_KEY_FILE")
    storage_client = storage.Client.from_service_account_json(keyfile)

    bucket_name = os.environ.get("GS_BUCKET_NAME")
    latest_blob = get_latest_file(storage_client, bucket_name)
    js = load_json(latest_blob)
    js = json_to_aggregate(js)
    js = add_county(js)
    js_aggregated = to_aggregate(js)
    insert_to_mongo(js_aggregated)

with DAG(dag_id="download_inat_data_dag",
         start_date=datetime.datetime(2023, 2, 24),
         schedule_interval='@daily') as dag:

    # https://github.com/apache/airflow/discussions/24463
    os.environ["no_proxy"] = "*"  # set this for airflow errors.

    download_inat_data = PythonOperator(task_id="download_inat_data",
                                        python_callable=retrieve_and_save_inat_data)

    process_and_push_to_mongo = PythonOperator(task_id="process_and_push_to_mongo",
                                    python_callable=process_api_insert)

    save_nlcd_data = PythonOperator(task_id="save_nlcd_data",
                                    python_callable=upload_nlcd)

    download_inat_data >> process_and_push_to_mongo >> save_nlcd_data

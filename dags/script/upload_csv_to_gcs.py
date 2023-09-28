import csv
import json
import os
from sqlalchemy import create_engine
from configparser import ConfigParser
import pandas as pd


from google.cloud import storage
from google.oauth2 import service_account

OUTPUT_PATH = "/opt/airflow/output"
CONFIG_PATH="/opt/airflow/env_conf"
GCS_SERVICE_ACCOUNT_PATH = f"{CONFIG_PATH}/carsome-load-csv-to-gcs.json"

PROJECT_ID = "carsome-400009"
BUCKET_NAME = "cars_second_handed_retail_bucket"
BUSINESS_DOMAIN = "AUTOMOTIVE"
DATA_SET = "carsome_web_scraped"

location = "asia-southeast1"


def _load_data_to_gcs(file_name):

    source_file_name = f"{OUTPUT_PATH}/{file_name}"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA_SET}/{file_name}"

    # Setup Google Cloud Storage connection
    gcs_service_account_info = json.load(open(GCS_SERVICE_ACCOUNT_PATH))
    gcs_credentials = service_account.Credentials.from_service_account_info(gcs_service_account_info)
    gcs_client = storage.Client(project=PROJECT_ID, credentials=gcs_credentials)

    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

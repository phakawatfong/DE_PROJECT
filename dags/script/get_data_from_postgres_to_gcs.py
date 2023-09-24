import csv
import json
import os
import psycopg2
from configparser import ConfigParser

from google.cloud import storage
from google.oauth2 import service_account

OUTPUT_PATH = "/opt/airflow/output"
CONFIG_PATH="/opt/airflow/env_conf"

POSTGRES_CONFIG_PATH = f"{CONFIG_PATH}/config.conf"
GCS_SERVICE_ACCOUNT_PATH = f"{CONFIG_PATH}/carsome-load-csv-to-gcs.json"

PROJECT_ID = "carsome-400009"
BUCKET_NAME = "cars_second_handed_retail_bucket"
BUSINESS_DOMAIN = "AUTOMOTIVE"
DATA = "carsome_web_scraped"

location = "asia-southeast1"

def _get_data_from_postgres():

    # setup Postgres Configuration
    psql_config = ConfigParser()
    psql_config.read(POSTGRES_CONFIG_PATH)
    details_dict = dict(psql_config.items("PG_CONF"))

    # make a connection
    conn = psycopg2.connect(database=details_dict["db"],
                            user=details_dict["user"],
                            password=details_dict["password"],
                            host=details_dict["host"],
                            port=details_dict["port"],
                            )

    cursor = conn.cursor()

    query = """
            SELECT * FROM carsome_scraped;
            """
    
    cursor.execute(query=query)
    results = cursor.fetchall()

    with open(f"{OUTPUT_PATH}/carsome_postgres.csv", "w") as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in results:
            csvwriter.writerow(row)


def _load_data_to_gcs():

    source_file_name = f"{OUTPUT_PATH}/carsome_postgres.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"

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

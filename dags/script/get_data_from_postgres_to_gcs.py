import csv
import json
import os
import psycopg2
from configparser import ConfigParser
import pandas as pd


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

def _etl_then_save_to_csv():

    # setup Postgres Configuration
    psql_config = ConfigParser()
    psql_config.read(POSTGRES_CONFIG_PATH)
    details_dict = dict(psql_config.items("PG_CONF"))

    # make a connection
    with psycopg2.connect(database=details_dict["db"],
                            user=details_dict["user"],
                            password=details_dict["password"],
                            host=details_dict["host"],
                            port=details_dict["port"],
                            ) as conn:
        
        query = """
                SELECT * FROM carsome_scraped;
                """

        carsome_df = pd.read_sql_query(query, conn)

    # Perform ETL     
    ## drop null value
    crt_carsome_df=carsome_df.dropna(how='all')

    ## replace currency from 'บาท'  to 'THB' for further analytics
    crt_carsome_df["currency"] = crt_carsome_df["currency"].apply(lambda x: "THB" if x=="บาท" else x)

    ## replace currency from 'เกียร์ธรรมดา'  to 'Manual' for further analytics
    crt_carsome_df["transmission_type"] = crt_carsome_df['transmission_type'].apply(lambda x: 'Manual' if x=="เกียร์ธรรมดา" else x)


    # extract numeric data from pay_per_month column and the unit of installment from scraped data
    ppm = pd.Series(crt_carsome_df['pay_per_month'])
    ppm_value = ppm.str.extract(r'(^[0-9]+)')
    ppm_unit = ppm.str.extract(r'([ก-๛].*)')

    crt_carsome_df['pay_per_month_value'] = pd.DataFrame(ppm_value)
    crt_carsome_df['pay_per_month_unit'] = pd.DataFrame(ppm_unit)

    ###### change pay_per_month_unit to be in english pattern.
    crt_carsome_df["pay_per_month_unit"] = crt_carsome_df['pay_per_month_unit'].apply(lambda x: 'THB/MONTH' if x=="บาท /เดือน" else x)
    payment_type_group = crt_carsome_df.groupby(['pay_per_month_unit'])['pay_per_month_unit'].count()
    print(f"payment_type_group={payment_type_group}")
    crt_carsome_df=crt_carsome_df.drop('pay_per_month', axis=1)

    print(f"crt_carsome")
    print(crt_carsome_df.head(10))

    crt_carsome_df.to_csv(f"{OUTPUT_PATH}/crt_carsome_web_scraped.csv" , index=False)



# def _load_data_to_gcs():

#     source_file_name = f"{OUTPUT_PATH}/carsome_postgres.csv"
#     destination_blob_name = f"{BUSINESS_DOMAIN}/{DATA}/{DATA}.csv"

#     # Setup Google Cloud Storage connection
#     gcs_service_account_info = json.load(open(GCS_SERVICE_ACCOUNT_PATH))
#     gcs_credentials = service_account.Credentials.from_service_account_info(gcs_service_account_info)
#     gcs_client = storage.Client(project=PROJECT_ID, credentials=gcs_credentials)

#     bucket = gcs_client.bucket(BUCKET_NAME)
#     blob = bucket.blob(destination_blob_name)

#     blob.upload_from_filename(source_file_name)

#     print(
#         f"File {source_file_name} uploaded to {destination_blob_name}."
#     )

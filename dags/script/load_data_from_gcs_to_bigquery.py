import json

from google.oauth2 import service_account
from google.cloud import bigquery

carsome_web_scraped_schema = [bigquery.SchemaField("index", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("brand", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("model", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("currency", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("kilometers_driven", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("transmission_type", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("pay_per_month_value", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("pay_per_month_unit", bigquery.enums.SqlTypeNames.STRING),
                            ]


manufacturer_country_schema = [bigquery.SchemaField("brand", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING),
                            ]

schema_dict = {
    'carsome_web_scraped_schema' : [bigquery.SchemaField("index", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("brand", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("model", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("year", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("price", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("currency", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("kilometers_driven", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("transmission_type", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("pay_per_month_value", bigquery.enums.SqlTypeNames.NUMERIC),
                            bigquery.SchemaField("pay_per_month_unit", bigquery.enums.SqlTypeNames.STRING),
                            ],
    'manufacturer_country_schema' : [bigquery.SchemaField("brand", bigquery.enums.SqlTypeNames.STRING),
                            bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING),
                            ]
}

CONFIG_PATH='/opt/airflow/env_conf'
PROJECT_ID = 'carsome-400009'
BUCKET_NAME = 'cars_second_handed_retail_bucket'
BUSINESS_DOMAIN = 'AUTOMOTIVE'
DATA_SET = 'carsome_web_scraped'

LOCATION = 'asia-southeast1'
BQ_SERVICE_ACCOUNT_PATH = f'{CONFIG_PATH}/carsome-get-data-from-gcs-then-load-to-bq.json'


def _get_data_from_gcs_then_load_to_bq(table_name):
    
    if table_name == 'crt_carsome_web_scraped':
        schema_name=schema_dict['carsome_web_scraped_schema']
        TRG_TABLE = 'crt_carsome_web_scraped'
        TRG_CSV_FILE = 'crt_carsome_web_scraped.csv'
    elif table_name == 'manufacturer_country':
        schema_name=schema_dict['manufacturer_country_schema']
        TRG_TABLE = 'dim_manufactured_country_of_each_brand'
        TRG_CSV_FILE = 'manufactured_country_of_each_brand.csv'

    bigquery_service_account_info = json.load(open(BQ_SERVICE_ACCOUNT_PATH))
    bigquery_credentials = service_account.Credentials.from_service_account_info(bigquery_service_account_info)
    bigquery_client = bigquery.Client(project=PROJECT_ID, credentials=bigquery_credentials)


    job_config = bigquery.LoadJobConfig(
        schema = schema_name,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows = 1,
        # The source format defaults to CSV, so the line below is optional.
        source_format = bigquery.SourceFormat.CSV,
    )

    # uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
    # gs://cars_second_handed_retail_bucket/AUTOMOTIVE/carsome_web_scraped/crt_carsome_web_scraped.csv

    uri = f"gs://{BUCKET_NAME}/{BUSINESS_DOMAIN}/{DATA_SET}/{TRG_CSV_FILE}"

    # table_id = "your-project.your_dataset.your_table_name"
    table_id = f'{PROJECT_ID}.{DATA_SET}.{TRG_TABLE}'

    load_job = bigquery_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bigquery_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    # [END bigquery_load_table_gcs_csv]
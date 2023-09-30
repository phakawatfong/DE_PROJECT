from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone

from script.carsome_web_scrape import _scrape_data
from script.etl_then_insert_to_curated_zone_postgres import _etl_then_save_to_csv #, _load_data_to_gcs
from script.convert_json_data_to_csv import _get_car_brand_data
from script.upload_csv_to_gcs import _load_data_to_gcs
from script.load_data_from_gcs_to_bigquery import _get_data_from_gcs_then_load_to_bq
from script.get_fake_customer_data_from_api import _get_fake_cusomter_data_from_api_then_save_to_csv

csv_file_list = ["crt_carsome_web_scraped.csv", "manufactured_country_of_each_brand.csv"]

## Define DAGS
# https://airflow.apache.org/docs/apache-airflow/1.10.12/tutorial.html
default_args = {
    'owner': 'kids pkf',
    'start_date': timezone.datetime(2023, 5, 1),
    'email': ['phakawat.fongchai.code@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='carsome_web_scraping',
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    concurrency=10, 
    max_active_runs=10,
    tags=["carsome"]
):
    start = EmptyOperator(task_id="start")

    create_carsome_raw_table = PostgresOperator(
    task_id='create_raw_posgres_table',
    postgres_conn_id='postgres_carsome_db_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS raw_carsome_scraped (
            index int,
            brand varchar(45),
            model varchar(255),
            year int,
            price int,
            currency varchar(45),
            pay_per_month varchar(45),
            kilometers_driven varchar(45),
            transmission_type varchar(45)
            )
    """
    )

    scrape_carsome_website_to_csv = PythonOperator(
        task_id = "scrape_data_then_save_to_csv",
        python_callable = _scrape_data,
        op_kwargs = { "mode_input" : "to_csv" },
    )

    get_brand_and_country_of_car_from_json_file = PythonOperator(
        task_id = "convert_json_brand_of_car_to_csv",
        python_callable = _get_car_brand_data,
    )

    get_fake_cusomter_data_from_api_task = PythonOperator(
        task_id = "get_fake_customer_data_from_api_then_save_to_csv",
        python_callable = _get_fake_cusomter_data_from_api_then_save_to_csv,
    )

    insert_data_to_postgres = PythonOperator(
        task_id = "insert_scraped_data_to_postgres",
        python_callable = _scrape_data,
        op_kwargs = { "mode_input" : "to_postgres" },
    )

    perform_etl_then_save_to_csv = PythonOperator(
        task_id = "perform_etl_then_save_to_csv",
        python_callable = _etl_then_save_to_csv,
    )

    create_carsome_curate_table = PostgresOperator(
    task_id='create_crt_posgres_table',
    postgres_conn_id='postgres_carsome_db_conn',
    sql="""
        CREATE TABLE IF NOT EXISTS crt_carsome_scraped (
            index int,
            brand varchar(45),
            model varchar(255),
            year int,
            price int,
            currency varchar(45),
            kilometers_driven varchar(45),
            transmission_type varchar(45),
            pay_per_month_value  int,
            pay_per_month_unit char(9)
            )
    """
    )

    load_curated_data_to_google_cloud_storage = PythonOperator(
        task_id = f"load_curated_from_csv_to_gcs",
        python_callable = _load_data_to_gcs,
        op_kwargs = { "file_name" : "crt_carsome_web_scraped.csv" },
        )
    
    load_manufacture_brand_country_data_to_google_cloud_storage = PythonOperator(
        task_id = f"load_manufacture_brand_country_from_csv_to_gcs",
        python_callable = _load_data_to_gcs,
        op_kwargs = { "file_name" : "manufactured_country_of_each_brand.csv" },
        )

    staging = EmptyOperator(task_id="staging", trigger_rule=TriggerRule.ALL_SUCCESS)

    get_carsome_from_gcs_then_load_to_bq = PythonOperator(
        task_id = "get_carsome_data_from_gcs_then_load_to_bq",
        python_callable = _get_data_from_gcs_then_load_to_bq,
        op_kwargs={'table_name': 'crt_carsome_web_scraped'},
    )

    get_brand_data_from_gcs_then_load_to_bq = PythonOperator(
        task_id = "get_brand_data_from_gcs_then_load_to_bq",
        python_callable = _get_data_from_gcs_then_load_to_bq,
        op_kwargs={'table_name': 'manufacturer_country'},
        )



    # Task dependencies
    start >> [scrape_carsome_website_to_csv, create_carsome_raw_table, get_brand_and_country_of_car_from_json_file, get_fake_cusomter_data_from_api_task]
    create_carsome_raw_table >> insert_data_to_postgres
    insert_data_to_postgres >> create_carsome_curate_table >> perform_etl_then_save_to_csv >> load_curated_data_to_google_cloud_storage
    get_brand_and_country_of_car_from_json_file>> load_manufacture_brand_country_data_to_google_cloud_storage
    [load_curated_data_to_google_cloud_storage, load_manufacture_brand_country_data_to_google_cloud_storage] >> staging
    staging >> [get_carsome_from_gcs_then_load_to_bq, get_brand_data_from_gcs_then_load_to_bq]
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

from script.carsome_web_scrape_then_csv import _scrape_data_to_dataframe_then_csv
from script.carsome_web_scrape_insert_to_postgres import _scrape_data_then_insert_to_postgres
from script.etl_then_insert_to_curated_zone_postgres import _etl_then_save_to_csv #, _load_data_to_gcs
from script.convert_json_data_to_csv import _get_car_brand_data

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
        task_id = "scrape_data_then_save_to_csv_file",
        python_callable = _scrape_data_to_dataframe_then_csv,
    )

    get_brand_and_country_of_car_from_json_file = PythonOperator(
        task_id = "convert_json_brand_of_car_to_csv",
        python_callable = _get_car_brand_data,
    )

    insert_data_to_postgres = PythonOperator(
        task_id = "insert_scraped_data_to_postgres",
        python_callable = _scrape_data_then_insert_to_postgres,
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

    # load_data_to_google_cloud_storage = PythonOperator(
    #     task_id = "load_data_from_csv_to_gcs",
    #     python_callable = _load_data_to_gcs,
    # )




    # Task dependencies
    start >> [scrape_carsome_website_to_csv, create_carsome_raw_table, get_brand_and_country_of_car_from_json_file]
    create_carsome_raw_table >> insert_data_to_postgres
    insert_data_to_postgres >> create_carsome_curate_table >> perform_etl_then_save_to_csv  # >> load_data_to_google_cloud_storage
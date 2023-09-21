from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils import timezone

from script.carsome_web_scrape_then_csv import _scrape_data


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

    scrape_data_from_carsome_website = PythonOperator(
        task_id = "scrape_carsome_website_then_save_to_csv",
        python_callable = _scrape_data,
    )

    create_carsome_table = PostgresOperator(
    task_id='create_posgres_table',
    postgres_conn_id='postgres_no_schema',
    sql="""
        CREATE TABLE IF NOT EXISTS carsome_scraped (
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

    start >> [scrape_data_from_carsome_website, create_carsome_table]
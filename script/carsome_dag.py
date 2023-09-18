from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'KIDS_AIRFLOW_CARSOME_PROJ',
    'start_date': datetime(2023, 7, 8),
}

# Define the DAG
dag = DAG(
    'KIDS_AIRFLOW_CARSOME_PROJ',
    default_args=default_args,
    schedule_interval=None
)

# Task 1: Call carsome_scrape.py
scrape_task = BashOperator(
    task_id='carsome_scrape',
    bash_command='python /c/Users/asus/Desktop/Kids/Kids_Programming_Project/de_car_proj/carsome_scrape.py',
    dag=dag
)

# Task 2: Call carsome_etl.py
etl_task = BashOperator(
    task_id='carsome_etl',
    bash_command='python /c/Users/asus/Desktop/Kids/Kids_Programming_Project/de_car_proj/carsome_etl.py',
    dag=dag
)

# Task 3: Call brand_country.py
brand_country_task = BashOperator(
    task_id='brand_country',
    bash_command='python /c/Users/asus/Desktop/Kids/Kids_Programming_Project/de_car_proj/brand_country.py',
    dag=dag
)

# Task 4: Merge dataframes and save to CSV
merge_task = BashOperator(
    task_id='merge_dataframes',
    bash_command='python /c/Users/asus/Desktop/Kids/Kids_Programming_Project/de_car_proj/merge_dataframes.py',
    dag=dag
)

# Define the task dependencies
scrape_task >> etl_task >> brand_country_task >> merge_task

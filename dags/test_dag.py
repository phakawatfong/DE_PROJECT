import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator

from sqlalchemy import create_engine
from configparser import ConfigParser

default_args = {
    "owner" : "kids pkf",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5),
}


def _scrape_data_into_postgres(**context):
    
    config = ConfigParser()
    config.read("/opt/airflow/env_conf/config.conf")
    details_dict = dict(config.items("PG_CONF"))

    ## noted that configuration file will not be published into github.

    # create sqlalchemy engine
    engine = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}"
                           .format(user=details_dict["user"],
                                   pw=details_dict["password"],
                                   host=details_dict["host"],
                                   port=details_dict["port"],
                                   db=details_dict["db"]))


    pg_hook = PostgresHook(
        postgres_conn_id='postgres_localhost',
        schema='test')

    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    car_brand_list=[]
    car_title_list=[]
    car_year_list=[]
    price_list=[]
    pay_per_month=[]
    km_driven_list=[]
    drive_type_list=[]
    currency_list=[]

    # total number of pages = 38
    # for page_num in range(1,39,1):
    for page_num in range(1,2,1):
        base_url = f"https://www.carsome.co.th/buy-car?utm_source=google&utm_medium=search&utm_campaign=18136584206.th-b2c-th-conv-search_core&utm_content=152731939332.th_b2c_generic_core&utm_term=652763940374.%E0%B8%A3%E0%B8%96%E0%B8%A1%E0%B8%B7%E0%B8%AD%202.e.c&pageNo={page_num}"

        html = requests.get(base_url)
        soup = BeautifulSoup(html.text, 'lxml')

        car_title_html = soup.find_all('a', {'class': 'mod-card__title'})

        for title_element in car_title_html:
            # car_name=title_element.replace("\n", "")
            each_car_el=title_element.text.splitlines()

            # index[0] == 'white space' ; get the year when the car was manufactured on index[1]. use regular expression to replace "spaces, tabs and newline chars" with "_"
            car_year=re.sub(r'\s+', '_',''.join(each_car_el[1]).strip())
            car_year_list.append(int(car_year))
        
            # get car_model_name from index 2-7 ; len(each_car_el) == 8. use regular expression to replace "spaces, tabs and newline chars" with "_"
            car_model=re.sub(r'\s+', '_',''.join(each_car_el[3:7]).strip())
            car_title_list.append(car_model)

            # get car_brand from index 3 ; len(each_car_el) == 8. use regular expression to replace "spaces, tabs and newline chars" with "_"
            car_brand=re.sub(r'\s+', '',''.join(each_car_el[2]).strip())
            car_brand_list.append(car_brand)

        price_html = soup.find_all('div', { 'class' : 'mod-card__price__total' })
        for each_price in price_html:
            each_car_el=each_price.text.splitlines()
            # use regular expression to replace "spaces, tabs, commas (',') and newline chars" with ""
            car_price=re.sub(r'\s+', '',''.join(each_car_el[0]).strip()).replace(',','')
            currency=re.sub(r'\s+', '',''.join(each_car_el[1]).strip())
            price_list.append(car_price)
            currency_list.append(currency)

        installment_html = soup.find_all('div', { 'class' : 'mod-tooltipMonthPay' })
        for each_inst in installment_html:
            each_car_el=each_inst.text.splitlines()
            ins_price=re.sub(r'\s+',' ',each_car_el[1]).strip().replace(',','')
            pay_per_month.append(ins_price)


        km_html = soup.find_all('div', { 'class' : 'mod-card__car-other' })
        for each_km in km_html:
            each_car_el=each_km.text.split(' ')
            km_driven=re.sub(r'\s+', '_',''.join(each_car_el[0:1]).strip()).replace(',','')
            km_driven_list.append(km_driven)

            drive_type=re.sub(r'\s+', '_',''.join(each_car_el[2]).strip())
            drive_type_list.append(drive_type)

    df = pd.DataFrame({'brand' : car_brand_list,'model':car_title_list, 'year' : car_year_list, 'price' : price_list, 'currency' : currency_list \
                    , 'pay_per_month' : pay_per_month , 'kilometers_driven' : km_driven_list ,'transmission_type' : drive_type_list})
    
    df.to_sql('car_info', con = engine, if_exists = 'append', chunksize = 1000)


with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2023, 9, 19),
    schedule_interval='0 0  * * *',
    tags=['carsome']
) as dag:
    
    task1 = PostgresOperator(
        task_id='create_posgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS car_info (
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

    task2 = PythonOperator(
        task_id="scrape_data_then_insert_to_postgres",
        python_callable=_scrape_data_into_postgres,
    )

    # define task dependencies 
    task1 >> task2
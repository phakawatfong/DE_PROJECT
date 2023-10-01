import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 
from datetime import date

from sqlalchemy import create_engine
from configparser import ConfigParser

def _scrape_data(mode_input):

    # define configuration
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
    
    today = date.today()
    OUTPUT_PATH='/opt/airflow/output'
    base_url = "https://www.carsome.co.th/buy-car"
    html = requests.get(base_url)
    soup = BeautifulSoup(html.text, "html.parser")

    car_button_html = soup.find('div', { 'class' : 'list-card__bottom__pagination' })

    page_list = []
    for lis in car_button_html.find_all('li'):
        try:
            page_list.append(int(lis.get_text()))
        except ValueError:
            continue

    # get max number
    sorted_list = sorted(page_list, reverse=True)
    num_of_max_page = int(sorted_list[0])
    print(f"number of the maximum pages that will be scraped : {num_of_max_page}")

    car_brand_list=[]
    car_title_list=[]
    car_year_list=[]
    price_list=[]
    pay_per_month=[]
    km_driven_list=[]
    drive_type_list=[]
    currency_list=[]

    # total number of pages parse from num_of_max_page
    for page_num in range(1, num_of_max_page+1, 1):
    # for page_num in range(1,2,1):
        base_url = f"https://www.carsome.co.th/buy-car?pageNo={page_num}"

        html = requests.get(base_url)
        soup = BeautifulSoup(html.text, "html.parser")

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

    df = pd.DataFrame({'brand' : car_brand_list,
                       'model':car_title_list, 
                       'year' : car_year_list, 
                       'price' : price_list, 
                       'currency' : currency_list , 
                       'pay_per_month' : pay_per_month , 
                       'kilometers_driven' : km_driven_list, 
                       'transmission_type' : drive_type_list}
                       )
    
    if mode_input == 'to_postgres' :
        conn = engine.connect()
        conn.execute("TRUNCATE TABLE raw_carsome_scraped")
        df.to_sql('raw_carsome_scraped', con = engine, if_exists = 'append', chunksize = 1000)
    elif mode_input == 'to_csv' :
        df.to_csv(f"{OUTPUT_PATH}/raw_carsome_web_scraped-{today}.csv" , index=False)
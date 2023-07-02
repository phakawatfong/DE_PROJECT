import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

exec_path = os.getcwd()

# page_num=1
car_title_list=[]
car_year_list=[]
price_list=[]
pay_per_month=[]
km_driven_list=[]
drive_type_list=[]

for page_num in range(0,38,1):
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
        car_model=re.sub(r'\s+', '_',''.join(each_car_el[2:7]).strip())
        car_title_list.append(car_model)

    price_html = soup.find_all('div', { 'class' : 'mod-card__price__total' })
    for each_price in price_html:
        each_car_el=each_price.text.splitlines()
        # use regular expression to replace "spaces, tabs, commas (',') and newline chars" with ""
        car_price=re.sub(r'\s+', ' ',''.join(each_car_el).strip()).replace(',','')
        price_list.append(car_price)

    installment_html = soup.find_all('div', { 'class' : 'mod-tooltipMonthPay' })
    for each_inst in installment_html:
        each_car_el=each_inst.text.splitlines()
        ins_price=re.sub(r'\s+',' ',each_car_el[1]).strip().replace(',','')
        pay_per_month.append(ins_price)


    km_html = soup.find_all('div', { 'class' : 'mod-card__car-other' })
    for each_km in km_html:
        each_car_el=each_km.text.split(' ')
        km_driven=re.sub(r'\s+', '_',''.join(each_car_el[0:1]).strip())
        km_driven_list.append(km_driven)

        drive_type=re.sub(r'\s+', '_',''.join(each_car_el[2]).strip())
        drive_type_list.append(drive_type)

df = pd.DataFrame({'model':car_title_list, 'year' : car_year_list, 'price' : price_list , 'pay_per_month' : pay_per_month \
                   , 'kilometers_driven' : km_driven_list ,'transmission_type' : drive_type_list})

# print(df)

df.to_csv('carsome.csv', index=False,encoding='utf-8')
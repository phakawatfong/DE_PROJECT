import requests
import json
import pandas as pd
from datetime import date

from configparser import ConfigParser


# get currency exchange rate free API from fixer.io 
# https://fixer.io/documentation
def _get_fake_cusomter_data_from_api_then_save_to_csv():
    today = date.today()
    OUTPUT_PATH='/opt/airflow/output'
    CONFIG_PATH = '/opt/airflow/env_conf/config.conf'
    # CONFIG_PATH = 'C:\\Users\\asus\\Desktop\\Kids\\git\\de_car_proj\\env_conf\\config.conf'
    config = ConfigParser()
    config.read(CONFIG_PATH)
    config_dict = dict(config.items('EXCHANGE_RATE_API'))

    # Setup API for currency exchange rate
    # print(config_dict)
    # https://anyapi.io/currency-exchange-api
    #### 1 USD =  X THB
    BASE_CURRENCY = 'USD'
    TRG_CURRENCY = 'THB'

    api_key = config_dict['anya_pi_exchange_rate']
    api_url = f"https://anyapi.io/api/v1/exchange/rates?base={BASE_CURRENCY}&apiKey={api_key}"
    response = requests.get(api_url)

    if response.status_code == 200:
        exchange_rate_data = response.json()
        # print(data)
    else:
        print('Currency Exchange API request failed.')

    USD_TO_THB_CONVERSION_RATE = f"{exchange_rate_data['rates'][TRG_CURRENCY]}"
    USD_TO_THB_CONVERSION_RATE = float(USD_TO_THB_CONVERSION_RATE)
    print(USD_TO_THB_CONVERSION_RATE)


    # set up api config for fake customer data
    fake_customer_url = "https://api.slingacademy.com/v1/sample-data/files/customers.json"
    fake_customer_response = requests.get(fake_customer_url)

    fake_customer_data = fake_customer_response.text
    fake_customer_data_jsonstr = json.loads(fake_customer_data)
    df = pd.DataFrame.from_dict(fake_customer_data_jsonstr)

    # extract only needed column(s).
    customer_data_df=df.filter(['first_name', 'last_name', 'email', 'phone', 'gender', 'age', 'spent', 'is_married'], axis=1)

    ## Let's assume that spent column infers to customer salary for further use. 
    customer_data_df.rename(columns ={'spent' : 'salary_per_month_usd'}, inplace=True)

    customer_data_df['salary_per_month_in_THB'] = customer_data_df['salary_per_month_usd'] * USD_TO_THB_CONVERSION_RATE

    customer_data_df.to_csv(f"{OUTPUT_PATH}/fake_customer_data.csv", index=False)
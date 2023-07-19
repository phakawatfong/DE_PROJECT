import requests
import json
import pandas as pd
import os
import send_mail



def getCustomerAPI(url, CONVERSION_RATE):
    CONVERSION_RATE=CONVERSION_RATE
    url_link = url
    response_API = requests.get(url_link)

    data = response_API.text
    jsonStr = json.loads(data)
    df = pd.DataFrame.from_dict(jsonStr)

    # extract only needed column(s).
    customer_data_df=df.filter(['first_name', 'last_name', 'email', 'phone', 'gender', 'age', 'spent', 'is_married'], axis=1)

    ## Let's assume that spent column infers to customer salary for further use. 
    customer_data_df.rename(columns ={'spent' : 'salary_per_day_usd'}, inplace=True)

    customer_data_df['salary_in_THB'] = customer_data_df['salary_per_day_usd'] * CONVERSION_RATE

    return customer_data_df

def customerTobeSendEmail(df, output_file_path):
    # EDA statement /  get data of the customers which has salary more than 30,000 THB / months
    high_salary_cust = df[df['salary_in_THB'] > 30000]

    #### print statement.
    # print(high_salary_cust[['first_name', 'email', 'salary_in_THB']])

    ## save  customers which has salary more than 30,000 THB / months into csv file for further use.
    high_salary_cust.to_csv(output_file_path, index=False)
    
    s=""
    for email in high_salary_cust['email']:
        s= s + f"{email} ,"
    # remove "," at the end of the string.
    list_of_email = s[:-1]
    return list_of_email

def writeEmailfile(output_file_path, list_of_email):
    # append customer's email with more than 30,000 thb/month into conf file
    config_file_read= open(output_file_path, 'w')
    config_file_read.write(list_of_email)
    config_file_read.close()

#### set up parameter 
current_dir=os.getcwd()
customer_email_file=f"{current_dir}\\customer_email_file.txt"
output_dir=f"{current_dir}\\output"
output_etl_file=f"{output_dir}\\high_salary_cust.csv"
url="https://api.slingacademy.com/v1/sample-data/files/customers.json"
CONVERSION_RATE=float(34.62) # as of 2023-07-16

customer_data_df = getCustomerAPI(url, CONVERSION_RATE)
list_of_email_txt = customerTobeSendEmail(customer_data_df, output_etl_file)
writeEmailfile(customer_email_file, list_of_email_txt)
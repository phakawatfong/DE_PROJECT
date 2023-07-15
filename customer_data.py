import requests
import json
import pandas as pd

url = "https://api.slingacademy.com/v1/sample-data/files/customers.json"
response_API = requests.get(url)
CONVERSION_RATE=float(34.62) # as of 2023-07-15

data = response_API.text

jsonStr = json.loads(data)

df = pd.DataFrame.from_dict(jsonStr)

customer_data_df=df.filter(['first_name', 'last_name', 'email', 'phone', 'gender', 'age', 'spent', 'is_married'], axis=1)

## Let's assume that spent column is infer to customer salary for further use. 
customer_data_df.rename(columns ={'spent' : 'salary_per_day_usd'}, inplace=True)

# print(customer_data_df.head(10))

customer_data_df['salary_in_THB'] = customer_data_df['salary_per_day_usd'] * CONVERSION_RATE
print(customer_data_df.describe())
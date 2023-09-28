from google.oauth2 import service_account
from google.cloud import bigquery

carsome_web_scraped_header = ['index',
                            'brand',
                            'model',
                            'year',
                            'price',
                            'currency',
                            'kilometers_driven',
                            'transmission_type',
                            'pay_per_month_value',
                            'pay_per_month_unit']

manufacturer_country_header = ['brand','Country']


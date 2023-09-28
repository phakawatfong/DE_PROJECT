import csv
import json
import os
from sqlalchemy import create_engine
from configparser import ConfigParser
import pandas as pd

OUTPUT_PATH = "/opt/airflow/output"
CONFIG_PATH="/opt/airflow/env_conf"
POSTGRES_CONFIG_PATH=f"{CONFIG_PATH}/config.conf"

def _etl_then_save_to_csv():

    # setup Postgres Configuration
    psql_config = ConfigParser()
    psql_config.read(POSTGRES_CONFIG_PATH)
    details_dict = dict(psql_config.items("PG_CONF"))

    # create sqlalchemy engine
    engine = create_engine("postgresql://{user}:{pw}@{host}:{port}/{db}"
                        .format(user=details_dict["user"],
                                pw=details_dict["password"],
                                host=details_dict["host"],
                                port=details_dict["port"],
                                db=details_dict["db"]))
    
    conn = engine.connect()    
    query = """
            SELECT * FROM raw_carsome_scraped;
            """

    carsome_df = pd.read_sql_query(query, conn)

    # Perform ETL     
    ## drop null value
    crt_carsome_df=carsome_df.dropna(how='all')

    ## replace currency from 'บาท'  to 'THB' for further analytics
    crt_carsome_df["currency"] = crt_carsome_df["currency"].apply(lambda x: "THB" if x=="บาท" else x)

    ## replace currency from 'เกียร์ธรรมดา'  to 'Manual' for further analytics
    crt_carsome_df["transmission_type"] = crt_carsome_df['transmission_type'].apply(lambda x: 'Manual' if x=="เกียร์ธรรมดา" else x)


    # extract numeric data from pay_per_month column and the unit of installment from scraped data
    ppm = pd.Series(crt_carsome_df['pay_per_month'])
    ppm_value = ppm.str.extract(r'(^[0-9]+)')
    ppm_unit = ppm.str.extract(r'([ก-๛].*)')

    crt_carsome_df['pay_per_month_value'] = pd.DataFrame(ppm_value)
    crt_carsome_df['pay_per_month_unit'] = pd.DataFrame(ppm_unit)

    ###### change pay_per_month_unit to be in english pattern.
    crt_carsome_df["pay_per_month_unit"] = crt_carsome_df['pay_per_month_unit'].apply(lambda x: 'THB/MONTH' if x=="บาท /เดือน" else x)
    payment_type_group = crt_carsome_df.groupby(['pay_per_month_unit'])['pay_per_month_unit'].count()
    print(f"payment_type_group={payment_type_group}")
    crt_carsome_df=crt_carsome_df.drop('pay_per_month', axis=1)

    print(f"crt_carsome")
    print(crt_carsome_df.head(10))

    crt_carsome_df.to_csv(f"{OUTPUT_PATH}/crt_carsome_web_scraped.csv", index=False)
    
    conn.execute("TRUNCATE TABLE crt_carsome_scraped")
    crt_carsome_df.to_sql('crt_carsome_scraped', con=conn, if_exists='replace',index=False)

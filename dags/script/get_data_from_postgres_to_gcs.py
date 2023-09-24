import csv 
import os
import pandas as pd
import psycopg2
from configparser import ConfigParser

from google.cloud import storage
from google.oauth2 import service_account


def _get_data_from_postgres_then_load_to_gcs():

    # setup Postgres Configuration
    POSTGRES_CONFIG_PATH="/opt/airflow/env_conf/config.conf"
    config = ConfigParser()
    config.read(POSTGRES_CONFIG_PATH)
    details_dict = dict(config.items("PG_CONF"))

    # make a connection
    conn = psycopg2.connect(database=details_dict["db"],
                            user=details_dict["user"],
                            password=details_dict["password"],
                            host=details_dict["host"],
                            port=details_dict["port"],
                            )

    query = """SELECT * FROM carsome_scraped;
    """

    with conn.cursor() as cursor:
        cursor.execute(query=query)
        result = cursor.fetchall()

    carsome_df = pd.DataFrame(result, index=None)


    print("test test")
    print(carsome_df)

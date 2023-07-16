import pymysql
import os
import pandas as pd
from configparser import ConfigParser



def etl_process(config_dir, param_dict, output_etl_path):
    def get_config_dict(config_dir, param_dict):
        config = ConfigParser()
        config.read(config_dir)
        details_dict = dict(config.items(param_dict))
        return details_dict

    configuration_param = get_config_dict(config_dir, CONFIG_KEY)


    class Config():
        MYSQL_HOST = configuration_param["host"]
        MYSQL_PORT = int(configuration_param["port"])       
        MYSQL_USER =  configuration_param["user"]
        MYSQL_PASSWORD = configuration_param["password"]
        MYSQL_DB =  configuration_param["db"]
        MYSQL_CHARSET = 'utf8mb4'

    connection = pymysql.connect(host=Config.MYSQL_HOST,
                                port=Config.MYSQL_PORT,
                                user=Config.MYSQL_USER,
                                password=Config.MYSQL_PASSWORD,
                                db=Config.MYSQL_DB,
                                charset=Config.MYSQL_CHARSET,
                                cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM car_info")
        result = cursor.fetchall()

    carsome_df = pd.DataFrame(result, index=None)


    def EDA(df):
        ################# PEFORM EDA ####################
        print("################# PEFORM EDA ####################\n")
        print(df.describe)
        print(df.head(10))
        print(df.info)

        """There is no null record, no need to delete null record."""
        print(df.isnull().sum())

        """found out that df['currency'] contains only \"บาท\""""
        currency_group = df.groupby(['currency'])['currency'].count()
        print(currency_group)
        print("####################################################\n")

        """found out that df[\'transmission_type\'] contains : [\'Automatic\' , \'เกียร์ธรรมดา\']"""
        transmission_group = df.groupby(['transmission_type'])['transmission_type'].count()
        print(transmission_group)

        """Check for DATA IN PAY_PER_MONTH Column"""
        print(df['pay_per_month'].head(10))
        print("####################################################\n")



    def ETL(df):
        ################# PERFORM ETL ####################
        print("################# PEFORM ETL ####################\n")
        # drop nan value, incase there is any null value.
        df=df.dropna(how='all')
        df["currency"] = df['currency'].apply(lambda x: 'THB' if x=="บาท" else x)
        df["transmission_type"] = df['transmission_type'].apply(lambda x: 'Manual' if x=="เกียร์ธรรมดา" else x)


        # print(df.head(10))
        ppm = pd.Series(df['pay_per_month'])
        ppm_value = ppm.str.extract(r'(^[0-9]+)')
        ppm_unit = ppm.str.extract(r'([ก-๛].*)')

        df['pay_per_month_value'] = pd.DataFrame(ppm_value)
        df['pay_per_month_unit'] = pd.DataFrame(ppm_unit)

        ###### change pay_per_month_unit to be in english pattern.
        df["pay_per_month_unit"] = df['pay_per_month_unit'].apply(lambda x: 'THB/MONTH' if x=="บาท /เดือน" else x)
        payment_type_group = df.groupby(['pay_per_month_unit'])['pay_per_month_unit'].count()
        print(f"payment_type_group={payment_type_group}")
        df=df.drop('pay_per_month', axis=1)

        return df



    # CALL EDA PROCESS
    EDA(carsome_df)

    # CALL ETL PROCESS
    carsome_df = ETL(carsome_df)


    print(carsome_df.head(10))
    print("##################### DONE ETL ##############")


    ### save data_frame to csv file to do further visualization.

    carsome_df.to_csv(output_etl_file, index=False)


# set up parameter
current_dir=os.getcwd()
config_dir=f"{current_dir}\\config.conf"
output_dir=f"{current_dir}\\output"
output_etl_file=f"{output_dir}\\carsome_clean.csv"
CONFIG_KEY="MYSQL_CONF"


# Main Program
etl_process(config_dir, CONFIG_KEY, output_etl_file)


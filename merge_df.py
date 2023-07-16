import pandas as pd
import os

def merge_data(read_from_carsome_etl_path, read_from_brand_country_path, merged_csv_path):
    # Read the 'carsome_clean.csv' file into a dataframe
    df_carsome_clean = pd.read_csv(read_from_carsome_etl_path)

    # Read the 'manufactured_country.csv' file into a dataframe
    df_manufactured_country = pd.read_csv(read_from_brand_country_path)

    # Join the dataframes on the 'index' column
    df_merged = df_carsome_clean.merge(df_manufactured_country, on='index')

    # Print the merged dataframe
    df_merged.to_csv(merged_csv_path)

current_dir=os.getcwd()
config_dir=f"{current_dir}\\config.conf"
output_dir=f"{current_dir}\\output"

read_from_brand_country_path=f"{output_dir}\\manufactured_country.csv"
read_from_carsome_etl_path=f"{output_dir}\\carsome_clean.csv"
merged_csv_path=f"{output_dir}\\final_carsome.csv"

merge_data(read_from_carsome_etl_path, read_from_brand_country_path, merged_csv_path)
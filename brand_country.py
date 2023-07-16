import json
import pandas as pd
import os

def get_car_brand_data(json_file_param, carsome_clean_param, brand_country_param):
    # Read the JSON file and load the data into a dictionary
    with open(json_file_param, 'r') as f:
        loaded_dict = json.load(f)

    df = pd.read_csv(carsome_clean_param)

    # Create a mapping dictionary from the loaded_dict
    brand_country_map = {}
    for country, brands in loaded_dict.items():
        for brand in brands:
            brand_country_map[brand] = country

    # Create a new dataframe with 'brand', 'original_country', and 'index' columns
    new_df = pd.DataFrame()
    new_df['brand'] = df['brand']
    new_df['original_country'] = df['brand'].map(brand_country_map)
    new_df['index'] = df.index

    # Save the new dataframe as a CSV file
    new_df.to_csv(brand_country_param, index=False)

# Set up parameter.
current_dir=os.getcwd()
json_manufacture_path=f"{current_dir}\\manufacturer_country.json"
output_path=f"{current_dir}\\output"
manufactured_country_path=f"{output_path}\\manufactured_country.csv"
carsome_clean_path=f"{output_path}\\carsome_clean.csv"

# Main Program
get_car_brand_data(json_manufacture_path, carsome_clean_path, manufactured_country_path)
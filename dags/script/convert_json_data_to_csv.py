import json
import pandas as pd


# Set up parameter.
import_data_path="/opt/airflow/import_data"
json_manufacture_path=f"{import_data_path}/manufacturer_country.json"
output_path=f"/opt/airflow/output"
output_path_manufacture_path=f"{output_path}/manufactured_country_of_each_brand.csv"


def _get_car_brand_data():
    # Read the JSON file and load the data into a dictionary
    with open(json_manufacture_path, 'r') as f:
        loaded_dict = json.load(f)

    # df = pd.read_csv(carsome_clean_param)

    # Create a mapping dictionary from the loaded_dict
    # brand_country_map = {}
    brand_country_list = []
    for country, brands in loaded_dict.items():
        for brand in brands:
            brand_country_list.append({"Brand": brand, "Country": country})

        # for brand in brands:
        #     brand_country_map[brand] = country
    
    car_brand_and_country = pd.DataFrame(brand_country_list)
    # Create a new dataframe with 'brand', 'original_country', and 'index' columns
    # new_df = pd.DataFrame()
    # new_df['brand'] = df['brand']
    # new_df['original_country'] = df['brand'].map(brand_country_map)
    # new_df['index'] = df.index


    # Save the new dataframe as a CSV file
    car_brand_and_country.to_csv(output_path_manufacture_path, index=False)

    # print(new_df)

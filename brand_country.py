import json
import pandas as pd

# Read the JSON file and load the data into a dictionary
with open('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\manufacturer_country.json', 'r') as f:
    loaded_dict = json.load(f)

df = pd.read_csv('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\output\\carsome_clean.csv')

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
new_df.to_csv('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\output\\manufactured_country.csv', index=False)


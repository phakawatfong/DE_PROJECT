import pandas as pd

# Read the 'carsome_clean.csv' file into a dataframe
df_carsome_clean = pd.read_csv('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\output\\carsome_clean.csv')

# Read the 'manufactured_country.csv' file into a dataframe
df_manufactured_country = pd.read_csv('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\output\\manufactured_country.csv')

# Join the dataframes on the 'index' column
df_merged = df_carsome_clean.merge(df_manufactured_country, on='index')

# Print the merged dataframe
df_merged.to_csv('C:\\Users\\asus\\Desktop\\Kids\\Kids_Programming_Project\\de_car_proj\\output\\final_carsome.csv')


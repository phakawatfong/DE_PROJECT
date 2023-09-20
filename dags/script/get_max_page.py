import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 


base_url = "https://www.carsome.co.th/buy-car"

html = requests.get(base_url)
soup = BeautifulSoup(html.text, "html.parser")

car_button_html = soup.find('div', { 'class' : 'list-card__bottom__pagination' })

# print(soup)

# print(car_button_html)

# for page in car_page_html:
#     each = page.text.splitline()

#     print(each)
date_list = []
for lis in car_button_html.find_all('li'):
    try:
        date_list.append(int(lis.get_text()))
    except ValueError:
        continue

sorted_list = sorted(date_list, reverse=True)

print(sorted_list[0])
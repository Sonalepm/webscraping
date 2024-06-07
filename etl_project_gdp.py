import pandas as pd 
import requests
import sqlite3
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29' 
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP' 
csv_path = '/home/project/Countries_by_GDP.csv'
df = pd.DataFrame(columns = ["Country","GDP_USD_millions"])

html_page = requests.get(url).text
data = BeautifulSoup(html_page,'html.parser')

tables = data.find_all('tbody')
rows = tables[2].find_all('tr')

for row in rows :
    col = row.find_all('td')
    if len(col)!=0:
        if col[0].find('a') is not None and '—' not in col[2]:
            data_dict = {"Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

#Storing Data in csv file
df.to_csv(csv_path)
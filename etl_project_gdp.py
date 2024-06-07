import pandas as pd 
import numpy as np 
import requests
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29' 
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP' 
csv_path = '/home/project/Countries_by_GDP.csv'
table_columns = ["Country","GDP_USD_millions"]
log_path = '/home/project/log_project_gdp.txt'

### Extract 
def extract(url,table_columns):

    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    df = pd.DataFrame(columns = table_columns)

    for row in rows :
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                    "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

### Transform
def transform(df):
    gdp = df['GDP_USD_millions'].to_list()
    gdp = [float("".join(x.split(','))) for x in gdp]
    #round to 2 decimal points
    gdp = [np.round(x/1000,2) for x in gdp]
    df["GDP_USD_millions"] = gdp
    df = df.rename(columns={"GDP_USD_millions":"GDP_USD_billions"})

    return df

### Load

# Load data to csv file
def load_to_csv(df,csv_path):
    df.to_csv(csv_path)

# Load to database sqlite3

def load_to_db(df):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

### Query the DB

def run_query(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_path,"a") as f:
        f.write(timestamp+","+message+"\n")


try:
    log_progress("Data extraction started")
    df=extract(url,table_columns)
    log_progress("Data extracted successfuly")
except Exception as e:
    log_progress(f"Error while extracting data: {e}")

try:       
    df=transform(df)
    log_progress("Data transformed successfuly")
except Exception as e:
    log_progress(f"Error while transforming data: {e}")    

try:       
    load_to_csv(df,csv_path)
    log_progress("Data loaded to CSV successfuly")
except Exception as e:
    log_progress(f"Error while loading data to CSV: {e}")   

log_progress('SQL Connection initiated.')

try:       
    load_to_db(df,db_name)
    log_progress("Data loaded to DB successfuly")
except Exception as e:
    log_progress(f"Error while loading data to DB: {e}")  

sql_connection = sqlite3.connect('World_Economies.db')

try:       
    query_statement = f"SELECT * FROM {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query_statement,sql_connection)
except Exception as e:
    log_progress(f"Error while querying data: {e}")

log_progress('Process complete.')

sql_connection.close()


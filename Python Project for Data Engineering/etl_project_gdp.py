import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import numpy as np

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'Countries_by_GDP.csv'

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    # ''' This function saves the final dataframe as a `CSV` file 
    # in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)
def load_to_db(df, sql_connection, table_name):
    # ''' This function saves the final dataframe as a database table
    # with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection ,if_exists='replace' ,index=False)
def run_query(query_statement, sql_connection):
    # ''' This function runs the stated query on the database table and
    # prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement ,sql_connection)
    print(query_output)
def log_progress(message):
    # ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    return
# ''' Here, you define the required entities and call the relevant 
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''

log_progress('start extracting')

df = extract(url,table_attribs)

log_progress('transforming')

transformed_data = transform(df)

log_progress('load data to csv')

load_to_csv(transformed_data, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(transformed_data,sql_connection,table_name)

log_progress('Data saved to database')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
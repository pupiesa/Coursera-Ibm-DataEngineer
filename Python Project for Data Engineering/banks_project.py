import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

data_url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
extract_table_attrb = ['Name', 'MC_USD_Billion']
csv_path = 'exchange_rate.csv'
output_csv_path = 'Largest_banks_data.csv'
db_name = 'Band.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'



def log_progress(message):
    time = datetime.now()
    formatted_time = time.strftime("|%Y-%m-%d | %H:%M:%S|")
    print(f'{formatted_time} "{message}"')

    with open(log_file,'a') as file:
        file.write(formatted_time + ':' + message + '\n')
                
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    response = requests.get(data_url)
    content = response.text

    soup = BeautifulSoup(content,'html.parser')
    table = soup.find_all('table')[0]
    rows = table.find_all('tr')
    # print(row)
    df = pd.DataFrame(columns= table_attribs)
    for row in rows[1:]:
        cols = row.find_all('td')
        if len(cols) >= 3:
            name = cols[1].text.strip()
            market_cap = cols[2].text.strip()
            try:
                market_cap = float(market_cap)
            except:
                continue        
            new_row = pd.DataFrame([[name,market_cap]], columns = table_attribs)
            df = pd.concat([df,new_row], ignore_index=True)
    # print(df)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    ex_rate = pd.read_csv(csv_path)
    ex_rate = ex_rate.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*ex_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*ex_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*ex_rate['INR'],2) for x in df['MC_USD_Billion']]
    # print(df)
    return df
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
def load_to_db(df, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
def run_query(query_statement):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    conn = sqlite3.connect(db_name)
    result = pd.read_sql(query_statement, conn)
    print(result)
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
log_progress('Initialize extracting data')
df = extract(data_url,extract_table_attrb)
log_progress('Start transforming data')
transform(df,csv_path)
log_progress('Start saving data to csv')
load_to_csv(df,output_csv_path)
log_progress('Start saving data to database')
load_to_db(df,table_name)
load_to_csv(df,output_csv_path)
log_progress('Start querying data form database')
run_query('SELECT * FROM Largest_banks')
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks')
run_query('SELECT Name from Largest_banks LIMIT 5')
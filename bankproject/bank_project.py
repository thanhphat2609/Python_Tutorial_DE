# Code for ETL operations on Country-GDP data


# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

# Initialize entities
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
db_name = "Banks.db"
table_attribs = ["Bank_Name", "Market_Cap_USD"]
table_name = "Largest_banks"
log_file = './code_log.txt'
csv_path = "./exchange_rate.csv"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    # Get data
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    # Create initial dataframe
    df = pd.DataFrame(columns=table_attribs)

    # Find table and rows
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')

    # Find Bank_Name, Market_Cap
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Bank_Name": bank_name,
                             "Market_Cap_USD": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    dataframe = pd.read_csv(csv_path)
    dict_exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']

    # Create column MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion
    df['MC_GBP_Billion'] = [np.round(x * dict_exchange_rate['GBP'], 2) for x in df['Market_Cap_USD']]
    df['MC_EUR_Billion'] = [np.round(x * dict_exchange_rate['EUR'], 2) for x in df['Market_Cap_USD']]
    df['MC_INR_Billion'] = [np.round(x * dict_exchange_rate['INR'], 2) for x in df['Market_Cap_USD']]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index = False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(header_query, query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"================= {header_query} =================")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

df_bank = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df_bank = transform(df_bank, csv_path)

log_progress('Data transformation complete. Initiating loading process')

output_path = './transformed_data/transformed.csv'
load_to_csv(df_bank, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df_bank, sql_connection, table_name)

header_query = "All data"
run_query(header_query, "SELECT * FROM Largest_banks", sql_connection)

header_query = "Average Market GBP"
run_query(header_query, "SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)

header_query = "Top 5 banks"
run_query(header_query, "SELECT Bank_Name from Largest_banks LIMIT 5", sql_connection)

log_progress('Data loaded to Database as table. Running the query')

log_progress('Process Complete.')

sql_connection.close()

log_progress('Connection Closed.')




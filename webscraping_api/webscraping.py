# Consider that we have been hired by a Multiplex management organization 
# to extract the information of the top 50 movies with the best average rating from the web link shared below.
# Web linK:
    # https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films

# The information required is Average Rank, Film, and Year.

# You are required to write a Python script webscraping_movies.py that extracts the information 
    # saves it to a CSV file top_50_films.csv. 
#You are also required to save the same information to a database Movies.db under the table name Top_50.


# import needed libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


# Initialize entities
url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = "./result/Movies.db"
table_name = "./result/Top_50"
csv_path = "./result/top_50_movies.csv"
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0


# Loading website
def loading_web(url):
    # Find the web page
    html_page = requests.get(url).text

    # Launch data of web page
    data = BeautifulSoup(html_page, 'html.parser')
    
    return data

# Find rows data
def find_rows(data, tag_table, tag_parent):
    # Find where data show(table)
    tables = data.find_all(tag_table)
    rows = tables[0].find_all(tag_parent)
    return rows

# Find top 50 movies
def find_data(rows, tag_data, df):
    count = 0

    for row in rows:
        if count < 50:
            col = row.find_all(tag_data)
            if len(col)!=0:
                data_dict = {"Average Rank": col[0].contents[0],
                            "Film": col[1].contents[0],
                            "Year": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count += 1
        else:
            break
    return df

target_file = './result/result.csv'
def load_data(dataframe, target_file):
    dataframe.to_csv(target_file, index = False)

log_file = "./result/log_file.txt"
# Log result of progress
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current time
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Run the process
log_progress("Start the process......")

data = loading_web(url)

log_progress("Scraping Data")

rows = find_rows(data, 'tbody', 'tr')

dataframe = find_data(rows, 'td', df)

log_progress("Loading Data")

load_data(dataframe, target_file)

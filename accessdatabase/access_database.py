# Import libraries needed
import sqlite3
import pandas as pd


# Connect to sqllite and create a new database (STAFF)
conn = sqlite3.connect("STAFF.db")

# Create table for store data from INSTRUCTOR.csv (Emtpy table)
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Read a data file
file_path = './INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)

# Create table with data (Table have data)
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')

# Function for query data and export to folder
def query_output(header_query, query_statement, conn):
    print(f"================= {header_query} =================")
    query_output = pd.read_sql(query_statement, conn)
    with open(f"./query_output/{header_query}.txt", "x") as f:
        df_string = query_output.to_string(header = False, index = False)
        f.write(df_string)
    print(query_output)

# 1. View all data from table
header_query = "View all data from table"
query_statement = f"SELECT * FROM {table_name}"    
query_output(header_query, query_statement, conn)

# 2. Select FName from table
header_query = "Select FName from table"
query_statement = f"SELECT FName FROM {table_name}"    
query_output(header_query, query_statement, conn)

# 3. Count how many data in to table
header_query = "Count number of data entries"
query_statement = f"SELECT count(*) FROM {table_name}"
query_output(header_query, query_statement, conn)

# Append data
data_dict = {'ID' : [100],
            'FNAME' : ['John'],
            'LNAME' : ['Doe'],
            'CITY' : ['Paris'],
            'CCODE' : ['FR']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully')

# 3. Count how many data in to table
header_query = "Count number of data after entries"
query_statement = f"SELECT count(*) FROM {table_name}"
query_output(header_query, query_statement, conn)

conn.close()
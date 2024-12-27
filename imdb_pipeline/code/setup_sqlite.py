import glob
import os
import sqlite3
import pandas as pd
from config.constants import processed_file_dir

def csv_to_sqlite(db_conn, csv_file, table_name):

    # read csv files into dataframe
    df = pd.read_csv(csv_file)

    # insert table sqlite database
    df.to_sql(table_name, db_conn, if_exists="replace", index=False)

    print(f"Data from {csv_file} has been inserted into {table_name} table")

def identify_table_name(csv_file_path):

    # get basename of file
    file_name = os.path.basename(csv_file_path)

    # split text and take first element
    table_name = os.path.splitext(file_name)[0]

    # replace . with _
    table_name = table_name.replace(".", "_")

    return table_name

# path containing csv files
# processed_file_dir = os.path.join("..", "data", "processed")

# filter for csv files
filtered_paths = glob.glob(f"{processed_file_dir}/*.csv")

# conn connection 
db_path = f"{processed_file_dir}/raw_imdb_database.db"

# create database or connect if it doesn't exist
conn = sqlite3.connect(db_path)

for csv_path in filtered_paths:

    # get table name
    table_name = identify_table_name(csv_path)

    # add table to database
    csv_to_sqlite(conn, csv_path, table_name)

# close connection
conn.close()



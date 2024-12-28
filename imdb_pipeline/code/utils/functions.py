import requests
from bs4 import BeautifulSoup
import shutil
import os
import gzip
import pandas as pd
import sqlite3
import glob

def get_file_links(base_url, file_extension):
    '''
    Scrape file links from IMDB's non-commerical use database
    '''

    # get response from page
    response = requests.get(base_url)

    # parse data
    soup = BeautifulSoup(response.content, "html.parser")

    # to store links
    rel_links = []

    # store all file links with file_extension
    for link in soup.find_all("a", href=True):
        # get url part of link
        href = link["href"]
        # identify links with file_extension
        if file_extension in href:
            rel_links.append(href)

    # filter links for file_extension
    return rel_links

def download_file(url, output_file):
    '''
    download the raw files
    '''
    # Send an HTTP GET request to the specified URL and stream the response
    response = requests.get(url, stream=True)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open the output file in write-binary mode
        with open(output_file, 'wb') as file:
            # Copy the content from the response to the output file
            shutil.copyfileobj(response.raw, file)
        print(f'Downloaded {output_file}')
    else:
        # Print an error message if the request failed
        print(f'Failed to download {url}')

def convert_tsv_gz_to_csv(input_path, output_path_tsv, output_path_csv, chunk_size=100000):
    # Unzip and write in chunks
    with gzip.open(input_path, "rt", encoding="utf-8") as f_in:
        with open(output_path_tsv, 'wt', encoding='utf-8') as f_out:
            # read file in sizes of chunk until no value returned ie., ''
            for chunk in iter(lambda: f_in.read(chunk_size), ''):
                # save chunk
                f_out.write(chunk)
    
    # Read and process the TSV file as csv in chunks of chunck_size
    chunks = pd.read_csv(output_path_tsv, sep="\t", chunksize=chunk_size)
    
    # Write chunks to CSV
    for i, chunk in enumerate(chunks):
        chunk.to_csv(output_path_csv, mode='a', index=False, header=(i == 0))  # Append mode and write header only once

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

def move_file_single(filename, dst, src, ext):

    # pattern to search
    pattern = f"{src}/*{ext}"
    # list of files matching pattern
    files = glob.glob(pattern)
    # filter for file name
    file = [x for x in files if filename in os.path.basename(x)]
    # move copied version of file
    try:
        shutil.copy2(file[0], dst)
        print("file successfully copied and moved")
    except Exception as e: 
        print(f"file not moved. Error: {e}")

def rename_file(file_to_rename, new_name):
    try:
        # attempt to rename file
        os.rename(file_to_rename, new_name)
        print("rename successful")
    except OSError as e:
        # report error if error occurs
        print(f"error renaming file: {e}")

def update_nulls(db_path, table, missing_placeholder):
    '''Function to programmatically query database to alter all missing_placeholder values
    to NULL for each column in table'''
    try:
        # connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # get table info
        cursor.execute(f"PRAGMA table_info({table})")
        column_info_tuple = cursor.fetchall()
        # create list of column names in table, second index contains the column name
        columns = [info[1] for info in column_info_tuple]
        # execute update query
        for column in columns:
            cursor.execute(f"UPDATE {table} SET {column} = NULL WHERE {column} = ?", (missing_placeholder,))
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            # commit changes
            conn.commit()
            conn.close()

def run_query(db_path, query_string, params=()):

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # execute query string with given parameteres
        cursor.execute(query_string, params)
        # identify if read query
        read_query = query_string.lower().strip().startswith("select")
        if read_query:
            # if read query
            results = cursor.fetchall()
            conn.close()
            return results
        else:
            # if any query that modifies the database
            conn.commit()
            conn.close()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
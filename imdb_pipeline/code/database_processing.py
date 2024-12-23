import shutil
import glob
import os
import sqlite3
from constants.constants import raw_file_dir, processed_file_dir, raw_database_name, processed_database_name

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

def run_query(db_path, query_string):

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # execute query string
        cursor.execute(query_string)
        # identify if read query
        read_query = "SELECT * FROM title_ratings".lower().strip().startswith("select")
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
            # cursor.execute(f"UPDATE {table} SET {column} = NULL WHERE {column} = {missing_placeholder}")
            cursor.execute(f"UPDATE {table} SET {column} = NULL WHERE {column} = ?", (missing_placeholder,))
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            # commit changes
            conn.commit()
            conn.close()
     
# variables
raw_db = f"{processed_file_dir}/{raw_database_name}"
processed_db = f"{processed_file_dir}/{processed_database_name}"

# rename db file if specified name doesn't exist
if os.path.exists(processed_db) == False:
    move_file_single(raw_database_name, processed_file_dir, raw_file_dir, '.db')
    print("db file transferred")
    rename_file(raw_db, processed_db)
else:
    print("db file already exists")

# database table names
table_names = ["name_basics", "title_akas", "title_basics", "title_crew", "title_episode", "title_principals", "title_ratings"]
for name in table_names:
    # replace /N missing placeholder with NULL across all tables
    update_nulls(processed_db, name, r"\N")
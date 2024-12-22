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

def query_read(db_path, query_string):

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query_string)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
    
# variables
old_db_name = f"{processed_file_dir}/{raw_database_name}"
new_db_name = f"{processed_file_dir}/{processed_database_name}"

if os.path.exists(new_db_name) != True:
    move_file_single(raw_database_name, processed_file_dir, raw_file_dir, ".db")
    print("db file transferred")
    rename_file(old_db_name, new_db_name)
else:
    print("db file already exists")
# function to run queries
# def execute_query(db, query):
#     print("test")

# create copy of database

# replace /n with appropriate missing value

# check primary key and foregin key is unique in each table

# check data type corresponds to the value in each column (ie., a string column only has strings)

# Ensure consistent formatting for columns that should follow specific formats (e.g., dates, phone numbers, email addresses).

# Verify that numeric columns fall within acceptable ranges (i.e., a death should not be before a birth)
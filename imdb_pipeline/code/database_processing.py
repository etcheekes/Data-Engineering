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
            return results
        else:
            # if any query that modifies the database
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
    finally:
        conn.close()
     
# variables
old_db_name = f"{processed_file_dir}/{raw_database_name}"
new_db_name = f"{processed_file_dir}/{processed_database_name}"

# rename db file if specified name doesn't exist
if os.path.exists(new_db_name) != True:
    move_file_single(raw_database_name, processed_file_dir, raw_file_dir, ".db")
    print("db file transferred")
    rename_file(old_db_name, new_db_name)
else:
    print("db file already exists")


# function to run queries
query = "SELECT * FROM title_ratings"
test = run_query(new_db_name, query)
print(test)


# check structure: 1) identify issue, 2) update issue, 3) read query to check issue is fixed

# replace /n with NULL



# check primary key and foregin key is unique in each table

# check data type corresponds to the value in each column (ie., a string column only has strings)

# Ensure consistent formatting for columns that should follow specific formats (e.g., dates, phone numbers, email addresses).

# Verify that numeric columns fall within acceptable ranges (i.e., a death should not be before a birth)
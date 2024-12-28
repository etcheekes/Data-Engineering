import os
from constants.constants import raw_file_dir, processed_file_dir, raw_database_name, processed_database_name
from utils.functions import move_file_single, rename_file, update_nulls
     
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
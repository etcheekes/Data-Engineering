import os
import sqlite3
import glob
from constants.constants import raw_file_dir, processed_file_dir, raw_database_name
from utils.functions import get_file_links, download_file, convert_tsv_gz_to_csv, csv_to_sqlite, identify_table_name

# get all file linkes
file_links = get_file_links("https://datasets.imdbws.com/", ".tsv.gz")

# download raw files
for link in file_links:
    # get file name
    file_name = os.path.basename(link)
    # download file
    print(f"Downloading {file_name}")
    download_file(link, os.path.join(raw_file_dir, file_name))
    print(f"Downloading {file_name} complete")

# get tsv file names
raw_file_names = os.listdir(raw_file_dir)

# filter for .tsv.gz files
raw_file_names = [file for file in raw_file_names if ".tsv.gz" in file]

# convert files to csv
for file in raw_file_names:
    print(f"handling {file}")
    raw_file_path = os.path.join(raw_file_dir, file)
    # file name for processed file
    name_tsv = file.split(".")
    name_tsv = '.'.join(name_tsv[:-1])
    tsv_file_path = os.path.join(raw_file_dir, name_tsv)
    # processed file path tsv file
    processed_file_path_tsv = os.path.join(processed_file_dir, name_tsv)
    # file name for processed csv file
    name_csv = file.split(".")
    name_csv = '.'.join(name_csv[:-2])
    name_csv = name_csv + ".csv"
    # processed file path csv file
    processed_file_path_csv = os.path.join(processed_file_dir, name_csv)
    # convert to csv and save
    convert_tsv_gz_to_csv(raw_file_path, processed_file_path_tsv, processed_file_path_csv)

# filter for csv files
filtered_paths = glob.glob(f"{processed_file_dir}/*.csv")

# connection for database
db_path = f"{raw_file_dir}/{raw_database_name}"

# create database or connect if it doesn't exist
conn = sqlite3.connect(db_path)

for csv_path in filtered_paths:

    # get table name
    table_name = identify_table_name(csv_path)

    # add table to database
    csv_to_sqlite(conn, csv_path, table_name)

# close connection
conn.close()

print("data extracted to database :)")
import requests
from bs4 import BeautifulSoup
import shutil
import os
import gzip
import pandas as pd

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

# variables
script_dir = os.path.dirname(__file__)

# get all file linkes
file_links = get_file_links("https://datasets.imdbws.com/", ".tsv.gz")

# download raw files
for link in file_links:
    # get file name
    file_name = os.path.basename(link)
    # download file
    print(f"Downloading {file_name}")
    download_file(link, os.path.join("..", "data", "raw", file_name))
    print(f"Downloading {file_name} complete")
    
# relevant folders
raw_file_dir = os.path.join("..", "data", "raw")
processed_file_dir = os.path.join("..", "data", "processed")

# get tsv file names
raw_file_names = os.listdir(raw_file_dir)

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
    

print("data ready")
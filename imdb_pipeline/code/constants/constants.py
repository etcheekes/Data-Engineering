import os
from pathlib import Path

# get root directory
root_dir = Path(__file__).resolve().parent.parent.parent

# relevant folders
raw_file_dir = os.path.join(root_dir, "data", "raw")
processed_file_dir = os.path.join(root_dir, "data", "processed")

# raw database name
raw_database_name = "raw_imdb_database.db"

# processed database name
processed_database_name = "processed_imdb_database.db"

# script paths
python_file_paths = [
    "extract_process.py",
    "database_processing.py",
    "create_views.py"
]
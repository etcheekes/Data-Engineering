# Erik Cheekes - Data Engineering Portfolio

This is a repository to show my personal data engineering projects.

## IMDB Data Pipeline

**Project Overview**

To extract, process, and make publically available data IMDb available in an SQLite database while also including several pre-made SQL views.

View the project folder and code [here](https://github.com/etcheekes/Data-Engineering/tree/main/imdb_pipeline).

**Technologies and Tools Used**

- Python (Pandas, Sqlite3, BeautifulSoup, Shutil, Glob).
- SQLite (DB Browser).

**Data Sources**

- IMDb public data.

**Data Pipeline Process**

- **Extract**: Download and extract files from IMDb.
- **Transform**: Process tar.gz files into CSV format.
- **Load**: Load CSV files into an SQLite database, creating separate tables for each CSV file.
- **Clean**: Replace all Null values in the database tables.

**Python Files Descriptions**

- **code/extract_process.py**: This code handles downloading and extracting files from IMDb and converting the tsv.gz files into CSV files.
- **code/database_processing.py**: This code processes the extracted tar.gz files into CSV format and loads each CSV file into the SQLite database as individual tables.
- **code/create_views.py**: This code creates pre-made views in the SQLite database to help answer specific questions and organise data for future analysis.
- **code/run_all.py**: This master code runs all the above scripts in succession, ensuring the entire pipeline executes smoothly.
- **code/utils/functions.py**: Stores all the functions used in the above python files.
- **code/constants/constants.py**: Stores variables that remain consistent and are used throughout the main python files.

**Results and Outcomes**

- SQLite database of IMDB publically available data ready for analysts to use.
- Several pre-made views to help answer specific questions.

**Conclusion and Future Work**

- Create visualisations based on the created views.
- Create additional SQL views.

**Conclusion**

This project successfully developed a data pipeline to extract, process, and store IMDb data in an SQLite database. The pipeline automates data extraction and processing. Furthermore, by creating pre-made views, this project provides a foundation for future data analysis and insights. Overall it demonstrates the effectiveness of using Python and SQLite for a small-scale data pipeline and ETL process.


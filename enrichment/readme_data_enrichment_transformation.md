What does this script do:

## data_enrichment_crossref.py

Data Import and Preprocessing:
     - Reads a JSON file into a Pandas DataFrame.
     - Filters out rows with missing values in the 'DOI' column. Since DOI used for quering data from API.

Crossref Data Fetching:
     - For each DOI (Digital Object Identifier) in the DataFrame, fetches additional data from Crossref, such as publication type, publisher, volume, issue, publication date, reference count, and ISSN.

Data Transformation and Export:
     - Transforms the fetched data into a DataFrame.
     - Checks for an existing output file and removes it if present.
     - Exports the enriched data to a Parquet file.

NB! Exported parquet files include only DOI and fetched data. In next steps need to join back with original data.
NB! Since Crossref might block too extensive requests, limited DOI queries to 50 per 1 second. (For one JSON chunk that is approx 17 min)

## data_prep_export_to_dwh.py
Data Import:
     - Loads JSON data from a file 'chunk_*.json', df arxiv
     - Loads enriched Parquet data from a file named 'en_chunk_*.parquet', df enriched

Data Preparation:
     - Merges enriched and arxiv DataFrames, selecting specific columns for merging.

Data Transformation:
     - Creates separate DataFrames for articles, authors, categories, types, publishers, journals, and published dates.
     - Processes author names by hashing them.
     - Expands and formats categories and published dates.

Database Connection:
     - Establishes a connection to a PostgreSQL database using provided credentials.

Database Table Creation:
     - Creates several tables in the database if they do not already exist. These tables include dimensions for types, publishers, journals, published dates, authors, categories, and a fact table for articles.

Export Temporary Tables to Database:
     - Exports the transformed DataFrames to the database as temporary tables.

Data Transfer to Permanent Tables:
     - Inserts data from temporary tables into the corresponding permanent tables.
     - Handles conflicts and data updating using SQL commands.
     - Establishes relationships between articles and authors, articles and categories through bridge tables.

Drop Temporary Tables:
     - Deletes all temporary tables from the database after the data transfer is complete.

Error Handling:
     - Includes error handling for reading files, executing SQL commands, and database transactions.

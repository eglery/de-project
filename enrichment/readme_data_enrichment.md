What does this script do:

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

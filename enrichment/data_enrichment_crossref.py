import pandas as pd
import requests
from urllib.parse import quote, urlencode
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os

api_email = 'egle.ryytli@gmail.com' #no need to register, just needed so Crossref wouldn't block you
api_max_workers = 20
select_columns = ['doi']
drop_nan_columns = ['doi'] # for dropping all columns with missing DOI

raw_file = '../data/chunks/chunk_14.json' #input
enriched_file = '../data/enriched/en_chunk_14.parquet' #output



start_time = datetime.now()

# initialize a cache dictionary
cache = {}

def fetch_crossref_data(doi, rate_limit_interval=1.0/50):  # keep the requests to 50 requests per second, Crossref might block you when too many requests
    time.sleep(rate_limit_interval)

    # check if the result is already in the cache
    if doi in cache:
        return cache[doi]

    try:
        # URL-encode the DOI, was suggested by Crossref
        encoded_doi = quote(doi, safe='')

        params = {'mailto': api_email}
        query_string = urlencode(params)
        url = f"https://api.crossref.org/works/{encoded_doi}?{query_string}"

        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json().get('message', {})

            fetched_data = {
                'type': data.get('type'),
                'publisher': data.get('publisher'),
                'volume': data.get('volume'),
                'issue': data.get('issue'),
                'published_online': data.get('published-online', {}).get('date-parts', [[]])[0],
                'is_referenced_by_count': data.get('is-referenced-by-count', 0),
                'references_count': data.get('references-count', 0),
                'container_title': ' '.join(data.get('container-title', [])),
                'short_container_title': ' '.join(data.get('short-container-title', [])),
                'ISSN': ' '.join(data.get('ISSN', []))
            }
            
            cache[doi] = fetched_data
            return fetched_data
        else:
            return {}
    except Exception as e:
        print(f"Error fetching data for DOI {doi}: {e}")
        return {}

def fetch_data_concurrently(df, func):
    results = []
    total = len(df['doi'])
    completed = 0

    with ThreadPoolExecutor(max_workers=api_max_workers) as executor:
        future_to_doi = {executor.submit(func, doi): doi for doi in df['doi']}
        for future in as_completed(future_to_doi):
            doi = future_to_doi[future]
            data = future.result()
            data['doi'] = doi
            results.append(data)
            completed += 1
            print(f"Completed {completed}/{total} DOI queries.", end='\r')

    return results

print(f"Processing file: {raw_file}")


# importing data
df = pd.read_json(raw_file, lines=True)

# print original row count
original_row_count = len(df)
print(f"Original row count: {original_row_count}")

# exclude rows with NaN in the 'DOI' column
df_filtered = df.dropna(subset= drop_nan_columns)

# print row count after dropping rows
row_count_after_dropping = len(df_filtered)
print(f"Row count after dropping NaNs in 'DOI': {row_count_after_dropping}")

df_selected = df_filtered[select_columns]

# query CrossRef API for additional data in parallel
new_data = fetch_data_concurrently(df_selected, fetch_crossref_data)

# convert new_data into a DataFrame
new_data_df = pd.DataFrame(new_data)

# check if the file exists and delete if it does
if os.path.exists(enriched_file):
    os.remove(enriched_file)

# export the DataFrame to a Parquet file
new_data_df.to_parquet(enriched_file, index=False)

end_time = datetime.now()
print(f"Finished data enrichment processing. Duration: {end_time - start_time}")

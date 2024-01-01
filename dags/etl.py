import os
import sys
import time
import requests
import json
import pandas as pd
import hashlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from urllib.parse import quote, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_ARGS = {
    'owner': 'DataEEngineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DATA_FOLDER = '/tmp/data'
CHUNKS_PATH = f'{DATA_FOLDER}/chunks'
ENRICHED_PATH = f'{DATA_FOLDER}/enriched'
CHUNK_SIZE = 50000
API_EMAIL = ['egle.ryytli@gmail.com'] #for Crossref, no need to register, just need to use some e-mail
API_MAX_WORKERS = 20
JSON_CHUNK = f'{CHUNKS_PATH}/chunk_2.json'
ENRICHED_PARQUET = f'{ENRICHED_PATH}/en_chunk_2.parquet'
DROP_NAN_COLUMNS = ['doi'] # for dropping all columns with missing DOI
SELECT_COLUMNS = ['title', 'doi', 'categories', 'authors_parsed']
ARTICLE_COUNT_TO_ENRICH = 10 #to speed up the process use only some DOI codes to enrich
#NB! to query data for all DOIs in a file use #len(df['doi'])

load_arxiv_data = DAG(
    'load_arxiv_data',
    default_args=DEFAULT_ARGS,
    description='ARXIV data load',
    schedule_interval=None,
    start_date=datetime(2023, 11, 6),
    catchup=False,
    template_searchpath=DATA_FOLDER,
)

# def check_if_data_exists():
#     hook = PostgresHook(postgres_conn_id='postgres-data')
#     conn = hook.get_conn()
#     cursor = conn.cursor()

#     cursor.execute("""
#         SELECT EXISTS (
#             SELECT FROM information_schema.tables 
#             WHERE table_schema = 'public' 
#             AND table_name = 'arxiv_table'
#         );
#     """)
#     table_exists = cursor.fetchone()[0]

#     if not table_exists:
#         cursor.close()
#         conn.close()
#         return True

#     cursor.execute('SELECT EXISTS (SELECT 1 FROM arxiv_table LIMIT 1);')
#     data_exists = cursor.fetchone()[0]

#     cursor.close()
#     conn.close()
#     return not data_exists

# check_data_operator = ShortCircuitOperator(
#     task_id='check_if_data_exists',
#     python_callable=check_if_data_exists,
#     provide_context=True,
#     dag=load_arxiv_data,
# )


# def create_tables_statement():
#     try:
#         sql_statement = """
#             CREATE TABLE IF NOT EXISTS arxiv_table (
#                 id SERIAL PRIMARY KEY,
#                 file_id TEXT,
#                 submitter TEXT,
#                 authors TEXT,
#                 title TEXT,
#                 comments TEXT,
#                 journal_ref TEXT,
#                 doi TEXT,
#                 report_no TEXT,
#                 categories TEXT,
#                 license TEXT,
#                 abstract TEXT,
#                 versions TEXT,
#                 update_date TEXT,
#                 authors_parsed TEXT
#             );
#         """

#         with open(f'{DATA_FOLDER}/insert_arxiv_tables.sql', 'w') as file:
#             file.write(sql_statement)
#     except:
#         LoggingMixin().log.info(sys.exc_info())

# create_tables_operator = PythonOperator(
#     task_id='create_tables_statement',
#     dag=load_arxiv_data,
#     python_callable=create_tables_statement,
# )

# insert_tables_to_db_operator = PostgresOperator(
#     task_id='insert_tables_to_db',
#     postgres_conn_id='postgres-data',
#     sql='insert_arxiv_tables.sql',
#     dag=load_arxiv_data,
# )

def split_file_into_chunks(file_path):
    if not os.path.exists(CHUNKS_PATH):
        try:
            os.makedirs(CHUNKS_PATH)
        except Exception as e:
            LoggingMixin().log.error("An error occurred while creating the chunks directory: %s", str(e))
            return []
    if os.path.exists(CHUNKS_PATH):
        try:
            files = os.listdir(CHUNKS_PATH)
            files = list(filter(lambda file: file != 'readme.Md', files))
            LoggingMixin().log.info(f'mida mm {files}')
            if len(files) > 0:
                return [os.path.join(CHUNKS_PATH, file) for file in files]
        except Exception as e:
            LoggingMixin().log.error("An error occurred while removing old chunks : %s", str(e))
            return []

    try:
        with open(file_path, 'r', encoding='UTF-8') as big_file:
            chunk_files = []
            chunk = []
            for i, line in enumerate(big_file):
                chunk.append(line)
                if (i + 1) % CHUNK_SIZE == 0:
                    chunk_file = f'{CHUNKS_PATH}/chunk_{i//CHUNK_SIZE}.json'
                    with open(chunk_file, 'w', encoding='UTF-8') as f:
                        f.writelines(chunk)
                    chunk_files.append(chunk_file)
                    chunk = []
            if chunk:
                chunk_file = f'{CHUNKS_PATH}/chunk_{i//CHUNK_SIZE}.json'
                with open(chunk_file, 'w', encoding='UTF-8') as f:
                    f.writelines(chunk)
                chunk_files.append(chunk_file)
        return chunk_files
    except Exception as e:
        LoggingMixin().log.error("An error occurred while splitting the file: %s", str(e))
        return []

split_file_operator = PythonOperator(
    task_id='split_file_into_chunks',
    python_callable=split_file_into_chunks,
    op_kwargs={
        'file_path': f'{DATA_FOLDER}/arxiv-metadata-oai-snapshot.json',
        # 'file_path': f'{DATA_FOLDER}/demo.json',
    },
    dag=load_arxiv_data,
)

def create_pg_tables():
    log = LoggingMixin().log
    try:
        sql_file = os.path.join(os.path.dirname(__file__), 'dwh_view', 'create_star_schema_tables.sql')
        hook = PostgresHook(postgres_conn_id='postgres-data')
        conn = hook.get_conn()
        cursor = conn.cursor()
        with open(sql_file, 'r') as file:
            sql_commands = file.read().split(';')  # split the file into individual SQL commands

        for sql_command in sql_commands:
            if sql_command.strip() != '':
                log.info(f"Executing SQL command: {sql_command}")
                try:
                    cursor.execute(sql_command)
                    log.info("SQL command executed successfully.")
                except Exception as e:
                    log.error(f"Error executing SQL command: {e}")
                conn.commit()
    except Exception as e:
        raise Exception(f"Error executing SQL script: {e}")
    finally:
        cursor.close()
        conn.close()
    return True  # return True so downstream tasks are not skipped

create_pg_tables_operator = ShortCircuitOperator(
    task_id='create_pg_tables',
    python_callable=create_pg_tables,
    dag=load_arxiv_data,
)

def create_pg_mat_views():
    log = LoggingMixin().log
    try:
        sql_file = os.path.join(os.path.dirname(__file__), 'dwh_view', 'create_mat_views.sql')
        hook = PostgresHook(postgres_conn_id='postgres-data')
        conn = hook.get_conn()
        cursor = conn.cursor()
        with open(sql_file, 'r') as file:
            sql_commands = file.read().split(';')  # split the file into individual SQL commands

        for sql_command in sql_commands:
            if sql_command.strip() != '':
                log.info(f"Executing SQL command: {sql_command}")
                try:
                    cursor.execute(sql_command)
                    log.info("SQL command executed successfully.")
                except Exception as e:
                    log.error(f"Error executing SQL command: {e}")
                conn.commit()
    except Exception as e:
        raise Exception(f"Error executing SQL script: {e}")
    finally:
        cursor.close()
        conn.close()
    return True  # return True so downstream tasks are not skipped

create_pg_mat_views_operator = ShortCircuitOperator(
    task_id='create_pg_mat_views',
    python_callable=create_pg_mat_views,
    dag=load_arxiv_data,
)

cache = {}  # cache for storing fetched data
def fetch_data_concurrently(df, func):
    log = LoggingMixin().log
    results = []
    total = ARTICLE_COUNT_TO_ENRICH #len(df['doi']) #NB! to query data for all DOIs in a file use len()
    completed = 0

    with ThreadPoolExecutor(max_workers=API_MAX_WORKERS) as executor:
        future_to_doi = {executor.submit(func, doi): doi for doi in df['doi'][:total]}  # Slice the DataFrame here
        for future in as_completed(future_to_doi):
            try:
                doi = future_to_doi[future]
                data = future.result()
                if data is not None:  # Check if data is None
                    data['doi'] = doi
                    results.append(data)
                completed += 1
                log.info(f"Completed {completed}/{total} DOI queries.")
            except Exception as e:
                log.error(f"An error occurred: {e}")

    return results

def fetch_crossref_data(doi, rate_limit_interval=1.0/50):  # keep the requests to 50 requests per second, Crossref might block you when too many requests
    time.sleep(rate_limit_interval)

    if doi in cache:
        return cache[doi]

    try:
        encoded_doi = quote(doi, safe='') # URL-encode the DOI, was suggested by Crossref

        params = {'mailto': API_EMAIL}
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
            }
            print(f"Fetched data: {fetched_data}")
            cache[doi] = fetched_data

            return fetched_data
        else:
            return None
    except Exception as e:
        return None
    
def read_json_chunk_and_enrich(raw_file):
    log = LoggingMixin().log
    try:
        with open(raw_file, 'r') as file:
            df = pd.read_json(raw_file, lines=True)
            original_row_count = len(df)
            log.info(f"Original row count: {original_row_count}")
            df_filtered = df.dropna(subset = DROP_NAN_COLUMNS)
            row_count_after_dropping = len(df_filtered)
            log.info(f"Row count after dropping NaNs in 'DOI': {row_count_after_dropping}")
            df_selected = df_filtered[SELECT_COLUMNS]
            new_data = fetch_data_concurrently(df_selected, fetch_crossref_data)
            enriched_df = pd.DataFrame(new_data)
            enriched_df['is_referenced_by_count'] = enriched_df['is_referenced_by_count'].fillna(0).astype(int)
            enriched_df['references_count'] = enriched_df['references_count'].fillna(0).astype(int)
            file_path = ENRICHED_PARQUET
            if os.path.exists(file_path):
                os.remove(file_path)
                log.info(f"File {file_path} exists and has been deleted.")       
            enriched_df.to_parquet(file_path)
            log.info(f"Created a new file at {file_path}.")
        return df
    except Exception as e:
        log.error(f"An error occurred: {e}")

data_enrichment_operator = PythonOperator(
    task_id=f'data_enrichment',
    python_callable=read_json_chunk_and_enrich,
    op_kwargs={'raw_file': f'{CHUNKS_PATH}/chunk_0.json'},
    dag=load_arxiv_data,
)

def hash_author(name):
    return hashlib.md5(name.encode()).hexdigest()

def list_to_datetime(date_list):
    if date_list is not None:
        return pd.to_datetime('-'.join(map(str, date_list)))
    else:
        return pd.NaT
    
def create_temp_table(cursor, table_name, df):
    columns = ', '.join([f'{col} {dtype}' for col, dtype in zip(df.columns, df.dtypes)])
    cursor.execute(f'CREATE TEMP TABLE {table_name} ({columns}) ON COMMIT DELETE ROWS')
    for row in df.itertuples(index=False):
        cursor.execute(f'INSERT INTO {table_name} VALUES {row}')

def prep_and_import_to_pg_database(parquet_file):
    log = LoggingMixin().log
    try:

        hook = PostgresHook('postgres-data')
        conn = hook.get_conn()
        cursor = conn.cursor()
        arxiv_enriched = pd.read_parquet(parquet_file)
        log.info(f"{arxiv_enriched} loaded successfully.")

        articles_df = arxiv_enriched[['doi', 'title', 'is_referenced_by_count', 'references_count']]
        authors_exploded = arxiv_enriched.explode('authors_parsed') # unnest authors
        authors_doi_df = pd.DataFrame({
                                        'doi': authors_exploded['doi'],
                                        'author': authors_exploded['authors_parsed'].apply(lambda x: ', '.join(filter(None, x)))
                                        })
        authors_doi_df['author_hash'] = authors_doi_df['author'].apply(hash_author)
        doi_author_hash_df = authors_doi_df[['doi','author_hash']]
        authors_df = authors_doi_df[['author_hash','author']].drop_duplicates()
        # split the 'categories' column into separate rows
        categories_expanded = arxiv_enriched['categories'].str.split(' ', expand=True).stack()
        categories_expanded.name = 'category'
        categories_expanded_df = arxiv_enriched.drop(columns=['categories']).join(categories_expanded.reset_index(level=1, drop=True))
        doi_category_df = categories_expanded_df[['doi','category']]
        categories_df = doi_category_df[['category']].drop_duplicates()
        types_df = arxiv_enriched[['doi', 'type']]
        publishers_df = arxiv_enriched[['doi', 'publisher']]
        journals_df = arxiv_enriched[['doi', 'ISSN', 'container_title', 'short_container_title', 'volume', 'issue']]
        journals_df = journals_df.dropna(subset=['ISSN'])
        published_df = arxiv_enriched[['doi', 'published_online']]
        published_df['published_date'] = published_df['published_online'].apply(list_to_datetime)
        published_df['published_online'] = published_df['published_online'].apply(lambda x: str(x) if x is not None else None)
        
        # Create temporary tables
        create_temp_table(cursor, 'temp_articles', articles_df)
        create_temp_table(cursor, 'temp_authors', authors_df)
        create_temp_table(cursor, 'temp_doi_author_hash', doi_author_hash_df)
        create_temp_table(cursor, 'temp_categories', categories_df)
        create_temp_table(cursor, 'temp_doi_category', doi_category_df)
        create_temp_table(cursor, 'temp_types', types_df)
        create_temp_table(cursor, 'temp_publishers', publishers_df)
        create_temp_table(cursor, 'temp_journals', journals_df)
        create_temp_table(cursor, 'temp_published', published_df)

    except BaseException as e:
        log.error(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

prep_data_and_import_to_pg_database_operator = PythonOperator(
    task_id=f'prep_and_import_to_pg_database',
    python_callable=prep_and_import_to_pg_database,
    op_kwargs={'parquet_file': ENRICHED_PARQUET},
    dag=load_arxiv_data,
)

def insert_data_to_pg_tables():
    log = LoggingMixin().log
    try:
        sql_file = os.path.join(os.path.dirname(__file__), 'dwh_view', 'insert_data_to_pg_tagles.sql')
        hook = PostgresHook(postgres_conn_id='postgres-data')
        conn = hook.get_conn()
        cursor = conn.cursor()
        with open(sql_file, 'r') as file:
            sql_commands = file.read().split(';')  # split the file into individual SQL commands

        for sql_command in sql_commands:
            if sql_command.strip() != '':
                log.info(f"Executing SQL command: {sql_command}")
                try:
                    cursor.execute(sql_command)
                    log.info("SQL command executed successfully.")
                except Exception as e:
                    log.error(f"Error executing SQL command: {e}")
                conn.commit()
    except Exception as e:
        raise Exception(f"Error executing SQL script: {e}")
    finally:
        cursor.close()
        conn.close()
    return True  # return True so downstream tasks are not skipped

insert_data_to_pg_tables_operator = ShortCircuitOperator(
    task_id='insert_data_to_pg_tables',
    python_callable=insert_data_to_pg_tables,
    dag=load_arxiv_data,
)

def refresh_mat_views():
    log = LoggingMixin().log
    hook = PostgresHook(postgres_conn_id='postgres-data')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            REFRESH MATERIALIZED VIEW author_h_index;
            REFRESH MATERIALIZED VIEW journal_h_index;
            REFRESH MATERIALIZED VIEW category_h_index;
            REFRESH MATERIALIZED VIEW publisher_yearly_rank;
            REFRESH MATERIALIZED VIEW author_diversity_rank;
            REFRESH MATERIALIZED VIEW avg_citation_difference_by_type;
        """)
    except Exception as e:
        log.info(f"Error refreshing materialized views: {e}")
    finally:
        cur.close()
        conn.close()
    return True  # return True so downstream tasks are not skipped

refresh_mat_views_operator = ShortCircuitOperator(
    task_id='refresh_mat_views',
    python_callable=refresh_mat_views,
    provide_context=True,
    dag=load_arxiv_data
)

def drop_temp_tables():
    log = LoggingMixin().log
    hook = PostgresHook(postgres_conn_id='postgres-data')
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("""
            DROP TEMP TABLE IF EXISTS temp_articles;
            DROP TEMP TABLE IF EXISTS temp_authors;
            DROP TEMP TABLE IF EXISTS temp_doi_author_hash;
            DROP TEMP TABLE IF EXISTS temp_categories;
            DROP TEMP TABLE IF EXISTS temp_doi_category;
            DROP TEMP TABLE IF EXISTS temp_types;
            DROP TEMP TABLE IF EXISTS temp_publishers;
            DROP TEMP TABLE IF EXISTS temp_journals;
            DROP TEMP TABLE IF EXISTS temp_published;
        """)
    except Exception as e:
        log.info(f"Error refreshing materialized views: {e}")
    finally:
        cur.close()
        conn.close()
    return True  # return True so downstream tasks are not skipped

drop_temp_tables_operator = ShortCircuitOperator(
    task_id='drop_temp_tables',
    python_callable=drop_temp_tables,
    provide_context=True,
    dag=load_arxiv_data
)
# def prepare_insert_statement_for_chunk_statement():
#     files = os.listdir(CHUNKS_PATH)
#     files = list(filter(lambda file: file != 'readme.Md' and file.split('.')[1] == 'json', files))

#     for file in files:
#         chunk_file = f'{CHUNKS_PATH}/{file}'
#         try:
#             sql_statements = []
#             with open(chunk_file, 'r', encoding='UTF-8') as file:
#                 for line in file:
#                     paper = json.loads(line)
#                     format_text = lambda text: str(text) \
#                         .replace("\n", "") \
#                         .replace("\t", "") \
#                         .replace("\\", "") \
#                         .replace("'", "\\'")
#                     sql_statement = f"""
#                         INSERT INTO arxiv_table (file_id, submitter, authors, title, comments, journal_ref, doi, report_no, categories, license, abstract, versions, update_date, authors_parsed)
#                             VALUES (
#                                 E'{format_text(paper['id'])}',
#                                 E'{format_text(paper['submitter'])}',
#                                 E'{format_text(paper['authors'])}',
#                                 E'{format_text(paper['title'])}',
#                                 E'{format_text(paper['comments'])}',
#                                 E'{format_text(paper['journal-ref'])}',
#                                 E'{format_text(paper['doi'])}',
#                                 E'{format_text(paper['report-no'])}',
#                                 E'{format_text(paper['categories'])}',
#                                 E'{format_text(paper['license'])}',
#                                 E'{format_text(paper['abstract'])}',
#                                 E'{format_text(json.dumps(paper['versions']))}',
#                                 E'{format_text(paper['update_date'])}',
#                                 E'{format_text(json.dumps(paper['authors_parsed']))}'
#                             );
#                     """
#                     sql_statements.append(sql_statement)

#             chunk_sql_file = f"{chunk_file.replace('.json', '.sql')}"
#             with open(chunk_sql_file, 'w') as file:
#                 # file.write('{% raw %}')
#                 file.write(''.join(sql_statements))
#                 # file.write('{% endraw %}')

#         except:
#             LoggingMixin().log.error("An error occurred: %s", sys.exc_info()[0])

# prepare_chunk_operator = PythonOperator(
#     task_id=f'prepare_insert_statement_for_chunk_statement',
#     python_callable=prepare_insert_statement_for_chunk_statement,
#     dag=load_arxiv_data,
# )

# def execute_sql_chunks_statement():
#     files = os.listdir(CHUNKS_PATH)
#     sql_files = filter(lambda file: file.endswith('.sql'), files)

#     hook = PostgresHook(postgres_conn_id='postgres-data')
#     conn = hook.get_conn()
#     cursor = conn.cursor()

#     for sql_file in sql_files:
#         try:
#             with open(f'{CHUNKS_PATH}/{sql_file}', 'r') as file:
#                 sql = file.read()
#             cursor.execute(sql)
#             conn.commit()
#         except Exception as e:
#             LoggingMixin().log.error("An error occurred while executing SQL chunk: %s, %s", sql_file, str(e))
#             conn.rollback()

#     cursor.close()
#     conn.close()

# execute_chunks_operator = PythonOperator(
#     task_id='execute_sql_chunks_statement',
#     python_callable=execute_sql_chunks_statement,
#     dag=load_arxiv_data,
# )

# def prepare_neo4j_files():
#     import csv
#     import re

#     files = os.listdir(CHUNKS_PATH)
#     files = list(filter(lambda file: file != 'readme.Md' and file.split('.')[1] == 'json', files))

#     i = 0
#     authors = {}
#     author_id = 0

#     for file in files:
#         chunk_file = f'{CHUNKS_PATH}/{file}'
            
#         rows = []
#         papers = []
#         with open(chunk_file, 'r') as file:
#             for row in file:
#                 row = json.loads(row)
#                 row = {col: row[col] for col in row if col not in ['comments', 'abstract']}
#                 row['title'] = row['title'].replace('"','').replace("'",'').replace('\\','').replace('\n','')
#                 row['submitter'] = 'null' if row['submitter'] is None else row['submitter']
#                 row['submitter'] = row['submitter'].replace('"','').replace("'",'').replace('\\','').replace('\n','')
#                 row['journal-ref'] = 'null' if row['journal-ref'] is None else row['journal-ref']
#                 row['doi'] = 'null' if row['doi'] is None else row['doi']
#                 row['report-no'] = 'null' if row['report-no'] is None else row['report-no']
#                 row['categories'] = 'null' if row['categories'] is None else row['categories']
#                 row['license'] = 'null' if row['license'] is None else row['license']
#                 row['update_date'] = 'null' if row['update_date'] is None else row['update_date']
#                 papers.append({'paperId:ID': row['id'], 'title': row['title'], 'journal-ref': row['journal-ref'], 'doi': row['doi'], 'report-no': row['report-no'], 'categories': row['categories'], 'license': row['license'], 'update_date': row['update_date'], ':LABEL': 'Paper'})
#                 row['authors'] = re.sub(r'\s*\([^()]*\)', '', re.sub(r'\s*\([^()]*\)', '', row['authors']))
#                 row['authors'] = re.split(', | and ', row['authors'])
#                 for author in row['authors']:
#                     exploded = row.copy()
#                     exploded['authors'] = author.replace('"','').replace("'",'').replace('\\','').replace('\n','')
#                     if 'collaboration' in exploded['authors'].lower() or 'et al' in exploded['authors'].lower() or exploded['authors'] == 'Jr.':
#                         continue
#                     if exploded['authors'] not in authors:
#                         authors[exploded['authors']] = author_id
#                         author_id += 1
#                     rows.append(exploded)
#         relations = [{':START_ID': authors[row['authors']], ':END_ID': row['id'], ':TYPE': 'WROTE'} for row in rows]
#         relations = relations + [{':START_ID': authors[row['authors']], ':END_ID': row['id'], ':TYPE': 'SUBMITTED'} for row in rows if row['submitter'].split(' ')[-1] in row['authors']]

#         with open(f'/tmp/import/papers_{i}.csv', 'w', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, list(papers[0].keys()))
#             writer.writerows(papers)
#         with open(f'/tmp/import/relations_{i}.csv', 'w', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, list(relations[0].keys()))
#             writer.writerows(relations)
        
#         if i == 0:
#             with open('/tmp/import/authors_header.csv', 'w', encoding='utf-8') as csvfile:
#                 writer = csv.DictWriter(csvfile, ['personId:ID', 'name', ':LABEL'])
#                 writer.writeheader()
#             with open('/tmp/import/papers_header.csv', 'w', encoding='utf-8') as csvfile:
#                 writer = csv.DictWriter(csvfile, list(papers[0].keys()))
#                 writer.writeheader()
#             with open('/tmp/import/relations_header.csv', 'w', encoding='utf-8') as csvfile:
#                 writer = csv.DictWriter(csvfile, list(relations[0].keys()))
#                 writer.writeheader()

#         if i == len(files)-1:
#             batch_size = len(authors) // len(files)
#             prev_batch = 0
#             for j in range(len(files)):
#                 with open(f'/tmp/import/authors_{j}.csv', 'w') as csvfile:
#                     writer = csv.DictWriter(csvfile, ['personId:ID', 'name', ':LABEL'])
#                     if j != len(files)-1:
#                         batch = [{'personId:ID': v, 'name': k, ':LABEL': 'Author'} for k, v in authors.items() if prev_batch <= v < prev_batch+batch_size]
#                     else:
#                         batch = [{'personId:ID': v, 'name': k, ':LABEL': 'Author'} for k, v in authors.items() if prev_batch <= v]
#                     prev_batch = prev_batch + batch_size
#                     writer.writerows(batch)
        
#         i += 1

# execute_prepare_neo4j_operator = PythonOperator(
#     task_id='execute_prepare_neo4j_files',
#     python_callable=prepare_neo4j_files,
#     dag=load_arxiv_data,
# )

# execute_neo4j_import_operator = BashOperator(
#     task_id='execute_neo4j_import',
#     bash_command='touch /tmp/import/started_import.flag && while [ -f /tmp/import/started_import.flag ]; do sleep 1; done && echo "started_import.flag has been deleted, finishing the process."',
#     dag=load_arxiv_data,
# )

#check_data_operator >> insert_tables_to_db_operator >> 
split_file_operator >> create_pg_tables_operator >> create_pg_mat_views_operator >> data_enrichment_operator >> prep_data_and_import_to_pg_database_operator >> insert_data_to_pg_tables_operator >> refresh_mat_views_operator >> drop_temp_tables_operator
#>> execute_prepare_neo4j_operator >> execute_neo4j_import_operator #>> refresh_mat_views_operator
#>> prepare_chunk_operator >> execute_chunks_operator 

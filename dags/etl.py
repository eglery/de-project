import json
import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

DEFAULT_ARGS = {
    'owner': 'DataEEngineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DATA_FOLDER = '/tmp/data'
CHUNKS_PATH = f'{DATA_FOLDER}/chunks'
CHUNK_SIZE = 50000

load_arxiv_data = DAG(
    'load_arxiv_data',
    default_args=DEFAULT_ARGS,
    description='ARXIV data load',
    schedule_interval=None,
    start_date=datetime(2023, 11, 6),
    catchup=False,
    template_searchpath=DATA_FOLDER,
)


def check_if_data_exists():
    hook = PostgresHook(postgres_conn_id='postgres-data')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'arxiv_table'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        cursor.close()
        conn.close()
        return True

    cursor.execute('SELECT EXISTS (SELECT 1 FROM arxiv_table LIMIT 1);')
    data_exists = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return not data_exists

check_data_operator = ShortCircuitOperator(
    task_id='check_if_data_exists',
    python_callable=check_if_data_exists,
    provide_context=True,
    dag=load_arxiv_data,
)


def create_tables_statement():
    try:
        sql_statement = """
            CREATE TABLE IF NOT EXISTS arxiv_table (
                id SERIAL PRIMARY KEY,
                file_id TEXT,
                submitter TEXT,
                authors TEXT,
                title TEXT,
                comments TEXT,
                journal_ref TEXT,
                doi TEXT,
                report_no TEXT,
                categories TEXT,
                license TEXT,
                abstract TEXT,
                versions TEXT,
                update_date TEXT,
                authors_parsed TEXT
            );
        """

        with open(f'{DATA_FOLDER}/insert_arxiv_tables.sql', 'w') as file:
            file.write(sql_statement)
    except:
        LoggingMixin().log.info(sys.exc_info())

create_tables_operator = PythonOperator(
    task_id='create_tables_statement',
    dag=load_arxiv_data,
    python_callable=create_tables_statement,
)

insert_tables_to_db_operator = PostgresOperator(
    task_id='insert_tables_to_db',
    postgres_conn_id='postgres-data',
    sql='insert_arxiv_tables.sql',
    dag=load_arxiv_data,
)

def split_file_into_chunks(file_path):
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


def prepare_insert_statement_for_chunk_statement():
    files = os.listdir(CHUNKS_PATH)
    files = list(filter(lambda file: file != 'readme.Md' and file.split('.')[1] == 'json', files))

    for file in files:
        chunk_file = f'{CHUNKS_PATH}/{file}'
        try:
            sql_statements = []
            with open(chunk_file, 'r', encoding='UTF-8') as file:
                for line in file:
                    paper = json.loads(line)
                    format_text = lambda text: str(text) \
                        .replace("\n", "") \
                        .replace("\t", "") \
                        .replace("\\", "") \
                        .replace("'", "\\'")
                    sql_statement = f"""
                        INSERT INTO arxiv_table (file_id, submitter, authors, title, comments, journal_ref, doi, report_no, categories, license, abstract, versions, update_date, authors_parsed)
                            VALUES (
                                E'{format_text(paper['id'])}',
                                E'{format_text(paper['submitter'])}',
                                E'{format_text(paper['authors'])}',
                                E'{format_text(paper['title'])}',
                                E'{format_text(paper['comments'])}',
                                E'{format_text(paper['journal-ref'])}',
                                E'{format_text(paper['doi'])}',
                                E'{format_text(paper['report-no'])}',
                                E'{format_text(paper['categories'])}',
                                E'{format_text(paper['license'])}',
                                E'{format_text(paper['abstract'])}',
                                E'{format_text(json.dumps(paper['versions']))}',
                                E'{format_text(paper['update_date'])}',
                                E'{format_text(json.dumps(paper['authors_parsed']))}'
                            );
                    """
                    sql_statements.append(sql_statement)

            chunk_sql_file = f"{chunk_file.replace('.json', '.sql')}"
            with open(chunk_sql_file, 'w') as file:
                # file.write('{% raw %}')
                file.write(''.join(sql_statements))
                # file.write('{% endraw %}')

        except:
            LoggingMixin().log.error("An error occurred: %s", sys.exc_info()[0])

prepare_chunk_operator = PythonOperator(
    task_id=f'prepare_insert_statement_for_chunk_statement',
    python_callable=prepare_insert_statement_for_chunk_statement,
    dag=load_arxiv_data,
)


def execute_sql_chunks_statement():
    files = os.listdir(CHUNKS_PATH)
    sql_files = filter(lambda file: file.endswith('.sql'), files)

    hook = PostgresHook(postgres_conn_id='postgres-data')
    conn = hook.get_conn()
    cursor = conn.cursor()

    for sql_file in sql_files:
        try:
            with open(f'{CHUNKS_PATH}/{sql_file}', 'r') as file:
                sql = file.read()
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            LoggingMixin().log.error("An error occurred while executing SQL chunk: %s, %s", sql_file, str(e))
            conn.rollback()

    cursor.close()
    conn.close()

execute_chunks_operator = PythonOperator(
    task_id='execute_sql_chunks_statement',
    python_callable=execute_sql_chunks_statement,
    dag=load_arxiv_data,
)

def prepare_neo4j_files():
    import csv
    import re

    files = os.listdir(CHUNKS_PATH)
    files = list(filter(lambda file: file != 'readme.Md' and file.split('.')[1] == 'json', files))

    i = 0
    authors = {}
    author_id = 0

    for file in files:
        chunk_file = f'{CHUNKS_PATH}/{file}'
            
        rows = []
        papers = []
        with open(chunk_file, 'r') as file:
            for row in file:
                row = json.loads(row)
                row = {col: row[col] for col in row if col not in ['comments', 'abstract']}
                row['title'] = row['title'].replace('"','').replace("'",'').replace('\\','').replace('\n','')
                row['submitter'] = 'null' if row['submitter'] is None else row['submitter']
                row['submitter'] = row['submitter'].replace('"','').replace("'",'').replace('\\','').replace('\n','')
                row['journal-ref'] = 'null' if row['journal-ref'] is None else row['journal-ref']
                row['doi'] = 'null' if row['doi'] is None else row['doi']
                row['report-no'] = 'null' if row['report-no'] is None else row['report-no']
                row['categories'] = 'null' if row['categories'] is None else row['categories']
                row['license'] = 'null' if row['license'] is None else row['license']
                row['update_date'] = 'null' if row['update_date'] is None else row['update_date']
                papers.append({'paperId:ID': row['id'], 'title': row['title'], 'journal-ref': row['journal-ref'], 'doi': row['doi'], 'report-no': row['report-no'], 'categories': row['categories'], 'license': row['license'], 'update_date': row['update_date'], ':LABEL': 'Paper'})
                row['authors'] = re.sub(r'\s*\([^()]*\)', '', re.sub(r'\s*\([^()]*\)', '', row['authors']))
                row['authors'] = re.split(', | and ', row['authors'])
                for author in row['authors']:
                    exploded = row.copy()
                    exploded['authors'] = author.replace('"','').replace("'",'').replace('\\','').replace('\n','')
                    if exploded['authors'] not in authors:
                        authors[exploded['authors']] = author_id
                        author_id += 1
                    rows.append(exploded)
        relations = [{':START_ID': authors[row['authors']], ':END_ID': row['id'], ':TYPE': 'WROTE'} for row in rows]
        relations = relations + [{':START_ID': authors[row['authors']], ':END_ID': row['id'], ':TYPE': 'SUBMITTED'} for row in rows if row['submitter'].split(' ')[-1] in row['authors']]

        with open(f'/tmp/import/papers_{i}.csv', 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, list(papers[0].keys()))
            writer.writerows(papers)
        with open(f'/tmp/import/relations_{i}.csv', 'w', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, list(relations[0].keys()))
            writer.writerows(relations)
        
        if i == 0:
            with open('/tmp/import/authors_header.csv', 'w', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, ['personId:ID', 'name', ':LABEL'])
                writer.writeheader()
            with open('/tmp/import/papers_header.csv', 'w', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, list(papers[0].keys()))
                writer.writeheader()
            with open('/tmp/import/relations_header.csv', 'w', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, list(relations[0].keys()))
                writer.writeheader()

        if i == len(files)-1:
            batch_size = len(authors) // len(files)
            prev_batch = 0
            for j in range(len(files)):
                with open(f'/tmp/import/authors_{j}.csv', 'w') as csvfile:
                    writer = csv.DictWriter(csvfile, ['personId:ID', 'name', ':LABEL'])
                    if j != len(files)-1:
                        batch = [{'personId:ID': v, 'name': k, ':LABEL': 'Author'} for k, v in authors.items() if prev_batch <= v < prev_batch+batch_size]
                    else:
                        batch = [{'personId:ID': v, 'name': k, ':LABEL': 'Author'} for k, v in authors.items() if prev_batch <= v]
                    prev_batch = prev_batch + batch_size
                    writer.writerows(batch)
        
        i += 1

execute_prepare_neo4j_operator = PythonOperator(
    task_id='execute_prepare_neo4j_files',
    python_callable=prepare_neo4j_files,
    dag=load_arxiv_data,
)

execute_neo4j_import_operator = BashOperator(
    task_id='execute_neo4j_import',
    bash_command='touch /tmp/import/started_import.flag && while [ -f /tmp/import/started_import.flag ]; do sleep 1; done && echo "started_import.flag has been deleted, finishing the process."',
    dag=load_arxiv_data,
)

check_data_operator >> insert_tables_to_db_operator >> split_file_operator >> prepare_chunk_operator >> execute_chunks_operator >> execute_prepare_neo4j_operator >> execute_neo4j_import_operator
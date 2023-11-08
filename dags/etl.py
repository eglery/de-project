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
from airflow.operators.python_operator import BranchPythonOperator

DEFAULT_ARGS = {
    'owner': 'DataEEngineering',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

DATA_FOLDER = '/tmp/data'
CHUNK_SIZE = 150000
CHUNK_RESERVE = 3000000 // CHUNK_SIZE + 1
# CHUNK_SIZE = 1000
# CHUNK_RESERVE = 30

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
    chunks_path = f'{DATA_FOLDER}/chunks'

    LoggingMixin().log.info(f'mida {os.path.exists(chunks_path)}')

    if os.path.exists(chunks_path):
        try:
            files = os.listdir(chunks_path)
            if len(files) > 0:
                return [os.path.join(chunks_path, file) for file in files]
            # for file in files:
            #     chunk_path = os.path.join(chunks_path, file)
            #     if os.path.isfile(chunk_path):
            #         os.remove(chunk_path)
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
                    chunk_file = f'{chunks_path}/chunk_{i//CHUNK_SIZE}.json'
                    with open(chunk_file, 'w', encoding='UTF-8') as f:
                        f.writelines(chunk)
                    chunk_files.append(chunk_file)
                    chunk = []
            if chunk:
                chunk_file = f'{chunks_path}/chunk_{i//CHUNK_SIZE}.json'
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


def prepare_insert_statement_for_chunk(chunk_file):
    LoggingMixin().log.info(f'I was called {chunk_file}')
    try:
        sql_statements = []
        with open(chunk_file, 'r', encoding='UTF-8') as file:
            for line in file:
                paper = json.loads(line)
                format_text = lambda text: str(text)\
                    .replace("\n", "")\
                    .replace("\t", "")\
                    .replace("\\", "")\
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
            file.write('{% raw %}')
            file.write(''.join(sql_statements))
            file.write('{% endraw %}')

        return chunk_sql_file
    except:
        LoggingMixin().log.error("An error occurred: %s", sys.exc_info()[0])
        return None

def choose_tasks_to_execute(**context):
    task_instance = context['ti']
    chunk_files = task_instance.xcom_pull(task_ids='split_file_into_chunks')
    LoggingMixin().log.info(chunk_files)

    if not chunk_files:
        raise ValueError("No chunk files to process.")

    return [f'prepare_insert_statement_for_chunk_{i}' for i, _ in enumerate(chunk_files)]

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_tasks_to_execute,
    provide_context=True,
    dag=load_arxiv_data,
)

for i in range(CHUNK_RESERVE):
    prepare_chunk_operator = PythonOperator(
        task_id=f'prepare_insert_statement_for_chunk_{i}',
        python_callable=prepare_insert_statement_for_chunk,
        op_kwargs={'chunk_file': f'{DATA_FOLDER}/chunks/chunk_{i}.json'},
        dag=load_arxiv_data,
    )

    insert_chunk_to_db_operator = PostgresOperator(
        task_id=f'insert_chunk_{i}_to_db',
        postgres_conn_id='postgres-data',
        sql=f"chunks/chunk_{i}.sql",
        dag=load_arxiv_data,
    )

    prepare_chunk_operator >> insert_chunk_to_db_operator
    branching >> prepare_chunk_operator

check_data_operator >> insert_tables_to_db_operator >> split_file_operator >> branching
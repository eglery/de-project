import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine

username = 'data_user'
password = 'data_password'
host = 'localhost'
port = '8093'
database = 'data_db'

engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

### Drop star schema tables
# List of tables to be dropped
# NB! use only when waht to clear the database
tables_to_drop = [
    'articles_fact', 
    'authors_dim', 
    'article_authors_bridge',
    'categories_dim', 
    'article_category_bridge', 
    'types_dim', 
    'publishers_dim', 
    'journals_dim', 
    'published_online_dim'
]

try:
    with engine.connect() as connection:
        # Begin a transaction
        trans = connection.begin()
        for table in tables_to_drop:
            sql = text(f"DROP TABLE IF EXISTS {table} CASCADE;")
            connection.execute(sql)
            print(f"Dropped table {table}")
        # Commit the transaction
        trans.commit()
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")


### Drop temp tables
# List of temporary tables to be dropped
temp_tables = [
    'temp_articles', 
    'temp_authors', 
    'temp_doi_author_hash',
    'temp_categories', 
    'temp_doi_category', 
    'temp_types', 
    'temp_publishers', 
    'temp_journals', 
    'temp_published'
]

try:
    with engine.connect() as connection:
        # Begin a transaction
        trans = connection.begin()
        for table in temp_tables:
            sql = text(f"DROP TABLE IF EXISTS {table};")
            connection.execute(sql)
            print(f"Dropped table {table}")
        # Commit the transaction
        trans.commit()
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")
#not part of the ETL
#use when want to drop all tables and mat views in the postgres database

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
        trans = connection.begin()
        for table in tables_to_drop:
            sql = text(f"DROP TABLE IF EXISTS {table} CASCADE;")
            connection.execute(sql)
            print(f"Dropped TABLE {table}")
        trans.commit()
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")

mat_views_to_drop = [
    'author_h_index', 
    'journal_h_index', 
    'category_h_index',
    'publisher_yearly_rank', 
    'author_diversity_rank', 
    'avg_citation_difference_by_type'
]

try:
    with engine.connect() as connection:
        trans = connection.begin()
        for table in mat_views_to_drop:
            sql = text(f"DROP MATERIALIZED VIEW IF EXISTS {table} CASCADE;")
            connection.execute(sql)
            print(f"Dropped MATERIALIZED VIEW {table}")
        trans.commit()
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")

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
        trans = connection.begin()
        for table in temp_tables:
            sql = text(f"DROP TABLE IF EXISTS {table};")
            connection.execute(sql)
            print(f"Dropped TABLE {table}")
        trans.commit()
except SQLAlchemyError as e:
    print(f"An error occurred: {e}")
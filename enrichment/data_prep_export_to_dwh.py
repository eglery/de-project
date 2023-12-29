import pandas as pd
import hashlib
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine

raw_file = '../data/chunks/chunk_0.json'
enriched_file = '../data/enriched/en_chunk_0.parquet'

select_columns_arxiv_raw = ['doi', 'title', 'authors_parsed', 'categories'] #['doi', 'authors_parsed', 'title', 'categories', 'journal-ref', 'versions', 'update_date']


username = 'data_user'
password = 'data_password'
host = 'localhost'
port = '8093'
database = 'data_db'


# Data import
try:
    arxiv = pd.read_json(raw_file, lines=True)
    print(f"{raw_file} loaded successfully.")
    #display(arxiv.head())
except Exception as e:
    print(f"Error occurred while reading the file: {e}")

try:
    enriched = pd.read_parquet(enriched_file)
    print(f"{enriched_file} loaded successfully.")
    #display(enriched.head())
except Exception as e:
    print(f"Error occurred while reading the file: {e}")

# Data prep
# merge enriched and original arxiv
arxiv_enriched = enriched.merge(arxiv[select_columns_arxiv_raw], on='doi', how='left')
arxiv_enriched['is_referenced_by_count'] = arxiv_enriched['is_referenced_by_count'].fillna(0).astype(int)
arxiv_enriched['references_count'] = arxiv_enriched['references_count'].fillna(0).astype(int)

## Articles
articles_df = arxiv_enriched[['doi', 'title', 'is_referenced_by_count', 'references_count']]


## Authors
# unnest authors
authors_exploded = arxiv_enriched.explode('authors_parsed')

authors_doi_df = pd.DataFrame({
    'doi': authors_exploded['doi'],
    'author': authors_exploded['authors_parsed'].apply(lambda x: ', '.join(filter(None, x)))
})

# fn to hash author names using MD5
def hash_author(name):
    return hashlib.md5(name.encode()).hexdigest()

# Apply the hash function to the 'author' column
authors_doi_df['author_hash'] = authors_doi_df['author'].apply(hash_author)

doi_author_hash_df = authors_doi_df[['doi','author_hash']]
authors_df = authors_doi_df[['author_hash','author']].drop_duplicates()


## Categories
# unnest categories
# split the 'categories' column into separate rows
categories_expanded = arxiv_enriched['categories'].str.split(' ', expand=True).stack()
categories_expanded.name = 'category'

categories_expanded_df = arxiv_enriched.drop(columns=['categories']).join(categories_expanded.reset_index(level=1, drop=True))

doi_category_df = categories_expanded_df[['doi','category']]

categories_df = doi_category_df[['category']].drop_duplicates()


## Type
types_df = arxiv_enriched[['doi', 'type']]


## Publisher
publishers_df = arxiv_enriched[['doi', 'publisher']]


## Journal
journals_df = arxiv_enriched[['doi', 'ISSN', 'container_title', 'short_container_title', 'volume', 'issue']]
journals_df = journals_df.dropna(subset=['ISSN'])


## Published online
published_df = arxiv_enriched[['doi', 'published_online']]

def list_to_datetime(date_list):
    if date_list is not None:
        return pd.to_datetime('-'.join(map(str, date_list)))
    else:
        return pd.NaT

published_df['published_date'] = published_df['published_online'].apply(list_to_datetime)
published_df['published_online'] = published_df['published_online'].apply(lambda x: str(x) if x is not None else None)


# Export to database
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

### Create tables
# SQL statements for table creation
create_tables = [
    """
    CREATE TABLE IF NOT EXISTS types_dim (
        id SERIAL PRIMARY KEY,
        type TEXT UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS publishers_dim (
        id SERIAL PRIMARY KEY,
        publisher TEXT UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS journals_dim (
        id SERIAL PRIMARY KEY,
        issn TEXT UNIQUE,
        container_title TEXT,
        short_container_title TEXT,
        volume TEXT,
        issue TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS published_online_dim (
        id SERIAL PRIMARY KEY,
        published_date DATE UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS articles_fact (
        id SERIAL PRIMARY KEY,
        doi TEXT UNIQUE,
        title TEXT,
        type_id INT,
        publisher_id INT,
        journal_id INT,
        published_online_id INT,
        is_referenced_by_count INT,
        references_count INT,
        FOREIGN KEY (type_id) REFERENCES types_dim (id),
        FOREIGN KEY (publisher_id) REFERENCES publishers_dim (id),
        FOREIGN KEY (journal_id) REFERENCES journals_dim (id),
        FOREIGN KEY (published_online_id) REFERENCES published_online_dim (id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS authors_dim (
        id SERIAL PRIMARY KEY,
        author_hash TEXT UNIQUE,
        author TEXT
);
    """,
    """
    CREATE TABLE IF NOT EXISTS article_authors_bridge (
        article_id INT,
        author_id INT,
        FOREIGN KEY (article_id) REFERENCES articles_fact (id),
        FOREIGN KEY (author_id) REFERENCES authors_dim (id),
        UNIQUE (article_id, author_id)
);
    """,
    """
    CREATE TABLE IF NOT EXISTS categories_dim (
        id SERIAL PRIMARY KEY,
        category TEXT UNIQUE
);
    """,
    """
    CREATE TABLE IF NOT EXISTS article_category_bridge (
        article_id INT,
        category_id INT,
        FOREIGN KEY (article_id) REFERENCES articles_fact (id),
        FOREIGN KEY (category_id) REFERENCES categories_dim (id),
        UNIQUE (article_id, category_id)
);
"""
]

with engine.connect() as connection:
    for sql_command in create_tables:
        trans = connection.begin()  # Start a new transaction
        try:
            connection.execute(text(sql_command))
            trans.commit()  # Commit the transaction
            print(f"Executed SQL command:\n{sql_command}")
        except Exception as e:
            trans.rollback()  # Rollback the transaction on error
            print(f"Error executing SQL command:\n{sql_command}\nError: {e}")



### Export temp tables
# create temp tables
articles_df.to_sql('temp_articles', engine, if_exists='replace', index=False)

authors_df.to_sql('temp_authors', engine, if_exists='replace', index=False)
doi_author_hash_df.to_sql('temp_doi_author_hash', engine, if_exists='replace', index=False)

categories_df.to_sql('temp_categories', engine, if_exists='replace', index=False)
doi_category_df.to_sql('temp_doi_category', engine, if_exists='replace', index=False)

types_df.to_sql('temp_types', engine, if_exists='replace', index=False)
publishers_df.to_sql('temp_publishers', engine, if_exists='replace', index=False)
journals_df.to_sql('temp_journals', engine, if_exists='replace', index=False)
published_df.to_sql('temp_published', engine, if_exists='replace', index=False)



### SQL-d to transfer data
# SQL statements to be executed
sql_statements = [
    """
    INSERT INTO types_dim (type)
        SELECT DISTINCT type FROM temp_types
        ON CONFLICT (type) DO NOTHING;
    """,
    """
    INSERT INTO publishers_dim (publisher)
        SELECT DISTINCT publisher FROM temp_publishers
        ON CONFLICT (publisher) DO NOTHING;
    """,
    """
    INSERT INTO journals_dim (issn, container_title, short_container_title, volume, issue)
        SELECT "ISSN", container_title, short_container_title, volume, issue FROM temp_journals
        ON CONFLICT (issn) DO NOTHING;
    """,
    """
    INSERT INTO published_online_dim (published_date)
        SELECT DISTINCT published_date FROM temp_published
        ON CONFLICT (published_date) DO NOTHING;
    """,
    """    
    INSERT INTO authors_dim (author_hash, author)
        SELECT DISTINCT author_hash, author FROM temp_authors
        ON CONFLICT (author_hash) DO NOTHING;  
    """,
    """
    INSERT INTO categories_dim (category)
        SELECT DISTINCT category FROM temp_categories
        ON CONFLICT (category) DO NOTHING;   
    """,
    """
    INSERT INTO articles_fact (doi, title, is_referenced_by_count, references_count)
        SELECT doi, title, is_referenced_by_count, references_count 
        FROM temp_articles
        ON CONFLICT (doi) DO NOTHING;
    """,
    """
    UPDATE articles_fact af
        SET type_id = mapping.type_id
        FROM ( 
        SELECT tt.doi, td.id as type_id
        FROM temp_types tt
        JOIN types_dim td ON tt.type = td.type
    ) as mapping
    WHERE af.doi = mapping.doi;
    """,
    """
    UPDATE articles_fact af
    SET publisher_id = mapping.publisher_id
    FROM ( 
        SELECT tp.doi, pd.id as publisher_id
        FROM temp_publishers tp
        JOIN publishers_dim pd ON tp.publisher = pd.publisher
    ) as mapping
    WHERE af.doi = mapping.doi;
    """,
    """
    UPDATE articles_fact af
        SET journal_id = mapping.journal_id
        FROM ( 
        SELECT tj.doi, jd.id as journal_id
        FROM temp_journals tj
        JOIN journals_dim jd ON tj."ISSN" = jd.issn
    ) as mapping
    WHERE af.doi = mapping.doi;
    """,
    """
    UPDATE articles_fact af
        SET published_online_id = mapping.published_online_id
        FROM ( 
        SELECT tp.doi, pod.id as published_online_id
        FROM temp_published tp
    JOIN published_online_dim pod ON tp.published_date = pod.published_date
    ) as mapping
    WHERE af.doi = mapping.doi;
    """,
    """
    INSERT INTO article_authors_bridge (article_id, author_id)
	    SELECT DISTINCT af.id AS article_id, ad.id AS author_id
	    FROM temp_doi_author_hash tdah 
	    LEFT JOIN articles_fact af ON tdah.doi = af.doi
	    LEFT JOIN authors_dim ad ON tdah.author_hash = ad.author_hash
	    WHERE af.id IS NOT NULL AND ad.id IS NOT NULL
        ON CONFLICT (article_id, author_id) DO NOTHING;
    """,
    """
    INSERT INTO article_category_bridge (article_id, category_id)
        SELECT DISTINCT af.id AS article_id, cd.id AS category_id
        FROM temp_doi_category tac
        LEFT JOIN articles_fact af ON tac.doi = af.doi
        LEFT JOIN categories_dim cd ON tac.category = cd.category
        WHERE af.id IS NOT NULL AND cd.id IS NOT NULL
        ON CONFLICT (article_id, category_id) DO NOTHING;
"""
]

# Execute each SQL statement
with engine.connect() as connection:
    for sql in sql_statements:
        trans = connection.begin()  # Start a new transaction
        try:
            result = connection.execute(text(sql))
            trans.commit()  # Commit the transaction
            print(f"Executed SQL command:\n{sql}")
            print(f"Number of rows affected: {result.rowcount}")
        except Exception as e:
            trans.rollback()  # Rollback the transaction on error
            print(f"Error executing SQL command:\n{sql}\nError: {e}")



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


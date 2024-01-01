CREATE TABLE IF NOT EXISTS types_dim (
        id SERIAL PRIMARY KEY,
        type TEXT UNIQUE
    );

CREATE TABLE IF NOT EXISTS publishers_dim (
        id SERIAL PRIMARY KEY,
        publisher TEXT UNIQUE
    );

CREATE TABLE IF NOT EXISTS journals_dim (
        id SERIAL PRIMARY KEY,
        issn TEXT UNIQUE,
        container_title TEXT,
        short_container_title TEXT,
        volume TEXT,
        issue TEXT
    );

CREATE TABLE IF NOT EXISTS published_online_dim (
        id SERIAL PRIMARY KEY,
        published_date DATE UNIQUE
    );

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

CREATE TABLE IF NOT EXISTS authors_dim (
        id SERIAL PRIMARY KEY,
        author_hash TEXT UNIQUE,
        author TEXT
);

CREATE TABLE IF NOT EXISTS article_authors_bridge (
        article_id INT,
        author_id INT,
        FOREIGN KEY (article_id) REFERENCES articles_fact (id),
        FOREIGN KEY (author_id) REFERENCES authors_dim (id),
        UNIQUE (article_id, author_id)
);

CREATE TABLE IF NOT EXISTS categories_dim (
        id SERIAL PRIMARY KEY,
        category TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS article_category_bridge (
        article_id INT,
        category_id INT,
        FOREIGN KEY (article_id) REFERENCES articles_fact (id),
        FOREIGN KEY (category_id) REFERENCES categories_dim (id),
        UNIQUE (article_id, category_id)
);


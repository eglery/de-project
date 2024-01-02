DROP TABLE IF EXISTS temp_articles;
DROP TABLE IF EXISTS temp_authors;
DROP TABLE IF EXISTS temp_doi_author_hash;
DROP TABLE IF EXISTS temp_categories;
DROP TABLE IF EXISTS temp_doi_category;
DROP TABLE IF EXISTS temp_types;
DROP TABLE IF EXISTS temp_publishers;
DROP TABLE IF EXISTS temp_journals;
DROP TABLE IF EXISTS temp_published;

CREATE TEMP TABLE temp_articles (
    doi TEXT,
    title TEXT,
    is_referenced_by_count INT,
    references_count INT
);

CREATE TEMP TABLE temp_authors (
    author_hash TEXT,
    author TEXT
);

CREATE TEMP TABLE temp_doi_author_hash (
    doi TEXT,
    author_hash INT
);

CREATE TEMP TABLE temp_category (
    category TEXT
);

CREATE TEMP TABLE temp_doi_category (
    doi TEXT,
    category TEXT
);

CREATE TEMP TABLE temp_types (
    doi TEXT,
    type TEXT
);

CREATE TEMP TABLE temp_publishers (
    doi TEXT,
    publisher TEXT
);

CREATE TEMP TABLE temp_journals (
    doi TEXT,
    "ISSN" TEXT,
    container_title TEXT,
    short_container_title TEXT,
    volume TEXT,
    issue TEXT
);

CREATE TEMP TABLE temp_published (
    doi TEXT,
    published_online TEXT,
    published_date TEXT
);
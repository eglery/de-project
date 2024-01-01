INSERT INTO types_dim (type)
SELECT DISTINCT type 
FROM temp_types
ON CONFLICT (type) DO NOTHING;

INSERT INTO publishers_dim (publisher)
SELECT DISTINCT publisher 
FROM temp_publishers
ON CONFLICT (publisher) DO NOTHING;

INSERT INTO journals_dim (issn, container_title, short_container_title, volume, issue)
SELECT "ISSN", container_title, short_container_title, volume, issue 
FROM temp_journals
ON CONFLICT (issn) DO NOTHING;

INSERT INTO published_online_dim (published_date)
SELECT DISTINCT published_date 
FROM temp_published
ON CONFLICT (published_date) DO NOTHING;

INSERT INTO authors_dim (author_hash, author)
SELECT DISTINCT author_hash, author 
FROM temp_authors
ON CONFLICT (author_hash) DO NOTHING;  

INSERT INTO categories_dim (category)
SELECT DISTINCT category 
FROM temp_categories
ON CONFLICT (category) DO NOTHING;   

INSERT INTO articles_fact (doi, title, is_referenced_by_count, references_count)
SELECT doi, title, is_referenced_by_count, references_count 
FROM temp_articles
ON CONFLICT (doi) DO NOTHING;

UPDATE articles_fact af
SET type_id = mapping.type_id
FROM ( 
    SELECT tt.doi, td.id as type_id
    FROM temp_types tt
    JOIN types_dim td ON tt.type = td.type
) as mapping
WHERE af.doi = mapping.doi;

UPDATE articles_fact af
SET publisher_id = mapping.publisher_id
FROM ( 
    SELECT tp.doi, pd.id as publisher_id
    FROM temp_publishers tp
    JOIN publishers_dim pd ON tp.publisher = pd.publisher
) as mapping
WHERE af.doi = mapping.doi;

UPDATE articles_fact af
SET journal_id = mapping.journal_id
FROM ( 
    SELECT tj.doi, jd.id as journal_id
    FROM temp_journals tj
    JOIN journals_dim jd ON tj."ISSN" = jd.issn
) as mapping
WHERE af.doi = mapping.doi;

UPDATE articles_fact af
SET published_online_id = mapping.published_online_id
FROM ( 
    SELECT tp.doi, pod.id as published_online_id
    FROM temp_published tp
    JOIN published_online_dim pod ON tp.published_date = pod.published_date
) as mapping
WHERE af.doi = mapping.doi;

INSERT INTO article_authors_bridge (article_id, author_id)
SELECT DISTINCT af.id AS article_id, ad.id AS author_id
FROM temp_doi_author_hash tdah 
LEFT JOIN articles_fact af ON tdah.doi = af.doi
LEFT JOIN authors_dim ad ON tdah.author_hash = ad.author_hash
WHERE af.id IS NOT NULL AND ad.id IS NOT NULL
ON CONFLICT (article_id, author_id) DO NOTHING;

INSERT INTO article_category_bridge (article_id, category_id)
SELECT DISTINCT af.id AS article_id, cd.id AS category_id
FROM temp_doi_category tac
LEFT JOIN articles_fact af ON tac.doi = af.doi
LEFT JOIN categories_dim cd ON tac.category = cd.category
WHERE af.id IS NOT NULL AND cd.id IS NOT NULL
ON CONFLICT (article_id, category_id) DO NOTHING;
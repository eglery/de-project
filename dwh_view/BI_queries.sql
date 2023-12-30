CREATE MATERIALIZED VIEW author_h_index AS
SELECT
    ad.id AS author_id,
    ad.author,
    MAX(sub.rn) AS h_index
FROM
    authors_dim ad
JOIN
    (SELECT
         aab.author_id,
         af.is_referenced_by_count,
         ROW_NUMBER() OVER (PARTITION BY aab.author_id ORDER BY af.is_referenced_by_count DESC) AS rn
     FROM
         article_authors_bridge aab
     INNER JOIN articles_fact af ON aab.article_id = af.id) AS sub
ON ad.id = sub.author_id
WHERE
    sub.is_referenced_by_count >= sub.rn
GROUP BY ad.id, ad.author;

REFRESH MATERIALIZED VIEW author_h_index;

CREATE MATERIALIZED VIEW journal_h_index AS
SELECT
    jd.id AS journal_id,
    jd.container_title,
    MAX(sub.rn) AS h_index
FROM
    journals_dim jd
JOIN
    (SELECT
         af.journal_id,
         af.is_referenced_by_count,
         ROW_NUMBER() OVER (PARTITION BY af.journal_id ORDER BY af.is_referenced_by_count DESC) AS rn
     FROM
         articles_fact af) AS sub
ON jd.id = sub.journal_id
WHERE
    sub.is_referenced_by_count >= sub.rn
GROUP BY jd.id, jd.container_title;

REFRESH MATERIALIZED VIEW journal_h_index;

CREATE MATERIALIZED VIEW category_h_index AS
SELECT
    cd.id AS category_id,
    cd.category,
    MAX(sub.rn) AS h_index
FROM
    categories_dim cd
JOIN
    (SELECT
         acb.category_id,
         af.is_referenced_by_count,
         ROW_NUMBER() OVER (PARTITION BY acb.category_id ORDER BY af.is_referenced_by_count DESC) AS rn
     FROM
         article_category_bridge acb
     INNER JOIN articles_fact af ON acb.article_id = af.id) AS sub
ON cd.id = sub.category_id
WHERE
    sub.is_referenced_by_count >= sub.rn
GROUP BY cd.id, cd.category;

REFRESH MATERIALIZED VIEW category_h_index;





-- publisher_yearly_rank
CREATE MATERIALIZED VIEW publisher_yearly_rank AS
WITH ranked_publishers AS (
    SELECT
        EXTRACT(YEAR FROM pod.published_date) AS publishing_year,
        pd.publisher,
        COUNT(af.id) AS articles_published,
        RANK() OVER (
            PARTITION BY EXTRACT(YEAR FROM pod.published_date)
            ORDER BY COUNT(af.id) DESC
        ) as rank
    FROM
        articles_fact af
    INNER JOIN publishers_dim pd ON af.publisher_id = pd.id
    INNER JOIN published_online_dim pod ON af.published_online_id = pod.id
    GROUP BY
        pd.publisher,
        EXTRACT(YEAR FROM pod.published_date)
),
ranked_with_previous_next AS (
    SELECT
        publishing_year,
        publisher,
        articles_published,
        rank,
        LAG(rank) OVER (PARTITION BY publisher ORDER BY publishing_year) AS previous_year_rank,
        LEAD(rank) OVER (PARTITION BY publisher ORDER BY publishing_year) AS next_year_rank
    FROM
        ranked_publishers
)
SELECT
    publishing_year,
    publisher,
    articles_published,
    rank,
    previous_year_rank,
    next_year_rank
FROM
    ranked_with_previous_next;


SELECT
    td.type,
    AVG(af.is_referenced_by_count - af.references_count) AS avg_citation_difference
FROM
    articles_fact af
INNER JOIN types_dim td ON af.type_id = td.id
GROUP BY
    td.type
ORDER BY
    avg_citation_difference DESC;

   
DROP MATERIALIZED VIEW IF EXISTS author_diversity_rank;
CREATE MATERIALIZED VIEW IF NOT EXISTS author_diversity_rank AS
WITH author_category_rank AS (
    SELECT
        ad.author,
        COUNT(DISTINCT acb.category_id) AS category_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT acb.category_id) DESC) AS category_rank
    FROM
        authors_dim ad
    JOIN article_authors_bridge aab ON ad.id = aab.author_id
    JOIN article_category_bridge acb ON aab.article_id = acb.article_id
    GROUP BY ad.author
),
author_publisher_rank AS (
    SELECT
        ad.author,
        COUNT(DISTINCT af.publisher_id) AS publisher_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT af.publisher_id) DESC) AS publisher_rank
    FROM
        authors_dim ad
    JOIN article_authors_bridge aab ON ad.id = aab.author_id
    JOIN articles_fact af ON aab.article_id = af.id
    GROUP BY ad.author
),
author_type_rank AS (
    SELECT
        ad.author,
        COUNT(DISTINCT af.type_id) AS type_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT af.type_id) DESC) AS type_rank
    FROM
        authors_dim ad
    JOIN article_authors_bridge aab ON ad.id = aab.author_id
    JOIN articles_fact af ON aab.article_id = af.id
    GROUP BY ad.author
),
combined_rank AS (
    SELECT
        acr.author,
        acr.category_rank,
        apr.publisher_rank,
        atr.type_rank,
        ROUND((acr.category_rank + apr.publisher_rank + atr.type_rank) / 3.0, 0) AS average_rank
    FROM
        author_category_rank acr
    JOIN author_publisher_rank apr ON acr.author = apr.author
    JOIN author_type_rank atr ON acr.author = atr.author
)
SELECT
    author,
    category_rank,
    publisher_rank,
    type_rank,
    average_rank
FROM
    combined_rank
ORDER BY
    average_rank;
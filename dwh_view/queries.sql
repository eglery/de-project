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


--SELECT * FROM author_h_index LIMIT 10;

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

--SELECT * FROM journal_h_index LIMIT 10;


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
SELECT
    relname AS "Table",
    pg_size_pretty(pg_total_relation_size(relid)) AS "Size"
FROM
    pg_catalog.pg_statio_user_tables
ORDER BY
    pg_total_relation_size(relid) DESC;
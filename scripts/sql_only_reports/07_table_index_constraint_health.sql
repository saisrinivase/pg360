/* Topics: Table and Index Health, Constraint Fk Health,
   Index Bloat Usage Patterns, Sequence Health,
   Materialized Foreign Unlogged Inventory */

SELECT 'Table and Index Health' AS report_section;

SELECT
    s.schemaname,
    s.relname,
    s.seq_scan,
    s.idx_scan,
    s.n_live_tup,
    s.n_dead_tup,
    s.vacuum_count,
    s.autovacuum_count,
    s.analyze_count,
    s.autoanalyze_count
FROM pg_stat_user_tables s
ORDER BY s.n_live_tup DESC
LIMIT 300;

SELECT 'Constraint Fk Health' AS report_section;

WITH fk AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        con.conname AS fk_name,
        con.conrelid,
        con.conkey,
        array_to_string(
            ARRAY(
                SELECT a.attname
                FROM unnest(con.conkey) WITH ORDINALITY k(attnum, ord)
                JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                ORDER BY k.ord
            ), ', '
        ) AS fk_columns
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE con.contype = 'f'
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
)
SELECT
    schema_name,
    table_name,
    fk_name,
    fk_columns,
    CASE WHEN EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = fk.conrelid
          AND i.indisvalid
          AND i.indisready
          AND i.indnatts >= cardinality(fk.conkey)
          AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
    ) THEN 'YES' ELSE 'NO' END AS index_exists
FROM fk
ORDER BY schema_name, table_name, fk_name;

SELECT 'Index Bloat Usage Patterns' AS report_section;

SELECT
    s.schemaname,
    s.relname,
    s.indexrelname,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    CASE
        WHEN s.idx_scan = 0 AND pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE'
        WHEN pg_relation_size(s.indexrelid) >= 21474836480 THEN 'REINDEX_REVIEW'
        ELSE 'MONITOR'
    END AS action_hint
FROM pg_stat_user_indexes s
ORDER BY pg_relation_size(s.indexrelid) DESC
LIMIT 500;

SELECT 'Sequence Health' AS report_section;

SELECT
    schemaname,
    sequencename,
    data_type,
    start_value,
    min_value,
    max_value,
    increment_by,
    cycle
FROM pg_sequences
WHERE schemaname !~ '^pg_'
  AND schemaname <> 'information_schema'
ORDER BY schemaname, sequencename;

SELECT 'Materialized Foreign Unlogged Inventory' AS report_section;

SELECT
    n.nspname AS schema_name,
    c.relname AS object_name,
    c.relkind,
    c.relpersistence,
    CASE c.relkind WHEN 'm' THEN 'MATERIALIZED_VIEW' WHEN 'f' THEN 'FOREIGN_TABLE' ELSE 'OTHER' END AS object_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
  AND (c.relkind IN ('m','f') OR c.relpersistence = 'u')
ORDER BY schema_name, object_type, object_name;

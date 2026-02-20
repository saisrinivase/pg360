/* Topics: Bloat and XID Risk, Xid Multixact Wraparound Watch,
   Partitioning Inventory, Partition Maintenance Risk,
   Toast Large Values */

SELECT 'Bloat and XID Risk' AS report_section;

SELECT
    s.schemaname,
    s.relname,
    s.n_live_tup,
    s.n_dead_tup,
    round((s.n_dead_tup::numeric * 100.0) / nullif((s.n_live_tup + s.n_dead_tup)::numeric, 0), 2) AS dead_pct,
    age(relfrozenxid) AS relfrozenxid_age
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
ORDER BY dead_pct DESC NULLS LAST, relfrozenxid_age DESC
LIMIT 300;

SELECT 'Xid Multixact Wraparound Watch' AS report_section;

SELECT
    datname,
    age(datfrozenxid) AS datfrozenxid_age,
    mxid_age(datminmxid) AS datminmxid_age
FROM pg_database
ORDER BY datfrozenxid_age DESC, datminmxid_age DESC;

SELECT 'Partitioning Inventory' AS report_section;

SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE p.partstrat WHEN 'r' THEN 'RANGE' WHEN 'l' THEN 'LIST' WHEN 'h' THEN 'HASH' ELSE 'UNKNOWN' END AS partition_strategy,
    (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
FROM pg_partitioned_table p
JOIN pg_class c ON c.oid = p.partrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY schema_name, table_name;

SELECT 'Partition Maintenance Risk' AS report_section;

WITH pt AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.oid AS relid,
        (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
    FROM pg_partitioned_table p
    JOIN pg_class c ON c.oid = p.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
)
SELECT
    schema_name,
    table_name,
    partition_count,
    CASE WHEN partition_count > 1000 THEN 'HIGH' WHEN partition_count > 200 THEN 'MEDIUM' ELSE 'LOW' END AS maintenance_risk,
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = relid AND i.indisvalid AND i.indisready) THEN 'NO_PARENT_INDEX'
        ELSE 'OK'
    END AS index_maintenance_state
FROM pt
ORDER BY partition_count DESC, schema_name, table_name;

SELECT 'Toast Large Values' AS report_section;

SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    t.relname AS toast_table,
    pg_size_pretty(pg_total_relation_size(t.oid)) AS toast_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class t ON t.oid = c.reltoastrelid
WHERE c.reltoastrelid <> 0
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY pg_total_relation_size(t.oid) DESC
LIMIT 300;

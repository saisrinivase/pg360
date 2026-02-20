/* Topics: Database and IO Health, IO Profile (pg_stat_io),
   Tablespace Storage Layout, Top Objects By Size,
   Table Growth Churn Hotspots, Database Growth Objects 90D */

SELECT 'Database and IO Health' AS report_section;

SELECT
    datname,
    numbackends,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    temp_files,
    temp_bytes,
    deadlocks
FROM pg_stat_database
WHERE datname = current_database();

SELECT 'IO Profile (Cross-Version Fallback)' AS report_section;

SELECT
    schemaname,
    relname,
    (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read) AS blocks_read,
    (heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit) AS blocks_hit,
    CASE
        WHEN (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read +
              heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit) > 0
        THEN round(
            (heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit)::numeric * 100.0 /
            (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read +
             heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit)::numeric, 2
        )
        ELSE NULL
    END AS cache_hit_pct
FROM pg_statio_user_tables
ORDER BY blocks_read DESC NULLS LAST
LIMIT 200;

SELECT
    current_setting('server_version_num')::int AS server_version_num,
    CASE WHEN to_regclass('pg_stat_io') IS NULL THEN 'NO' ELSE 'YES' END AS pg_stat_io_available,
    CASE
        WHEN to_regclass('pg_stat_io') IS NULL THEN 'Using pg_statio_user_tables fallback'
        ELSE 'pg_stat_io available for deeper object/context I/O split'
    END AS note;

SELECT 'Tablespace Storage Layout' AS report_section;

SELECT
    spcname AS tablespace_name,
    pg_size_pretty(pg_tablespace_size(oid)) AS size,
    pg_tablespace_location(oid) AS location
FROM pg_tablespace
ORDER BY pg_tablespace_size(oid) DESC;

SELECT 'Top Objects By Size' AS report_section;

SELECT
    n.nspname AS schema_name,
    c.relname AS object_name,
    c.relkind,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
  AND c.relkind IN ('r','p','m','i')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 300;

SELECT 'Table Growth Churn Hotspots' AS report_section;

SELECT
    schemaname,
    relname,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_mod_since_analyze,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
LIMIT 300;

SELECT 'Database Growth Objects 90D' AS report_section;

SELECT
    'Snapshot history not available in core catalogs by default. Use periodic size snapshots (daily) for true 90D growth trend.'::text AS note,
    current_database() AS database_name,
    now() AS generated_at;

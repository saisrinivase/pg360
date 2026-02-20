/* Topics: Instance Configuration, Diagnostic Readiness,
   Autovacuum Worker Thresholds, Settings Antipatterns,
   Plan Cache Prepared And Advisory Locks */

SELECT 'Instance Configuration' AS report_section;

SELECT
    name,
    setting,
    unit,
    source,
    pending_restart
FROM pg_settings
WHERE name IN (
    'max_connections',
    'shared_buffers',
    'work_mem',
    'maintenance_work_mem',
    'effective_cache_size',
    'autovacuum',
    'max_wal_size',
    'checkpoint_timeout',
    'checkpoint_completion_target',
    'wal_level',
    'archive_mode',
    'track_io_timing'
)
ORDER BY name;

SELECT 'Diagnostic Readiness' AS report_section;

SELECT
    'pg_stat_statements'::text AS check_name,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'PASS' ELSE 'WARN' END AS status,
    'Install extension for SQL hotspot and regression diagnostics'::text AS recommendation
UNION ALL
SELECT
    'track_io_timing',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on','true','1') THEN 'PASS' ELSE 'WARN' END,
    'Enable track_io_timing for precise I/O attribution';

SELECT 'Autovacuum Worker Thresholds' AS report_section;

SELECT
    name,
    setting,
    unit
FROM pg_settings
WHERE name IN (
    'autovacuum',
    'autovacuum_max_workers',
    'autovacuum_naptime',
    'autovacuum_vacuum_threshold',
    'autovacuum_vacuum_scale_factor',
    'autovacuum_analyze_threshold',
    'autovacuum_analyze_scale_factor'
)
ORDER BY name;

SELECT 'Settings Antipatterns' AS report_section;

SELECT
    'autovacuum_off'::text AS antipattern,
    CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') IN ('on','true','1') THEN 'NO' ELSE 'YES' END AS present,
    'Must be OFF only in exceptional controlled scenarios'::text AS impact
UNION ALL
SELECT
    'low_max_wal_size',
    CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) < 4294967296 THEN 'YES' ELSE 'NO' END,
    'May increase checkpoint pressure on write-heavy systems'
UNION ALL
SELECT
    'low_checkpoint_timeout',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') < 600 THEN 'YES' ELSE 'NO' END,
    'Frequent checkpoints can increase I/O spikes';

SELECT 'Plan Cache Prepared And Advisory Locks' AS report_section;

SELECT
    (SELECT count(*)::bigint FROM pg_prepared_statements) AS prepared_statement_count,
    (SELECT count(*)::bigint FROM pg_locks WHERE locktype = 'advisory') AS advisory_lock_count;

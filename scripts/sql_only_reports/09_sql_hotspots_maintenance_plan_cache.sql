/* Topics: SQL Hotspots (pg_stat_statements), Maintenance Progress,
   Statistics Quality Analyze Health, Plan Cache Prepared And Advisory Locks */

SELECT 'SQL Hotspots (pg_stat_statements)' AS report_section;

SELECT
    current_setting('server_version_num')::int AS server_version_num,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'INSTALLED' ELSE 'NOT_INSTALLED' END AS pg_stat_statements_status,
    CASE
        WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
            THEN 'Extension detected. Run extension-specific hotspot query in this environment if needed.'
        ELSE 'Install pg_stat_statements for historical SQL hotspot analysis.'
    END AS note;

SELECT
    pid,
    usename,
    application_name,
    state,
    now() - query_start AS runtime,
    wait_event_type,
    wait_event,
    left(query, 300) AS query_snippet
FROM pg_stat_activity
WHERE state <> 'idle'
  AND query_start IS NOT NULL
ORDER BY runtime DESC
LIMIT 200;

SELECT 'Maintenance Progress' AS report_section;

SELECT
    pid,
    datname,
    relid::regclass AS relation,
    phase
FROM pg_stat_progress_vacuum
ORDER BY pid;

SELECT 'Statistics Quality Analyze Health' AS report_section;

SELECT
    schemaname,
    relname,
    n_live_tup,
    n_mod_since_analyze,
    last_analyze,
    last_autoanalyze,
    CASE
        WHEN n_live_tup >= 100000 AND n_mod_since_analyze >= 50000 AND (last_analyze IS NULL OR last_analyze < now() - interval '1 day') THEN 'STALE'
        ELSE 'OK'
    END AS analyze_health
FROM pg_stat_user_tables
ORDER BY n_mod_since_analyze DESC
LIMIT 400;

SELECT 'Plan Cache Prepared And Advisory Locks' AS report_section;

SELECT
    (SELECT count(*)::bigint FROM pg_prepared_statements) AS prepared_statement_count,
    (SELECT count(*)::bigint FROM pg_locks WHERE locktype = 'advisory') AS advisory_lock_count,
    (SELECT count(*)::bigint FROM pg_locks WHERE locktype = 'advisory' AND granted = false) AS advisory_lock_waiters;

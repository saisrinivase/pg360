-- =============================================================================
-- PG360 Repository Capture
-- PURPOSE: Capture one optional history snapshot for PG360 trend/diff reporting
-- SAFETY : This script writes into pg360_history. It is not read-only.
-- =============================================================================

\set ON_ERROR_STOP 1
\if :{?pg360_repo_label}
\else
\set pg360_repo_label nightly_capture
\endif
\if :{?pg360_repo_topn}
\else
\set pg360_repo_topn 100
\endif

BEGIN;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;

INSERT INTO pg360_history.run_snapshot (
  dbname,
  run_label,
  source,
  server_version,
  notes
)
SELECT
  current_database(),
  :'pg360_repo_label',
  'pg360_repo_capture',
  version(),
  'Top SQL rows captured=' || :'pg360_repo_topn'
RETURNING run_id, captured_at, dbname
\gset pg360_repo_

WITH dbs AS (
  SELECT *
  FROM pg_stat_database
  WHERE datname = current_database()
), activity AS (
  SELECT
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid())::int AS sessions_total,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state = 'active')::int AS sessions_active,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state = 'idle in transaction')::int AS sessions_idle_tx,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND wait_event IS NOT NULL)::int AS waiters,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock')::int AS lock_waiters
  FROM pg_stat_activity
  WHERE datname = current_database()
), wal AS (
  SELECT CASE WHEN to_regclass('pg_stat_wal') IS NOT NULL THEN (SELECT wal_bytes::numeric FROM pg_stat_wal LIMIT 1) ELSE NULL::numeric END AS wal_bytes
), calc AS (
  SELECT
    d.stats_reset,
    d.xact_commit,
    d.xact_rollback,
    d.blks_read,
    d.blks_hit,
    d.tup_returned,
    d.tup_fetched,
    d.tup_inserted,
    d.tup_updated,
    d.tup_deleted,
    d.temp_files,
    d.temp_bytes,
    d.deadlocks,
    a.sessions_total,
    a.sessions_active,
    a.sessions_idle_tx,
    a.waiters,
    a.lock_waiters,
    pg_database_size(current_database()) AS db_size_bytes,
    w.wal_bytes,
    COALESCE(d.blks_hit::numeric / NULLIF(d.blks_hit + d.blks_read, 0), 0) AS cache_hit_ratio,
    COALESCE(d.tup_fetched::numeric / NULLIF(d.tup_inserted + d.tup_updated + d.tup_deleted, 0), 0) AS read_write_ratio,
    COALESCE(round((d.xact_commit + d.xact_rollback)::numeric / NULLIF(EXTRACT(epoch FROM (now() - d.stats_reset)), 0), 2), NULL) AS tps_estimate
  FROM dbs d, activity a, wal w
)
INSERT INTO pg360_history.db_snapshot (
  run_id,
  dbname,
  stats_reset,
  xact_commit,
  xact_rollback,
  blks_read,
  blks_hit,
  tup_returned,
  tup_fetched,
  tup_inserted,
  tup_updated,
  tup_deleted,
  temp_files,
  temp_bytes,
  deadlocks,
  sessions_total,
  sessions_active,
  sessions_idle_tx,
  waiters,
  lock_waiters,
  db_size_bytes,
  wal_bytes,
  cache_hit_ratio,
  read_write_ratio,
  tps_estimate
)
SELECT
  :pg360_repo_run_id,
  current_database(),
  stats_reset,
  xact_commit,
  xact_rollback,
  blks_read,
  blks_hit,
  tup_returned,
  tup_fetched,
  tup_inserted,
  tup_updated,
  tup_deleted,
  temp_files,
  temp_bytes,
  deadlocks,
  sessions_total,
  sessions_active,
  sessions_idle_tx,
  waiters,
  lock_waiters,
  db_size_bytes,
  wal_bytes,
  cache_hit_ratio,
  read_write_ratio,
  tps_estimate
FROM calc;

SELECT
  CASE
    WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
    THEN 'on' ELSE 'off'
  END AS pg360_repo_has_pgss
\gset

\if :pg360_repo_has_pgss
INSERT INTO pg360_history.sql_snapshot (
  run_id,
  captured_at,
  dbname,
  fingerprint,
  queryid_text,
  userid,
  rolname,
  calls,
  total_exec_time,
  mean_exec_time,
  stddev_exec_time,
  rows,
  shared_blks_hit,
  shared_blks_read,
  temp_blks_written,
  wal_bytes,
  query_text
)
SELECT
  :pg360_repo_run_id,
  :'pg360_repo_captured_at'::timestamptz,
  current_database(),
  s.fingerprint,
  s.queryid_text,
  s.userid,
  s.rolname,
  s.calls,
  s.total_exec_time,
  s.mean_exec_time,
  s.stddev_exec_time,
  s.rows,
  s.shared_blks_hit,
  s.shared_blks_read,
  s.temp_blks_written,
  NULL::numeric,
  s.query_text
FROM (
  SELECT DISTINCT ON (fingerprint)
    md5(ps.query || '|' || ps.userid::text || '|' || ps.dbid::text) AS fingerprint,
    COALESCE(ps.queryid::text, md5(ps.query)) AS queryid_text,
    ps.userid,
    r.rolname,
    ps.calls,
    ps.total_exec_time,
    ps.mean_exec_time,
    ps.stddev_exec_time,
    ps.rows,
    ps.shared_blks_hit,
    ps.shared_blks_read,
    ps.temp_blks_written,
    regexp_replace(ps.query, E'\\s+', ' ', 'g') AS query_text
  FROM pg_stat_statements ps
  LEFT JOIN pg_roles r ON r.oid = ps.userid
  WHERE ps.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND ps.query NOT ILIKE '%pg360%'
    AND ps.query NOT ILIKE '%pg_stat_statements%'
    AND ps.query NOT ILIKE 'BEGIN%'
    AND ps.query NOT ILIKE 'COMMIT%'
    AND ps.query NOT ILIKE 'SET %'
  ORDER BY fingerprint, ps.total_exec_time DESC, ps.calls DESC
) s
ORDER BY s.total_exec_time DESC
LIMIT :pg360_repo_topn;
\endif

COMMIT;

\echo PG360 repository snapshot captured.
\echo run_id=:pg360_repo_run_id captured_at=:pg360_repo_captured_at db=:pg360_repo_dbname

-- =============================================================================
-- PG360 Repository Setup
-- PURPOSE: Optional history repository for PG360 trend, baseline, and diff views
-- SAFETY : This script creates schema objects and writes metadata tables.
-- =============================================================================

\set ON_ERROR_STOP 1

BEGIN;

CREATE SCHEMA IF NOT EXISTS pg360_history;

CREATE TABLE IF NOT EXISTS pg360_history.run_snapshot (
  run_id bigserial PRIMARY KEY,
  captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  dbname text NOT NULL,
  run_label text,
  source text NOT NULL DEFAULT 'pg360_repo_capture',
  server_version text,
  notes text
);

CREATE TABLE IF NOT EXISTS pg360_history.db_snapshot (
  run_id bigint PRIMARY KEY REFERENCES pg360_history.run_snapshot(run_id) ON DELETE CASCADE,
  dbname text NOT NULL,
  stats_reset timestamptz,
  xact_commit bigint,
  xact_rollback bigint,
  blks_read bigint,
  blks_hit bigint,
  tup_returned bigint,
  tup_fetched bigint,
  tup_inserted bigint,
  tup_updated bigint,
  tup_deleted bigint,
  temp_files bigint,
  temp_bytes bigint,
  deadlocks bigint,
  sessions_total integer,
  sessions_active integer,
  sessions_idle_tx integer,
  waiters integer,
  lock_waiters integer,
  db_size_bytes bigint,
  wal_bytes numeric,
  cache_hit_ratio numeric(12,4),
  read_write_ratio numeric(14,4),
  tps_estimate numeric(14,2)
);

CREATE TABLE IF NOT EXISTS pg360_history.sql_snapshot (
  run_id bigint NOT NULL REFERENCES pg360_history.run_snapshot(run_id) ON DELETE CASCADE,
  captured_at timestamptz NOT NULL,
  dbname text NOT NULL,
  fingerprint text NOT NULL,
  queryid_text text,
  userid oid,
  rolname text,
  calls bigint,
  total_exec_time double precision,
  mean_exec_time double precision,
  stddev_exec_time double precision,
  rows bigint,
  shared_blks_hit bigint,
  shared_blks_read bigint,
  temp_blks_written bigint,
  wal_bytes numeric,
  query_text text,
  PRIMARY KEY (run_id, fingerprint)
);

CREATE INDEX IF NOT EXISTS ix_pg360_run_snapshot_db_time
  ON pg360_history.run_snapshot (dbname, captured_at DESC);

CREATE INDEX IF NOT EXISTS ix_pg360_sql_snapshot_db_time
  ON pg360_history.sql_snapshot (dbname, captured_at DESC);

CREATE INDEX IF NOT EXISTS ix_pg360_sql_snapshot_fingerprint
  ON pg360_history.sql_snapshot (dbname, fingerprint, captured_at DESC);

COMMIT;

\echo PG360 repository schema is ready.
\echo Next step: run pg360_repo_capture.sql on a schedule to build history.

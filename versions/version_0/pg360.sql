-- =============================================================================
-- PG360 vNext - PostgreSQL Master Diagnostic Report
-- =============================================================================
-- PURPOSE   : Consultant-grade read-only PostgreSQL 360 assessment engine
-- AUTHOR    : PG360 Project
-- LICENSE   : MIT
-- DESIGN    : Self-contained single SQL report generator (no external CSS/JS dependency)
--
-- SAFETY GUARANTEE:
--   * Every statement in this file is SELECT only
--   * The session is explicitly set to READ ONLY at line 1
--   * No INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, TRUNCATE
--   * No ANALYZE, VACUUM, REINDEX, CLUSTER
--   * No pg_read_file(), pg_ls_dir(), pg_execute_server_program()
--   * No access to pg_shadow or plaintext credentials
--   * Safe on Production, RDS, Aurora, Cloud SQL, Supabase, Neon
--
-- MINIMUM PRIVILEGE:
--   GRANT pg_monitor      TO pg360_user;   -- PG10+
--   GRANT pg_read_all_stats TO pg360_user; -- PG10+
--   -- For buffer cache section:
--   GRANT EXECUTE ON FUNCTION pg_buffercache_pages() TO pg360_user;
--
-- USAGE:
--   Recommended (non-production first):
--   psql -U pg360_user -d mydb -X -A -t \
--     -v pg360_ack_nonprod_first=on \
--     -f pg360.sql
--
--   Optional override when high load is detected (use only with change approval):
--   psql -U pg360_user -d mydb -X -A -t \
--     -v pg360_ack_nonprod_first=on \
--     -v pg360_force_high_load=on \
--     -f pg360.sql
--
--   Default output (single execution):
--     ./reports/latest/index.html (single-file report with in-page links)
--
-- SECTIONS:
--   M01  Executive Summary
--   M02  Platform and Diagnostic Context
--   M03  Instance and Database Profile
--   M04  Monitoring and Observability Readiness
--   M05  Workload Characterization
--   M06  SQL Performance and Plan Risk
--   M07  Concurrency, Waits, and Lock Pressure
--   M08  Memory Efficiency and IO Pressure
--   M09  Table Storage Health
--   M10  Index Strategy and Access Paths
--   M11  Vacuum, Analyze, and Statistics Health
--   M12  Configuration Review
--   M13  Connection Management
--   M14  WAL, Replication, and Recovery Readiness
--   M15  Capacity Planning and Growth
--   M16  Security and Governance
--   M17  Schema Design and Data Quality
--   M18  Prioritized Remediation Plan
--   M19  Appendix and Evidence
-- =============================================================================

-- =============================================================================
-- OUTPUT CLEANUP (no psql command tags/row counters in HTML)
-- =============================================================================
\set QUIET 1
\set ON_ERROR_STOP 1
\pset footer off
\pset tuples_only on
\pset format unaligned
\pset pager off
\if :{?pg360_redact_user}
\else
\set pg360_redact_user on
\endif
\if :{?pg360_redaction_token}
\else
\set pg360_redaction_token pg360_user
\endif
\if :{?pg360_redact_paths}
\else
\set pg360_redact_paths on
\endif
\if :{?pg360_redact_path_prefix}
\else
\set pg360_redact_path_prefix /opt/homebrew/var/postgresql@18
\endif
\if :{?pg360_redacted_path_token}
\else
\set pg360_redacted_path_token '<redacted_path>'
\endif
\if :{?pg360_output_dir}
\else
\set pg360_output_dir ./reports/latest
\endif
\if :{?pg360_full_report_file}
\set pg360_full_report_file_auto off
\else
\set pg360_full_report_file_auto on
\set pg360_full_report_file pg360_pending.html
\endif
\if :{?pg360_history_days}
\else
\set pg360_history_days 14
\endif

-- =============================================================================
-- PRE-FLIGHT SAFETY GATE
-- =============================================================================
-- Hard requirement: acknowledge non-production-first execution policy.
\if :{?pg360_ack_nonprod_first}
\else
\qecho 'ERROR: PG360 safety gate blocked execution.'
\qecho 'Reason: missing required flag -v pg360_ack_nonprod_first=on'
\qecho 'Policy: test in sandbox/non-production first; schedule production run during low-load window.'
\qecho 'Example: psql -X -A -t -v ON_ERROR_STOP=1 -v pg360_ack_nonprod_first=on -d <db> -f pg360.sql'
SELECT 1/0 AS pg360_safety_gate_abort;
\endif

WITH load_metrics AS (
  SELECT
    current_setting('max_connections')::int AS max_connections,
    (
      SELECT count(*)
      FROM pg_stat_activity a
      WHERE a.datname = current_database()
        AND a.pid <> pg_backend_pid()
        AND a.state = 'active'
    )::int AS active_sessions,
    (
      SELECT count(*)
      FROM pg_stat_activity a
      WHERE a.datname = current_database()
        AND a.pid <> pg_backend_pid()
        AND a.wait_event_type = 'Lock'
    )::int AS lock_wait_sessions,
    (
      SELECT COALESCE(max(EXTRACT(EPOCH FROM (now() - a.query_start))), 0)::int
      FROM pg_stat_activity a
      WHERE a.datname = current_database()
        AND a.pid <> pg_backend_pid()
        AND a.state = 'active'
    ) AS max_active_query_sec
)
SELECT
  current_database() AS pg360_target_db,
  current_user AS pg360_run_user,
  max_connections AS pg360_max_connections,
  active_sessions AS pg360_active_sessions,
  lock_wait_sessions AS pg360_lock_wait_sessions,
  max_active_query_sec AS pg360_max_active_query_sec,
  CASE
    WHEN active_sessions > GREATEST(20, (max_connections * 0.50)::int)
      OR lock_wait_sessions > 0
      OR max_active_query_sec > 900
    THEN 'on'
    ELSE 'off'
  END AS pg360_high_load
FROM load_metrics
\gset

WITH run_clock AS (
  SELECT timezone('America/New_York', clock_timestamp()) AS local_ts
)
SELECT
  to_char(local_ts, 'YYYYMMDD_HH24MISS') AS pg360_run_ts,
  to_char(local_ts, 'YYYY-MM-DD HH24:MI:SS') AS pg360_run_ts_human,
  'America/New_York' AS pg360_report_tz,
  'pg360_' || to_char(local_ts, 'YYYY-MM-DD HH24:MI:SS') || '.html' AS pg360_default_report_file
FROM run_clock
\gset

\if :pg360_full_report_file_auto
\set pg360_full_report_file :pg360_default_report_file
\endif

\if :{?pg360_force_high_load}
\qecho 'WARNING: pg360_force_high_load=on supplied. Proceeding despite preflight load risk.'
\qecho 'Preflight metrics: active=:pg360_active_sessions/:pg360_max_connections lock_wait=:pg360_lock_wait_sessions max_active_query_sec=:pg360_max_active_query_sec'
\else
  \if :pg360_high_load
\qecho 'ERROR: PG360 preflight detected elevated workload; execution blocked by default.'
\qecho 'Database=:pg360_target_db active_sessions=:pg360_active_sessions/:pg360_max_connections lock_wait_sessions=:pg360_lock_wait_sessions max_active_query_sec=:pg360_max_active_query_sec'
\qecho 'Action: rerun in a low-load window, or use -v pg360_force_high_load=on with explicit approval.'
SELECT 1/0 AS pg360_high_load_abort;
  \endif
\endif

-- =============================================================================
-- SECURITY BLOCK 1: Force read-only session - CANNOT be overridden by later SQL
-- =============================================================================
BEGIN;
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout   = '60s';
SET LOCAL lock_timeout        = '2s';
SET LOCAL idle_in_transaction_session_timeout = '60s';
SET LOCAL work_mem            = '64MB';
SET LOCAL search_path         = pg_catalog, public;

SELECT
  CASE
    WHEN to_regclass('pg360_history.run_snapshot') IS NOT NULL
     AND to_regclass('pg360_history.db_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_history.run_snapshot', 'SELECT')
     AND has_table_privilege(current_user, 'pg360_history.db_snapshot', 'SELECT')
    THEN 'on' ELSE 'off'
  END AS pg360_has_history_db,
  CASE
    WHEN to_regclass('pg360_history.sql_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_history.sql_snapshot', 'SELECT')
    THEN 'on' ELSE 'off'
  END AS pg360_has_history_sql
\gset

-- =============================================================================
-- SECURITY BLOCK 2: Privilege self-check
-- =============================================================================
DO $$
BEGIN
  IF NOT (
    pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR
    pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER') OR
    (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)
  ) THEN
    RAISE NOTICE
      'WARNING: pg360 recommends pg_monitor role. '
      'Some sections may be incomplete. '
      'Run: GRANT pg_monitor TO %;', current_user;
  END IF;
END $$;

-- =============================================================================
-- SECURITY BLOCK 3: HTML escape helper (used in every query output)
-- All database values rendered into HTML go through this transformation.
-- Prevents XSS from malicious object names, query text, error messages.
-- =============================================================================
-- Pattern used throughout:
--   replace(replace(replace(replace(replace(val,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
-- Abbreviated as: pg360_escape(val) in comments

-- =============================================================================
-- HTML DOCUMENT START
-- =============================================================================
SELECT :'pg360_output_dir' || '/' || :'pg360_full_report_file' AS pg360_full_report_path \gset
\o :pg360_full_report_path
\qecho '<!DOCTYPE html>'
\qecho '<html lang="en">'
\qecho '<head>'
\qecho '<meta charset="UTF-8">'
\qecho '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
\qecho '<meta http-equiv="Content-Security-Policy" content="default-src ''self''; script-src ''self'' ''unsafe-inline''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:;">'
SELECT
  '<title>PG360 Technical Diagnostics Report - ' ||
  replace(replace(replace(replace(replace(current_database(),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</title>';
\qecho '<style>'
\qecho ':root {'
\qecho '  --bg: #ffffff;'
\qecho '  --line: #c4d2de;'
\qecho '  --line-soft: #dde7ef;'
\qecho '  --panel: #ffffff;'
\qecho '  --panel-alt: #fafcff;'
\qecho '  --header: #f4f8fb;'
\qecho '  --ink: #152231;'
\qecho '  --muted: #5c6d7f;'
\qecho '  --text-muted: #5c6d7f;'
\qecho '  --accent-blue: #336699;'
\qecho '  --accent-green: #2b7a54;'
\qecho '  --accent-yellow: #a66a19;'
\qecho '  --accent-red: #bd4456;'
\qecho '  --accent-orange: #b66a24;'
\qecho '  --font-ui: "Helvetica Neue", Arial, Helvetica, sans-serif;'
\qecho '  --space-1: 4px;'
\qecho '  --space-2: 8px;'
\qecho '  --space-3: 10px;'
\qecho '  --radius-sm: 0;'
\qecho '  --font-body: 10pt;'
\qecho '  --font-title: 16px;'
\qecho '  --font-subtitle: 14px;'
\qecho '}'
\qecho ''
\qecho '* {'
\qecho '  box-sizing: border-box;'
\qecho '}'
\qecho ''
\qecho 'html {'
\qecho '  scroll-behavior: smooth;'
\qecho '  -webkit-text-size-adjust: 100%;'
\qecho '  text-size-adjust: 100%;'
\qecho '}'
\qecho ''
\qecho 'body {'
\qecho '  margin: 0;'
\qecho '  background: var(--bg);'
\qecho '  color: var(--ink);'
\qecho '  font-family: var(--font-ui);'
\qecho '  font-size: var(--font-body);'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho 'body.pg360-index-loading .section {'
\qecho '  display: none;'
\qecho '}'
\qecho ''
\qecho 'body.density-compact {'
\qecho '  --space-1: 4px;'
\qecho '  --space-2: 7px;'
\qecho '  --space-3: 9px;'
\qecho '  --font-body: 10.5px;'
\qecho '  --font-title: 14px;'
\qecho '  --font-subtitle: 13px;'
\qecho '}'
\qecho ''
\qecho 'body.density-comfortable {'
\qecho '  --space-1: 6px;'
\qecho '  --space-2: 9px;'
\qecho '  --space-3: 12px;'
\qecho '  --font-body: 11.5px;'
\qecho '  --font-title: 16px;'
\qecho '  --font-subtitle: 14px;'
\qecho '}'
\qecho ''
\qecho 'a {'
\qecho '  color: var(--accent-blue);'
\qecho '  text-decoration: none;'
\qecho '}'
\qecho ''
\qecho 'a:hover {'
\qecho '  text-decoration: underline;'
\qecho '}'
\qecho ''
\qecho 'pre {'
\qecho '  font: 8pt monospace, Monaco, "Courier New", Courier;'
\qecho '  margin: 4px 0 8px;'
\qecho '}'
\qecho ''
\qecho '.container,'
\qecho '.wrap {'
\qecho '  max-width: none;'
\qecho '  margin: 0 auto;'
\qecho '  padding: 4px 10px;'
\qecho '}'
\qecho ''
\qecho '.hero,'
\qecho '.panel,'
\qecho '.report-index,'
\qecho '.table-wrap,'
\qecho '.finding,'
\qecho '.card,'
\qecho '.security-notice,'
\qecho '.oracle-origin-banner,'
\qecho '.risk-cell,'
\qecho '.code-block,'
\qecho '.finding-fix {'
\qecho '  box-shadow: none;'
\qecho '}'
\qecho ''
\qecho '.hero {'
\qecho '  border: 0;'
\qecho '  background: var(--panel);'
\qecho '  margin-bottom: var(--space-2);'
\qecho '  border-radius: var(--radius-sm);'
\qecho '  overflow: hidden;'
\qecho '}'
\qecho ''
\qecho 'h1 {'
\qecho '  margin: 0;'
\qecho '  padding: 0 0 4px;'
\qecho '  border-bottom: 1px solid var(--accent-blue);'
\qecho '  text-align: left;'
\qecho '  font-size: 16pt;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho 'h2 {'
\qecho '  margin: 4pt 0 0;'
\qecho '  font-size: 14pt;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.meta {'
\qecho '  margin: 4px 0 0;'
\qecho '  color: var(--muted);'
\qecho '  font-size: 10pt;'
\qecho '  line-height: 1.4;'
\qecho '}'
\qecho ''
\qecho '.panel {'
\qecho '  background: var(--panel);'
\qecho '  border: 0;'
\qecho '  padding: 0;'
\qecho '}'
\qecho ''
\qecho 'pre {'
\qecho '  margin: 4px 0 10px;'
\qecho '  padding: 2px 0 10px;'
\qecho '  border-bottom: 1px solid var(--accent-blue);'
\qecho '  color: #000000;'
\qecho '  font: 10pt "Courier New", Courier, monospace;'
\qecho '  white-space: pre-wrap;'
\qecho '}'
\qecho ''
\qecho '.topbar,'
\qecho '#topbar {'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  gap: 8px;'
\qecho '}'
\qecho ''
\qecho '.toggle {'
\qecho '  height: 28px;'
\qecho '}'
\qecho ''
\qecho '.sections {'
\qecho '  display: flex;'
\qecho '  flex-wrap: wrap;'
\qecho '  gap: 4px;'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: var(--panel-alt);'
\qecho '  padding: var(--space-1);'
\qecho '  margin-bottom: var(--space-2);'
\qecho '}'
\qecho ''
\qecho '.top-nav {'
\qecho '  position: sticky;'
\qecho '  top: 0;'
\qecho '  z-index: 30;'
\qecho '  border-top: 0;'
\qecho '}'
\qecho ''
\qecho '.sections a {'
\qecho '  display: inline-block;'
\qecho '  font-size: var(--font-body);'
\qecho '  color: var(--accent-blue);'
\qecho '  background: #ffffff;'
\qecho '  border: 1px solid var(--line);'
\qecho '  padding: 2px 7px;'
\qecho '}'
\qecho ''
\qecho '.sections a:hover,'
\qecho '.sections a.active {'
\qecho '  background: #e7f0f7;'
\qecho '  border-color: #8ea8bf;'
\qecho '}'
\qecho ''
\qecho '.nav-actions {'
\qecho '  margin-left: auto;'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  gap: 6px;'
\qecho '}'
\qecho ''
\qecho '.density-toggle,'
\qecho '.theme-toggle {'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: #ffffff;'
\qecho '  color: #334255;'
\qecho '  height: 24px;'
\qecho '  padding: 0 10px;'
\qecho '  font-size: 11px;'
\qecho '  font-weight: 700;'
\qecho '  cursor: pointer;'
\qecho '}'
\qecho ''
\qecho '.density-toggle:hover,'
\qecho '.theme-toggle:hover {'
\qecho '  background: #eef4f9;'
\qecho '}'
\qecho ''
\qecho '.security-notice {'
\qecho '  border: 0;'
\qecho '  background: #ffffff;'
\qecho '  color: #4f5f6e;'
\qecho '  padding: 2px 0 0;'
\qecho '  margin-bottom: var(--space-2);'
\qecho '  border-radius: var(--radius-sm);'
\qecho '  font-size: 8pt;'
\qecho '  line-height: 1.4;'
\qecho '}'
\qecho ''
\qecho '.report-index {'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: #ffffff;'
\qecho '  margin-bottom: var(--space-2);'
\qecho '  border-radius: 0;'
\qecho '  overflow: hidden;'
\qecho '}'
\qecho ''
\qecho '.evidence-catalog-shell {'
\qecho '  margin-top: 8px;'
\qecho '  border-top: 2px solid var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.report-index-head {'
\qecho '  background: #ffffff;'
\qecho '  border-left: 0;'
\qecho '  border-bottom: 1px solid var(--line);'
\qecho '  padding: 4px 8px;'
\qecho '}'
\qecho ''
\qecho '.report-index-title {'
\qecho '  font-size: 12pt;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  letter-spacing: 0.01em;'
\qecho '}'
\qecho ''
\qecho '.report-index-sub {'
\qecho '  font-size: 8pt;'
\qecho '  color: var(--muted);'
\qecho '  margin-top: 2px;'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho '.index-grid {'
\qecho '  display: grid;'
\qecho '  grid-template-columns: repeat(4, minmax(0, 1fr));'
\qecho '  gap: 8px;'
\qecho '  padding: 10px 12px 12px;'
\qecho '}'
\qecho ''
\qecho '.index-card {'
\qecho '  display: block;'
\qecho '  border: 1px solid var(--line-soft);'
\qecho '  border-radius: var(--radius-sm);'
\qecho '  padding: 9px 11px;'
\qecho '  min-height: 0;'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.index-card:hover {'
\qecho '  background: #eff5f0;'
\qecho '}'
\qecho ''
\qecho '.idx-id {'
\qecho '  display: inline-block;'
\qecho '  font-size: 10px;'
\qecho '  font-weight: 700;'
\qecho '  color: #42566e;'
\qecho '  background: #e4edf5;'
\qecho '  border: 1px solid var(--line);'
\qecho '  padding: 1px 6px;'
\qecho '  margin-bottom: 3px;'
\qecho '}'
\qecho ''
\qecho '.idx-title {'
\qecho '  display: block;'
\qecho '  font-size: 12px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--ink);'
\qecho '  line-height: 1.25;'
\qecho '}'
\qecho ''
\qecho '.idx-desc {'
\qecho '  display: block;'
\qecho '  font-size: 10px;'
\qecho '  color: var(--muted);'
\qecho '  margin-top: 4px;'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho '.module-index .index-grid {'
\qecho '  grid-template-columns: repeat(2, minmax(0, 1fr));'
\qecho '  gap: 8px;'
\qecho '  padding: 8px;'
\qecho '}'
\qecho ''
\qecho '.index-grid.module-index {'
\qecho '  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));'
\qecho '  gap: 8px;'
\qecho '  padding: 10px 12px 12px;'
\qecho '  background: #f4f7f5;'
\qecho '}'
\qecho ''
\qecho '.module-index .index-card {'
\qecho '  border: 1px solid var(--line);'
\qecho '  border-right: 1px solid var(--line);'
\qecho '  border-bottom: 1px solid var(--line);'
\qecho '  min-height: 0;'
\qecho '  padding: 8px 10px;'
\qecho '}'
\qecho ''
\qecho '.index-grid.module-index .index-card {'
\qecho '  border: 1px solid var(--line);'
\qecho '  min-height: 0;'
\qecho '  padding: 9px 11px;'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.module-index .idx-id {'
\qecho '  margin-bottom: 4px;'
\qecho '}'
\qecho ''
\qecho '.module-index .idx-title {'
\qecho '  line-height: 1.25;'
\qecho '}'
\qecho ''
\qecho '.module-index .idx-desc {'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho '.index-grid.module-index .idx-title {'
\qecho '  line-height: 1.2;'
\qecho '}'
\qecho ''
\qecho '.index-grid.module-index .idx-desc {'
\qecho '  line-height: 1.3;'
\qecho '  margin-top: 4px;'
\qecho '}'
\qecho ''
\qecho '.catalog-summary {'
\qecho '  display: none;'
\qecho '  grid-template-columns: repeat(3, minmax(0, 1fr));'
\qecho '  gap: 0;'
\qecho '  padding: 0;'
\qecho '  border-bottom: 1px solid var(--line);'
\qecho '}'
\qecho ''
\qecho '.catalog-stat {'
\qecho '  background: #ffffff;'
\qecho '  border-right: 1px solid var(--line);'
\qecho '  padding: 7px 10px;'
\qecho '}'
\qecho '.catalog-stat:last-child {'
\qecho '  border-right: 0;'
\qecho '}'
\qecho ''
\qecho '.catalog-stat-label {'
\qecho '  font-size: 10px;'
\qecho '  text-transform: uppercase;'
\qecho '  letter-spacing: 0.04em;'
\qecho '  color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.catalog-stat-value {'
\qecho '  margin-top: 3px;'
\qecho '  font-size: 15px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--ink);'
\qecho '}'
\qecho ''
\qecho '.catalog-grid {'
\qecho '  display: grid;'
\qecho '  grid-template-columns: repeat(4, minmax(0, 1fr));'
\qecho '  gap: 0;'
\qecho '  padding: 0;'
\qecho '  align-items: start;'
\qecho '  border-top: 1px solid var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.catalog-group {'
\qecho '  background: #ffffff;'
\qecho '  border-right: 1px solid var(--accent-blue);'
\qecho '  min-height: 100%;'
\qecho '  padding: 6px 10px 12px;'
\qecho '}'
\qecho '.catalog-group:nth-child(4n) {'
\qecho '  border-right: 0;'
\qecho '}'
\qecho ''
\qecho '.catalog-group-head {'
\qecho '  display: flex;'
\qecho '  gap: 4px;'
\qecho '  align-items: baseline;'
\qecho '  padding: 0 0 6px;'
\qecho '  background: transparent;'
\qecho '  border-left: 0;'
\qecho '  border-bottom: 0;'
\qecho '  flex-wrap: wrap;'
\qecho '}'
\qecho ''
\qecho '.catalog-group-id {'
\qecho '  font-size: 14pt;'
\qecho '  font-weight: 700;'
\qecho '  letter-spacing: 0;'
\qecho '  color: var(--accent-blue);'
\qecho '  text-transform: none;'
\qecho '}'
\qecho ''
\qecho '.catalog-group-title {'
\qecho '  font-size: 14pt;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  line-height: 1.25;'
\qecho '}'
\qecho ''
\qecho '.catalog-list {'
\qecho '  list-style: decimal;'
\qecho '  margin: 0;'
\qecho '  padding: 0 0 0 22px;'
\qecho '}'
\qecho ''
\qecho '.catalog-item {'
\qecho '  display: list-item;'
\qecho '  padding: 0 4px 2px;'
\qecho '  border-top: 0;'
\qecho '  color: #000000;'
\qecho '  font-size: 8pt;'
\qecho '  line-height: 1.25;'
\qecho '}'
\qecho ''
\qecho '.catalog-item:first-child {'
\qecho '  border-top: 0;'
\qecho '}'
\qecho ''
\qecho '.catalog-check-id {'
\qecho '  display: none;'
\qecho '  font: 600 12px "Helvetica Neue", Arial, Helvetica, sans-serif;'
\qecho '  color: #000000;'
\qecho '  background: transparent;'
\qecho '  border: 0;'
\qecho '  border-radius: 0;'
\qecho '  padding: 0;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.catalog-link {'
\qecho '  color: #000000;'
\qecho '  text-decoration: none;'
\qecho '  font-weight: 400;'
\qecho '  line-height: 1.25;'
\qecho '}'
\qecho ''
\qecho '.catalog-link:hover {'
\qecho '  color: var(--accent-blue);'
\qecho '  text-decoration: underline;'
\qecho '}'
\qecho ''
\qecho '.catalog-format,'
\qecho '.catalog-count {'
\qecho '  font-size: 8pt;'
\qecho '  color: #936031;'
\qecho '  border: 0;'
\qecho '  background: transparent;'
\qecho '  border-radius: 0;'
\qecho '  padding: 0;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho '.catalog-format {'
\qecho '  text-decoration: underline;'
\qecho '  margin-left: 4px;'
\qecho '}'
\qecho '.catalog-count {'
\qecho '  margin-left: 2px;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog {'
\qecho '  margin: 4px 0 14px;'
\qecho '  table-layout: fixed;'
\qecho '  width: 100%;'
\qecho '  border-collapse: collapse;'
\qecho '  border-top: 1px solid var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog h2 {'
\qecho '  margin-top: 10px;'
\qecho '  margin-bottom: 6px;'
\qecho '  padding-top: 0;'
\qecho '  border-top: 0;'
\qecho '  font-size: 13pt;'
\qecho '  line-height: 1.08;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog td {'
\qecho '  width: 20%;'
\qecho '  padding: 0 12px 14px;'
\qecho '  vertical-align: top;'
\qecho '  border-top: 0;'
\qecho '  border-bottom: 0;'
\qecho '}'
\qecho '.pg360-catalog td + td {'
\qecho '  border-left: 1px solid var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog ol {'
\qecho '  margin: 0 0 14px;'
\qecho '  padding-left: 34px;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog li {'
\qecho '  font-size: 8pt;'
\qecho '  color: black;'
\qecho '  padding-left: 0;'
\qecho '  padding-right: 4px;'
\qecho '  padding-bottom: 2px;'
\qecho '  line-height: 1.22;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog li::marker {'
\qecho '  color: black;'
\qecho '  font-size: 8pt;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog .catalog-item-title {'
\qecho '  color: black;'
\qecho '  margin-right: 2px;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog .catalog-format {'
\qecho '  color: #663300;'
\qecho '  text-decoration: underline;'
\qecho '  margin-left: 0;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog .catalog-count {'
\qecho '  color: inherit;'
\qecho '  margin-left: 1px;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.pg360-catalog .catalog-count em {'
\qecho '  color: #000000;'
\qecho '}'
\qecho ''
\qecho '.check-chip {'
\qecho '  display: inline;'
\qecho '  margin-right: 8px;'
\qecho '  padding: 0;'
\qecho '  border-radius: 0;'
\qecho '  background: transparent;'
\qecho '  border: 0;'
\qecho '  color: var(--accent-blue);'
\qecho '  font: 700 11px "Helvetica Neue", Arial, Helvetica, sans-serif;'
\qecho '  vertical-align: middle;'
\qecho '}'
\qecho ''
\qecho '.section {'
\qecho '  margin-bottom: 16px;'
\qecho '  scroll-margin-top: 72px;'
\qecho '}'
\qecho ''
\qecho '.section-header {'
\qecho '  display: flex;'
\qecho '  align-items: flex-start;'
\qecho '  gap: 10px;'
\qecho '  border: 0;'
\qecho '  border-top: 1px solid var(--accent-blue);'
\qecho '  background: transparent;'
\qecho '  padding: 6px 0 8px;'
\qecho '  margin-bottom: 12px;'
\qecho '}'
\qecho ''
\qecho '.section-id {'
\qecho '  font-size: 12px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  background: transparent;'
\qecho '  border: 0;'
\qecho '  padding: 0;'
\qecho '  border-radius: 0;'
\qecho '}'
\qecho ''
\qecho '.section-title {'
\qecho '  font-size: 14pt;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  line-height: 1.15;'
\qecho '}'
\qecho ''
\qecho '.section-desc {'
\qecho '  margin-top: 4px;'
\qecho '  font-size: 11px;'
\qecho '  color: var(--muted);'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho '.section-back {'
\qecho '  margin-left: auto;'
\qecho '  font-size: 10px;'
\qecho '  color: var(--accent-blue);'
\qecho '  border: 0;'
\qecho '  background: transparent;'
\qecho '  padding: 0;'
\qecho '  border-radius: 0;'
\qecho '}'
\qecho ''
\qecho '.subsection {'
\qecho '  margin-bottom: 16px;'
\qecho '}'
\qecho ''
\qecho '.subsection-title {'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  gap: 0;'
\qecho '  font-size: var(--font-subtitle);'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '  padding: 0 0 2px;'
\qecho '  margin-bottom: 8px;'
\qecho '  border-bottom: 0;'
\qecho '  line-height: 1.18;'
\qecho '}'
\qecho ''
\qecho '.subsection-details {'
\qecho '  border: 1px solid var(--line);'
\qecho '  border-radius: 0;'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.subsection-summary {'
\qecho '  list-style: none;'
\qecho '  cursor: pointer;'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  justify-content: space-between;'
\qecho '  gap: var(--space-2);'
\qecho '  padding: var(--space-1) var(--space-2);'
\qecho '  border-bottom: 1px solid var(--line-soft);'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.subsection-summary::-webkit-details-marker {'
\qecho '  display: none;'
\qecho '}'
\qecho ''
\qecho '.subsection-summary-text {'
\qecho '  font-size: var(--font-subtitle);'
\qecho '  font-weight: 700;'
\qecho '  color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.subsection-summary-marker::before {'
\qecho '  content: "+";'
\qecho '  color: var(--muted);'
\qecho '  font-weight: 700;'
\qecho '}'
\qecho ''
\qecho '.subsection-details[open] > .subsection-summary .subsection-summary-marker::before {'
\qecho '  content: "-";'
\qecho '}'
\qecho ''
\qecho '.subsection-content {'
\qecho '  margin: 0;'
\qecho '  padding: var(--space-1) var(--space-2);'
\qecho '}'
\qecho ''
\qecho '.card-grid {'
\qecho '  display: grid;'
\qecho '  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));'
\qecho '  gap: 10px;'
\qecho '  border: 0;'
\qecho '  margin-bottom: 12px;'
\qecho '}'
\qecho ''
\qecho '.card,'
\qecho '.kpi {'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  justify-content: space-between;'
\qecho '  gap: 8px;'
\qecho '  border: 1px solid var(--line-soft);'
\qecho '  padding: 10px 12px;'
\qecho '  min-height: 0;'
\qecho '  border-radius: 0;'
\qecho '  font-size: var(--font-body);'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.card-grid .card {'
\qecho '  padding: var(--space-1) var(--space-2);'
\qecho '}'
\qecho ''
\qecho '.executive-grid {'
\qecho '  grid-template-columns: repeat(4, minmax(0, 1fr));'
\qecho '  gap: var(--space-2);'
\qecho '  border: none;'
\qecho '}'
\qecho ''
\qecho '.executive-grid .card {'
\qecho '  display: flex;'
\qecho '  flex-direction: column;'
\qecho '  align-items: flex-start;'
\qecho '  justify-content: flex-start;'
\qecho '  gap: 6px;'
\qecho '  min-height: 118px;'
\qecho '  padding: 10px 12px;'
\qecho '  border: 1px solid var(--line);'
\qecho '  border-left: 1px solid var(--line);'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho '.executive-grid .card-label {'
\qecho '  font-size: 12px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--muted);'
\qecho '  line-height: 1.35;'
\qecho '  white-space: normal;'
\qecho '}'
\qecho ''
\qecho '.executive-grid .card-value {'
\qecho '  margin-left: 0;'
\qecho '  font-size: 22px;'
\qecho '  font-weight: 800;'
\qecho '  line-height: 1.15;'
\qecho '  color: var(--ink);'
\qecho '  white-space: normal;'
\qecho '  overflow-wrap: anywhere;'
\qecho '}'
\qecho ''
\qecho '.executive-grid .card-sub {'
\qecho '  margin-top: auto;'
\qecho '  font-size: 10px;'
\qecho '  line-height: 1.35;'
\qecho '  color: var(--muted);'
\qecho '  white-space: normal;'
\qecho '}'
\qecho ''
\qecho '.kpi {'
\qecho '  padding: var(--space-1) var(--space-2);'
\qecho '}'
\qecho ''
\qecho '.card-label,'
\qecho '.kpi-label {'
\qecho '  font-size: var(--font-body);'
\qecho '  color: var(--muted);'
\qecho '  font-weight: 600;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.card-value,'
\qecho '.kpi-value {'
\qecho '  margin-left: auto;'
\qecho '  font-size: var(--font-body);'
\qecho '  font-weight: 700;'
\qecho '  color: var(--ink);'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.card-sub {'
\qecho '  font-size: 10px;'
\qecho '  color: var(--muted);'
\qecho '  white-space: normal;'
\qecho '}'
\qecho ''
\qecho '.badge {'
\qecho '  border: 1px solid var(--line);'
\qecho '  border-radius: 8px;'
\qecho '  padding: 1px 6px;'
\qecho '  font-size: 10px;'
\qecho '  font-weight: 700;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.b-amber {'
\qecho '  color: var(--accent-yellow);'
\qecho '  background: #fff5e8;'
\qecho '  border-color: #e0c38d;'
\qecho '}'
\qecho ''
\qecho '.card.warning {'
\qecho '  border-left: 3px solid var(--accent-yellow);'
\qecho '}'
\qecho ''
\qecho '.card.critical {'
\qecho '  border-left: 3px solid var(--accent-red);'
\qecho '}'
\qecho ''
\qecho '.card.good {'
\qecho '  border-left: 3px solid var(--accent-green);'
\qecho '}'
\qecho ''
\qecho '.table-wrap {'
\qecho '  overflow-x: auto;'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: #ffffff;'
\qecho '  margin: 4px 0 12px;'
\qecho '  border-radius: var(--radius-sm);'
\qecho '  box-shadow: none;'
\qecho '}'
\qecho ''
\qecho 'table.pg360,'
\qecho 'table {'
\qecho '  width: 100%;'
\qecho '  border-collapse: collapse;'
\qecho '  font-size: var(--font-body);'
\qecho '}'
\qecho ''
\qecho 'table.pg360 thead th,'
\qecho 'table thead th {'
\qecho '  font-size: var(--font-body);'
\qecho '  font-weight: 700;'
\qecho '  color: #ffffff;'
\qecho '  background: #336699;'
\qecho '  border-bottom: 1px solid var(--line);'
\qecho '  padding: 8px 9px;'
\qecho '  text-align: left;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho 'table.pg360 tbody td,'
\qecho 'table tbody td {'
\qecho '  color: var(--ink);'
\qecho '  border-top: 1px solid var(--line-soft);'
\qecho '  padding: 7px 9px;'
\qecho '  vertical-align: top;'
\qecho '  line-height: 1.32;'
\qecho '}'
\qecho ''
\qecho 'table.pg360 tbody tr:nth-child(even),'
\qecho 'table tbody tr:nth-child(even) {'
\qecho '  background: #fbfdff;'
\qecho '}'
\qecho ''
\qecho 'table.pg360 tbody tr:hover,'
\qecho 'table tbody tr:hover {'
\qecho '  background: #f1f6fb;'
\qecho '}'
\qecho ''
\qecho 'td.c {'
\qecho '  text-align: center;'
\qecho '}'
\qecho ''
\qecho 'tr.main {'
\qecho '  color: black;'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho 'tr.main:hover {'
\qecho '  color: black;'
\qecho '  background: #ffffff;'
\qecho '}'
\qecho ''
\qecho 'td.num,'
\qecho 'th.num,'
\qecho 'td.numeric,'
\qecho 'th.numeric {'
\qecho '  text-align: right;'
\qecho '}'
\qecho ''
\qecho 'th.sortable {'
\qecho '  cursor: pointer;'
\qecho '  user-select: none;'
\qecho '  position: relative;'
\qecho '  padding-right: 20px;'
\qecho '}'
\qecho ''
\qecho 'th.sortable::after {'
\qecho '  content: "↕";'
\qecho '  position: absolute;'
\qecho '  right: 6px;'
\qecho '  color: #7f8fa1;'
\qecho '  font-size: 10px;'
\qecho '}'
\qecho ''
\qecho 'th.sort-asc::after {'
\qecho '  content: "↑";'
\qecho '  color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho 'th.sort-desc::after {'
\qecho '  content: "↓";'
\qecho '  color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.num {'
\qecho '  text-align: right;'
\qecho '}'
\qecho ''
\qecho '.good {'
\qecho '  color: var(--accent-green);'
\qecho '}'
\qecho ''
\qecho '.warn {'
\qecho '  color: var(--accent-yellow);'
\qecho '}'
\qecho ''
\qecho '.crit {'
\qecho '  color: var(--accent-red);'
\qecho '}'
\qecho ''
\qecho '.table-empty {'
\qecho '  text-align: center;'
\qecho '  color: var(--muted);'
\qecho '  padding: 8px;'
\qecho '}'
\qecho ''
\qecho '.table-search {'
\qecho '  width: 100%;'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: var(--panel);'
\qecho '  padding: 4px 6px;'
\qecho '  margin-bottom: 4px;'
\qecho '  font-size: var(--font-body);'
\qecho '}'
\qecho ''
\qecho '.table-search:focus {'
\qecho '  outline: 0;'
\qecho '  border-color: #6a8aa8;'
\qecho '}'
\qecho ''
\qecho '.finding {'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: #fbfcfe;'
\qecho '  padding: 10px 12px;'
\qecho '  margin-bottom: 12px;'
\qecho '  border-left-width: 3px;'
\qecho '  border-left-style: solid;'
\qecho '  border-left-color: var(--line);'
\qecho '  border-radius: 0;'
\qecho '}'
\qecho ''
\qecho '.finding.critical {'
\qecho '  border-left-color: var(--accent-red);'
\qecho '}'
\qecho ''
\qecho '.finding.high {'
\qecho '  border-left-color: var(--accent-yellow);'
\qecho '}'
\qecho ''
\qecho '.finding.medium {'
\qecho '  border-left-color: var(--accent-orange);'
\qecho '}'
\qecho ''
\qecho '.finding.good {'
\qecho '  border-left-color: var(--accent-green);'
\qecho '}'
\qecho ''
\qecho '.finding.info {'
\qecho '  border-left-color: var(--accent-blue);'
\qecho '}'
\qecho ''
\qecho '.finding-header {'
\qecho '  display: flex;'
\qecho '  align-items: center;'
\qecho '  gap: 8px;'
\qecho '  margin-bottom: 6px;'
\qecho '}'
\qecho ''
\qecho '.finding-id {'
\qecho '  font-size: 10px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--muted);'
\qecho '}'
\qecho ''
\qecho '.finding-title {'
\qecho '  font-size: 12px;'
\qecho '  font-weight: 700;'
\qecho '  color: var(--ink);'
\qecho '}'
\qecho ''
\qecho '.finding-body {'
\qecho '  font-size: 11px;'
\qecho '  color: #435563;'
\qecho '  line-height: 1.35;'
\qecho '}'
\qecho ''
\qecho '.finding-fix,'
\qecho '.code-block {'
\qecho '  margin-top: 4px;'
\qecho '  border: 1px solid var(--line-soft);'
\qecho '  background: #f5f8fb;'
\qecho '  padding: var(--space-1) var(--space-2);'
\qecho '  font-family: "Courier New", monospace;'
\qecho '  font-size: 10px;'
\qecho '  white-space: pre-wrap;'
\qecho '  overflow-x: auto;'
\qecho '  border-radius: 0;'
\qecho '}'
\qecho ''
\qecho '.copyable {'
\qecho '  cursor: pointer;'
\qecho '}'
\qecho ''
\qecho '.copy-flash {'
\qecho '  background: #e9f7ef !important;'
\qecho '}'
\qecho ''
\qecho '.severity-pill {'
\qecho '  margin-left: auto;'
\qecho '  font-size: 10px;'
\qecho '  font-weight: 700;'
\qecho '  border: 1px solid var(--line);'
\qecho '  padding: 1px 6px;'
\qecho '  background: #f4f8fc;'
\qecho '  color: #2f5f86;'
\qecho '  border-radius: 8px;'
\qecho '}'
\qecho ''
\qecho '.pill-critical {'
\qecho '  color: var(--accent-red);'
\qecho '  border-color: #e0b2bc;'
\qecho '  background: #fff3f5;'
\qecho '}'
\qecho ''
\qecho '.pill-high {'
\qecho '  color: var(--accent-yellow);'
\qecho '  border-color: #e4cd9b;'
\qecho '  background: #fff8ed;'
\qecho '}'
\qecho ''
\qecho '.pill-medium {'
\qecho '  color: var(--accent-orange);'
\qecho '  border-color: #e7ccb0;'
\qecho '  background: #fff5ee;'
\qecho '}'
\qecho ''
\qecho '.pill-good {'
\qecho '  color: var(--accent-green);'
\qecho '  border-color: #b8ddc3;'
\qecho '  background: #eef8f1;'
\qecho '}'
\qecho ''
\qecho '.pill-info {'
\qecho '  color: var(--accent-blue);'
\qecho '  border-color: #b6cadc;'
\qecho '  background: #eff3f9;'
\qecho '}'
\qecho ''
\qecho '.oracle-origin-banner {'
\qecho '  border: 1px solid var(--line);'
\qecho '  background: #eff3f9;'
\qecho '  border-radius: 0;'
\qecho '  padding: var(--space-2) var(--space-3);'
\qecho '  margin-bottom: var(--space-2);'
\qecho '}'
\qecho ''
\qecho '.risk-matrix {'
\qecho '  display: grid;'
\qecho '  grid-template-columns: repeat(3, 1fr);'
\qecho '  gap: 6px;'
\qecho '}'
\qecho ''
\qecho '.risk-cell {'
\qecho '  background: var(--panel);'
\qecho '  border: 1px solid var(--line);'
\qecho '  border-radius: 0;'
\qecho '  padding: var(--space-2);'
\qecho '  text-align: center;'
\qecho '}'
\qecho ''
\qecho '.risk-count {'
\qecho '  font-size: 18px;'
\qecho '  font-weight: 700;'
\qecho '}'
\qecho ''
\qecho '.risk-label {'
\qecho '  font-size: 11px;'
\qecho '  color: var(--muted);'
\qecho '  margin-top: 2px;'
\qecho '}'
\qecho ''
\qecho '.clip {'
\qecho '  max-width: 420px;'
\qecho '  overflow: hidden;'
\qecho '  text-overflow: ellipsis;'
\qecho '  white-space: nowrap;'
\qecho '}'
\qecho ''
\qecho '.small,'
\qecho '.small-note {'
\qecho '  font-size: 11px;'
\qecho '}'
\qecho ''
\qecho '.muted {'
\qecho '  color: var(--text-muted);'
\qecho '}'
\qecho ''
\qecho '.kpi-critical {'
\qecho '  color: var(--accent-red);'
\qecho '}'
\qecho ''
\qecho '@media (max-width: 1024px) {'
\qecho '  .index-grid {'
\qecho '    grid-template-columns: repeat(2, minmax(0, 1fr));'
\qecho '  }'
\qecho ''
\qecho '  .catalog-grid {'
\qecho '    grid-template-columns: 1fr;'
\qecho '  }'
\qecho ''
\qecho '  .module-index .index-grid {'
\qecho '    grid-template-columns: 1fr;'
\qecho '  }'
\qecho ''
\qecho '  .card-grid {'
\qecho '    grid-template-columns: repeat(2, minmax(0, 1fr));'
\qecho '  }'
\qecho ''
\qecho '  .executive-grid {'
\qecho '    grid-template-columns: repeat(2, minmax(0, 1fr));'
\qecho '  }'
\qecho '}'
\qecho ''
\qecho '@media (max-width: 700px) {'
\qecho '  .index-grid,'
\qecho '  .catalog-grid,'
\qecho '  .catalog-summary,'
\qecho '  .card-grid,'
\qecho '  .risk-matrix {'
\qecho '    grid-template-columns: 1fr;'
\qecho '  }'
\qecho ''
\qecho '  .section-header {'
\qecho '    flex-wrap: wrap;'
\qecho '  }'
\qecho '}'
\qecho ''
\qecho '</style>'
\qecho '</head>'
SELECT
  '<body class="pg360-index-loading" data-pg360-redact-user="' ||
  CASE
    WHEN lower(:'pg360_redact_user') IN ('off','false','0','no') THEN 'off'
    ELSE 'on'
  END ||
  '" data-pg360-redaction-token="' ||
  replace(replace(replace(replace(replace(:'pg360_redaction_token','&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '">';
\qecho '<div class="container">'
\qecho '<div class="hero">'
SELECT
  '<h1>PG360 Technical Diagnostics Report</h1>';
\qecho '</div>'
\qecho '<div class="panel">'

-- =============================================================================
-- SIDEBAR NAVIGATION
-- =============================================================================
\if false
\qecho '<div id="sidebar">'
\qecho '  <div id="sidebar-header">'
\qecho '    <div class="sidebar-logo">PG360</div>'
\qecho '    <div class="sidebar-sub">PostgreSQL 360 Diagnostic</div>'
\qecho '  </div>'
\qecho '  <nav>'
\qecho '    <div class="nav-section-title">Overview</div>'
\qecho '    <a class="nav-item active" href="#s00">Environment</a>'
\qecho '    <a class="nav-item" href="#s01">Database Overview</a>'
\qecho '    <a class="nav-item" href="#s18">Health Score</a>'
\qecho '    <div class="nav-section-title">Performance</div>'
\qecho '    <a class="nav-item" href="#s02">Top SQL Analysis</a>'
\qecho '    <a class="nav-item" href="#s03">Wait Events</a>'
\qecho '    <a class="nav-item" href="#s04">Lock Analysis</a>'
\qecho '    <a class="nav-item" href="#s07">Buffer Cache &amp; I/O</a>'
\qecho '    <a class="nav-item" href="#s11">Workload &amp; Config</a>'
\qecho '    <a class="nav-item" href="#s19">HOT Updates &amp; Fillfactor</a>'
\qecho '    <a class="nav-item" href="#s20">Planner Statistics</a>'
\qecho '    <div class="nav-section-title">Storage &amp; Objects</div>'
\qecho '    <a class="nav-item" href="#s05">Table Health</a>'
\qecho '    <a class="nav-item" href="#s06">Index Health</a>'
\qecho '    <a class="nav-item" href="#s24">Index Bloat</a>'
\qecho '    <a class="nav-item" href="#s13">Partitioning</a>'
\qecho '    <a class="nav-item" href="#s26">Capacity &amp; Growth</a>'
\qecho '    <div class="nav-section-title">Maintenance</div>'
\qecho '    <a class="nav-item" href="#s21">Autovacuum Full Advisor</a>'
\qecho '    <a class="nav-item" href="#s10">Vacuum Status</a>'
\qecho '    <div class="nav-section-title">Infrastructure</div>'
\qecho '    <a class="nav-item" href="#s08">WAL &amp; Replication</a>'
\qecho '    <a class="nav-item" href="#s09">Connections</a>'
\qecho '    <a class="nav-item" href="#s22">Connection Pool Advisor</a>'
\qecho '    <a class="nav-item" href="#s17">HA &amp; DR Readiness</a>'
\qecho '    <div class="nav-section-title">Configuration</div>'
\qecho '    <a class="nav-item" href="#s23">Config Audit (40+ params)</a>'
\qecho '    <a class="nav-item" href="#s29">Extension Inventory</a>'
\qecho '    <div class="nav-section-title">Governance</div>'
\qecho '    <a class="nav-item" href="#s12">Security Audit</a>'
\qecho '    <a class="nav-item" href="#s25">Security &amp; Access Review</a>'
\qecho '    <a class="nav-item" href="#s15">Data Quality</a>'
\qecho '    <a class="nav-item" href="#s30">Join Risk Detection</a>'
\qecho '    <a class="nav-item" href="#s31">Parallel Query Efficiency</a>'
\qecho '    <a class="nav-item" href="#s32">JIT Usage Analysis</a>'
\qecho '    <a class="nav-item" href="#s33">JSONB Workload Detection</a>'
\qecho '    <div class="nav-section-title">Action Plan</div>'
\qecho '    <a class="nav-item" href="#s28">Remediation Action Plan</a>'
\qecho '  </nav>'
\qecho '</div>'

-- =============================================================================
-- MAIN CONTENT
-- =============================================================================
\qecho '<div id="main">'

-- TOP BAR
\qecho '<div id="topbar">'

SELECT
  '<span class="db-name">' ||
  replace(replace(replace(replace(replace(
    current_database(),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  ' @ ' ||
  replace(replace(replace(replace(replace(
    COALESCE(
      (SELECT setting FROM pg_settings WHERE name='listen_addresses' LIMIT 1),
      'localhost'
    ),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</span>';

SELECT
  '<span class="report-meta">PostgreSQL ' ||
  replace(replace(split_part(version(),' ',2),'<','&lt;'),'>','&gt;') ||
  ' &nbsp;|&nbsp; Report generated: ' ||
  :'pg360_run_ts_human' || ' ' || :'pg360_report_tz' ||
  ' &nbsp;|&nbsp; Uptime: ' ||
  replace(replace(
    to_char(now() - pg_postmaster_start_time(), 'DD"d" HH24"h" MI"m"'),
    '<','&lt;'),'>','&gt;') ||
  '</span>';

\qecho '</div>'

\qecho '<div id="content">'
\endif
\qecho '<a id="report_index"></a>'
SELECT
  '<pre>dbname:' ||
  replace(replace(replace(replace(replace(current_database(),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  ' version:' ||
  replace(replace(split_part(version(),' ',2),'<','&lt;'),'>','&gt;') ||
  ' host:' ||
  replace(replace(replace(replace(replace(
    COALESCE((SELECT setting FROM pg_settings WHERE name='listen_addresses' LIMIT 1), 'localhost'),
    '&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  ' generated:' || :'pg360_run_ts_human' || ' ' || :'pg360_report_tz' ||
  ' mode:read-only</pre>';
\qecho '<table class="pg360-catalog" id="evidence_catalog_shell">'
\qecho '  <tr class="main">'
\qecho '    <td id="catalog_col_1"></td>'
\qecho '    <td id="catalog_col_2"></td>'
\qecho '    <td id="catalog_col_3"></td>'
\qecho '    <td id="catalog_col_4"></td>'
\qecho '    <td id="catalog_col_5"></td>'
\qecho '  </tr>'
\qecho '</table>'

-- =============================================================================
-- MODULE M01: EXECUTIVE SUMMARY
-- =============================================================================
\qecho '<div class="section" id="m01">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">1</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Executive Summary</div>'
\qecho '    <div class="section-desc">Immediate understanding of health, risk concentration, workload shape, and the first fixes that matter most.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Executive Dashboard</div>'
WITH base AS (
  SELECT
    COALESCE((SELECT age(datfrozenxid)::numeric / 2000000000
              FROM pg_database WHERE datname = current_database()), 0) AS xid_pct,
    COALESCE((SELECT blks_hit::numeric / NULLIF(blks_hit + blks_read, 0)
              FROM pg_stat_database WHERE datname = current_database()), 0) AS cache_hit,
    COALESCE((SELECT temp_files::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS temp_files,
    COALESCE((SELECT deadlocks::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS deadlocks,
    COALESCE((SELECT SUM(n_dead_tup)::numeric / NULLIF(SUM(n_live_tup + n_dead_tup), 0)
              FROM pg_stat_user_tables), 0) AS dead_ratio,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_replication_slots WHERE NOT active), 0) AS inactive_slots,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_index WHERE NOT indisvalid OR NOT indisready), 0) AS invalid_indexes,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_sequences WHERE last_value IS NOT NULL), 0) AS sequence_count,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_extension WHERE extname = 'pg_stat_statements'), 0) AS has_pgss,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_extension WHERE extname = 'pgstattuple'), 0) AS has_pgstattuple,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_extension WHERE extname = 'pg_buffercache'), 0) AS has_pg_buffercache,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle' AND wait_event IS NOT NULL), 0) AS waiting_sessions,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction'), 0) AS idle_tx_sessions,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_activities'), 'on') AS track_activities,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'), 'off') AS track_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_wal_io_timing'), 'off') AS track_wal_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'compute_query_id'), 'auto') AS compute_query_id_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_lock_waits'), 'off') AS log_lock_waits_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_min_duration_statement'), '-1') AS log_min_duration_statement,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_temp_files'), '-1') AS log_temp_files_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_autovacuum_min_duration'), '-1') AS log_autovacuum_min_duration_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_activity_query_size'), '0') AS track_activity_query_size_setting,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    COALESCE(current_setting('pg_stat_statements.track', true), 'top') AS pgss_track_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'statement_timeout'), '0') AS statement_timeout_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'lock_timeout'), '0') AS lock_timeout_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'), '0') AS idle_tx_timeout_setting,
    CASE
      WHEN current_setting('auto_explain.log_min_duration', true) IS NOT NULL
        OR COALESCE(current_setting('shared_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('session_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('local_preload_libraries', true), '') ILIKE '%auto_explain%'
      THEN 1 ELSE 0
    END AS has_auto_explain,
    COALESCE((SELECT COUNT(*)::numeric FROM information_schema.role_table_grants WHERE grantee = 'PUBLIC'), 0) AS public_grants
), counts AS (
  SELECT
    (CASE WHEN xid_pct > 0.75 THEN 1 ELSE 0 END +
     CASE WHEN inactive_slots > 0 THEN 1 ELSE 0 END +
     CASE WHEN invalid_indexes > 0 THEN 1 ELSE 0 END +
     CASE WHEN public_grants > 0 THEN 1 ELSE 0 END)::int AS critical_cnt,
    (CASE WHEN dead_ratio > 0.20 THEN 1 ELSE 0 END +
     CASE WHEN temp_files > 500 THEN 1 ELSE 0 END +
     CASE WHEN idle_tx_sessions > 0 THEN 1 ELSE 0 END +
     CASE WHEN track_io_timing <> 'on' THEN 1 ELSE 0 END +
     CASE WHEN log_min_duration_statement = '-1' THEN 1 ELSE 0 END)::int AS high_cnt,
    (CASE WHEN cache_hit < 0.95 THEN 1 ELSE 0 END +
     CASE WHEN waiting_sessions > 0 THEN 1 ELSE 0 END +
     CASE WHEN lock_timeout_setting IN ('0','0ms') THEN 1 ELSE 0 END +
     CASE WHEN idle_tx_timeout_setting IN ('0','0ms') THEN 1 ELSE 0 END +
     CASE WHEN has_pg_buffercache = 0 THEN 1 ELSE 0 END +
     CASE WHEN has_pgstattuple = 0 THEN 1 ELSE 0 END)::int AS medium_cnt,
    (CASE WHEN has_pgss = 0 THEN 1 ELSE 0 END)::int AS low_cnt,
    GREATEST(0, LEAST(100,
      100
      - CASE WHEN xid_pct > 0.75 THEN 35 WHEN xid_pct > 0.50 THEN 18 ELSE 0 END
      - CASE WHEN inactive_slots > 0 THEN 18 ELSE 0 END
      - CASE WHEN invalid_indexes > 0 THEN 12 ELSE 0 END
      - CASE WHEN dead_ratio > 0.20 THEN 10 WHEN dead_ratio > 0.10 THEN 5 ELSE 0 END
      - CASE WHEN temp_files > 500 THEN 8 WHEN temp_files > 100 THEN 4 ELSE 0 END
      - CASE WHEN idle_tx_sessions > 0 THEN 6 ELSE 0 END
      - CASE WHEN waiting_sessions > 0 THEN 4 ELSE 0 END
      - CASE WHEN cache_hit < 0.90 THEN 8 WHEN cache_hit < 0.95 THEN 4 ELSE 0 END
      - CASE WHEN lock_timeout_setting IN ('0','0ms') THEN 3 ELSE 0 END
      - CASE WHEN idle_tx_timeout_setting IN ('0','0ms') THEN 3 ELSE 0 END
      - CASE WHEN track_io_timing <> 'on' THEN 3 ELSE 0 END
      - CASE WHEN log_min_duration_statement = '-1' THEN 2 ELSE 0 END
    ))::int AS overall_score,
    (CASE
      WHEN (SELECT tup_fetched::numeric / NULLIF(tup_inserted + tup_updated + tup_deleted, 0)
            FROM pg_stat_database WHERE datname = current_database()) > 10 THEN 'OLTP / READ-HEAVY'
      WHEN (SELECT temp_files FROM pg_stat_database WHERE datname = current_database()) > 100 THEN 'OLAP / ANALYTICAL'
      ELSE 'MIXED'
    END) AS workload_class,
    round((
      (CASE WHEN compute_query_id_setting IN ('on','auto') THEN 12 ELSE 0 END) +
      (CASE
         WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 15
         WHEN has_pgss = 1 THEN 8
         ELSE 0
       END) +
      (CASE WHEN pgss_track_setting = 'all' THEN 4 ELSE 0 END) +
      (CASE
         WHEN track_activity_query_size_setting ~ '^[0-9]+$' AND track_activity_query_size_setting::int >= 2048 THEN 6
         ELSE 0
       END) +
      (CASE WHEN track_activities = 'on' THEN 10 ELSE 0 END) +
      (CASE WHEN track_io_timing = 'on' THEN 8 ELSE 0 END) +
      (CASE WHEN track_wal_io_timing = 'on' THEN 4 ELSE 0 END) +
      (CASE WHEN log_min_duration_statement <> '-1' THEN 8 ELSE 0 END) +
      (CASE WHEN log_lock_waits_setting = 'on' THEN 6 ELSE 0 END) +
      (CASE WHEN log_temp_files_setting <> '-1' THEN 5 ELSE 0 END) +
      (CASE WHEN log_autovacuum_min_duration_setting <> '-1' THEN 4 ELSE 0 END) +
      (CASE WHEN has_auto_explain = 1 THEN 8 ELSE 0 END)
    ) * 100.0 / 90.0)::int AS completeness_score,
    CASE
      WHEN round((
        (CASE WHEN compute_query_id_setting IN ('on','auto') THEN 12 ELSE 0 END) +
        (CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 15
           WHEN has_pgss = 1 THEN 8
           ELSE 0
         END) +
        (CASE WHEN pgss_track_setting = 'all' THEN 4 ELSE 0 END) +
        (CASE
           WHEN track_activity_query_size_setting ~ '^[0-9]+$' AND track_activity_query_size_setting::int >= 2048 THEN 6
           ELSE 0
         END) +
        (CASE WHEN track_activities = 'on' THEN 10 ELSE 0 END) +
        (CASE WHEN track_io_timing = 'on' THEN 8 ELSE 0 END) +
        (CASE WHEN track_wal_io_timing = 'on' THEN 4 ELSE 0 END) +
        (CASE WHEN log_min_duration_statement <> '-1' THEN 8 ELSE 0 END) +
        (CASE WHEN log_lock_waits_setting = 'on' THEN 6 ELSE 0 END) +
        (CASE WHEN log_temp_files_setting <> '-1' THEN 5 ELSE 0 END) +
        (CASE WHEN log_autovacuum_min_duration_setting <> '-1' THEN 4 ELSE 0 END) +
        (CASE WHEN has_auto_explain = 1 THEN 8 ELSE 0 END)
      ) * 100.0 / 90.0) >= 75 THEN 'Gold'
      WHEN round((
        (CASE WHEN compute_query_id_setting IN ('on','auto') THEN 12 ELSE 0 END) +
        (CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 15
           WHEN has_pgss = 1 THEN 8
           ELSE 0
         END) +
        (CASE WHEN pgss_track_setting = 'all' THEN 4 ELSE 0 END) +
        (CASE
           WHEN track_activity_query_size_setting ~ '^[0-9]+$' AND track_activity_query_size_setting::int >= 2048 THEN 6
           ELSE 0
         END) +
        (CASE WHEN track_activities = 'on' THEN 10 ELSE 0 END) +
        (CASE WHEN track_io_timing = 'on' THEN 8 ELSE 0 END) +
        (CASE WHEN track_wal_io_timing = 'on' THEN 4 ELSE 0 END) +
        (CASE WHEN log_min_duration_statement <> '-1' THEN 8 ELSE 0 END) +
        (CASE WHEN log_lock_waits_setting = 'on' THEN 6 ELSE 0 END) +
        (CASE WHEN log_temp_files_setting <> '-1' THEN 5 ELSE 0 END) +
        (CASE WHEN log_autovacuum_min_duration_setting <> '-1' THEN 4 ELSE 0 END) +
        (CASE WHEN has_auto_explain = 1 THEN 8 ELSE 0 END)
      ) * 100.0 / 90.0) >= 55 THEN 'Silver'
      ELSE 'Bronze'
    END AS completeness_band
  FROM base
)
SELECT
  '<div class="card-grid executive-grid">' ||
  '<div class="card ' || CASE WHEN overall_score < 60 THEN 'critical' WHEN overall_score < 80 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Overall Health Score</div><div class="card-value">' || overall_score || '/100</div><div class="card-sub">Weighted stability, performance, maintenance, and observability indicators</div></div>' ||
  '<div class="card ' || CASE WHEN critical_cnt > 0 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Critical Issues</div><div class="card-value">' || critical_cnt || '</div><div class="card-sub">Immediate outage, data-loss, or severe governance risks</div></div>' ||
  '<div class="card ' || CASE WHEN high_cnt > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">High Issues</div><div class="card-value">' || high_cnt || '</div><div class="card-sub">Severe performance or resilience problems</div></div>' ||
  '<div class="card"><div class="card-label">Medium / Low</div><div class="card-value">' || medium_cnt || ' / ' || low_cnt || '</div><div class="card-sub">Efficiency and best-practice improvements</div></div>' ||
  '<div class="card"><div class="card-label">Workload Class</div><div class="card-value">' || workload_class || '</div><div class="card-sub">Auto-detected from read/write and temp usage signals</div></div>' ||
  '<div class="card ' || CASE WHEN waiting_sessions > 0 OR idle_tx_sessions > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Waiters / Idle In Tx</div><div class="card-value">' || waiting_sessions || ' / ' || idle_tx_sessions || '</div><div class="card-sub">Current wait pressure and stale transaction exposure</div></div>' ||
  '<div class="card ' ||
    CASE
      WHEN lock_timeout_setting IN ('0','0ms') AND idle_tx_timeout_setting IN ('0','0ms') THEN 'warning'
      WHEN lock_timeout_setting IN ('0','0ms') OR idle_tx_timeout_setting IN ('0','0ms') THEN 'warning'
      ELSE 'good'
    END || '"><div class="card-label">Timeout Guardrails</div><div class="card-value">' ||
    CASE
      WHEN lock_timeout_setting IN ('0','0ms') AND idle_tx_timeout_setting IN ('0','0ms') THEN 'OPEN'
      WHEN lock_timeout_setting IN ('0','0ms') OR idle_tx_timeout_setting IN ('0','0ms') THEN 'PARTIAL'
      ELSE 'SET'
    END || '</div><div class="card-sub">statement=' || statement_timeout_setting || ', lock=' || lock_timeout_setting || ', idle-in-tx=' || idle_tx_timeout_setting || '</div></div>' ||
  '<div class="card ' || CASE WHEN completeness_score < 55 THEN 'warning' WHEN completeness_score < 75 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Diagnostic Completeness</div><div class="card-value">' || completeness_score || '/100</div><div class="card-sub">' || completeness_band || ' telemetry confidence for PG360 diagnosis</div></div>' ||
  '</div>'
FROM counts, base;
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Historical Baseline Coverage</div>'
\if :pg360_has_history_db
WITH hist AS (
  SELECT
    COUNT(*)::int AS snapshot_count,
    MIN(r.captured_at) AS first_capture,
    MAX(r.captured_at) AS last_capture
  FROM pg360_history.run_snapshot r
  JOIN pg360_history.db_snapshot d ON d.run_id = r.run_id
  WHERE r.dbname = current_database()
    AND r.captured_at >= now() - (:'pg360_history_days' || ' days')::interval
), curr AS (
  SELECT
    COALESCE((SELECT temp_bytes::numeric FROM pg_stat_database WHERE datname = current_database()), 0) AS curr_temp_bytes,
    COALESCE((SELECT count(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()), 0) AS curr_sessions,
    COALESCE((SELECT count(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle' AND wait_event IS NOT NULL), 0) AS curr_waiters
), base AS (
  SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.temp_bytes, 0)::numeric) AS median_temp_bytes,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.sessions_total, 0)::numeric) AS median_sessions,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.waiters, 0)::numeric) AS median_waiters
  FROM pg360_history.run_snapshot r
  JOIN pg360_history.db_snapshot d ON d.run_id = r.run_id
  WHERE r.dbname = current_database()
    AND r.captured_at >= now() - (:'pg360_history_days' || ' days')::interval
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN snapshot_count >= 7 THEN 'good' WHEN snapshot_count >= 3 THEN 'warning' ELSE 'warning' END || '"><div class="card-label">History Snapshots</div><div class="card-value">' || snapshot_count || '</div><div class="card-sub">Captured in last ' || :'pg360_history_days' || ' days</div></div>' ||
  '<div class="card"><div class="card-label">First / Last Capture</div><div class="card-value">' ||
    COALESCE(to_char(first_capture, 'MM-DD HH24:MI'), 'N/A') || ' -> ' || COALESCE(to_char(last_capture, 'MM-DD HH24:MI'), 'N/A') ||
    '</div><div class="card-sub">Repository-backed trend window</div></div>' ||
  '<div class="card ' || CASE WHEN curr_temp_bytes > COALESCE(median_temp_bytes, 0) * 1.5 AND COALESCE(median_temp_bytes, 0) > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Temp Pressure vs Median</div><div class="card-value">' ||
    CASE
      WHEN COALESCE(median_temp_bytes, 0) = 0 THEN 'N/A'
      ELSE to_char(round((curr_temp_bytes / NULLIF(median_temp_bytes, 0))::numeric, 2), 'FM999,990.00') || 'x'
    END || '</div></div>' ||
  '<div class="card ' || CASE WHEN curr_sessions > COALESCE(median_sessions, 0) * 1.5 AND COALESCE(median_sessions, 0) > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Sessions vs Median</div><div class="card-value">' ||
    CASE
      WHEN COALESCE(median_sessions, 0) = 0 THEN 'N/A'
      ELSE to_char(round((curr_sessions / NULLIF(median_sessions, 0))::numeric, 2), 'FM999,990.00') || 'x'
    END || '</div></div>' ||
  '<div class="card ' || CASE WHEN curr_waiters > COALESCE(median_waiters, 0) AND curr_waiters > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Waiters vs Median</div><div class="card-value">' ||
    CASE
      WHEN COALESCE(median_waiters, 0) = 0 AND curr_waiters = 0 THEN 'Stable'
      WHEN COALESCE(median_waiters, 0) = 0 THEN to_char(curr_waiters, 'FM999,990')
      ELSE to_char(round((curr_waiters / NULLIF(median_waiters, 0))::numeric, 2), 'FM999,990.00') || 'x'
    END || '</div><div class="card-sub">Current waiters compared to repository median</div></div>' ||
  '</div>'
FROM hist, curr, base;
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Repository mode not enabled yet</span><span class="severity-pill pill-info">HISTORY</span></div><div class="finding-body">PG360 is running in snapshot-only mode. Add pg360_history tables and capture runs to unlock trend context, baseline comparisons, and SQLd360-style drift reporting.</div></div>';
\endif
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top Immediate Risks</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Priority</th><th>Severity</th><th>Finding</th><th>Why it Matters</th><th>Owner</th><th>Supporting Evidence</th>'
\qecho '</tr></thead><tbody>'
WITH risks AS (
  SELECT 1 AS prio, 'Critical'::text AS severity, 'XID age approaching safety threshold'::text AS finding,
         'Write outage risk if wraparound protection is reached.'::text AS why_it_matters,
         'DBA'::text AS owner, '#s21'::text AS evidence
  WHERE (SELECT age(datfrozenxid) FROM pg_database WHERE datname = current_database()) > 1000000000
  UNION ALL
  SELECT 2, 'Critical', 'Inactive replication slot retention',
         'Unconsumed WAL can grow until disk pressure causes operational failure.',
         'Infra', '#s08'
  WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE NOT active)
  UNION ALL
  SELECT 3, 'High', 'Dead tuple pressure remains elevated',
         'Storage inefficiency and planner distortion increase query cost.',
         'DBA', '#s05'
  WHERE COALESCE((SELECT SUM(n_dead_tup)::numeric / NULLIF(SUM(n_live_tup + n_dead_tup), 0) FROM pg_stat_user_tables),0) > 0.10
  UNION ALL
  SELECT 4, 'High', 'Observability baseline is incomplete',
         'Missing telemetry reduces confidence in all downstream tuning decisions.',
         'DBA / Infra', '#m04'
  WHERE NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
     OR COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'),'off') <> 'on'
     OR COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_min_duration_statement'),'-1') = '-1'
  UNION ALL
  SELECT 5, 'High', 'Wait pressure lacks timeout guardrails',
         'Open lock and idle-in-transaction timeouts allow blockers and stale transactions to persist longer during incidents.',
         'DBA / App', '#m02'
  WHERE (
    EXISTS (
      SELECT 1
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid()
        AND (wait_event_type = 'Lock' OR state = 'idle in transaction')
    )
  )
    AND (
      COALESCE((SELECT setting FROM pg_settings WHERE name = 'lock_timeout'),'0') IN ('0','0ms')
      OR COALESCE((SELECT setting FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'),'0') IN ('0','0ms')
    )
  UNION ALL
  SELECT 6, 'High', 'Public grants widen data exposure surface',
         'Broad grants increase governance and accidental access risk.',
         'Security', '#s25'
  WHERE EXISTS (SELECT 1 FROM information_schema.role_table_grants WHERE grantee = 'PUBLIC')
)
SELECT COALESCE(
  string_agg(
    '<tr><td class="num">' || prio || '</td><td><span class="severity-pill ' ||
      CASE severity WHEN 'Critical' THEN 'pill-critical' WHEN 'High' THEN 'pill-high' WHEN 'Medium' THEN 'pill-medium' ELSE 'pill-info' END ||
      '">' || severity || '</span></td><td>' || finding || '</td><td>' || why_it_matters || '</td><td>' || owner ||
      '</td><td><a href="' || evidence || '">Open Evidence</a></td></tr>',
    E'\n' ORDER BY prio
  ),
  '<tr><td colspan="6" class="table-empty">No top-priority risks crossed the executive-summary thresholds in this run.</td></tr>'
) FROM risks;
\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- MODULE M02: PLATFORM AND DIAGNOSTIC CONTEXT
-- =============================================================================
\qecho '<div class="section" id="m02">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">2</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Platform and Diagnostic Context</div>'
\qecho '    <div class="section-desc">Environment facts, visibility constraints, and telemetry caveats that affect interpretation confidence.</div>'
\qecho '  </div>'
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Diagnostic Readiness Snapshot</div>'
WITH ctx AS (
  SELECT
    split_part(version(),' ',2) AS pg_version,
    to_char(now() - pg_postmaster_start_time(), 'DD"d" HH24"h" MI"m"') AS uptime_text,
    current_setting('server_encoding') AS server_encoding,
    current_setting('lc_messages', true) AS lc_messages,
    current_setting('TimeZone', true) AS timezone_name,
    COALESCE((SELECT datcollate FROM pg_database WHERE datname = current_database()), '(unknown)') AS db_collation,
    COALESCE((SELECT datctype FROM pg_database WHERE datname = current_database()), '(unknown)') AS db_ctype,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'Installed' ELSE 'Missing' END AS pgss_status,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'), 'off') AS track_io_timing,
    COALESCE((SELECT to_char(stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ') FROM pg_stat_database WHERE datname = current_database()), 'Not exposed') AS stats_reset,
    CASE
      WHEN EXISTS (
        SELECT 1 FROM pg_roles
        WHERE rolname = current_user
          AND (rolsuper OR pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'))
      ) THEN 'Broad diagnostics'
      ELSE 'Limited diagnostics'
    END AS privilege_scope
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">PostgreSQL Version</div><div class="card-value">' || pg_version || '</div></div>' ||
  '<div class="card"><div class="card-label">Uptime</div><div class="card-value">' || uptime_text || '</div></div>' ||
  '<div class="card"><div class="card-label">Encoding</div><div class="card-value">' || server_encoding || '</div><div class="card-sub">' || replace(replace(replace(replace(replace(db_collation,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</div></div>' ||
  '<div class="card ' || CASE WHEN pgss_status = 'Installed' THEN 'good' ELSE 'warning' END || '"><div class="card-label">pg_stat_statements</div><div class="card-value">' || pgss_status || '</div></div>' ||
  '<div class="card ' || CASE WHEN track_io_timing = 'on' THEN 'good' ELSE 'warning' END || '"><div class="card-label">track_io_timing</div><div class="card-value">' || track_io_timing || '</div><div class="card-sub">' ||
    CASE
      WHEN track_io_timing = 'on' THEN 'I/O latency attribution is available for pg_stat_io, EXPLAIN, and SQL telemetry.'
      ELSE 'Enable this for pg_stat_io, EXPLAIN, and slower-query I/O diagnosis.'
    END || '</div></div>' ||
  '<div class="card"><div class="card-label">Last Stats Reset</div><div class="card-value">' || stats_reset || '</div><div class="card-sub">Measurement window start for cumulative database statistics</div></div>' ||
  '<div class="card"><div class="card-label">Privilege Scope</div><div class="card-value">' || privilege_scope || '</div><div class="card-sub">Current user: ' || replace(replace(replace(replace(replace(current_user,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</div></div>' ||
  '</div>'
FROM ctx;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Visibility Warnings</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Status</th><th>Impact on Interpretation</th><th>Supporting Evidence</th>'
\qecho '</tr></thead><tbody>'
WITH flags AS (
  SELECT 'pg_stat_statements availability'::text AS signal,
         CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'OK' ELSE 'LIMITED' END AS status,
         CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'Full SQL ranking and workload evidence available.' ELSE 'SQL performance modules will be less complete without normalized workload telemetry.' END AS impact,
         '#s29'::text AS evidence
  UNION ALL
  SELECT 'track_io_timing',
         CASE WHEN COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'),'off') = 'on' THEN 'OK' ELSE 'LIMITED' END,
         CASE
           WHEN COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'),'off') = 'on' THEN
             'I/O timing evidence is available for pg_stat_io, EXPLAIN, and SQL-level telemetry.'
           ELSE
             'Enable track_io_timing so PG360 can distinguish slow I/O from merely busy I/O.'
         END,
         '#s29'
  UNION ALL
  SELECT 'Statistics freshness',
         CASE
           WHEN (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) IS NULL THEN 'LIMITED'
           ELSE 'INFO'
         END,
         CASE
           WHEN (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) IS NULL THEN
             'Last stats reset timestamp is not exposed for this database. Reset-window metrics should be interpreted cautiously.'
           ELSE
             'Last stats reset: ' || COALESCE((SELECT to_char(stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ') FROM pg_stat_database WHERE datname = current_database()), 'Not exposed') ||
             '. This defines the measurement window for cumulative performance statistics.'
         END,
         '#s00'
  UNION ALL
  SELECT 'Privilege envelope',
         CASE WHEN EXISTS (
           SELECT 1 FROM pg_roles
           WHERE rolname = current_user
             AND (rolsuper OR pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'))
         ) THEN 'OK' ELSE 'LIMITED' END,
         CASE WHEN EXISTS (
           SELECT 1 FROM pg_roles
           WHERE rolname = current_user
             AND (rolsuper OR pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'))
         ) THEN 'Diagnostic privileges are sufficient for broad visibility.' ELSE 'Some low-level statistics and evidence may be incomplete.' END,
         '#s00'
)
SELECT string_agg(
  '<tr><td>' || signal || '</td><td>' || status || '</td><td>' || impact || '</td><td><a href="' || evidence || '">Open Evidence</a></td></tr>',
  E'\n'
) FROM flags;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Timeout Guardrails and Session Safety</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Guardrail</th><th>Observed</th><th>Status</th><th>Why it Matters</th><th>Recommended Posture</th>'
\qecho '</tr></thead><tbody>'
WITH cfg AS (
  SELECT
    current_setting('statement_timeout', true) AS statement_timeout,
    current_setting('lock_timeout', true) AS lock_timeout,
    current_setting('idle_in_transaction_session_timeout', true) AS idle_tx_timeout,
    current_setting('transaction_timeout', true) AS transaction_timeout,
    current_setting('idle_session_timeout', true) AS idle_session_timeout,
    current_setting('tcp_user_timeout', true) AS tcp_user_timeout,
    current_setting('client_connection_check_interval', true) AS client_connection_check_interval,
    current_setting('log_lock_waits', true) AS log_lock_waits,
    current_setting('deadlock_timeout', true) AS deadlock_timeout
), rows AS (
  SELECT 1 AS ord, 'statement_timeout'::text AS guardrail,
         COALESCE(statement_timeout, 'not exposed') AS observed,
         CASE
           WHEN statement_timeout IS NULL THEN 'LIMITED'
           WHEN statement_timeout ~ '^0($|ms$|s$)' THEN 'REVIEW'
           ELSE 'SET'
         END AS status,
         CASE
           WHEN statement_timeout IS NULL THEN 'Current branch or privilege scope does not expose this setting.'
           WHEN statement_timeout ~ '^0($|ms$|s$)' THEN 'Statements can run indefinitely unless the caller or operator intervenes. This is often acceptable for admin sessions, but risky for pooled or latency-sensitive application paths.'
           ELSE 'Statements have a runtime guardrail. Confirm the limit matches workload expectations and is scoped intentionally.'
         END AS why_it_matters,
         CASE
           WHEN statement_timeout IS NULL THEN 'Review role or platform visibility before relying on statement-time enforcement.'
           WHEN statement_timeout ~ '^0($|ms$|s$)' THEN 'Prefer role, application, or pool-level statement limits instead of a one-size-fits-all cluster default.'
           ELSE 'Keep statement_timeout aligned with service SLOs, retries, and maintenance windows.'
         END AS recommendation
  FROM cfg
  UNION ALL
  SELECT 2, 'lock_timeout',
         COALESCE(lock_timeout, 'not exposed'),
         CASE
           WHEN lock_timeout IS NULL THEN 'LIMITED'
           WHEN lock_timeout ~ '^0($|ms$|s$)' THEN 'OPEN'
           ELSE 'SET'
         END,
         CASE
           WHEN lock_timeout IS NULL THEN 'Current branch or privilege scope does not expose this setting.'
           WHEN lock_timeout ~ '^0($|ms$|s$)' THEN 'Blocked statements can wait indefinitely on locks, which prolongs user-facing incidents and rollback storms.'
           ELSE 'Lock waits are bounded, which reduces how long blocked transactions can amplify an incident.'
         END,
         CASE
           WHEN lock_timeout IS NULL THEN 'Validate lock-timeout posture in platform controls or parameter groups.'
           WHEN lock_timeout ~ '^0($|ms$|s$)' THEN 'Set a scoped lock_timeout for application roles, migration jobs, and online DDL paths.'
           ELSE 'Verify the current bound is long enough for legitimate maintenance but short enough to fail fast during blockers.'
         END
  FROM cfg
  UNION ALL
  SELECT 3, 'idle_in_transaction_session_timeout',
         COALESCE(idle_tx_timeout, 'not exposed'),
         CASE
           WHEN idle_tx_timeout IS NULL THEN 'LIMITED'
           WHEN idle_tx_timeout ~ '^0($|ms$|s$)' THEN 'OPEN'
           ELSE 'SET'
         END,
         CASE
           WHEN idle_tx_timeout IS NULL THEN 'Current branch or privilege scope does not expose this setting.'
           WHEN idle_tx_timeout ~ '^0($|ms$|s$)' THEN 'Idle transactions can hold locks, preserve old snapshots, and block cleanup indefinitely.'
           ELSE 'Idle transactions are forced to expire, which helps prevent stale blockers and vacuum interference.'
         END,
         CASE
           WHEN idle_tx_timeout IS NULL THEN 'Validate idle transaction controls in platform parameters or role settings.'
           WHEN idle_tx_timeout ~ '^0($|ms$|s$)' THEN 'Set idle_in_transaction_session_timeout for application roles and pooled sessions.'
           ELSE 'Keep the timeout aggressive enough to kill forgotten sessions without disrupting deliberate maintenance.'
         END
  FROM cfg
  UNION ALL
  SELECT 4, 'transaction_timeout',
         COALESCE(transaction_timeout, 'not available'),
         CASE
           WHEN transaction_timeout IS NULL THEN 'N/A'
           WHEN transaction_timeout ~ '^0($|ms$|s$)' THEN 'INFO'
           ELSE 'SET'
         END,
         CASE
           WHEN transaction_timeout IS NULL THEN 'This parameter is not available on every supported branch.'
           WHEN transaction_timeout ~ '^0($|ms$|s$)' THEN 'Overall transaction lifetime is not bounded by this setting.'
           ELSE 'Long-lived transactions are capped regardless of intermediate waits or client behavior.'
         END,
         CASE
           WHEN transaction_timeout IS NULL THEN 'Use statement and idle-in-transaction guardrails on older branches.'
           WHEN transaction_timeout ~ '^0($|ms$|s$)' THEN 'Consider transaction_timeout where very long transaction lifetimes create operational risk.'
           ELSE 'Confirm it does not conflict with legitimate batch jobs or maintenance windows.'
         END
  FROM cfg
  UNION ALL
  SELECT 5, 'idle_session_timeout',
         COALESCE(idle_session_timeout, 'not available'),
         CASE
           WHEN idle_session_timeout IS NULL THEN 'N/A'
           WHEN idle_session_timeout ~ '^0($|ms$|s$)' THEN 'INFO'
           ELSE 'SET'
         END,
         CASE
           WHEN idle_session_timeout IS NULL THEN 'This parameter is not available on every supported branch.'
           WHEN idle_session_timeout ~ '^0($|ms$|s$)' THEN 'Plain idle sessions are not forced to disconnect. This is usually acceptable when a pooler manages connection churn.'
           ELSE 'Idle sessions are capped, which can reduce abandoned-client buildup in non-pooled environments.'
         END,
         CASE
           WHEN idle_session_timeout IS NULL THEN 'Treat this as optional unless platform policy requires it.'
           WHEN idle_session_timeout ~ '^0($|ms$|s$)' THEN 'Use cautiously; it is more useful for direct-connect clients than for transaction pooling.'
           ELSE 'Keep the timeout compatible with poolers, keepalives, and long-lived admin sessions.'
         END
  FROM cfg
  UNION ALL
  SELECT 6, 'log_lock_waits',
         COALESCE(log_lock_waits, 'not exposed'),
         CASE
           WHEN log_lock_waits IS NULL THEN 'LIMITED'
           WHEN log_lock_waits = 'on' THEN 'SET'
           ELSE 'OFF'
         END,
         CASE
           WHEN log_lock_waits IS NULL THEN 'Current branch or privilege scope does not expose this setting.'
           WHEN log_lock_waits = 'on' THEN 'The server will emit log evidence for lock waits that exceed deadlock_timeout.'
           ELSE 'Blocking chains remain harder to reconstruct after the fact because the server is not logging lock waits.'
         END,
         CASE
           WHEN log_lock_waits IS NULL THEN 'Validate logging posture in platform controls or parameter groups.'
           WHEN log_lock_waits = 'on' THEN 'Keep this paired with an intentional deadlock_timeout and log review pipeline.'
           ELSE 'Enable log_lock_waits so incident review has blocker evidence even after the sessions are gone.'
         END
  FROM cfg
  UNION ALL
  SELECT 7, 'deadlock_timeout',
         COALESCE(deadlock_timeout, 'not exposed'),
         CASE WHEN deadlock_timeout IS NULL THEN 'LIMITED' ELSE 'SET' END,
         CASE
           WHEN deadlock_timeout IS NULL THEN 'Current branch or privilege scope does not expose this setting.'
           ELSE 'This controls how quickly PostgreSQL emits deadlock checks and lock-wait logging context.'
         END,
         CASE
           WHEN deadlock_timeout IS NULL THEN 'Validate this setting in the platform parameter source.'
           ELSE 'Keep deadlock_timeout low enough for useful incident logs, but high enough to avoid excessive churn for short-lived lock waits.'
         END
  FROM cfg
  UNION ALL
  SELECT 8, 'tcp_user_timeout',
         COALESCE(tcp_user_timeout, 'not available'),
         CASE
           WHEN tcp_user_timeout IS NULL THEN 'N/A'
           WHEN tcp_user_timeout ~ '^0($|ms$|s$)' THEN 'INFO'
           ELSE 'SET'
         END,
         CASE
           WHEN tcp_user_timeout IS NULL THEN 'This platform does not expose TCP_USER_TIMEOUT through PostgreSQL settings.'
           WHEN tcp_user_timeout ~ '^0($|ms$|s$)' THEN 'Broken network sessions rely on normal TCP behavior and pooler/application retries.'
           ELSE 'Connections with unacknowledged data can be closed sooner when the network path is broken.'
         END,
         CASE
           WHEN tcp_user_timeout IS NULL THEN 'Treat as platform-specific and validate with infrastructure before relying on it.'
           WHEN tcp_user_timeout ~ '^0($|ms$|s$)' THEN 'Consider this where blackholed TCP sessions are a recurring incident pattern.'
           ELSE 'Test carefully with load balancers, proxies, and poolers before tightening further.'
         END
  FROM cfg
  UNION ALL
  SELECT 9, 'client_connection_check_interval',
         COALESCE(client_connection_check_interval, 'not available'),
         CASE
           WHEN client_connection_check_interval IS NULL THEN 'N/A'
           WHEN client_connection_check_interval ~ '^0($|ms$|s$)' THEN 'INFO'
           ELSE 'SET'
         END,
         CASE
           WHEN client_connection_check_interval IS NULL THEN 'This platform or branch does not expose client connection checks.'
           WHEN client_connection_check_interval ~ '^0($|ms$|s$)' THEN 'Long-running statements may continue until normal socket detection notices a dead client.'
           ELSE 'PostgreSQL periodically checks whether the client is still connected while a query is running.'
         END,
         CASE
           WHEN client_connection_check_interval IS NULL THEN 'Treat as optional and platform-dependent.'
           WHEN client_connection_check_interval ~ '^0($|ms$|s$)' THEN 'Use where broken client connections or network blackholes prolong wasted work.'
           ELSE 'Keep the interval short enough to detect dead clients, but not so short that it becomes noisy.'
         END
  FROM cfg
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || guardrail || '</td><td>' ||
    replace(replace(replace(replace(replace(observed,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td class="' ||
    CASE status
      WHEN 'SET' THEN 'good">SET'
      WHEN 'OPEN' THEN 'warn">OPEN'
      WHEN 'REVIEW' THEN 'warn">REVIEW'
      WHEN 'OFF' THEN 'warn">OFF'
      WHEN 'LIMITED' THEN 'warn">LIMITED'
      WHEN 'N/A' THEN '">N/A'
      ELSE '">INFO'
    END ||
    '</td><td>' || why_it_matters || '</td><td>' || recommendation || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="5" class="table-empty">Timeout guardrail posture unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Timeout Starting Points by Workload</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Workload</th><th>statement_timeout</th><th>lock_timeout</th><th>idle_in_transaction_session_timeout</th><th>idle_session_timeout</th><th>transaction_timeout</th><th>Preferred Scope</th>'
\qecho '</tr></thead><tbody>'
WITH workload AS (
  SELECT CASE
    WHEN (SELECT tup_fetched::numeric / NULLIF(tup_inserted + tup_updated + tup_deleted, 0)
          FROM pg_stat_database WHERE datname = current_database()) > 10 THEN 'Latency-sensitive OLTP'
    WHEN (SELECT temp_files FROM pg_stat_database WHERE datname = current_database()) > 100 THEN 'Analytics / Reporting'
    ELSE 'Mixed OLTP + Reporting'
  END AS current_workload
), rows AS (
  SELECT 1 AS ord, 'Latency-sensitive OLTP'::text AS workload_name, '2s - 10s'::text AS statement_timeout,
         '200ms - 1000ms'::text AS lock_timeout, '30s - 120s'::text AS idle_tx_timeout,
         'Usually off'::text AS idle_session_timeout, 'Off or 30s - 120s'::text AS transaction_timeout,
         'ALTER ROLE / ALTER DATABASE / SET (preferred for app roles)'::text AS scope_guidance,
         current_workload = 'Latency-sensitive OLTP' AS current_match
  FROM workload
  UNION ALL
  SELECT 2, 'Mixed OLTP + Reporting',
         '5s - 30s',
         '500ms - 2000ms',
         '60s - 300s',
         'Off or 10m - 30m',
         'Off or 5m - 15m',
         'Separate OLTP and reporting roles; avoid one-size global defaults',
         current_workload = 'Mixed OLTP + Reporting'
  FROM workload
  UNION ALL
  SELECT 3, 'Analytics / Reporting',
         '1m - 10m',
         '1s - 5s',
         '5m - 15m',
         '1h - 4h',
         'Off or 15m - 60m',
         'Use reporting roles or session-level SET for BI workloads',
         current_workload = 'Analytics / Reporting'
  FROM workload
  UNION ALL
  SELECT 4, 'ETL / Batch',
         '10m - 60m or job scoped',
         '5s - 30s',
         '5m - 30m',
         'Off',
         'Off or 1h - 6h',
         'Apply at job/session scope so maintenance tasks are not killed unexpectedly',
         false
  FROM workload
  UNION ALL
  SELECT 5, 'DBA Interactive',
         'Off or very high',
         '0 - 120s',
         '5m - 15m',
         '1h - 8h',
         'Off',
         'Interactive admin roles only; do not copy blindly to pooled app roles',
         false
  FROM workload
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || workload_name ||
    CASE WHEN current_match THEN ' <strong>(Current match)</strong>' ELSE '' END ||
    '</td><td>' || statement_timeout || '</td><td>' || lock_timeout || '</td><td>' || idle_tx_timeout ||
    '</td><td>' || idle_session_timeout || '</td><td>' || transaction_timeout || '</td><td>' || scope_guidance || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="7" class="table-empty">Timeout starting points unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Managed Service Fingerprint</div>'
WITH provider AS (
  SELECT
    CASE
      WHEN lower(version()) LIKE '%aurora%' OR lower(version()) LIKE '%rds%' THEN 'AWS RDS / Aurora'
      WHEN lower(version()) LIKE '%cloud sql%' THEN 'Google Cloud SQL'
      WHEN lower(version()) LIKE '%alloydb%' THEN 'Google AlloyDB'
      WHEN lower(version()) LIKE '%azure%' THEN 'Azure Database for PostgreSQL'
      ELSE 'Self-managed or unknown'
    END AS provider_guess,
    version() AS version_text
), settings AS (
  SELECT
    COUNT(*)::int AS provider_setting_count,
    COUNT(*) FILTER (WHERE pending_restart)::int AS pending_restart_count
  FROM pg_settings
  WHERE name ~* '^(rds|aurora|cloudsql|alloydb|azure|google)'
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Provider Guess</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(provider_guess,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Provider-Specific Settings</div><div class="card-value">' || provider_setting_count || '</div><div class="card-sub">Settings exposed through pg_settings</div></div>' ||
  '<div class="card ' || CASE WHEN pending_restart_count > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Pending Restart Signals</div><div class="card-value">' || pending_restart_count || '</div><div class="card-sub">Provider-specific settings marked pending_restart</div></div>' ||
  '</div>'
FROM provider, settings;
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Provider Setting</th><th>Current Value</th><th>Source</th><th>Pending Restart</th>'
\qecho '</tr></thead><tbody>'
WITH rows AS (
  SELECT
    name,
    setting,
    source,
    pending_restart
  FROM pg_settings
  WHERE name ~* '^(rds|aurora|cloudsql|alloydb|azure|google)'
  ORDER BY name
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || replace(replace(replace(replace(replace(name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td>' || replace(replace(replace(replace(replace(setting,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td>' || replace(replace(replace(replace(replace(source,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td class="' || CASE WHEN pending_restart THEN 'warn">Yes' ELSE 'good">No' END || '</td></tr>',
    E'\n' ORDER BY name
  ),
  '<tr><td colspan="4" class="table-empty">No provider-specific settings detected from SQL. This usually means self-managed PostgreSQL or a provider that does not expose branded GUCs.</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Managed Service Apply Paths</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Platform</th><th>How to Apply</th><th>Restart / Change Window</th><th>Operator Notes</th>'
\qecho '</tr></thead><tbody>'
WITH rows AS (
  SELECT 1 AS ord, 'Self-managed PostgreSQL'::text AS platform,
         'Use ALTER ROLE / ALTER DATABASE / SET for scoped behavior, and postgresql.conf or ALTER SYSTEM for instance-wide posture.'::text AS apply_path,
         'Many logging and timeout settings can be reloaded; preload libraries and some telemetry settings may require restart.'::text AS restart_notes,
         'Best fit when you want role-scoped guardrails and maximum control over logging cadence.'::text AS operator_notes
  UNION ALL
  SELECT 2, 'Amazon RDS for PostgreSQL',
         'Use a custom DB parameter group for instance parameters; use ALTER ROLE, ALTER DATABASE, or SET where your privilege model permits it.',
         'Static parameters need a reboot; dynamic parameters can apply online through the parameter group.',
         'Good fit for role-scoped timeouts plus parameter-group managed telemetry settings.'
  UNION ALL
  SELECT 3, 'Amazon Aurora PostgreSQL',
         'Use cluster parameter groups for cluster-wide settings and instance parameter groups for node-specific behavior.',
         'Static changes require a controlled restart or failover window.',
         'Keep cluster-wide telemetry decisions separate from instance-level tuning knobs.'
  UNION ALL
  SELECT 4, 'Google Cloud SQL for PostgreSQL',
         'Use database flags for managed configuration and SQL-level role/database overrides where allowed.',
         'Some flag changes restart the primary and can affect replicas.',
         'Schedule telemetry-enabling changes with a restart window if required by the flag.'
  UNION ALL
  SELECT 5, 'Azure Database for PostgreSQL',
         'Use managed server parameters for instance-wide settings plus ALTER ROLE / ALTER DATABASE / SET for scoped behavior where supported.',
         'No OS or postgresql.conf access; restart behavior depends on the parameter class and service tier.',
         'Use platform parameters for shared defaults and SQL-level overrides for application guardrails.'
)
SELECT string_agg(
  '<tr><td>' || platform || '</td><td>' || apply_path || '</td><td>' || restart_notes || '</td><td>' || operator_notes || '</td></tr>',
  E'\n' ORDER BY ord
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Version Currency &amp; Security Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Observed</th><th>Status</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'
WITH v AS (
  SELECT
    current_setting('server_version_num')::int AS vnum,
    split_part(version(), ' ', 2) AS version_text,
    current_setting('password_encryption', true) AS password_encryption
), parsed AS (
  SELECT
    vnum,
    version_text,
    (vnum / 10000) AS major,
    CASE WHEN vnum >= 100000 THEN (vnum % 10000) ELSE ((vnum / 100) % 100) END AS minor,
    password_encryption,
    CASE (vnum / 10000)
      WHEN 18 THEN 3
      WHEN 17 THEN 9
      WHEN 16 THEN 13
      WHEN 15 THEN 17
      WHEN 14 THEN 22
      ELSE NULL
    END AS current_supported_minor
  FROM v
), hba AS (
  SELECT
    CASE
      WHEN to_regclass('pg_catalog.pg_hba_file_rules') IS NOT NULL THEN
        (SELECT COUNT(*)::int FROM pg_hba_file_rules WHERE auth_method = 'md5')
      ELSE NULL
    END AS md5_rules,
    CASE
      WHEN to_regclass('pg_catalog.pg_hba_file_rules') IS NOT NULL THEN
        (SELECT COUNT(*)::int FROM pg_hba_file_rules WHERE auth_method = 'scram-sha-256')
      ELSE NULL
    END AS scram_rules
)
SELECT
  '<tr><td>Version support branch</td><td>' || version_text || '</td><td class="' ||
  CASE WHEN major >= 14 THEN 'good">SUPPORTED' ELSE 'crit">UNSUPPORTED' END ||
  '</td><td>' ||
  CASE WHEN major >= 14 THEN 'Major version is still in PostgreSQL community support window.' ELSE 'Upgrade major version urgently; this branch is out of community support.' END ||
  '</td></tr>' ||
  '<tr><td>Minor-version currency</td><td>' || version_text || ' vs current ' ||
  CASE WHEN current_supported_minor IS NULL THEN 'unknown' ELSE major::text || '.' || current_supported_minor::text END || '</td><td class="' ||
  CASE
    WHEN current_supported_minor IS NULL THEN 'warn">CHECK'
    WHEN minor = current_supported_minor THEN 'good">CURRENT'
    WHEN minor >= current_supported_minor - 1 THEN 'warn">NEAR-CURRENT'
    ELSE 'crit">OUTDATED'
  END || '</td><td>' ||
  CASE
    WHEN current_supported_minor IS NULL THEN 'Branch currency mapping is unavailable in this PG360 catalog build.'
    WHEN minor = current_supported_minor THEN 'Minor release is current as of the PG360 support catalog dated 2026-03.'
    WHEN minor >= current_supported_minor - 1 THEN 'Upgrade soon to pick up recent bug and security fixes.'
    ELSE 'Minor release is materially behind. Prioritize maintenance upgrade.'
  END || '</td></tr>' ||
  '<tr><td>Password encryption default</td><td>' || COALESCE(password_encryption, 'unknown') || '</td><td class="' ||
  CASE WHEN password_encryption = 'scram-sha-256' THEN 'good">MODERN' ELSE 'warn">REVIEW' END ||
  '</td><td>' ||
  CASE WHEN password_encryption = 'scram-sha-256' THEN 'Password hashing default is modern.' ELSE 'Set password_encryption to scram-sha-256 for modern password hashes.' END ||
  '</td></tr>' ||
  '<tr><td>MD5 auth rules in pg_hba</td><td>' || COALESCE(md5_rules::text, 'unknown') || '</td><td class="' ||
  CASE
    WHEN md5_rules IS NULL THEN 'warn">LIMITED'
    WHEN md5_rules = 0 THEN 'good">CLEAR'
    ELSE 'warn">PRESENT'
  END || '</td><td>' ||
  CASE
    WHEN md5_rules IS NULL THEN 'Unable to inspect pg_hba_file_rules with current visibility.'
    WHEN md5_rules = 0 THEN 'No MD5 rules detected in pg_hba.'
    ELSE 'MD5 authentication rules remain. Migrate to SCRAM and review PostgreSQL 18 OAuth options where relevant.'
  END || '</td></tr>' ||
  '<tr><td>SCRAM coverage in pg_hba</td><td>' || COALESCE(scram_rules::text, 'unknown') || '</td><td class="' ||
  CASE
    WHEN scram_rules IS NULL THEN 'warn">LIMITED'
    WHEN scram_rules > 0 THEN 'good">PRESENT'
    ELSE 'warn">MISSING'
  END || '</td><td>' ||
  CASE
    WHEN scram_rules IS NULL THEN 'Unable to inspect pg_hba_file_rules with current visibility.'
    WHEN scram_rules > 0 THEN 'SCRAM authentication rules are present.'
    ELSE 'No SCRAM rules detected; review authentication modernization posture.'
  END || '</td></tr>'
FROM parsed, hba;
\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- MODULE M03: INSTANCE AND DATABASE PROFILE
-- =============================================================================
\qecho '<div class="section" id="m03">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">3</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Instance and Database Profile</div>'
\qecho '    <div class="section-desc">The shape of the system: database footprint, object inventory, storage layout, and connection envelope.</div>'
\qecho '  </div>'
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">System Shape Overview</div>'
WITH inv AS (
  SELECT
    pg_database_size(current_database()) AS db_bytes,
    (SELECT COUNT(*) FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')) AS schema_count,
    (SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')) AS table_count,
    (SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'i' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')) AS index_count,
    (SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'S' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')) AS sequence_count,
    (SELECT COUNT(*) FROM pg_tablespace) AS tablespace_count,
    current_setting('max_connections') AS max_connections
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Database Size</div><div class="card-value">' || pg_size_pretty(db_bytes) || '</div></div>' ||
  '<div class="card"><div class="card-label">Schemas</div><div class="card-value">' || schema_count || '</div></div>' ||
  '<div class="card"><div class="card-label">Tables</div><div class="card-value">' || table_count || '</div></div>' ||
  '<div class="card"><div class="card-label">Indexes</div><div class="card-value">' || index_count || '</div></div>' ||
  '<div class="card"><div class="card-label">Sequences</div><div class="card-value">' || sequence_count || '</div></div>' ||
  '<div class="card"><div class="card-label">Tablespaces / max_connections</div><div class="card-value">' || tablespace_count || ' / ' || max_connections || '</div></div>' ||
  '</div>'
FROM inv;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Profile Drill Paths</div>'
\qecho '<div class="module-index"><div class="index-grid">'
\qecho '  <a class="index-card" href="#s01"><span class="idx-title">Database Overview</span><span class="idx-desc">Primary inventory, size, and schema/object shape.</span></a>'
\qecho '  <a class="index-card" href="#s13"><span class="idx-title">Partitioning Health</span><span class="idx-desc">Partition structure and parent/child layout.</span></a>'
\qecho '  <a class="index-card" href="#s16"><span class="idx-title">Capacity &amp; Growth</span><span class="idx-desc">Large objects and growth trajectory.</span></a>'
\qecho '  <a class="index-card" href="#s26"><span class="idx-title">Capacity Enhanced View</span><span class="idx-desc">Storage composition and long-horizon shape.</span></a>'
\qecho '</div></div>'
\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- MODULE M04: MONITORING AND OBSERVABILITY READINESS
-- =============================================================================
\qecho '<div class="section" id="m04">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">4</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Monitoring and Observability Readiness</div>'
\qecho '    <div class="section-desc">Assess whether the database exposes enough telemetry to support trustworthy diagnosis and change validation.</div>'
\qecho '  </div>'
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Observability Baseline</div>'
WITH ext_status AS (
  SELECT
    COUNT(*) FILTER (WHERE extname = 'pg_stat_statements') AS has_pgss,
    COUNT(*) FILTER (WHERE extname = 'pgstattuple') AS has_pgstattuple,
    COUNT(*) FILTER (WHERE extname = 'pg_buffercache') AS has_pg_buffercache
  FROM pg_extension
), cfg AS (
  SELECT
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'),'off') AS track_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_min_duration_statement'),'-1') AS log_min_duration_statement,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_lock_waits'),'off') AS log_lock_waits
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN has_pgss = 1 THEN 'good' ELSE 'critical' END || '"><div class="card-label">pg_stat_statements</div><div class="card-value">' || CASE WHEN has_pgss = 1 THEN 'Ready' ELSE 'Missing' END || '</div></div>' ||
  '<div class="card ' || CASE WHEN has_pg_buffercache = 1 THEN 'good' ELSE 'warning' END || '"><div class="card-label">pg_buffercache</div><div class="card-value">' || CASE WHEN has_pg_buffercache = 1 THEN 'Ready' ELSE 'Gap' END || '</div></div>' ||
  '<div class="card ' || CASE WHEN has_pgstattuple = 1 THEN 'good' ELSE 'warning' END || '"><div class="card-label">pgstattuple</div><div class="card-value">' || CASE WHEN has_pgstattuple = 1 THEN 'Ready' ELSE 'Gap' END || '</div></div>' ||
  '<div class="card ' || CASE WHEN track_io_timing = 'on' THEN 'good' ELSE 'warning' END || '"><div class="card-label">track_io_timing</div><div class="card-value">' || track_io_timing || '</div></div>' ||
  '<div class="card ' || CASE WHEN log_min_duration_statement <> '-1' THEN 'good' ELSE 'warning' END || '"><div class="card-label">Slow Query Logging</div><div class="card-value">' || log_min_duration_statement || '</div></div>' ||
  '<div class="card ' || CASE WHEN log_lock_waits = 'on' THEN 'good' ELSE 'warning' END || '"><div class="card-label">Lock Wait Logging</div><div class="card-value">' || log_lock_waits || '</div></div>' ||
  '</div>'
FROM ext_status, cfg;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Logging Configuration Sanity</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current</th><th>Recommended</th><th>Status</th><th>Why it Matters</th>'
\qecho '</tr></thead><tbody>'
WITH expected(name, recommendation, rationale, ord) AS (
  VALUES
    ('log_line_prefix', 'include timestamp/pid/user/db/app/client', 'Supports fast root-cause correlation across sessions.', 1),
    ('log_lock_waits', 'on', 'Captures lock wait diagnostics in logs.', 2),
    ('deadlock_timeout', '100ms-1s', 'Lower values capture lock contention earlier.', 3),
    ('log_min_duration_statement', '>=0 in tuning windows, not -1', 'Required to capture slow SQL in logs.', 4),
    ('log_temp_files', '0 or small threshold', 'Detects temp spill storms and work_mem issues.', 5),
    ('log_checkpoints', 'on', 'Correlates checkpoint bursts with latency.', 6),
    ('log_autovacuum_min_duration', '0 to low threshold in investigations', 'Explains autovacuum impact during incidents.', 7)
), settings AS (
  SELECT
    e.ord,
    e.name,
    e.recommendation,
    e.rationale,
    s.setting,
    s.unit,
    s.source
  FROM expected e
  LEFT JOIN pg_settings s
    ON s.name = e.name
), rows AS (
  SELECT
    ord,
    name AS parameter_name,
    COALESCE(setting, '(not available)') || CASE WHEN COALESCE(unit,'') <> '' THEN ' ' || unit ELSE '' END AS current_value,
    recommendation,
    CASE
      WHEN name = 'log_line_prefix' AND setting ILIKE '%m%' AND setting ILIKE '%p%' AND setting ILIKE '%u%' AND setting ILIKE '%d%' THEN 'OK'
      WHEN name = 'log_lock_waits' AND setting = 'on' THEN 'OK'
      WHEN name = 'deadlock_timeout' AND setting ~ '^[0-9]+$' AND setting::int BETWEEN 100 AND 1000 THEN 'OK'
      WHEN name = 'log_min_duration_statement' AND setting <> '-1' THEN 'OK'
      WHEN name = 'log_temp_files' AND setting <> '-1' THEN 'OK'
      WHEN name = 'log_checkpoints' AND setting = 'on' THEN 'OK'
      WHEN name = 'log_autovacuum_min_duration' AND setting <> '-1' THEN 'OK'
      WHEN setting IS NULL THEN 'INFO'
      ELSE 'GAP'
    END AS status,
    rationale AS why_it_matters
  FROM settings
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || parameter_name || '</td><td>' ||
    replace(replace(replace(replace(replace(current_value,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td>' || recommendation || '</td><td class="' ||
    CASE status
      WHEN 'OK' THEN 'good">OK'
      WHEN 'GAP' THEN 'warn">GAP'
      ELSE '">INFO'
    END || '</td><td>' || why_it_matters || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="5" class="table-empty">Logging configuration visibility unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Performance Control Plane</div>'
WITH caps AS (
  SELECT
    COALESCE(current_setting('compute_query_id', true), 'auto') AS compute_query_id,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 1 ELSE 0 END AS has_pgss,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    COALESCE(current_setting('pg_stat_statements.track', true), 'top') AS pgss_track,
    COALESCE(current_setting('pg_stat_statements.track_planning', true), 'off') AS pgss_track_planning,
    COALESCE(current_setting('track_activity_query_size', true), '0') AS track_activity_query_size,
    COALESCE(current_setting('track_activities', true), 'on') AS track_activities,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('track_wal_io_timing', true), 'off') AS track_wal_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement,
    COALESCE(current_setting('log_lock_waits', true), 'off') AS log_lock_waits,
    COALESCE(current_setting('log_temp_files', true), '-1') AS log_temp_files,
    COALESCE(current_setting('log_autovacuum_min_duration', true), '-1') AS log_autovacuum_min_duration,
    CASE
      WHEN current_setting('auto_explain.log_min_duration', true) IS NOT NULL
        OR COALESCE(current_setting('shared_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('session_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('local_preload_libraries', true), '') ILIKE '%auto_explain%'
      THEN 1 ELSE 0
    END AS has_auto_explain,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = current_user
          AND (rolsuper OR pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'))
      ) THEN 1 ELSE 0
    END AS broad_diag
), rows AS (
  SELECT 1 AS ord, 'Query identity'::text AS capability, 12 AS max_points,
         CASE WHEN compute_query_id IN ('on','auto') THEN 12 ELSE 0 END AS awarded_points,
         CASE WHEN compute_query_id IN ('on','auto') THEN 'PASS' ELSE 'WARN' END AS status,
         'compute_query_id=' || compute_query_id AS observed,
         'Needed to correlate SQL, waits, plans, and logs consistently.' AS impact
  FROM caps
  UNION ALL
  SELECT 2, 'Statement statistics', 15,
         CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 15
           WHEN has_pgss = 1 THEN 8
           ELSE 0
         END,
         CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'PASS'
           WHEN has_pgss = 1 THEN 'PARTIAL'
           ELSE 'FAIL'
         END,
         'installed=' || CASE WHEN has_pgss = 1 THEN 'yes' ELSE 'no' END || ', preload=' ||
           CASE WHEN shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'yes' ELSE 'no' END,
         'Core per-query performance telemetry and historical fingerprinting.'
  FROM caps
  UNION ALL
  SELECT 3, 'Nested statement capture', 4,
         CASE WHEN pgss_track = 'all' THEN 4 ELSE 0 END,
         CASE WHEN pgss_track = 'all' THEN 'PASS' ELSE 'INFO' END,
         'pg_stat_statements.track=' || pgss_track,
         'Useful for stored procedures, nested SQL, and utility-heavy workloads.'
  FROM caps
  UNION ALL
  SELECT 4, 'Current SQL visibility', 6,
         CASE WHEN track_activity_query_size ~ '^[0-9]+$' AND track_activity_query_size::int >= 2048 THEN 6 ELSE 0 END,
         CASE WHEN track_activity_query_size ~ '^[0-9]+$' AND track_activity_query_size::int >= 2048 THEN 'PASS' ELSE 'WARN' END,
         'track_activity_query_size=' || track_activity_query_size,
         'Long query text is less likely to be truncated during live triage.'
  FROM caps
  UNION ALL
  SELECT 5, 'Wait telemetry', 10,
         CASE
           WHEN track_activities = 'on' AND broad_diag = 1 THEN 10
           WHEN track_activities = 'on' THEN 6
           ELSE 0
         END,
         CASE
           WHEN track_activities = 'on' AND broad_diag = 1 THEN 'PASS'
           WHEN track_activities = 'on' THEN 'PARTIAL'
           ELSE 'FAIL'
         END,
         'track_activities=' || track_activities || ', broad_stats=' || CASE WHEN broad_diag = 1 THEN 'yes' ELSE 'no' END,
         'Foundation for wait-centric triage and blocker analysis.'
  FROM caps
  UNION ALL
  SELECT 6, 'I/O latency attribution', 8,
         CASE WHEN track_io_timing = 'on' THEN 8 ELSE 0 END,
         CASE WHEN track_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         'track_io_timing=' || track_io_timing,
         'Separates slow I/O from merely busy I/O.'
  FROM caps
  UNION ALL
  SELECT 7, 'WAL latency attribution', 4,
         CASE WHEN track_wal_io_timing = 'on' THEN 4 ELSE 0 END,
         CASE WHEN track_wal_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         'track_wal_io_timing=' || track_wal_io_timing,
         'Needed to explain WAL write and sync latency cleanly.'
  FROM caps
  UNION ALL
  SELECT 8, 'Slow-query logging', 8,
         CASE WHEN log_min_duration_statement <> '-1' THEN 8 ELSE 0 END,
         CASE WHEN log_min_duration_statement <> '-1' THEN 'PASS' ELSE 'WARN' END,
         'log_min_duration_statement=' || log_min_duration_statement,
         'Provides production forensics outside cumulative-stat reset windows.'
  FROM caps
  UNION ALL
  SELECT 9, 'Lock-wait logging', 6,
         CASE WHEN log_lock_waits = 'on' THEN 6 ELSE 0 END,
         CASE WHEN log_lock_waits = 'on' THEN 'PASS' ELSE 'WARN' END,
         'log_lock_waits=' || log_lock_waits,
         'Captures blocker evidence once waits cross deadlock_timeout.'
  FROM caps
  UNION ALL
  SELECT 10, 'Temp spill logging', 5,
         CASE WHEN log_temp_files <> '-1' THEN 5 ELSE 0 END,
         CASE WHEN log_temp_files <> '-1' THEN 'PASS' ELSE 'WARN' END,
         'log_temp_files=' || log_temp_files,
         'Essential for work_mem and spill forensics.'
  FROM caps
  UNION ALL
  SELECT 11, 'Autovacuum logging', 4,
         CASE WHEN log_autovacuum_min_duration <> '-1' THEN 4 ELSE 0 END,
         CASE WHEN log_autovacuum_min_duration <> '-1' THEN 'PASS' ELSE 'WARN' END,
         'log_autovacuum_min_duration=' || log_autovacuum_min_duration,
         'Makes maintenance debt and skipped vacuum work explainable.'
  FROM caps
  UNION ALL
  SELECT 12, 'Plan capture readiness', 8,
         CASE
           WHEN has_auto_explain = 1 THEN 8
           WHEN pgss_track_planning = 'on' THEN 4
           ELSE 0
         END,
         CASE
           WHEN has_auto_explain = 1 THEN 'PASS'
           WHEN pgss_track_planning = 'on' THEN 'PARTIAL'
           ELSE 'WARN'
         END,
         'auto_explain=' || CASE WHEN has_auto_explain = 1 THEN 'available' ELSE 'not ready' END ||
         ', track_planning=' || pgss_track_planning,
         'Turns Top SQL from ranking only into explainable plan behavior.'
  FROM caps
), summary AS (
  SELECT
    COALESCE(SUM(awarded_points), 0) AS awarded_points,
    COALESCE(SUM(max_points), 0) AS max_points,
    COUNT(*) FILTER (WHERE status IN ('WARN','FAIL')) AS gap_count,
    COUNT(*) FILTER (WHERE status = 'FAIL') AS fail_count,
    COUNT(*) FILTER (WHERE status = 'PARTIAL') AS partial_count
  FROM rows
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN round(awarded_points * 100.0 / NULLIF(max_points, 0)) < 55 THEN 'critical' WHEN round(awarded_points * 100.0 / NULLIF(max_points, 0)) < 75 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Performance Control Plane</div><div class="card-value">' ||
    round(awarded_points * 100.0 / NULLIF(max_points, 0))::int || '/100</div><div class="card-sub">' ||
    CASE
      WHEN round(awarded_points * 100.0 / NULLIF(max_points, 0)) >= 75 THEN 'Gold'
      WHEN round(awarded_points * 100.0 / NULLIF(max_points, 0)) >= 55 THEN 'Silver'
      ELSE 'Bronze'
    END || ' readiness for confident diagnosis</div></div>' ||
  '<div class="card ' || CASE WHEN fail_count > 0 THEN 'critical' WHEN gap_count > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Gaps</div><div class="card-value">' || gap_count || '</div><div class="card-sub">Controls that reduce diagnostic confidence today</div></div>' ||
  '<div class="card ' || CASE WHEN partial_count > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Partial Signals</div><div class="card-value">' || partial_count || '</div><div class="card-sub">Telemetry that is useful but not fully production-grade yet</div></div>' ||
  '<div class="card"><div class="card-label">Scoring Basis</div><div class="card-value">' || awarded_points || ' / ' || max_points || '</div><div class="card-sub">Weighted telemetry completeness for performance triage</div></div>' ||
  '</div>'
FROM summary;
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Capability</th><th>Status</th><th>Observed</th><th>Points</th><th>Why it Matters</th>'
\qecho '</tr></thead><tbody>'
WITH caps AS (
  SELECT
    COALESCE(current_setting('compute_query_id', true), 'auto') AS compute_query_id,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 1 ELSE 0 END AS has_pgss,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    COALESCE(current_setting('pg_stat_statements.track', true), 'top') AS pgss_track,
    COALESCE(current_setting('pg_stat_statements.track_planning', true), 'off') AS pgss_track_planning,
    COALESCE(current_setting('track_activity_query_size', true), '0') AS track_activity_query_size,
    COALESCE(current_setting('track_activities', true), 'on') AS track_activities,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('track_wal_io_timing', true), 'off') AS track_wal_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement,
    COALESCE(current_setting('log_lock_waits', true), 'off') AS log_lock_waits,
    COALESCE(current_setting('log_temp_files', true), '-1') AS log_temp_files,
    COALESCE(current_setting('log_autovacuum_min_duration', true), '-1') AS log_autovacuum_min_duration,
    CASE
      WHEN current_setting('auto_explain.log_min_duration', true) IS NOT NULL
        OR COALESCE(current_setting('shared_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('session_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('local_preload_libraries', true), '') ILIKE '%auto_explain%'
      THEN 1 ELSE 0
    END AS has_auto_explain,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = current_user
          AND (rolsuper OR pg_has_role(current_user, 'pg_monitor', 'MEMBER') OR pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER'))
      ) THEN 1 ELSE 0
    END AS broad_diag
), rows AS (
  SELECT 1 AS ord, 'Query identity'::text AS capability, 'compute_query_id=' || compute_query_id AS observed,
         CASE WHEN compute_query_id IN ('on','auto') THEN 'PASS' ELSE 'WARN' END AS status,
         CASE WHEN compute_query_id IN ('on','auto') THEN '12 / 12' ELSE '0 / 12' END AS points_awarded,
         'Needed for correlating SQL, waits, logs, and deep-dive history.' AS impact
  FROM caps
  UNION ALL
  SELECT 2, 'Statement statistics',
         'installed=' || CASE WHEN has_pgss = 1 THEN 'yes' ELSE 'no' END || ', preload=' || CASE WHEN shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'yes' ELSE 'no' END,
         CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'PASS'
           WHEN has_pgss = 1 THEN 'PARTIAL'
           ELSE 'FAIL'
         END,
         CASE
           WHEN has_pgss = 1 AND shared_preload_libraries ILIKE '%pg_stat_statements%' THEN '15 / 15'
           WHEN has_pgss = 1 THEN '8 / 15'
           ELSE '0 / 15'
         END,
         'Enables top-query ranking, drift analysis, and SQL attribution.'
  FROM caps
  UNION ALL
  SELECT 3, 'Nested statement capture',
         'pg_stat_statements.track=' || pgss_track,
         CASE WHEN pgss_track = 'all' THEN 'PASS' ELSE 'INFO' END,
         CASE WHEN pgss_track = 'all' THEN '4 / 4' ELSE '0 / 4' END,
         'Important for stored-procedure heavy or nested-SQL workloads.'
  FROM caps
  UNION ALL
  SELECT 4, 'Current SQL visibility',
         'track_activity_query_size=' || track_activity_query_size,
         CASE WHEN track_activity_query_size ~ '^[0-9]+$' AND track_activity_query_size::int >= 2048 THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN track_activity_query_size ~ '^[0-9]+$' AND track_activity_query_size::int >= 2048 THEN '6 / 6' ELSE '0 / 6' END,
         'Prevents important query text from being truncated during incidents.'
  FROM caps
  UNION ALL
  SELECT 5, 'Wait telemetry',
         'track_activities=' || track_activities || ', broad_stats=' || CASE WHEN broad_diag = 1 THEN 'yes' ELSE 'no' END,
         CASE
           WHEN track_activities = 'on' AND broad_diag = 1 THEN 'PASS'
           WHEN track_activities = 'on' THEN 'PARTIAL'
           ELSE 'FAIL'
         END,
         CASE
           WHEN track_activities = 'on' AND broad_diag = 1 THEN '10 / 10'
           WHEN track_activities = 'on' THEN '6 / 10'
           ELSE '0 / 10'
         END,
         'Required for wait-centric triage, CPU-like detection, and blocker analysis.'
  FROM caps
  UNION ALL
  SELECT 6, 'I/O latency attribution',
         'track_io_timing=' || track_io_timing,
         CASE WHEN track_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN track_io_timing = 'on' THEN '8 / 8' ELSE '0 / 8' END,
         'Distinguishes busy I/O from slow I/O.'
  FROM caps
  UNION ALL
  SELECT 7, 'WAL latency attribution',
         'track_wal_io_timing=' || track_wal_io_timing,
         CASE WHEN track_wal_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN track_wal_io_timing = 'on' THEN '4 / 4' ELSE '0 / 4' END,
         'Explains WAL write and sync latency, not just volume.'
  FROM caps
  UNION ALL
  SELECT 8, 'Slow-query logging',
         'log_min_duration_statement=' || log_min_duration_statement,
         CASE WHEN log_min_duration_statement <> '-1' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_min_duration_statement <> '-1' THEN '8 / 8' ELSE '0 / 8' END,
         'Critical for forensics once cumulative statistics reset or roll over.'
  FROM caps
  UNION ALL
  SELECT 9, 'Lock-wait logging',
         'log_lock_waits=' || log_lock_waits,
         CASE WHEN log_lock_waits = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_lock_waits = 'on' THEN '6 / 6' ELSE '0 / 6' END,
         'Preserves blocker evidence after the incident has moved on.'
  FROM caps
  UNION ALL
  SELECT 10, 'Temp spill logging',
         'log_temp_files=' || log_temp_files,
         CASE WHEN log_temp_files <> '-1' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_temp_files <> '-1' THEN '5 / 5' ELSE '0 / 5' END,
         'Captures sorts, hashes, and spill-heavy plans that hurt latency.'
  FROM caps
  UNION ALL
  SELECT 11, 'Autovacuum logging',
         'log_autovacuum_min_duration=' || log_autovacuum_min_duration,
         CASE WHEN log_autovacuum_min_duration <> '-1' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_autovacuum_min_duration <> '-1' THEN '4 / 4' ELSE '0 / 4' END,
         'Makes maintenance debt visible instead of inferred only from symptoms.'
  FROM caps
  UNION ALL
  SELECT 12, 'Plan capture readiness',
         'auto_explain=' || CASE WHEN has_auto_explain = 1 THEN 'available' ELSE 'not ready' END || ', track_planning=' || pgss_track_planning,
         CASE
           WHEN has_auto_explain = 1 THEN 'PASS'
           WHEN pgss_track_planning = 'on' THEN 'PARTIAL'
           ELSE 'WARN'
         END,
         CASE
           WHEN has_auto_explain = 1 THEN '8 / 8'
           WHEN pgss_track_planning = 'on' THEN '4 / 8'
           ELSE '0 / 8'
         END,
         'Lets PG360 move from Top SQL ranking into “why this query is slow.”'
  FROM caps
)
SELECT string_agg(
  '<tr><td>' || capability || '</td><td class="' ||
  CASE status
    WHEN 'PASS' THEN 'good">PASS'
    WHEN 'PARTIAL' THEN 'warn">PARTIAL'
    WHEN 'FAIL' THEN 'crit">FAIL'
    ELSE 'warn">WARN'
  END || '</td><td>' ||
  replace(replace(replace(replace(replace(observed,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td><td>' || points_awarded || '</td><td>' || impact || '</td></tr>',
  E'\n' ORDER BY ord
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Advanced Statistics and Progress Coverage</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Capability</th><th>Status</th><th>Observed</th><th>Operational Value</th>'
\qecho '</tr></thead><tbody>'
WITH caps AS (
  SELECT
    current_setting('server_version_num')::int AS vnum,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'),'off') AS track_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_wal_io_timing'),'off') AS track_wal_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'compute_query_id'),'auto') AS compute_query_id
), rows AS (
  SELECT 1 AS ord, 'pg_stat_io'::text AS capability,
         CASE WHEN vnum >= 160000 AND to_regclass('pg_catalog.pg_stat_io') IS NOT NULL THEN 'OK' ELSE 'LIMITED' END AS status,
         CASE WHEN vnum >= 160000 AND to_regclass('pg_catalog.pg_stat_io') IS NOT NULL THEN 'View available' ELSE 'Not available on this branch / visibility envelope' END AS observed,
         'Per-context I/O pressure, backend writes, and client-backend fsync detection.' AS value
  FROM caps
  UNION ALL
  SELECT 2, 'pg_stat_checkpointer',
         CASE WHEN to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL THEN 'OK' ELSE 'LIMITED' END,
         CASE WHEN to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL THEN 'View available' ELSE 'Unavailable' END,
         'Checkpoint cadence, sync time, and forced-checkpoint pressure.'
  FROM caps
  UNION ALL
  SELECT 3, 'pg_stat_wal',
         CASE WHEN to_regclass('pg_catalog.pg_stat_wal') IS NOT NULL THEN 'OK' ELSE 'LIMITED' END,
         CASE WHEN to_regclass('pg_catalog.pg_stat_wal') IS NOT NULL THEN 'View available' ELSE 'Unavailable' END,
         'WAL generation, full-page image ratio, and write amplification.'
  FROM caps
  UNION ALL
  SELECT 4, 'track_io_timing',
         CASE WHEN track_io_timing = 'on' THEN 'OK' ELSE 'WARN' END,
         track_io_timing,
         'Required for timing fidelity in I/O-heavy investigations.'
  FROM caps
  UNION ALL
  SELECT 5, 'track_wal_io_timing',
         CASE WHEN track_wal_io_timing = 'on' THEN 'OK' ELSE 'WARN' END,
         track_wal_io_timing,
         'Required for WAL latency attribution in pg_stat_io where the object is wal.'
  FROM caps
  UNION ALL
  SELECT 6, 'compute_query_id',
         CASE WHEN compute_query_id IN ('on','auto') THEN 'OK' ELSE 'WARN' END,
         compute_query_id,
         'Improves query identity stability for pg_stat_statements and deep-dive analysis.'
  FROM caps
  UNION ALL
  SELECT 7, 'VACUUM / ANALYZE / CREATE INDEX progress views',
         CASE
           WHEN to_regclass('pg_catalog.pg_stat_progress_vacuum') IS NOT NULL
            AND to_regclass('pg_catalog.pg_stat_progress_create_index') IS NOT NULL
            AND to_regclass('pg_catalog.pg_stat_progress_analyze') IS NOT NULL
           THEN 'OK' ELSE 'LIMITED'
         END,
         'vacuum=' || CASE WHEN to_regclass('pg_catalog.pg_stat_progress_vacuum') IS NOT NULL THEN 'yes' ELSE 'no' END ||
         ', create_index=' || CASE WHEN to_regclass('pg_catalog.pg_stat_progress_create_index') IS NOT NULL THEN 'yes' ELSE 'no' END ||
         ', analyze=' || CASE WHEN to_regclass('pg_catalog.pg_stat_progress_analyze') IS NOT NULL THEN 'yes' ELSE 'no' END,
         'Live progress for maintenance windows, vacuum stalls, and index builds.'
  FROM caps
)
SELECT string_agg(
  '<tr><td>' || capability || '</td><td class="' ||
  CASE status WHEN 'OK' THEN 'good">OK' WHEN 'WARN' THEN 'warn">WARN' ELSE 'warn">LIMITED' END ||
  '</td><td>' || observed || '</td><td>' || value || '</td></tr>',
  E'\n' ORDER BY ord
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Incident Triage Telemetry Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Telemetry Signal</th><th>Observed</th><th>Status</th><th>Why it Matters During Incidents</th>'
\qecho '</tr></thead><tbody>'
WITH caps AS (
  SELECT
    COALESCE(current_setting('track_activities', true), 'on') AS track_activities,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('track_wal_io_timing', true), 'off') AS track_wal_io_timing,
    COALESCE(current_setting('compute_query_id', true), 'auto') AS compute_query_id,
    current_setting('log_lock_waits', true) AS log_lock_waits,
    current_setting('deadlock_timeout', true) AS deadlock_timeout,
    COALESCE(current_setting('log_temp_files', true), '-1') AS log_temp_files,
    COALESCE(current_setting('log_autovacuum_min_duration', true), '-1') AS log_autovacuum_min_duration,
    current_setting('pg_stat_statements.track_planning', true) AS pgss_track_planning,
    current_setting('pg_stat_statements.track', true) AS pgss_track,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries
), rows AS (
  SELECT 1 AS ord, 'Session wait visibility'::text AS signal,
         'track_activities=' || track_activities AS observed,
         CASE WHEN track_activities = 'on' THEN 'OK' ELSE 'LIMITED' END AS status,
         CASE
           WHEN track_activities = 'on' THEN 'pg_stat_activity can expose current wait_event and wait_event_type, which is the front door for live incident triage.'
           ELSE 'Without track_activities, session-level wait visibility is materially reduced.'
         END AS why_it_matters
  FROM caps
  UNION ALL
  SELECT 2, 'Lock-wait traceability',
         'log_lock_waits=' || COALESCE(log_lock_waits, 'unknown') || ', deadlock_timeout=' || COALESCE(deadlock_timeout, 'unknown'),
         CASE
           WHEN log_lock_waits = 'on' AND deadlock_timeout IS NOT NULL THEN 'OK'
           WHEN log_lock_waits = 'on' THEN 'PARTIAL'
           ELSE 'WARN'
         END,
         CASE
           WHEN log_lock_waits = 'on' THEN 'Server logs can preserve blocker evidence once waits cross deadlock_timeout.'
           ELSE 'Blocking events are harder to reconstruct after the fact because the server is not logging lock waits.'
         END
  FROM caps
  UNION ALL
  SELECT 3, 'I/O timing telemetry',
         'track_io_timing=' || track_io_timing,
         CASE WHEN track_io_timing = 'on' THEN 'OK' ELSE 'WARN' END,
         CASE
           WHEN track_io_timing = 'on' THEN 'Read and write timings are available for I/O-heavy investigations.'
           ELSE 'I/O latency evidence loses timing fidelity, which weakens root-cause analysis for stalls and slow queries.'
         END
  FROM caps
  UNION ALL
  SELECT 4, 'WAL latency toggle',
         'track_wal_io_timing=' || track_wal_io_timing,
         CASE WHEN track_wal_io_timing = 'on' THEN 'OK' ELSE 'WARN' END,
         CASE
           WHEN track_wal_io_timing = 'on' THEN 'WAL write and sync timing can be attributed directly when pg_stat_io reports WAL activity.'
           ELSE 'WAL latency remains volume-only unless this timing control is enabled.'
         END
  FROM caps
  UNION ALL
  SELECT 5, 'I/O context coverage',
         CASE WHEN to_regclass('pg_catalog.pg_stat_io') IS NOT NULL THEN 'pg_stat_io available' ELSE 'pg_stat_io unavailable' END,
         CASE WHEN to_regclass('pg_catalog.pg_stat_io') IS NOT NULL THEN 'OK' ELSE 'LIMITED' END,
         CASE
           WHEN to_regclass('pg_catalog.pg_stat_io') IS NOT NULL THEN 'Per-context I/O counters help separate backend pressure from checkpointer, autovacuum, and WAL activity.'
           ELSE 'Client-backend writes, fsync pressure, and per-context I/O patterns are harder to identify on this branch or platform.'
         END
  FROM caps
  UNION ALL
  SELECT 6, 'WAL timing coverage',
         CASE
           WHEN to_regclass('pg_catalog.pg_stat_wal') IS NULL THEN 'pg_stat_wal unavailable'
           ELSE 'pg_stat_wal available; timing columns=' ||
             CASE
               WHEN EXISTS (
                 SELECT 1
                 FROM information_schema.columns
                 WHERE table_schema = 'pg_catalog'
                   AND table_name = 'pg_stat_wal'
                   AND column_name IN ('wal_write_time','wal_sync_time')
               ) THEN 'present'
               ELSE 'missing'
             END
         END,
         CASE
           WHEN to_regclass('pg_catalog.pg_stat_wal') IS NULL THEN 'LIMITED'
           WHEN EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'pg_catalog'
               AND table_name = 'pg_stat_wal'
               AND column_name IN ('wal_write_time','wal_sync_time')
           ) THEN 'OK'
           ELSE 'PARTIAL'
         END,
         CASE
           WHEN to_regclass('pg_catalog.pg_stat_wal') IS NULL THEN 'WAL generation can still be inferred from other signals, but direct timing and write evidence is limited.'
           WHEN EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'pg_catalog'
               AND table_name = 'pg_stat_wal'
               AND column_name IN ('wal_write_time','wal_sync_time')
           ) THEN 'WAL write and sync timing can be correlated with commit latency, checkpoints, and replica stress.'
           ELSE 'WAL counters are present, but timing detail is not available on this branch or platform.'
         END
  FROM caps
  UNION ALL
  SELECT 7, 'SQL fingerprint telemetry',
         'pg_stat_statements=' ||
           CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'installed' ELSE 'missing' END ||
           ', compute_query_id=' || compute_query_id,
         CASE
           WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
            AND compute_query_id IN ('on','auto') THEN 'OK'
           WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'PARTIAL'
           ELSE 'MISSING'
         END,
         CASE
           WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
            AND compute_query_id IN ('on','auto') THEN 'Normalized SQL telemetry is ready for ranking, drift analysis, and cross-run comparison.'
           WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'pg_stat_statements is installed, but query identity stability should be verified.'
           ELSE 'Top-SQL analysis, workload attribution, and deep-dive drilldowns lose fidelity without pg_stat_statements.'
         END
  FROM caps
  UNION ALL
  SELECT 8, 'Plan telemetry detail',
         'pg_stat_statements.track=' || COALESCE(pgss_track, 'not exposed') ||
         ', track_planning=' || COALESCE(pgss_track_planning, 'not exposed') ||
         CASE WHEN shared_preload_libraries ILIKE '%auto_explain%' THEN ', auto_explain=preloaded' ELSE '' END,
         CASE
           WHEN pgss_track_planning = 'on' THEN 'OK'
           WHEN pgss_track_planning IS NULL THEN 'LIMITED'
           ELSE 'INFO'
         END,
         CASE
           WHEN pgss_track_planning = 'on' THEN 'Planning time is included in SQL telemetry, which helps isolate plan-shape regressions.'
           WHEN pgss_track_planning IS NULL THEN 'The platform does not expose pg_stat_statements.track_planning directly.'
           ELSE 'Execution statistics are available, but planning detail is lighter unless you enable planning telemetry or targeted auto_explain.'
         END
  FROM caps
  UNION ALL
  SELECT 9, 'Forensic logging depth',
         'log_temp_files=' || COALESCE(log_temp_files, 'unknown') || ', log_autovacuum_min_duration=' || COALESCE(log_autovacuum_min_duration, 'unknown'),
         CASE
           WHEN log_temp_files <> '-1' AND log_autovacuum_min_duration <> '-1' THEN 'OK'
           WHEN log_temp_files <> '-1' OR log_autovacuum_min_duration <> '-1' THEN 'PARTIAL'
           ELSE 'WARN'
         END,
         CASE
           WHEN log_temp_files <> '-1' AND log_autovacuum_min_duration <> '-1' THEN 'Spill and maintenance events will leave log evidence outside stats reset windows.'
           WHEN log_temp_files <> '-1' OR log_autovacuum_min_duration <> '-1' THEN 'Only part of the performance-forensics trail is preserved in logs.'
           ELSE 'Spill and autovacuum events are harder to explain after the fact because logging is disabled.'
         END
  FROM caps
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || signal || '</td><td>' ||
    replace(replace(replace(replace(replace(observed,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td class="' ||
    CASE status
      WHEN 'OK' THEN 'good">OK'
      WHEN 'PARTIAL' THEN 'warn">PARTIAL'
      WHEN 'WARN' THEN 'warn">WARN'
      WHEN 'LIMITED' THEN 'warn">LIMITED'
      WHEN 'MISSING' THEN 'crit">MISSING'
      ELSE '">INFO'
    END ||
    '</td><td>' || why_it_matters || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="4" class="table-empty">Incident telemetry posture unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Plan Capture Readiness</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed</th><th>Status</th><th>Recommended Use</th>'
\qecho '</tr></thead><tbody>'
WITH cfg AS (
  SELECT
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    current_setting('auto_explain.log_min_duration', true) AS auto_explain_log_min_duration,
    current_setting('auto_explain.log_analyze', true) AS auto_explain_log_analyze,
    current_setting('auto_explain.log_timing', true) AS auto_explain_log_timing,
    current_setting('auto_explain.log_buffers', true) AS auto_explain_log_buffers,
    current_setting('auto_explain.sample_rate', true) AS auto_explain_sample_rate,
    current_setting('pg_stat_statements.track_planning', true) AS pgss_track_planning,
    current_setting('pg_stat_statements.track', true) AS pgss_track
), rows AS (
  SELECT 1 AS ord, 'auto_explain availability'::text AS signal,
         CASE
           WHEN auto_explain_log_min_duration IS NOT NULL OR shared_preload_libraries ILIKE '%auto_explain%' THEN 'ready'
           ELSE 'not ready'
         END AS observed,
         CASE
           WHEN auto_explain_log_min_duration IS NOT NULL OR shared_preload_libraries ILIKE '%auto_explain%' THEN 'PASS'
           ELSE 'WARN'
         END AS status,
         'Use a targeted role or session baseline before enabling broad plan capture.' AS recommended_use
  FROM cfg
  UNION ALL
  SELECT 2, 'auto_explain.log_min_duration',
         COALESCE(auto_explain_log_min_duration, 'not exposed'),
         CASE WHEN auto_explain_log_min_duration IS NULL THEN 'WARN' ELSE 'INFO' END,
         'Start with 500ms to 1000ms for production-safe slow-plan capture.'
  FROM cfg
  UNION ALL
  SELECT 3, 'auto_explain.log_analyze',
         COALESCE(auto_explain_log_analyze, 'not exposed'),
         CASE WHEN auto_explain_log_analyze = 'on' THEN 'INFO' ELSE 'WARN' END,
         'If you enable this, keep log_timing off first to limit overhead.'
  FROM cfg
  UNION ALL
  SELECT 4, 'auto_explain.log_timing',
         COALESCE(auto_explain_log_timing, 'not exposed'),
         CASE WHEN auto_explain_log_timing = 'off' THEN 'PASS' ELSE 'WARN' END,
         'Off is the safer starting point when collecting plans under load.'
  FROM cfg
  UNION ALL
  SELECT 5, 'auto_explain.log_buffers',
         COALESCE(auto_explain_log_buffers, 'not exposed'),
         CASE WHEN auto_explain_log_buffers = 'on' THEN 'PASS' ELSE 'INFO' END,
         'Helpful for proving whether a slow plan is cache-heavy, read-heavy, or spill-prone.'
  FROM cfg
  UNION ALL
  SELECT 6, 'auto_explain.sample_rate',
         COALESCE(auto_explain_sample_rate, 'not exposed'),
         CASE WHEN auto_explain_sample_rate IS NULL THEN 'WARN' ELSE 'INFO' END,
         'Use a sample rate below 1.0 if the workload is too busy for full capture.'
  FROM cfg
  UNION ALL
  SELECT 7, 'pg_stat_statements.track_planning',
         COALESCE(pgss_track_planning, 'not exposed'),
         CASE WHEN pgss_track_planning = 'on' THEN 'PASS' ELSE 'INFO' END,
         'Planning telemetry improves regression detection even without full plan capture.'
  FROM cfg
  UNION ALL
  SELECT 8, 'pg_stat_statements.track',
         COALESCE(pgss_track, 'not exposed'),
         CASE WHEN pgss_track = 'all' THEN 'PASS' ELSE 'INFO' END,
         'Use all when nested statements matter more than the extra cardinality.'
  FROM cfg
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || signal || '</td><td>' ||
    replace(replace(replace(replace(replace(observed,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td class="' ||
    CASE status WHEN 'PASS' THEN 'good">PASS' WHEN 'WARN' THEN 'warn">WARN' ELSE '">INFO' END ||
    '</td><td>' || recommended_use || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="4" class="table-empty">Plan-capture readiness unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Observability Gaps</div>'
\qecho '<div class="module-index"><div class="index-grid">'
\qecho '  <a class="index-card" href="#s29"><span class="idx-title">Extension Inventory</span><span class="idx-desc">Installed extensions, monitoring readiness, and hardening guidance.</span></a>'
\qecho '  <a class="index-card" href="#s23"><span class="idx-title">Configuration Audit</span><span class="idx-desc">Logging, timing, and parameter visibility evidence.</span></a>'
\qecho '  <a class="index-card" href="#s00"><span class="idx-title">Environment &amp; Instance</span><span class="idx-desc">Privilege and runtime context affecting completeness.</span></a>'
\qecho '</div></div>'
\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- MODULE M05: WORKLOAD CHARACTERIZATION
-- =============================================================================
\qecho '<div class="section" id="m05">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">5</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Workload Characterization</div>'
\qecho '    <div class="section-desc">Describe the live workload before tuning: throughput shape, read/write balance, concurrency, and spill pressure.</div>'
\qecho '  </div>'
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload Snapshot</div>'
WITH d AS (
  SELECT *
  FROM pg_stat_database
  WHERE datname = current_database()
), a AS (
  SELECT
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid()) AS total_sessions,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state = 'active') AS active_sessions,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state = 'idle in transaction') AS idle_tx_sessions
  FROM pg_stat_activity
  WHERE datname = current_database()
), w AS (
  SELECT
    COALESCE(EXTRACT(epoch FROM (now() - stats_reset)), 0) AS stats_window_seconds
  FROM pg_stat_database
  WHERE datname = current_database()
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Read / Write Ratio</div><div class="card-value">' ||
    COALESCE(round(d.tup_fetched::numeric / NULLIF(d.tup_inserted + d.tup_updated + d.tup_deleted, 0), 2)::text, 'N/A') || '</div></div>' ||
  '<div class="card"><div class="card-label">TPS Estimate</div><div class="card-value">' ||
    COALESCE(round((d.xact_commit + d.xact_rollback)::numeric / NULLIF(w.stats_window_seconds, 0), 2)::text, 'N/A') || '</div><div class="card-sub">Based on stats window since reset</div></div>' ||
  '<div class="card ' || CASE WHEN d.temp_files > 500 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Temp Files</div><div class="card-value">' || d.temp_files || '</div><div class="card-sub">' || pg_size_pretty(d.temp_bytes) || '</div></div>' ||
  '<div class="card"><div class="card-label">Sessions</div><div class="card-value">' || a.total_sessions || '</div><div class="card-sub">Active: ' || a.active_sessions || ', Idle in tx: ' || a.idle_tx_sessions || '</div></div>' ||
  '<div class="card ' || CASE WHEN a.idle_tx_sessions > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Workload Shape</div><div class="card-value">' ||
    CASE
      WHEN (d.tup_fetched::numeric / NULLIF(d.tup_inserted + d.tup_updated + d.tup_deleted, 0)) > 10 THEN 'OLTP / READ'
      WHEN d.temp_files > 100 THEN 'ANALYTICAL'
      ELSE 'MIXED'
    END || '</div></div>' ||
  '<div class="card"><div class="card-label">Workload Drill Path</div><div class="card-value">SQL / Waits / Connections</div><div class="card-sub">Use linked evidence modules below</div></div>' ||
  '</div>'
FROM d, a, w;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Wait and Timeout Pressure Snapshot</div>'
WITH a AS (
  SELECT
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state <> 'idle' AND wait_event IS NOT NULL) AS waiting_sessions,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock') AS lock_waiters,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND wait_event_type = 'IO') AS io_waiters,
    COUNT(*) FILTER (
      WHERE pid <> pg_backend_pid()
        AND (
          wait_event ILIKE 'wal%'
          OR wait_event ILIKE '%wal%'
        )
    ) AS wal_waiters,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state = 'idle in transaction') AS idle_tx_sessions,
    COUNT(*) FILTER (
      WHERE pid <> pg_backend_pid()
        AND state = 'active'
        AND query_start IS NOT NULL
        AND query_start < now() - interval '5 minutes'
    ) AS long_active_sessions
  FROM pg_stat_activity
  WHERE datname = current_database()
), cfg AS (
  SELECT
    current_setting('statement_timeout', true) AS statement_timeout,
    current_setting('lock_timeout', true) AS lock_timeout,
    current_setting('idle_in_transaction_session_timeout', true) AS idle_tx_timeout
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN waiting_sessions > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Waiting Sessions</div><div class="card-value">' || waiting_sessions || '</div><div class="card-sub">All active waits visible in pg_stat_activity</div></div>' ||
  '<div class="card ' || CASE WHEN lock_waiters > 0 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Lock Waiters</div><div class="card-value">' || lock_waiters || '</div><div class="card-sub">Sessions currently blocked on locks</div></div>' ||
  '<div class="card ' || CASE WHEN io_waiters > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">I/O Waiters</div><div class="card-value">' || io_waiters || '</div><div class="card-sub">Backends waiting on data file or other I/O</div></div>' ||
  '<div class="card ' || CASE WHEN wal_waiters > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">WAL-Related Waiters</div><div class="card-value">' || wal_waiters || '</div><div class="card-sub">WAL write, sync, or WAL-related waits</div></div>' ||
  '<div class="card ' || CASE WHEN idle_tx_sessions > 0 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Idle in Transaction</div><div class="card-value">' || idle_tx_sessions || '</div><div class="card-sub">Stale sessions can hold locks and old snapshots</div></div>' ||
  '<div class="card ' || CASE WHEN long_active_sessions > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Long Active Sessions</div><div class="card-value">' || long_active_sessions || '</div><div class="card-sub">Running longer than five minutes</div></div>' ||
  '<div class="card ' ||
    CASE
      WHEN COALESCE(lock_timeout, '0') ~ '^0($|ms$|s$)' AND COALESCE(idle_tx_timeout, '0') ~ '^0($|ms$|s$)' THEN 'warning'
      WHEN COALESCE(lock_timeout, '0') ~ '^0($|ms$|s$)' OR COALESCE(idle_tx_timeout, '0') ~ '^0($|ms$|s$)' OR COALESCE(statement_timeout, '0') ~ '^0($|ms$|s$)' THEN 'warning'
      ELSE 'good'
    END || '"><div class="card-label">Guardrail Posture</div><div class="card-value">' ||
    CASE
      WHEN COALESCE(lock_timeout, '0') ~ '^0($|ms$|s$)' AND COALESCE(idle_tx_timeout, '0') ~ '^0($|ms$|s$)' THEN 'OPEN'
      WHEN COALESCE(lock_timeout, '0') ~ '^0($|ms$|s$)' OR COALESCE(idle_tx_timeout, '0') ~ '^0($|ms$|s$)' OR COALESCE(statement_timeout, '0') ~ '^0($|ms$|s$)' THEN 'PARTIAL'
      ELSE 'SET'
    END || '</div><div class="card-sub">statement=' ||
    replace(replace(replace(replace(replace(COALESCE(statement_timeout,'n/a'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    ', lock=' || replace(replace(replace(replace(replace(COALESCE(lock_timeout,'n/a'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    ', idle_tx=' || replace(replace(replace(replace(replace(COALESCE(idle_tx_timeout,'n/a'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</div></div>' ||
  '</div>'
FROM a, cfg;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Wait Diagnosis Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Category</th><th>Sessions in Category</th><th>Likely Meaning</th><th>Next Evidence Pivot</th><th>Owner Bias</th><th>Blast Radius</th>'
\qecho '</tr></thead><tbody>'
WITH wait_counts AS (
  SELECT
    CASE
      WHEN state = 'active' AND wait_event_type IS NULL AND wait_event IS NULL THEN 'CPU-like active'
      WHEN wait_event_type = 'Lock' THEN 'Lock'
      WHEN wait_event_type = 'LWLock' THEN 'LWLock'
      WHEN wait_event_type = 'IO' THEN 'IO'
      WHEN wait_event_type = 'Client' THEN 'Client'
      WHEN wait_event_type = 'Timeout' THEN 'Timeout'
      ELSE COALESCE(wait_event_type, 'Other')
    END AS category,
    COUNT(*)::int AS active_sessions
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
    AND (state = 'active' OR wait_event IS NOT NULL)
  GROUP BY 1
), rows AS (
  SELECT 1 AS ord, 'CPU-like active'::text AS category,
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'CPU-like active'), 0) AS active_sessions,
         'Sessions are running without a visible wait. Focus on expensive SQL, plan regressions, connection storms, or CPU saturation.'::text AS meaning,
         '#s02'::text AS pivot,
         'App / DBA'::text AS owner_bias,
         'Query or service level; can become cluster-wide under concurrency'::text AS blast_radius
  UNION ALL
  SELECT 2, 'Lock',
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'Lock'), 0),
         'Heavyweight lock contention. Investigate blockers, long transactions, and idle-in-transaction sessions first.',
         '#s04',
         'App / DBA',
         'Multi-query and often multi-service once pileups begin'
  UNION ALL
  SELECT 3, 'LWLock',
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'LWLock'), 0),
         'Internal shared-memory contention. Often tied to concurrency bursts, WAL pressure, or backend coordination hotspots.',
         '#s07',
         'DBA / Platform',
         'Usually cluster-wide or subsystem-wide'
  UNION ALL
  SELECT 4, 'IO',
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'IO'), 0),
         'Backends are blocked on reads, writes, or related storage activity. Separate volume from latency before tuning.',
         '#s07',
         'DBA / Platform',
         'Cluster-wide if storage is slow; query-level if access path is poor'
  UNION ALL
  SELECT 5, 'Client',
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'Client'), 0),
         'The database is waiting on the client or network path. Check pooling, round trips, and application pacing.',
         '#s09',
         'App / Platform',
         'Usually service-level or client-path specific'
  UNION ALL
  SELECT 6, 'Timeout',
         COALESCE((SELECT active_sessions FROM wait_counts WHERE category = 'Timeout'), 0),
         'A timeout-related wait is active. Confirm whether this is healthy guardrail behavior or a symptom of deeper contention.',
         '#m02',
         'DBA / App',
         'Guardrail-driven; can hide deeper lock or client issues'
  UNION ALL
  SELECT 7, 'Other',
         COALESCE((SELECT SUM(active_sessions) FROM wait_counts WHERE category NOT IN ('CPU-like active','Lock','LWLock','IO','Client','Timeout')), 0),
         'Mixed waits outside the main diagnosis paths. Use the detailed wait-event evidence to isolate the dominant signal.',
         '#s03',
         'DBA',
         'Depends on dominant event'
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || category || '</td><td class="num ' || CASE WHEN active_sessions > 0 THEN 'warn' ELSE 'good' END || '">' ||
    active_sessions || '</td><td>' || meaning || '</td><td><a href="' || pivot || '">Open Evidence</a></td><td>' || owner_bias ||
    '</td><td>' || blast_radius || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="6" class="table-empty">Wait diagnosis matrix unavailable</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Historical Baseline and Drift</div>'
\if :pg360_has_history_db
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Current</th><th>Median Baseline</th><th>P90 Band</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'
WITH curr AS (
  SELECT
    COALESCE(round((xact_commit + xact_rollback)::numeric / NULLIF(EXTRACT(epoch FROM (now() - stats_reset)), 0), 2), NULL) AS curr_tps,
    COALESCE(temp_bytes::numeric, 0) AS curr_temp_bytes,
    COALESCE(temp_files::numeric, 0) AS curr_temp_files
  FROM pg_stat_database
  WHERE datname = current_database()
), curr_activity AS (
  SELECT
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid())::numeric AS curr_sessions,
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND state <> 'idle' AND wait_event IS NOT NULL)::numeric AS curr_waiters
  FROM pg_stat_activity
  WHERE datname = current_database()
), hist AS (
  SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.tps_estimate, 0)) AS med_tps,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(d.tps_estimate, 0)) AS p90_tps,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.temp_bytes, 0)::numeric) AS med_temp_bytes,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(d.temp_bytes, 0)::numeric) AS p90_temp_bytes,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.sessions_total, 0)::numeric) AS med_sessions,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(d.sessions_total, 0)::numeric) AS p90_sessions,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.waiters, 0)::numeric) AS med_waiters,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(d.waiters, 0)::numeric) AS p90_waiters
  FROM pg360_history.run_snapshot r
  JOIN pg360_history.db_snapshot d ON d.run_id = r.run_id
  WHERE r.dbname = current_database()
    AND r.captured_at >= now() - (:'pg360_history_days' || ' days')::interval
), rows AS (
  SELECT 'TPS estimate'::text AS signal,
         COALESCE(to_char(curr_tps, 'FM999,990.00'), 'N/A') AS current_val,
         COALESCE(to_char(med_tps, 'FM999,990.00'), 'N/A') AS median_val,
         COALESCE(to_char(p90_tps, 'FM999,990.00'), 'N/A') AS p90_val,
         CASE
           WHEN med_tps IS NULL THEN 'Repository has not collected enough runs yet'
           WHEN curr_tps > p90_tps THEN 'Current throughput is above the recent high-water mark'
           WHEN curr_tps < med_tps * 0.5 AND med_tps > 0 THEN 'Throughput is well below normal; confirm whether the system is intentionally quiet'
           ELSE 'Throughput is within the recent operating band'
         END AS interpretation
  FROM curr, hist
  UNION ALL
  SELECT 'Temp write volume',
         pg_size_pretty(curr_temp_bytes::bigint),
         CASE WHEN med_temp_bytes IS NULL THEN 'N/A' ELSE pg_size_pretty(med_temp_bytes::bigint) END,
         CASE WHEN p90_temp_bytes IS NULL THEN 'N/A' ELSE pg_size_pretty(p90_temp_bytes::bigint) END,
         CASE
           WHEN med_temp_bytes IS NULL THEN 'Repository has not collected enough runs yet'
           WHEN curr_temp_bytes > p90_temp_bytes AND curr_temp_bytes > 0 THEN 'Current spill volume is above the recent high-water mark'
           WHEN curr_temp_bytes > med_temp_bytes * 1.5 AND med_temp_bytes > 0 THEN 'Temp pressure is elevated versus baseline'
           ELSE 'Spill volume is within the recent operating band'
         END
  FROM curr, hist
  UNION ALL
  SELECT 'Sessions',
         to_char(curr_sessions, 'FM999,990'),
         COALESCE(to_char(med_sessions, 'FM999,990.00'), 'N/A'),
         COALESCE(to_char(p90_sessions, 'FM999,990.00'), 'N/A'),
         CASE
           WHEN med_sessions IS NULL THEN 'Repository has not collected enough runs yet'
           WHEN curr_sessions > p90_sessions THEN 'Connection footprint is above the recent high-water mark'
           ELSE 'Connection footprint is within the recent operating band'
         END
  FROM curr_activity, hist
  UNION ALL
  SELECT 'Waiting sessions',
         to_char(curr_waiters, 'FM999,990'),
         COALESCE(to_char(med_waiters, 'FM999,990.00'), 'N/A'),
         COALESCE(to_char(p90_waiters, 'FM999,990.00'), 'N/A'),
         CASE
           WHEN med_waiters IS NULL THEN 'Repository has not collected enough runs yet'
           WHEN curr_waiters > p90_waiters AND curr_waiters > 0 THEN 'Contention is above the recent high-water mark'
           WHEN curr_waiters > med_waiters AND curr_waiters > 0 THEN 'Wait pressure is elevated versus baseline'
           ELSE 'Wait pressure is within the recent operating band'
         END
  FROM curr_activity, hist
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || signal || '</td><td>' || current_val || '</td><td>' || median_val || '</td><td>' || p90_val || '</td><td>' || interpretation || '</td></tr>',
    E'\n'
  ),
  '<tr><td colspan="5" class="table-empty">No historical drift rows available</td></tr>'
) FROM rows;
\qecho '</tbody></table></div>'
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Historical workload drift unavailable</span><span class="severity-pill pill-info">HISTORY</span></div><div class="finding-body">Run the repository capture script on a schedule to compare current workload with median and p90 operating bands.</div></div>';
\endif
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Supporting Evidence</div>'
\qecho '<div class="module-index"><div class="index-grid">'
\qecho '  <a class="index-card" href="#s11"><span class="idx-title">Workload Profile &amp; Tuning</span><span class="idx-desc">Primary workload profile and parameter hints.</span></a>'
\qecho '  <a class="index-card" href="#s02"><span class="idx-title">Top SQL Analysis</span><span class="idx-desc">Heavy statements, latency, and spill pressure.</span></a>'
\qecho '  <a class="index-card" href="#s03"><span class="idx-title">Wait Events &amp; Sessions</span><span class="idx-desc">Wait distribution and long-running activity.</span></a>'
\qecho '  <a class="index-card" href="#s09"><span class="idx-title">Connections &amp; Pooling</span><span class="idx-desc">Connection utilization and client behavior.</span></a>'
\qecho '  <a class="index-card" href="#s22"><span class="idx-title">Connection Pooling Advisor</span><span class="idx-desc">Connection management interpretation.</span></a>'
\qecho '</div></div>'
\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- MODULE M18: PRIORITIZED REMEDIATION PLAN
-- =============================================================================
\qecho '<div class="section" id="m18">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">6</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Prioritized Remediation Plan</div>'
\qecho '    <div class="section-desc">Fix-first ordering across DBA, application, infrastructure, and security owners with direct evidence paths.</div>'
\qecho '  </div>'
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Remediation Priority Board</div>'
WITH action_counts AS (
  SELECT
    (CASE WHEN (SELECT age(datfrozenxid) FROM pg_database WHERE datname=current_database()) > 1500000000 THEN 1 ELSE 0 END) +
    (SELECT COUNT(*) FROM pg_replication_slots WHERE NOT active)::int +
    (SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND NOT (p.proconfig::text ILIKE '%search_path%') AND n.nspname NOT IN ('pg_catalog','information_schema'))::int AS critical_count,
    (SELECT COUNT(*) FROM pg_index WHERE NOT indisvalid)::int +
    (SELECT COUNT(*) FROM pg_stat_user_tables WHERE n_dead_tup > 100000)::int +
    (SELECT COUNT(*) FROM pg_stat_user_indexes WHERE idx_scan=0 AND pg_relation_size(indexrelid)>1048576)::int AS high_count,
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_min_duration_statement') = '-1' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='track_io_timing') = 'off' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_checkpoints') = 'off' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_lock_waits') = 'off' THEN 1 ELSE 0 END) AS medium_count
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN critical_count > 0 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Critical</div><div class="card-value">' || critical_count || '</div><div class="card-sub">Immediate outage / data-loss risk</div></div>' ||
  '<div class="card ' || CASE WHEN high_count > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">High</div><div class="card-value">' || high_count || '</div><div class="card-sub">Severe stability or performance issues</div></div>' ||
  '<div class="card"><div class="card-label">Medium</div><div class="card-value">' || medium_count || '</div><div class="card-sub">Operational inefficiencies and control gaps</div></div>' ||
  '<div class="card"><div class="card-label">Ownership Model</div><div class="card-value">DBA / App / Infra / Security</div><div class="card-sub">Each action is intended to be assignable</div></div>' ||
  '</div>'
FROM action_counts;
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Baseline-Driven Priority Escalation</div>'
\if :pg360_has_history_db
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Owner</th><th>Drift Signal</th><th>Current vs Baseline</th><th>Action Bias</th>'
\qecho '</tr></thead><tbody>'
WITH curr AS (
  SELECT
    COALESCE((SELECT temp_bytes::numeric FROM pg_stat_database WHERE datname = current_database()), 0) AS curr_temp_bytes,
    COALESCE((SELECT count(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle' AND wait_event IS NOT NULL), 0) AS curr_waiters,
    COALESCE((SELECT count(*)::numeric FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()), 0) AS curr_sessions,
    COALESCE((SELECT xact_commit + xact_rollback FROM pg_stat_database WHERE datname = current_database())::numeric, 0) AS curr_xacts
), hist AS (
  SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.temp_bytes, 0)::numeric) AS med_temp_bytes,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.waiters, 0)::numeric) AS med_waiters,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.sessions_total, 0)::numeric) AS med_sessions,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(d.tps_estimate, 0)) AS med_tps
  FROM pg360_history.run_snapshot r
  JOIN pg360_history.db_snapshot d ON d.run_id = r.run_id
  WHERE r.dbname = current_database()
    AND r.captured_at >= now() - (:'pg360_history_days' || ' days')::interval
), current_rate AS (
  SELECT
    COALESCE(round((xact_commit + xact_rollback)::numeric / NULLIF(EXTRACT(epoch FROM (now() - stats_reset)), 0), 2), 0) AS curr_tps
  FROM pg_stat_database
  WHERE datname = current_database()
), rows AS (
  SELECT 'DBA'::text AS owner,
         'Temp spill pressure'::text AS drift_signal,
         CASE WHEN COALESCE(med_temp_bytes, 0) = 0 THEN 'No baseline yet' ELSE to_char(round((curr_temp_bytes / NULLIF(med_temp_bytes, 0))::numeric, 2), 'FM999,990.00') || 'x median' END AS drift_ratio,
         CASE
           WHEN COALESCE(med_temp_bytes, 0) = 0 THEN 'Build baseline first'
           WHEN curr_temp_bytes > med_temp_bytes * 1.5 THEN 'Escalate sort/hash spill investigation and memory review'
           ELSE 'Keep as watch item'
         END AS action_bias
  FROM curr, hist
  UNION ALL
  SELECT 'Infra',
         'Wait pressure',
         CASE WHEN COALESCE(med_waiters, 0) = 0 THEN CASE WHEN curr_waiters = 0 THEN 'Stable' ELSE to_char(curr_waiters, 'FM999,990') || ' waiters' END ELSE to_char(round((curr_waiters / NULLIF(med_waiters, 0))::numeric, 2), 'FM999,990.00') || 'x median' END,
         CASE
           WHEN curr_waiters > COALESCE(med_waiters, 0) AND curr_waiters > 0 THEN 'Escalate concurrency and blocking analysis'
           ELSE 'Keep as watch item'
         END
  FROM curr, hist
  UNION ALL
  SELECT 'App',
         'Throughput drift',
         CASE WHEN COALESCE(med_tps, 0) = 0 THEN 'No baseline yet' ELSE to_char(round((curr_tps / NULLIF(med_tps, 0))::numeric, 2), 'FM999,990.00') || 'x median' END,
         CASE
           WHEN COALESCE(med_tps, 0) > 0 AND curr_tps < med_tps * 0.5 THEN 'Investigate app-side throughput drop and SQL regressions'
           WHEN COALESCE(med_tps, 0) > 0 AND curr_tps > med_tps * 1.5 THEN 'Validate whether the current load is planned or abnormal'
           ELSE 'Keep as watch item'
         END
  FROM current_rate, hist
  UNION ALL
  SELECT 'DBA / App',
         'Connection footprint',
         CASE WHEN COALESCE(med_sessions, 0) = 0 THEN 'No baseline yet' ELSE to_char(round((curr_sessions / NULLIF(med_sessions, 0))::numeric, 2), 'FM999,990.00') || 'x median' END,
         CASE
           WHEN COALESCE(med_sessions, 0) > 0 AND curr_sessions > med_sessions * 1.5 THEN 'Escalate pooling and session management review'
           ELSE 'Keep as watch item'
         END
  FROM curr, hist
)
SELECT string_agg(
  '<tr><td>' || owner || '</td><td>' || drift_signal || '</td><td>' || drift_ratio || '</td><td>' || action_bias || '</td></tr>',
  E'\n'
) FROM rows;
\qecho '</tbody></table></div>'
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Priority escalation is currently snapshot-only</span><span class="severity-pill pill-info">DIFF</span></div><div class="finding-body">Once repository captures exist, PG360 will escalate remediation based on drift from normal operating baselines rather than current-state thresholds alone.</div></div>';
\endif
\qecho '</div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Timeout, Wait, and Telemetry Priorities</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Severity</th><th>Owner</th><th>Finding</th><th>Blast Radius</th><th>Confidence</th><th>Apply Path</th><th>Recommended Action</th><th>Supporting Evidence</th>'
\qecho '</tr></thead><tbody>'
WITH facts AS (
  SELECT
    COALESCE((SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction'), 0) AS idle_tx_sessions,
    COALESCE((SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'), 0) AS lock_waiters,
    COALESCE((SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type IS NOT NULL AND state <> 'idle'), 0) AS waiters,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'lock_timeout'), '0') AS lock_timeout,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'idle_in_transaction_session_timeout'), '0') AS idle_tx_timeout,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'statement_timeout'), '0') AS statement_timeout,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_io_timing'), 'off') AS track_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'track_wal_io_timing'), 'off') AS track_wal_io_timing,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'compute_query_id'), 'auto') AS compute_query_id,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_lock_waits'), 'off') AS log_lock_waits,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_temp_files'), '-1') AS log_temp_files,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'log_autovacuum_min_duration'), '-1') AS log_autovacuum_min_duration,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS has_pgss,
    to_regclass('pg_catalog.pg_stat_io') IS NOT NULL AS has_pg_stat_io,
    to_regclass('pg_catalog.pg_stat_wal') IS NOT NULL AS has_pg_stat_wal,
    current_setting('pg_stat_statements.track_planning', true) AS pgss_track_planning,
    current_setting('pg_stat_statements.track', true) AS pgss_track,
    CASE
      WHEN current_setting('auto_explain.log_min_duration', true) IS NOT NULL
        OR COALESCE(current_setting('shared_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('session_preload_libraries', true), '') ILIKE '%auto_explain%'
        OR COALESCE(current_setting('local_preload_libraries', true), '') ILIKE '%auto_explain%'
      THEN true ELSE false
    END AS has_auto_explain,
    EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'pg_catalog'
        AND table_name = 'pg_stat_wal'
        AND column_name IN ('wal_write_time','wal_sync_time')
    ) AS has_wal_timing
), rows AS (
  SELECT 1 AS ord, 'High'::text AS severity, 'DBA / App'::text AS owner,
         'Idle transactions have no expiry guardrail'::text AS finding,
         'Cluster-wide over time'::text AS blast_radius,
         'High'::text AS confidence,
         'ALTER ROLE / ALTER DATABASE / SET'::text AS apply_path,
         'Set scoped idle_in_transaction_session_timeout for application roles or pool users, then identify and terminate forgotten idle transactions.'::text AS action,
         '#m02'::text AS evidence
  FROM facts
  WHERE idle_tx_sessions > 0
    AND idle_tx_timeout ~ '^0($|ms$|s$)'
  UNION ALL
  SELECT 2, 'High', 'DBA / App',
         'Lock waits can persist without lock_timeout',
         'Service or multi-service contention',
         'High',
         'ALTER ROLE / ALTER DATABASE / SET',
         'Set lock_timeout for online DDL, migration jobs, and latency-sensitive application paths so blockers fail fast instead of piling up.',
         '#s04'
  FROM facts
  WHERE lock_waiters > 0
    AND lock_timeout ~ '^0($|ms$|s$)'
  UNION ALL
  SELECT 3, 'Medium', 'DBA / App',
         'Long statements are not bounded by statement_timeout',
         'Query or workload class',
         'Medium',
         'ALTER ROLE / ALTER DATABASE / SET',
         'Use role or workload-specific statement_timeout values for risky paths rather than a blanket cluster-wide default.',
         '#m02'
  FROM facts
  WHERE waiters > 0
    AND statement_timeout ~ '^0($|ms$|s$)'
  UNION ALL
  SELECT 4, 'Medium', 'DBA / Infra',
         'I/O latency telemetry is incomplete',
         'Cluster-wide diagnosis quality',
         'High',
         'Instance parameter / provider flag',
         'Enable track_io_timing and review pg_stat_io visibility so buffer, backend-write, and fsync investigations have timing evidence.',
         '#m04'
  FROM facts
  WHERE track_io_timing <> 'on'
     OR NOT has_pg_stat_io
  UNION ALL
  SELECT 5, 'Medium', 'DBA / App',
         'SQL fingerprint telemetry is incomplete',
         'Cluster-wide SQL diagnosis quality',
         'High',
         'shared_preload_libraries + CREATE EXTENSION + role/session settings',
         'Install pg_stat_statements and keep compute_query_id on or auto so query fingerprints remain stable across reports and deep dives.',
         '#m04'
  FROM facts
  WHERE NOT has_pgss
     OR compute_query_id NOT IN ('on','auto')
  UNION ALL
  SELECT 6, 'Medium', 'Infra / DBA',
         'WAL timing coverage is limited',
         'Cluster-wide WAL diagnosis quality',
         'Medium',
         'Instance parameter / provider flag',
         'Use pg_stat_wal timing columns where available to correlate commit latency, WAL pressure, and checkpoint behavior during incidents.',
         '#s08'
  FROM facts
  WHERE NOT has_pg_stat_wal
     OR NOT has_wal_timing
     OR track_wal_io_timing <> 'on'
  UNION ALL
  SELECT 7, 'Medium', 'DBA / Infra',
         'Blocking incidents are harder to reconstruct from logs',
         'Cluster-wide forensics',
         'High',
         'Logging parameter / provider flag',
         'Enable log_lock_waits and keep deadlock_timeout intentional so lock chains leave evidence even after sessions disappear.',
         '#m04'
  FROM facts
  WHERE log_lock_waits <> 'on'
  UNION ALL
  SELECT 8, 'Medium', 'DBA / Platform',
         'Temp spill forensics are incomplete',
         'Query to cluster-wide forensics',
         'High',
         'Logging parameter / provider flag',
         'Enable log_temp_files so sort, hash, and intermediate spill events can be tied back to work_mem and query-shape decisions.',
         '#s02'
  FROM facts
  WHERE log_temp_files = '-1'
  UNION ALL
  SELECT 9, 'Medium', 'DBA',
         'Autovacuum incident logging is incomplete',
         'Table to cluster-wide maintenance visibility',
         'High',
         'Logging parameter / provider flag',
         'Enable log_autovacuum_min_duration so skipped work, long vacuums, and cleanup debt leave evidence during incidents.',
         '#s10'
  FROM facts
  WHERE log_autovacuum_min_duration = '-1'
  UNION ALL
  SELECT 10, 'Medium', 'DBA',
         'Plan capture for slow SQL is not ready',
         'Query-level to service-level diagnosis quality',
         CASE WHEN pgss_track_planning = 'on' THEN 'Medium' ELSE 'High' END,
         'shared_preload_libraries / session preload / targeted role settings',
         'Prepare an auto_explain safe baseline and keep pg_stat_statements.track_planning enabled so PG360 can move from ranking to explainable plan behavior.',
         '#m04'
  FROM facts
  WHERE NOT has_auto_explain
     OR COALESCE(pgss_track, 'top') <> 'all'
)
SELECT COALESCE(
  string_agg(
    '<tr><td><span class="severity-pill ' ||
      CASE severity WHEN 'High' THEN 'pill-high' WHEN 'Medium' THEN 'pill-medium' ELSE 'pill-info' END ||
      '">' || severity || '</span></td><td>' || owner || '</td><td>' || finding || '</td><td>' || blast_radius ||
      '</td><td>' || confidence || '</td><td>' || apply_path || '</td><td>' || action ||
      '</td><td><a href="' || evidence || '">Open Evidence</a></td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="8" class="table-empty">No timeout, wait, or telemetry actions crossed the remediation thresholds in this run.</td></tr>'
) FROM rows;
\qecho '</tbody></table></div></div>'
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Fix-First Routing</div>'
\qecho '<div class="module-index"><div class="index-grid">'
\qecho '  <a class="index-card" href="#s28"><span class="idx-title">Legacy Action Plan Detail</span><span class="idx-desc">Existing detailed remediation queue and scripts.</span></a>'
\qecho '  <a class="index-card" href="#s21"><span class="idx-title">Autovacuum Full Advisor</span><span class="idx-desc">DBA-owned maintenance and XID safety actions.</span></a>'
\qecho '  <a class="index-card" href="#s08"><span class="idx-title">WAL &amp; Replication</span><span class="idx-desc">Infra-owned replication and slot safety actions.</span></a>'
\qecho '  <a class="index-card" href="#s25"><span class="idx-title">Security &amp; Access Review</span><span class="idx-desc">Security-owned privilege, membership, and access actions.</span></a>'
\qecho '  <a class="index-card" href="#s02"><span class="idx-title">Top SQL Analysis</span><span class="idx-desc">Application-owned SQL and call-pattern actions.</span></a>'
\qecho '</div></div>'
\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S00: ENVIRONMENT
-- =============================================================================
\qecho '<div class="section" id="s00">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Environment & Instance</div>'
\qecho '    <div class="section-desc">Identity fingerprint, uptime stability, platform detection, extension posture, configuration snapshot, and diagnostic completeness.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S00.1 Instance Fingerprint
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Instance Fingerprint (Identity &amp; Build)</div>'

WITH fp AS (
  SELECT
    version() AS full_version,
    current_setting('server_version_num', true) AS server_version_num,
    CASE
      WHEN version() ~ ' on [^,]+,' THEN regexp_replace(version(), '.* on ([^,]+),.*', '\1')
      ELSE 'unknown'
    END AS build_platform,
    CASE
      WHEN version() ~ ' compiled by [^,]+,' THEN regexp_replace(version(), '.* compiled by ([^,]+),.*', '\1')
      ELSE 'unknown'
    END AS compiler,
    current_setting('data_directory', true) AS data_directory,
    current_setting('config_file', true) AS config_file,
    current_setting('hba_file', true) AS hba_file,
    current_setting('ident_file', true) AS ident_file,
    NULLIF(current_setting('cluster_name', true), '') AS cluster_name,
    current_setting('TimeZone', true) AS timezone_name,
    (SELECT datcollate FROM pg_database WHERE datname = current_database()) AS lc_collate_name,
    (SELECT datctype FROM pg_database WHERE datname = current_database()) AS lc_ctype_name,
    current_setting('shared_preload_libraries', true) AS shared_preload_libraries,
    current_setting('default_transaction_isolation', true) AS default_tx_isolation,
    current_setting('default_transaction_read_only', true) AS default_tx_read_only,
    current_setting('track_commit_timestamp', true) AS track_commit_timestamp
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Server Version</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(split_part(full_version,' ',2),'unknown'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">server_version_num</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(server_version_num,'unknown'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Build Platform</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(build_platform,'unknown'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Compiler</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(compiler,'unknown'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '</div>'
FROM fp;

\qecho '<div class="finding-title">Instance Identity Keys</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Key</th><th>Value</th>'
\qecho '</tr></thead><tbody>'

WITH fp AS (
  SELECT
    version() AS full_version,
    current_setting('server_version_num', true) AS server_version_num,
    CASE
      WHEN version() ~ ' on [^,]+,' THEN regexp_replace(version(), '.* on ([^,]+),.*', '\1')
      ELSE 'unknown'
    END AS build_platform,
    CASE
      WHEN version() ~ ' compiled by [^,]+,' THEN regexp_replace(version(), '.* compiled by ([^,]+),.*', '\1')
      ELSE 'unknown'
    END AS compiler,
    current_setting('data_directory', true) AS data_directory,
    current_setting('config_file', true) AS config_file,
    current_setting('hba_file', true) AS hba_file,
    current_setting('ident_file', true) AS ident_file,
    NULLIF(current_setting('cluster_name', true), '') AS cluster_name,
    current_setting('TimeZone', true) AS timezone_name,
    (SELECT datcollate FROM pg_database WHERE datname = current_database()) AS lc_collate_name,
    (SELECT datctype FROM pg_database WHERE datname = current_database()) AS lc_ctype_name,
    current_setting('shared_preload_libraries', true) AS shared_preload_libraries,
    current_setting('default_transaction_isolation', true) AS default_tx_isolation,
    current_setting('default_transaction_read_only', true) AS default_tx_read_only,
    current_setting('track_commit_timestamp', true) AS track_commit_timestamp
),
kv AS (
  SELECT 1 AS ord, 'PostgreSQL version (full)' AS k, full_version AS v FROM fp
  UNION ALL SELECT 2, 'server_version_num', server_version_num FROM fp
  UNION ALL SELECT 3, 'Build platform / OS / arch', build_platform FROM fp
  UNION ALL SELECT 4, 'Compiler signature', compiler FROM fp
  UNION ALL SELECT 5, 'data_directory', data_directory FROM fp
  UNION ALL SELECT 6, 'config_file', config_file FROM fp
  UNION ALL SELECT 7, 'hba_file', hba_file FROM fp
  UNION ALL SELECT 8, 'ident_file', ident_file FROM fp
  UNION ALL SELECT 9, 'cluster_name', COALESCE(cluster_name, '(not set)') FROM fp
  UNION ALL SELECT 10, 'timezone', timezone_name FROM fp
  UNION ALL SELECT 11, 'lc_collate', lc_collate_name FROM fp
  UNION ALL SELECT 12, 'lc_ctype', lc_ctype_name FROM fp
  UNION ALL SELECT 13, 'shared_preload_libraries', COALESCE(NULLIF(shared_preload_libraries, ''), '(none)') FROM fp
  UNION ALL SELECT 14, 'default_transaction_isolation', default_tx_isolation FROM fp
  UNION ALL SELECT 15, 'default_transaction_read_only', default_tx_read_only FROM fp
  UNION ALL SELECT 16, 'track_commit_timestamp', track_commit_timestamp FROM fp
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' ||
      replace(replace(replace(replace(replace(k,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(
        CASE
          WHEN lower(:'pg360_redact_paths') IN ('on','true','1','yes')
          THEN replace(COALESCE(v,'(null)'), :'pg360_redact_path_prefix', :'pg360_redacted_path_token')
          ELSE COALESCE(v,'(null)')
        END
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="2" class="table-empty">Identity keys unavailable</td></tr>'
  )
FROM kv;

\qecho '</tbody></table></div></div>'

\qecho '<div class="finding-title">Paste-Ready Fingerprint</div>'
WITH fp AS (
  SELECT
    current_database() AS db_name,
    current_setting('server_version_num', true) AS server_version_num,
    CASE
      WHEN version() ~ ' on [^,]+,' THEN regexp_replace(version(), '.* on ([^,]+),.*', '\1')
      ELSE 'unknown'
    END AS build_platform,
    current_setting('data_directory', true) AS data_directory,
    current_setting('config_file', true) AS config_file,
    current_setting('TimeZone', true) AS timezone_name,
    (SELECT datcollate FROM pg_database WHERE datname = current_database()) AS lc_collate_name,
    (SELECT datctype FROM pg_database WHERE datname = current_database()) AS lc_ctype_name,
    COALESCE(NULLIF(current_setting('shared_preload_libraries', true), ''), '(none)') AS shared_preload_libraries,
    current_setting('default_transaction_isolation', true) AS default_tx_isolation,
    current_setting('default_transaction_read_only', true) AS default_tx_read_only
)
SELECT
  '<div class="code-block">' ||
  'db=' || replace(replace(replace(replace(replace(COALESCE(db_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'server_version_num=' || replace(replace(replace(replace(replace(COALESCE(server_version_num,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'platform=' || replace(replace(replace(replace(replace(COALESCE(build_platform,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'data_directory=' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_paths') IN ('on','true','1','yes') THEN replace(data_directory, :'pg360_redact_path_prefix', :'pg360_redacted_path_token') ELSE data_directory END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'config_file=' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_paths') IN ('on','true','1','yes') THEN replace(config_file, :'pg360_redact_path_prefix', :'pg360_redacted_path_token') ELSE config_file END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'timezone=' || replace(replace(replace(replace(replace(COALESCE(timezone_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'locale=' || replace(replace(replace(replace(replace(COALESCE(lc_collate_name,'') || '/' || COALESCE(lc_ctype_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'shared_preload_libraries=' || replace(replace(replace(replace(replace(COALESCE(shared_preload_libraries,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || E'\n' ||
  'default_transaction=' || replace(replace(replace(replace(replace(COALESCE(default_tx_isolation,'') || ', read_only=' || COALESCE(default_tx_read_only,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div>'
FROM fp;

-- S00.2 Uptime & Stability
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Uptime &amp; Stability</div>'
WITH st AS (
  SELECT
    pg_postmaster_start_time() AS postmaster_started_at,
    now() - pg_postmaster_start_time() AS uptime_interval,
    pg_is_in_recovery() AS in_recovery,
    (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) AS db_stats_reset,
    (SELECT stats_reset FROM pg_stat_bgwriter) AS bgwriter_stats_reset,
    CASE
      WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
       AND to_regclass('pg_stat_statements_info') IS NOT NULL
      THEN NULLIF(
        (xpath('/row/stats_reset/text()', query_to_xml(
          'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
          false, true, ''
        )))[1]::text,
        ''
      )
      ELSE NULL
    END AS pgss_stats_reset_txt,
    CASE
      WHEN to_regprocedure('pg_control_checkpoint()') IS NOT NULL
       AND has_function_privilege(to_regprocedure('pg_control_checkpoint()'), 'EXECUTE')
      THEN NULLIF(
        (xpath('/row/checkpoint_time/text()', query_to_xml(
          'SELECT checkpoint_time::text AS checkpoint_time FROM pg_control_checkpoint()',
          false, true, ''
        )))[1]::text,
        ''
      )
      ELSE NULL
    END AS checkpoint_time_txt
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Postmaster Started</div><div class="card-value">' ||
  to_char(postmaster_started_at, 'YYYY-MM-DD HH24:MI:SS TZ') || '</div></div>' ||
  '<div class="card"><div class="card-label">Uptime</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(uptime_interval::text,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</div></div>' ||
  '<div class="card"><div class="card-label">Node Role</div><div class="card-value">' ||
  CASE WHEN in_recovery THEN 'Standby' ELSE 'Primary' END || '</div></div>' ||
  '<div class="card"><div class="card-label">Stats Reset (DB)</div><div class="card-value">' ||
  COALESCE(to_char(db_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A') || '</div></div>' ||
  '</div>'
FROM st;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Stability Signal</th><th>Observed Value</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

WITH st AS (
  SELECT
    pg_postmaster_start_time() AS postmaster_started_at,
    now() - pg_postmaster_start_time() AS uptime_interval,
    (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) AS db_stats_reset,
    (SELECT stats_reset FROM pg_stat_bgwriter) AS bgwriter_stats_reset,
    CASE
      WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
       AND to_regclass('pg_stat_statements_info') IS NOT NULL
      THEN NULLIF(
        (xpath('/row/stats_reset/text()', query_to_xml(
          'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
          false, true, ''
        )))[1]::text,
        ''
      )
      ELSE NULL
    END AS pgss_stats_reset_txt,
    CASE
      WHEN to_regprocedure('pg_control_checkpoint()') IS NOT NULL
       AND has_function_privilege(to_regprocedure('pg_control_checkpoint()'), 'EXECUTE')
      THEN NULLIF(
        (xpath('/row/checkpoint_time/text()', query_to_xml(
          'SELECT checkpoint_time::text AS checkpoint_time FROM pg_control_checkpoint()',
          false, true, ''
        )))[1]::text,
        ''
      )
      ELSE NULL
    END AS checkpoint_time_txt
),
signals AS (
  SELECT 1 AS ord, 'Postmaster start time' AS signal,
         to_char(postmaster_started_at, 'YYYY-MM-DD HH24:MI:SS TZ') AS observed,
         'Baseline for uptime and restart context' AS interpretation
  FROM st
  UNION ALL
  SELECT 2, 'Uptime',
         uptime_interval::text,
         CASE
           WHEN uptime_interval < interval '1 hour' THEN 'Recent restart window; validate restart reason from logs'
           WHEN uptime_interval < interval '1 day' THEN 'Uptime is short; trend views may be volatile'
           ELSE 'Stable uptime window'
         END
  FROM st
  UNION ALL
  SELECT 3, 'Database stats_reset',
         COALESCE(to_char(db_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A'),
         CASE
           WHEN db_stats_reset IS NULL THEN 'No reset timestamp available'
           WHEN db_stats_reset > now() - interval '30 minutes' THEN 'Stats recently reset; some sections can look empty'
           ELSE 'Stats horizon is usable for trend interpretation'
         END
  FROM st
  UNION ALL
  SELECT 4, 'BGWriter stats_reset',
         COALESCE(to_char(bgwriter_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A'),
         'Used by checkpoint/write pressure diagnostics'
  FROM st
  UNION ALL
  SELECT 5, 'pg_stat_statements stats_reset',
         COALESCE(pgss_stats_reset_txt, 'N/A (extension/view unavailable)'),
         'If recent, SQL ranking sections reflect short history only'
  FROM st
  UNION ALL
  SELECT 6, 'Last checkpoint time (best effort)',
         COALESCE(checkpoint_time_txt, 'Unavailable (insufficient privilege or unsupported)'),
         'Checkpoint cadence indicator from control-plane metadata'
  FROM st
  UNION ALL
  SELECT 7, 'Restart reason visibility',
         'SQL-only signal',
         'Crash vs clean restart cannot be guaranteed from SQL alone; correlate with PostgreSQL logs or pg_controldata'
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' ||
      replace(replace(replace(replace(replace(signal,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(COALESCE(observed,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(COALESCE(interpretation,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="3" class="table-empty">No stability signals available</td></tr>'
  )
FROM signals;

\qecho '</tbody></table></div></div>'

-- S00.3 Platform Detection
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Platform Detection (Managed Service / Container / Role)</div>'

WITH env AS (
  SELECT
    version() AS full_version,
    COALESCE(current_setting('rds.extensions', true), '') AS rds_extensions,
    COALESCE(current_setting('data_directory', true), '') AS data_directory,
    COALESCE(current_setting('config_file', true), '') AS config_file,
    pg_is_in_recovery() AS in_recovery,
    (SELECT COUNT(*) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')) AS user_tablespace_count
),
det AS (
  SELECT
    CASE
      WHEN rds_extensions ILIKE '%aurora%' OR full_version ILIKE '%Aurora%' THEN 'AWS Aurora PostgreSQL'
      WHEN rds_extensions <> '' THEN 'AWS RDS PostgreSQL'
      WHEN full_version ILIKE '%Azure Database for PostgreSQL%' THEN 'Azure Database for PostgreSQL'
      WHEN full_version ILIKE '%Cloud SQL%' OR data_directory ILIKE '%cloudsql%' THEN 'Google Cloud SQL for PostgreSQL'
      WHEN data_directory ILIKE '/var/lib/postgresql/data%' OR data_directory ILIKE '/bitnami/postgresql%' OR data_directory ILIKE '%/docker/%' OR data_directory ILIKE '%/containers/%'
        THEN 'Self-managed PostgreSQL (containerized)'
      ELSE 'Self-managed PostgreSQL'
    END AS detected_platform,
    CASE
      WHEN rds_extensions <> '' OR full_version ILIKE '%Azure Database for PostgreSQL%' OR full_version ILIKE '%Cloud SQL%'
        THEN 'Managed service'
      ELSE 'Self-managed'
    END AS deployment_model,
    CASE WHEN in_recovery THEN 'Standby/Replica' ELSE 'Primary/Writer' END AS node_role,
    CASE
      WHEN data_directory ILIKE '/var/lib/postgresql/data%' OR data_directory ILIKE '/bitnami/postgresql%' OR data_directory ILIKE '%/docker/%' OR data_directory ILIKE '%/containers/%'
        THEN 'Likely container/K8s path pattern'
      ELSE 'No strong container signature from paths'
    END AS container_hint,
    CASE
      WHEN user_tablespace_count > 0 THEN user_tablespace_count::text || ' custom tablespace(s) configured'
      ELSE 'Default tablespace layout only'
    END AS storage_hint,
    data_directory,
    config_file,
    CASE
      WHEN rds_extensions <> '' THEN 'rds.extensions setting is present'
      ELSE 'No managed-service setting signature'
    END AS managed_signal
  FROM env
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Detected Platform</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(detected_platform,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Deployment Model</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(deployment_model,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Role</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(node_role,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Storage Hint</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(COALESCE(storage_hint,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '</div>'
FROM det;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Detection Signal</th><th>Observed Value</th><th>Guidance</th>'
\qecho '</tr></thead><tbody>'

WITH env AS (
  SELECT
    version() AS full_version,
    COALESCE(current_setting('rds.extensions', true), '') AS rds_extensions,
    COALESCE(current_setting('data_directory', true), '') AS data_directory,
    COALESCE(current_setting('config_file', true), '') AS config_file,
    pg_is_in_recovery() AS in_recovery,
    (SELECT COUNT(*) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')) AS user_tablespace_count
),
det AS (
  SELECT
    CASE
      WHEN rds_extensions ILIKE '%aurora%' OR full_version ILIKE '%Aurora%' THEN 'AWS Aurora PostgreSQL'
      WHEN rds_extensions <> '' THEN 'AWS RDS PostgreSQL'
      WHEN full_version ILIKE '%Azure Database for PostgreSQL%' THEN 'Azure Database for PostgreSQL'
      WHEN full_version ILIKE '%Cloud SQL%' OR data_directory ILIKE '%cloudsql%' THEN 'Google Cloud SQL for PostgreSQL'
      WHEN data_directory ILIKE '/var/lib/postgresql/data%' OR data_directory ILIKE '/bitnami/postgresql%' OR data_directory ILIKE '%/docker/%' OR data_directory ILIKE '%/containers/%'
        THEN 'Self-managed PostgreSQL (containerized)'
      ELSE 'Self-managed PostgreSQL'
    END AS detected_platform,
    CASE
      WHEN rds_extensions <> '' OR full_version ILIKE '%Azure Database for PostgreSQL%' OR full_version ILIKE '%Cloud SQL%'
        THEN 'Managed service'
      ELSE 'Self-managed'
    END AS deployment_model,
    CASE WHEN in_recovery THEN 'Standby/Replica' ELSE 'Primary/Writer' END AS node_role,
    CASE
      WHEN data_directory ILIKE '/var/lib/postgresql/data%' OR data_directory ILIKE '/bitnami/postgresql%' OR data_directory ILIKE '%/docker/%' OR data_directory ILIKE '%/containers/%'
        THEN 'Likely container/K8s path pattern'
      ELSE 'No strong container signature from paths'
    END AS container_hint,
    CASE
      WHEN user_tablespace_count > 0 THEN user_tablespace_count::text || ' custom tablespace(s) configured'
      ELSE 'Default tablespace layout only'
    END AS storage_hint,
    data_directory,
    config_file,
    CASE
      WHEN rds_extensions <> '' THEN 'rds.extensions setting is present'
      ELSE 'No managed-service setting signature'
    END AS managed_signal
  FROM env
),
signals AS (
  SELECT 1 AS ord, 'Detected platform' AS signal, detected_platform AS observed,
         CASE
           WHEN deployment_model = 'Managed service' THEN 'Use cloud-native controls (parameter groups, service metrics, platform limits)'
           ELSE 'Use host-level + PostgreSQL-level controls (OS/kernel/filesystem + postgresql.conf)'
         END AS guidance
  FROM det
  UNION ALL SELECT 2, 'Deployment model', deployment_model,
         'Determines whether changes are done through platform APIs or local config files' FROM det
  UNION ALL SELECT 3, 'Node role', node_role,
         'Primary nodes get write tuning focus; standby nodes prioritize replay lag and read scaling' FROM det
  UNION ALL SELECT 4, 'Container hint', container_hint,
         'Containerized paths imply ephemeral storage checks and resource-limit validation' FROM det
  UNION ALL SELECT 5, 'Storage hint', storage_hint,
         'Tablespace spread can isolate heavy I/O objects and reduce contention' FROM det
  UNION ALL SELECT 6, 'Managed-service signal', managed_signal,
         'Best-effort detection only; confirm with infrastructure metadata' FROM det
  UNION ALL SELECT 7, 'data_directory path', data_directory,
         'Used for storage and deployment heuristics' FROM det
  UNION ALL SELECT 8, 'config_file path', config_file,
         'Confirms where runtime configuration is loaded from' FROM det
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' ||
      replace(replace(replace(replace(replace(signal,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(
        CASE
          WHEN lower(:'pg360_redact_paths') IN ('on','true','1','yes')
          THEN replace(COALESCE(observed,''), :'pg360_redact_path_prefix', :'pg360_redacted_path_token')
          ELSE COALESCE(observed,'')
        END
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(COALESCE(guidance,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="3" class="table-empty">No platform signals available</td></tr>'
  )
FROM signals;

\qecho '</tbody></table></div></div>'

-- S00.4 Extensions Posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Extensions: Installed / Missing / Risk</div>'

WITH ext AS (
  SELECT
    e.extname,
    e.extversion,
    n.nspname AS ext_schema,
    e.extrelocatable,
    COALESCE(av.superuser, false) AS requires_superuser,
    COALESCE(av.trusted, false) AS is_trusted,
    EXISTS (
      SELECT 1
      FROM pg_depend d
      JOIN pg_proc p ON p.oid = d.objid
      JOIN pg_language l ON l.oid = p.prolang
      WHERE d.refobjid = e.oid
        AND d.classid = 'pg_proc'::regclass
        AND l.lanname = 'c'
    ) AS has_c_language
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  LEFT JOIN LATERAL (
    SELECT aev.superuser, aev.trusted
    FROM pg_available_extension_versions aev
    WHERE aev.name = e.extname
      AND aev.version = e.extversion
    LIMIT 1
  ) av ON TRUE
),
recommended AS (
  SELECT * FROM (VALUES
    ('pg_stat_statements', 'Required for SQL performance diagnostics (S02/S11/S28)'),
    ('pg_buffercache', 'Needed for deep buffer cache visibility (S07)'),
    ('pg_prewarm', 'Useful for cache warmup strategy validation'),
    ('auto_explain', 'Useful for plan capture of slow SQL in production-safe mode')
  ) AS r(extname, purpose)
),
summary AS (
  SELECT
    (SELECT COUNT(*) FROM ext) AS installed_cnt,
    (SELECT COUNT(*) FROM ext WHERE extname <> 'plpgsql' AND ((has_c_language AND requires_superuser) OR (has_c_language AND NOT is_trusted))) AS high_risk_cnt,
    (SELECT COUNT(*) FROM recommended r WHERE NOT EXISTS (SELECT 1 FROM ext e WHERE e.extname = r.extname)) AS missing_recommended_cnt
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Installed Extensions</div><div class="card-value">' || installed_cnt || '</div></div>' ||
  '<div class="card ' || CASE WHEN high_risk_cnt > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Higher-Risk Extensions</div><div class="card-value">' || high_risk_cnt || '</div></div>' ||
  '<div class="card ' || CASE WHEN missing_recommended_cnt > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Recommended Missing</div><div class="card-value">' || missing_recommended_cnt || '</div></div>' ||
  '<div class="card"><div class="card-label">Extension Posture Score</div><div class="card-value">' ||
  GREATEST(0, 100 - (high_risk_cnt * 15) - (missing_recommended_cnt * 10))::text || '</div></div>' ||
  '</div>'
FROM summary;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Extension</th><th>Version</th><th>Schema</th><th>Category</th><th>Relocatable</th><th>Risk</th><th>Notes</th>'
\qecho '</tr></thead><tbody>'

WITH ext AS (
  SELECT
    e.extname,
    e.extversion,
    n.nspname AS ext_schema,
    e.extrelocatable,
    COALESCE(av.superuser, false) AS requires_superuser,
    COALESCE(av.trusted, false) AS is_trusted,
    EXISTS (
      SELECT 1
      FROM pg_depend d
      JOIN pg_proc p ON p.oid = d.objid
      JOIN pg_language l ON l.oid = p.prolang
      WHERE d.refobjid = e.oid
        AND d.classid = 'pg_proc'::regclass
        AND l.lanname = 'c'
    ) AS has_c_language
  FROM pg_extension e
  JOIN pg_namespace n ON n.oid = e.extnamespace
  LEFT JOIN LATERAL (
    SELECT aev.superuser, aev.trusted
    FROM pg_available_extension_versions aev
    WHERE aev.name = e.extname
      AND aev.version = e.extversion
    LIMIT 1
  ) av ON TRUE
),
ext_eval AS (
  SELECT
    extname,
    extversion,
    ext_schema,
    extrelocatable,
    CASE
      WHEN extname IN ('pg_stat_statements','pg_buffercache','pg_prewarm','auto_explain') THEN 'Observability'
      WHEN extname IN ('pgcrypto','pgaudit') THEN 'Security'
      WHEN extname IN ('hypopg','pg_hint_plan') THEN 'Performance'
      ELSE 'Feature'
    END AS category,
    CASE
      WHEN extname = 'plpgsql' THEN 'LOW'
      WHEN (has_c_language AND requires_superuser) OR (has_c_language AND NOT is_trusted) THEN 'HIGH'
      WHEN has_c_language OR requires_superuser THEN 'MEDIUM'
      ELSE 'LOW'
    END AS risk_level,
    CASE
      WHEN extname = 'plpgsql'
        THEN 'Core procedural language (baseline PostgreSQL component)'
      WHEN extname = 'pg_stat_statements'
       AND COALESCE(current_setting('shared_preload_libraries', true), '') NOT ILIKE '%pg_stat_statements%'
        THEN 'Installed but not preloaded in shared_preload_libraries'
      WHEN extname = 'pg_stat_statements'
       AND COALESCE(current_setting('pg_stat_statements.track', true), '') IN ('', 'none')
        THEN 'pg_stat_statements.track is not collecting useful scope'
      WHEN has_c_language AND NOT is_trusted
        THEN 'C-language extension; validate source and patch cadence'
      WHEN requires_superuser
        THEN 'Superuser-required extension lifecycle'
      ELSE ''
    END AS notes
  FROM ext
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(extname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || replace(replace(replace(replace(replace(extversion,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(ext_schema,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(category,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN extrelocatable THEN 'Yes' ELSE 'No' END || '</td>' ||
      '<td><span class="severity-pill ' ||
        CASE risk_level WHEN 'HIGH' THEN 'pill-critical' WHEN 'MEDIUM' THEN 'pill-warning' ELSE 'pill-good' END ||
      '">' || risk_level || '</span></td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(notes,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY extname
    ),
    '<tr><td colspan="7" class="table-empty">No installed extensions found</td></tr>'
  )
FROM ext_eval;

\qecho '</tbody></table></div>'

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Recommended Extension</th><th>Status</th><th>Purpose</th>'
\qecho '</tr></thead><tbody>'

WITH recommended AS (
  SELECT * FROM (VALUES
    ('pg_stat_statements', 'Required for SQL performance diagnostics (S02/S11/S28)'),
    ('pg_buffercache', 'Needed for deep buffer cache visibility (S07)'),
    ('pg_prewarm', 'Useful for cache warmup strategy validation'),
    ('auto_explain', 'Useful for plan capture of slow SQL in production-safe mode')
  ) AS r(extname, purpose)
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(r.extname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td><span class="severity-pill ' ||
      CASE
        WHEN e.extname IS NOT NULL THEN 'pill-good">INSTALLED'
        WHEN r.extname = 'pg_stat_statements' THEN 'pill-critical">MISSING'
        ELSE 'pill-warning">MISSING'
      END ||
      '</span></td>' ||
      '<td>' || replace(replace(replace(replace(replace(r.purpose,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY r.extname
    ),
    '<tr><td colspan="3" class="table-empty">Recommendation catalog unavailable</td></tr>'
  )
FROM recommended r
LEFT JOIN pg_extension e ON e.extname = r.extname;

\qecho '</tbody></table></div></div>'

-- S00.5 Configuration Snapshot
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Configuration Posture Snapshot (Summary only)</div>'
\qecho '<div class="finding info"><div class="finding-header">'
\qecho '<span class="finding-title">This is a concise posture snapshot. Use S23 for full parameter audit and remediation depth.</span>'
\qecho '<span class="severity-pill pill-info">SUMMARY</span></div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current Value</th><th>Source</th><th>Status</th><th>Why It Matters</th>'
\qecho '</tr></thead><tbody>'

WITH cfg AS (
  SELECT * FROM (VALUES
    ('shared_buffers', 1, 'Core shared memory cache'),
    ('effective_cache_size', 2, 'Planner estimate for OS + shared cache'),
    ('work_mem', 3, 'Sort/hash memory before temp spill'),
    ('maintenance_work_mem', 4, 'VACUUM/CREATE INDEX maintenance memory'),
    ('max_connections', 5, 'Connection concurrency pressure'),
    ('superuser_reserved_connections', 6, 'Emergency admin access'),
    ('wal_level', 7, 'Replication/decoding capability'),
    ('max_wal_size', 8, 'Checkpoint frequency and WAL pressure'),
    ('checkpoint_timeout', 9, 'Checkpoint cadence'),
    ('autovacuum', 10, 'Dead tuple cleanup and freeze safety'),
    ('autovacuum_max_workers', 11, 'Autovacuum concurrency'),
    ('max_worker_processes', 12, 'Background worker capacity'),
    ('max_parallel_workers', 13, 'Parallel execution capacity'),
    ('max_parallel_workers_per_gather', 14, 'Parallel workers per query'),
    ('log_min_duration_statement', 15, 'Slow query forensic visibility'),
    ('log_lock_waits', 16, 'Lock wait incident visibility'),
    ('deadlock_timeout', 17, 'Deadlock diagnostics sensitivity'),
    ('track_io_timing', 18, 'I/O timing observability'),
    ('track_functions', 19, 'Function-level profiling visibility'),
    ('track_activity_query_size', 20, 'Captured SQL text length')
  ) AS t(name, sort_ord, rationale)
),
eval AS (
  SELECT
    c.name,
    c.sort_ord,
    c.rationale,
    s.setting,
    s.unit,
    s.source,
    CASE
      WHEN s.name IS NULL THEN 'WARN'
      WHEN c.name = 'autovacuum' AND s.setting = 'off' THEN 'FAIL'
      WHEN c.name = 'shared_buffers' AND s.setting::bigint * 8192 < 134217728 THEN 'WARN'
      WHEN c.name = 'work_mem' AND s.setting::bigint * 1024 < 4194304 THEN 'WARN'
      WHEN c.name = 'max_connections' AND s.setting::int > 500 THEN 'WARN'
      WHEN c.name = 'max_wal_size' AND s.setting::int < 1024 THEN 'WARN'
      WHEN c.name = 'log_min_duration_statement' AND s.setting = '-1' THEN 'WARN'
      WHEN c.name = 'log_lock_waits' AND s.setting = 'off' THEN 'WARN'
      WHEN c.name = 'track_io_timing' AND s.setting = 'off' THEN 'WARN'
      WHEN c.name = 'track_functions' AND s.setting = 'none' THEN 'WARN'
      WHEN c.name = 'track_activity_query_size' AND s.setting::int < 2048 THEN 'WARN'
      ELSE 'OK'
    END AS status
  FROM cfg c
  LEFT JOIN pg_settings s ON s.name = c.name
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(
        COALESCE(setting || CASE WHEN COALESCE(unit,'') <> '' THEN ' ' || unit ELSE '' END, 'N/A'),
        '&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(source,'N/A'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td><span class="severity-pill ' ||
      CASE status WHEN 'FAIL' THEN 'pill-critical' WHEN 'WARN' THEN 'pill-warning' ELSE 'pill-good' END ||
      '">' || status || '</span></td>' ||
      '<td>' || replace(replace(replace(replace(replace(rationale,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY sort_ord
    ),
    '<tr><td colspan="5" class="table-empty">No configuration data available</td></tr>'
  )
FROM eval;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S00.6 Diagnostic Completeness
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Diagnostic Completeness (Visibility Prerequisites)</div>'

WITH base AS (
  SELECT
    COALESCE(current_setting('track_activities', true), 'off') AS track_activities,
    COALESCE(current_setting('track_counts', true), 'off') AS track_counts,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement,
    COALESCE(current_setting('log_lock_waits', true), 'off') AS log_lock_waits,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS has_pgss_extension,
    COALESCE((SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()), NULL) AS db_stats_reset,
    COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false) AS is_superuser,
    CASE
      WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_read_all_stats')
        THEN pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER')
      ELSE false
    END AS has_pg_read_all_stats,
    has_table_privilege('pg_catalog.pg_stat_activity', 'SELECT') AS can_read_pg_stat_activity,
    has_table_privilege('pg_catalog.pg_settings', 'SELECT') AS can_read_pg_settings
),
checks AS (
  SELECT 1 AS ord, 'track_activities enabled' AS check_name,
         CASE WHEN track_activities = 'on' THEN 'PASS' ELSE 'FAIL' END AS status,
         'Required for session state and wait event visibility' AS impact,
         CASE WHEN track_activities = 'on' THEN 'Enabled' ELSE 'Disabled' END AS detail
  FROM base
  UNION ALL
  SELECT 2, 'track_counts enabled',
         CASE WHEN track_counts = 'on' THEN 'PASS' ELSE 'FAIL' END,
         'Required for table/index statistics used by multiple modules',
         CASE WHEN track_counts = 'on' THEN 'Enabled' ELSE 'Disabled' END
  FROM base
  UNION ALL
  SELECT 3, 'track_io_timing enabled',
         CASE WHEN track_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         'Needed for precise I/O latency attribution',
         CASE WHEN track_io_timing = 'on' THEN 'Enabled' ELSE 'Disabled' END
  FROM base
  UNION ALL
  SELECT 4, 'Slow query logging configured',
         CASE WHEN log_min_duration_statement <> '-1' THEN 'PASS' ELSE 'WARN' END,
         'Without slow-query logs, production forensics are limited',
         CASE WHEN log_min_duration_statement <> '-1' THEN 'log_min_duration_statement=' || log_min_duration_statement ELSE 'Disabled (-1)' END
  FROM base
  UNION ALL
  SELECT 5, 'Lock-wait logging enabled',
         CASE WHEN log_lock_waits = 'on' THEN 'PASS' ELSE 'WARN' END,
         'Improves root-cause evidence for lock contention',
         CASE WHEN log_lock_waits = 'on' THEN 'Enabled' ELSE 'Disabled' END
  FROM base
  UNION ALL
  SELECT 6, 'pg_stat_statements extension present',
         CASE WHEN has_pgss_extension THEN 'PASS' ELSE 'FAIL' END,
         'Required for SQL-level ranking and tuning modules',
         CASE WHEN has_pgss_extension THEN 'Installed' ELSE 'Missing' END
  FROM base
  UNION ALL
  SELECT 7, 'pg_stat_statements preloaded',
         CASE
           WHEN NOT has_pgss_extension THEN 'FAIL'
           WHEN shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'PASS'
           ELSE 'WARN'
         END,
         'Without preload + restart, statement metrics can be incomplete',
         CASE
           WHEN shared_preload_libraries = '' THEN '(none)'
           ELSE shared_preload_libraries
         END
  FROM base
  UNION ALL
  SELECT 8, 'Diagnostic privilege scope',
         CASE WHEN is_superuser OR has_pg_read_all_stats THEN 'PASS' ELSE 'WARN' END,
         'Limited privilege can hide sessions/query text and skew diagnosis',
         CASE WHEN is_superuser THEN 'superuser'
              WHEN has_pg_read_all_stats THEN 'pg_read_all_stats'
              ELSE 'limited role'
         END
  FROM base
  UNION ALL
  SELECT 9, 'Statistics reset horizon',
         CASE
           WHEN db_stats_reset IS NULL THEN 'WARN'
           WHEN db_stats_reset > now() - interval '30 minutes' THEN 'WARN'
           ELSE 'PASS'
         END,
         'Recent reset can produce low/empty counters in downstream modules',
         COALESCE(to_char(db_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A')
  FROM base
  UNION ALL
  SELECT 10, 'Required catalog view visibility',
         CASE WHEN can_read_pg_stat_activity AND can_read_pg_settings THEN 'PASS' ELSE 'FAIL' END,
         'If blocked, multiple diagnostic modules lose core telemetry',
         'pg_stat_activity=' || CASE WHEN can_read_pg_stat_activity THEN 'yes' ELSE 'no' END ||
         ', pg_settings=' || CASE WHEN can_read_pg_settings THEN 'yes' ELSE 'no' END
  FROM base
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card good"><div class="card-label">PASS Checks</div><div class="card-value">' || COUNT(*) FILTER (WHERE status = 'PASS') || '</div></div>' ||
  '<div class="card warning"><div class="card-label">WARN Checks</div><div class="card-value">' || COUNT(*) FILTER (WHERE status = 'WARN') || '</div></div>' ||
  '<div class="card critical"><div class="card-label">FAIL Checks</div><div class="card-value">' || COUNT(*) FILTER (WHERE status = 'FAIL') || '</div></div>' ||
  '<div class="card"><div class="card-label">Coverage</div><div class="card-value">' ||
  ROUND((COUNT(*) FILTER (WHERE status = 'PASS') * 100.0) / NULLIF(COUNT(*),0), 1) || '%</div></div>' ||
  '</div>'
FROM checks;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Status</th><th>Observed</th><th>Impact on Diagnostics</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    COALESCE(current_setting('track_activities', true), 'off') AS track_activities,
    COALESCE(current_setting('track_counts', true), 'off') AS track_counts,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement,
    COALESCE(current_setting('log_lock_waits', true), 'off') AS log_lock_waits,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS shared_preload_libraries,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS has_pgss_extension,
    COALESCE((SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()), NULL) AS db_stats_reset,
    COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false) AS is_superuser,
    CASE
      WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_read_all_stats')
        THEN pg_has_role(current_user, 'pg_read_all_stats', 'MEMBER')
      ELSE false
    END AS has_pg_read_all_stats,
    has_table_privilege('pg_catalog.pg_stat_activity', 'SELECT') AS can_read_pg_stat_activity,
    has_table_privilege('pg_catalog.pg_settings', 'SELECT') AS can_read_pg_settings
),
checks AS (
  SELECT 1 AS ord, 'track_activities enabled' AS check_name,
         CASE WHEN track_activities = 'on' THEN 'PASS' ELSE 'FAIL' END AS status,
         CASE WHEN track_activities = 'on' THEN 'Enabled' ELSE 'Disabled' END AS detail,
         'Required for session state and wait event visibility' AS impact
  FROM base
  UNION ALL
  SELECT 2, 'track_counts enabled',
         CASE WHEN track_counts = 'on' THEN 'PASS' ELSE 'FAIL' END,
         CASE WHEN track_counts = 'on' THEN 'Enabled' ELSE 'Disabled' END,
         'Required for table/index statistics used by multiple modules'
  FROM base
  UNION ALL
  SELECT 3, 'track_io_timing enabled',
         CASE WHEN track_io_timing = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN track_io_timing = 'on' THEN 'Enabled' ELSE 'Disabled' END,
         'Needed for precise I/O latency attribution'
  FROM base
  UNION ALL
  SELECT 4, 'Slow query logging configured',
         CASE WHEN log_min_duration_statement <> '-1' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_min_duration_statement <> '-1' THEN 'log_min_duration_statement=' || log_min_duration_statement ELSE 'Disabled (-1)' END,
         'Without slow-query logs, production forensics are limited'
  FROM base
  UNION ALL
  SELECT 5, 'Lock-wait logging enabled',
         CASE WHEN log_lock_waits = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN log_lock_waits = 'on' THEN 'Enabled' ELSE 'Disabled' END,
         'Improves root-cause evidence for lock contention'
  FROM base
  UNION ALL
  SELECT 6, 'pg_stat_statements extension present',
         CASE WHEN has_pgss_extension THEN 'PASS' ELSE 'FAIL' END,
         CASE WHEN has_pgss_extension THEN 'Installed' ELSE 'Missing' END,
         'Required for SQL-level ranking and tuning modules'
  FROM base
  UNION ALL
  SELECT 7, 'pg_stat_statements preloaded',
         CASE
           WHEN NOT has_pgss_extension THEN 'FAIL'
           WHEN shared_preload_libraries ILIKE '%pg_stat_statements%' THEN 'PASS'
           ELSE 'WARN'
         END,
         CASE
           WHEN shared_preload_libraries = '' THEN '(none)'
           ELSE shared_preload_libraries
         END,
         'Without preload + restart, statement metrics can be incomplete'
  FROM base
  UNION ALL
  SELECT 8, 'Diagnostic privilege scope',
         CASE WHEN is_superuser OR has_pg_read_all_stats THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN is_superuser THEN 'superuser'
              WHEN has_pg_read_all_stats THEN 'pg_read_all_stats'
              ELSE 'limited role'
         END,
         'Limited privilege can hide sessions/query text and skew diagnosis'
  FROM base
  UNION ALL
  SELECT 9, 'Statistics reset horizon',
         CASE
           WHEN db_stats_reset IS NULL THEN 'WARN'
           WHEN db_stats_reset > now() - interval '30 minutes' THEN 'WARN'
           ELSE 'PASS'
         END,
         COALESCE(to_char(db_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A'),
         'Recent reset can produce low/empty counters in downstream modules'
  FROM base
  UNION ALL
  SELECT 10, 'Required catalog view visibility',
         CASE WHEN can_read_pg_stat_activity AND can_read_pg_settings THEN 'PASS' ELSE 'FAIL' END,
         'pg_stat_activity=' || CASE WHEN can_read_pg_stat_activity THEN 'yes' ELSE 'no' END ||
         ', pg_settings=' || CASE WHEN can_read_pg_settings THEN 'yes' ELSE 'no' END,
         'If blocked, multiple diagnostic modules lose core telemetry'
  FROM base
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' ||
      replace(replace(replace(replace(replace(check_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td><span class="severity-pill ' ||
      CASE status WHEN 'FAIL' THEN 'pill-critical' WHEN 'WARN' THEN 'pill-warning' ELSE 'pill-good' END ||
      '">' || status || '</span></td><td>' ||
      replace(replace(replace(replace(replace(COALESCE(detail,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td>' ||
      replace(replace(replace(replace(replace(COALESCE(impact,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="4" class="table-empty">No completeness checks available</td></tr>'
  )
FROM checks;

\qecho '</tbody></table></div>'
\qecho '</div>'
\qecho '</div>'
-- =============================================================================
-- =============================================================================
-- SECTION S01: DATABASE OVERVIEW
-- =============================================================================
\qecho '<div class="section" id="s01">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Database Overview</div>'
\qecho '    <div class="section-desc">Storage baseline, workload intensity, cache/memory behavior, concurrency signals, object landscape, primary workload database detection, and baseline health flags.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S01 context banner: stats reset horizon
WITH resets AS (
  SELECT
    (SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()) AS db_stats_reset,
    (SELECT stats_reset FROM pg_stat_bgwriter) AS bgwriter_stats_reset,
    CASE
      WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
       AND to_regclass('pg_stat_statements_info') IS NOT NULL
      THEN NULLIF(
        (xpath('/row/stats_reset/text()', query_to_xml(
          'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
          false, true, ''
        )))[1]::text,
        ''
      )::timestamptz
      ELSE NULL
    END AS pgss_stats_reset
), horizon AS (
  SELECT
    db_stats_reset,
    bgwriter_stats_reset,
    pgss_stats_reset,
    CASE
      WHEN db_stats_reset IS NULL AND bgwriter_stats_reset IS NULL AND pgss_stats_reset IS NULL THEN NULL
      ELSE GREATEST(
        COALESCE(db_stats_reset, '-infinity'::timestamptz),
        COALESCE(bgwriter_stats_reset, '-infinity'::timestamptz),
        COALESCE(pgss_stats_reset, '-infinity'::timestamptz)
      )
    END AS latest_reset
  FROM resets
)
SELECT
  CASE
    WHEN latest_reset IS NOT NULL AND latest_reset > now() - interval '24 hours' THEN
      '<div class="finding high"><div class="finding-header">' ||
      '<span class="finding-title">Metrics horizon is short due to recent stats reset</span>' ||
      '<span class="severity-pill pill-warning">CONTEXT</span></div>' ||
      '<div class="finding-body">Metrics reflect approximately <strong>' ||
      round(extract(epoch from (now() - latest_reset)) / 3600.0, 1) ||
      ' hours</strong> since last reset (' || to_char(latest_reset, 'YYYY-MM-DD HH24:MI:SS TZ') || '). ' ||
      'Interpret low counters carefully and use trend windows after reset stabilization.</div></div>'
    WHEN latest_reset IS NULL THEN
      '<div class="finding info"><div class="finding-header">' ||
      '<span class="finding-title">Stats reset timestamp unavailable</span>' ||
      '<span class="severity-pill pill-info">INFO</span></div>' ||
      '<div class="finding-body">Stats reset timestamps are not fully available from current role/context. ' ||
      'Use S00.2 for visible reset signals.</div></div>'
    ELSE
      '<div class="finding good"><div class="finding-header">' ||
      '<span class="finding-title">Metrics horizon is stable</span>' ||
      '<span class="severity-pill pill-good">OK</span></div>' ||
      '<div class="finding-body">Latest visible stats reset: ' || to_char(latest_reset, 'YYYY-MM-DD HH24:MI:SS TZ') || '.</div></div>'
  END
FROM horizon;

-- S01.1 Storage Overview
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Storage Overview</div>'

WITH rel AS (
  SELECT
    SUM(CASE WHEN c.relkind IN ('r','p','m') THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS table_bytes,
    SUM(CASE WHEN c.relkind = 'i' THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS index_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Database Size</div><div class="card-value">' || pg_size_pretty(pg_database_size(current_database())) || '</div></div>' ||
  '<div class="card"><div class="card-label">Table Storage</div><div class="card-value">' || pg_size_pretty(COALESCE(table_bytes,0)::bigint) || '</div></div>' ||
  '<div class="card"><div class="card-label">Index Storage</div><div class="card-value">' || pg_size_pretty(COALESCE(index_bytes,0)::bigint) || '</div></div>' ||
  '<div class="card ' ||
    CASE WHEN COALESCE(index_bytes,0) / NULLIF(COALESCE(table_bytes,0),0) > 1.2 THEN 'warning' ELSE 'good' END ||
  '"><div class="card-label">Index-to-Table Ratio</div><div class="card-value">' ||
  COALESCE(round(index_bytes * 100.0 / NULLIF(table_bytes,0), 1)::text, 'N/A') || '%</div>' ||
  '<div class="card-sub">Index bytes / table bytes</div></div>' ||
  '</div>'
FROM rel;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Total Size</th><th>Table Size</th><th>Index Size</th><th>Index/Table %</th>'
\qecho '</tr></thead><tbody>'

WITH schema_sizes AS (
  SELECT
    n.nspname AS schema_name,
    SUM(pg_total_relation_size(c.oid))::numeric AS total_bytes,
    SUM(CASE WHEN c.relkind IN ('r','p','m') THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS table_bytes,
    SUM(CASE WHEN c.relkind = 'i' THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS index_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
  GROUP BY n.nspname
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || replace(replace(replace(replace(replace(schema_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || pg_size_pretty(total_bytes::bigint) || '</td>' ||
      '<td class="num">' || pg_size_pretty(table_bytes::bigint) || '</td>' ||
      '<td class="num">' || pg_size_pretty(index_bytes::bigint) || '</td>' ||
      '<td class="num ' || CASE WHEN index_bytes / NULLIF(table_bytes,0) > 1.2 THEN 'warn' ELSE 'good' END || '">' ||
      COALESCE(round(index_bytes * 100.0 / NULLIF(table_bytes,0),1)::text, 'N/A') || '%</td></tr>',
      E'\n' ORDER BY total_bytes DESC
    ),
    '<tr><td colspan="5" class="table-empty">No schema storage data available</td></tr>'
  )
FROM schema_sizes;

\qecho '</tbody></table></div></div>'

-- S01.2 Workload Intensity Snapshot
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload Intensity Snapshot</div>'

WITH dbs AS (
  SELECT
    d.xact_commit,
    d.xact_rollback,
    d.tup_returned,
    d.tup_fetched,
    d.tup_inserted,
    d.tup_updated,
    d.tup_deleted,
    d.temp_bytes,
    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::numeric AS uptime_seconds
  FROM pg_stat_database d
  WHERE d.datname = current_database()
), calc AS (
  SELECT
    xact_commit,
    xact_rollback,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    temp_bytes,
    uptime_seconds,
    (xact_commit + xact_rollback)::numeric / NULLIF(uptime_seconds,0) AS tps,
    (tup_returned + tup_fetched)::numeric / NULLIF(uptime_seconds,0) AS read_ops_per_sec,
    (tup_inserted + tup_updated + tup_deleted)::numeric / NULLIF(uptime_seconds,0) AS write_ops_per_sec,
    (xact_rollback * 100.0) / NULLIF(xact_commit + xact_rollback,0) AS rollback_pct
  FROM dbs
), cls AS (
  SELECT *,
    CASE
      WHEN temp_bytes > 1024::numeric * 1024 * 1024 AND read_ops_per_sec > write_ops_per_sec * 10 THEN 'Analytical pattern'
      WHEN write_ops_per_sec > read_ops_per_sec * 0.7 THEN 'Write-heavy'
      WHEN write_ops_per_sec < read_ops_per_sec * 0.05 THEN 'Read-heavy'
      ELSE 'Mixed OLTP'
    END AS workload_class
  FROM calc
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Transactions/sec</div><div class="card-value">' || COALESCE(round(tps,1)::text,'N/A') || '</div></div>' ||
  '<div class="card"><div class="card-label">Read Ops/sec</div><div class="card-value">' || COALESCE(round(read_ops_per_sec,1)::text,'N/A') || '</div></div>' ||
  '<div class="card"><div class="card-label">Write Ops/sec</div><div class="card-value">' || COALESCE(round(write_ops_per_sec,1)::text,'N/A') || '</div></div>' ||
  '<div class="card ' || CASE WHEN rollback_pct > 2 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Rollback Ratio</div><div class="card-value">' ||
  COALESCE(round(rollback_pct,2)::text,'N/A') || '%</div></div>' ||
  '<div class="card ' ||
    CASE workload_class WHEN 'Write-heavy' THEN 'warning' WHEN 'Analytical pattern' THEN 'warning' ELSE 'good' END ||
  '"><div class="card-label">Read/Write Class</div><div class="card-value">' || workload_class || '</div></div>' ||
  '</div>'
FROM cls;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

WITH dbs AS (
  SELECT
    d.xact_commit,
    d.xact_rollback,
    d.tup_returned,
    d.tup_fetched,
    d.tup_inserted,
    d.tup_updated,
    d.tup_deleted,
    d.temp_bytes,
    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::numeric AS uptime_seconds
  FROM pg_stat_database d
  WHERE d.datname = current_database()
), calc AS (
  SELECT
    (xact_commit + xact_rollback)::numeric / NULLIF(uptime_seconds,0) AS tps,
    (tup_returned + tup_fetched)::numeric / NULLIF(uptime_seconds,0) AS read_ops_per_sec,
    (tup_inserted + tup_updated + tup_deleted)::numeric / NULLIF(uptime_seconds,0) AS write_ops_per_sec,
    (xact_rollback * 100.0) / NULLIF(xact_commit + xact_rollback,0) AS rollback_pct,
    temp_bytes
  FROM dbs
), cls AS (
  SELECT *,
    CASE
      WHEN temp_bytes > 1024::numeric * 1024 * 1024 AND read_ops_per_sec > write_ops_per_sec * 10 THEN 'Analytical pattern'
      WHEN write_ops_per_sec > read_ops_per_sec * 0.7 THEN 'Write-heavy'
      WHEN write_ops_per_sec < read_ops_per_sec * 0.05 THEN 'Read-heavy'
      ELSE 'Mixed OLTP'
    END AS workload_class
  FROM calc
), signals AS (
  SELECT 1 AS ord, 'Workload class' AS signal, workload_class AS observed,
         'Feeds S11 tuning posture and memory/checkpoint/autovacuum expectations' AS interpretation
  FROM cls
  UNION ALL
  SELECT 2, 'Transactions/sec', COALESCE(round(tps,2)::text, 'N/A'),
         'Overall transaction intensity since postmaster start'
  FROM cls
  UNION ALL
  SELECT 3, 'Read operations/sec', COALESCE(round(read_ops_per_sec,2)::text, 'N/A'),
         'Approximate read pressure from tuple-return/fetch counters'
  FROM cls
  UNION ALL
  SELECT 4, 'Write operations/sec', COALESCE(round(write_ops_per_sec,2)::text, 'N/A'),
         'Approximate write pressure from insert/update/delete counters'
  FROM cls
  UNION ALL
  SELECT 5, 'Rollback ratio', COALESCE(round(rollback_pct,2)::text || '%', 'N/A'),
         'High rollback can indicate application retries, lock contention, or transaction errors'
  FROM cls
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || signal || '</td><td>' || observed || '</td><td>' || interpretation || '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="3" class="table-empty">No workload intensity signals available</td></tr>'
  )
FROM signals;

\qecho '</tbody></table></div></div>'

-- S01.3 Cache & Memory Behavior
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Cache &amp; Memory Behavior</div>'

WITH d AS (
  SELECT
    blks_hit,
    blks_read,
    temp_files,
    temp_bytes,
    xact_commit + xact_rollback AS xacts_total
  FROM pg_stat_database
  WHERE datname = current_database()
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN (blks_hit + blks_read) > 0 AND (blks_hit * 100.0 / (blks_hit + blks_read)) < 90 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Cache Hit Ratio</div><div class="card-value">' ||
  CASE WHEN blks_hit + blks_read = 0 THEN 'N/A' ELSE round(blks_hit * 100.0 / (blks_hit + blks_read),2)::text || '%' END || '</div></div>' ||
  '<div class="card ' || CASE WHEN temp_bytes > 1024::numeric*1024*1024 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Temp Written</div><div class="card-value">' || pg_size_pretty(temp_bytes) || '</div></div>' ||
  '<div class="card"><div class="card-label">Temp Files</div><div class="card-value">' || temp_files || '</div></div>' ||
  '<div class="card"><div class="card-label">Temp / Transaction</div><div class="card-value">' ||
  CASE WHEN xacts_total = 0 THEN 'N/A' ELSE pg_size_pretty((temp_bytes / NULLIF(xacts_total,0))::bigint) END ||
  '</div></div>' ||
  '</div>'
FROM d;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Observed</th><th>Status</th><th>Action Cue</th>'
\qecho '</tr></thead><tbody>'

WITH d AS (
  SELECT
    blks_hit,
    blks_read,
    temp_files,
    temp_bytes,
    xact_commit + xact_rollback AS xacts_total
  FROM pg_stat_database
  WHERE datname = current_database()
), metrics AS (
  SELECT 1 AS ord, 'Buffer cache hit ratio' AS metric,
         CASE WHEN blks_hit + blks_read = 0 THEN 'N/A' ELSE round(blks_hit * 100.0 / (blks_hit + blks_read),2)::text || '%' END AS observed,
         CASE WHEN blks_hit + blks_read = 0 THEN 'INFO' WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 90 THEN 'YELLOW' ELSE 'GREEN' END AS status,
         CASE WHEN blks_hit + blks_read = 0 THEN 'No buffer access yet'
              WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 90 THEN 'Review hot SQL and memory sizing (shared_buffers/effective_cache_size)'
              ELSE 'Healthy cache residency' END AS action
  FROM d
  UNION ALL
  SELECT 2, 'Temp spill volume', pg_size_pretty(temp_bytes),
         CASE WHEN temp_bytes > 1024::numeric*1024*1024 THEN 'YELLOW' ELSE 'GREEN' END,
         CASE WHEN temp_bytes > 1024::numeric*1024*1024 THEN 'Investigate work_mem pressure and temp-heavy SQL' ELSE 'No major spill pressure' END
  FROM d
  UNION ALL
  SELECT 3, 'Temp files generated', temp_files::text,
         CASE WHEN temp_files > 500 THEN 'YELLOW' ELSE 'GREEN' END,
         CASE WHEN temp_files > 500 THEN 'Correlate with S02 temp-I/O SQL list' ELSE 'File spill count not elevated' END
  FROM d
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || metric || '</td><td>' || observed || '</td><td><span class="severity-pill ' ||
      CASE status WHEN 'GREEN' THEN 'pill-good' WHEN 'YELLOW' THEN 'pill-warning' ELSE 'pill-info' END || '">' || status || '</span></td><td>' || action || '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="4" class="table-empty">No cache/memory metrics available</td></tr>'
  )
FROM metrics;

\qecho '</tbody></table></div></div>'

-- S01.4 Concurrency Signals
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Concurrency Signals</div>'

WITH c AS (
  SELECT
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()) AS total_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'active') AS active_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND state = 'idle in transaction') AS idle_in_txn,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event IS NOT NULL AND state = 'active') AS waiting_active,
    (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()) AS deadlocks,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Sessions</div><div class="card-value">' || total_sessions || '</div></div>' ||
  '<div class="card"><div class="card-label">Active</div><div class="card-value">' || active_sessions || '</div></div>' ||
  '<div class="card ' || CASE WHEN idle_in_txn > 5 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Idle In Transaction</div><div class="card-value">' || idle_in_txn || '</div></div>' ||
  '<div class="card ' || CASE WHEN waiting_active > 0 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Waiting Active</div><div class="card-value">' || waiting_active || '</div></div>' ||
  '<div class="card ' || CASE WHEN deadlocks > 0 THEN 'critical' ELSE 'good' END || '"><div class="card-label">Deadlocks</div><div class="card-value">' || deadlocks || '</div></div>' ||
  '<div class="card ' || CASE WHEN max_connections > 0 AND total_sessions * 100.0 / max_connections > 80 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Connection Utilization</div><div class="card-value">' ||
  round(CASE WHEN max_connections = 0 THEN 0 ELSE total_sessions * 100.0 / max_connections END,1) || '%</div></div>' ||
  '</div>'
FROM c;

\qecho '</div>'

-- S01.5 Object Landscape
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Object Landscape</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Tables</th><th>Views</th><th>Matviews</th><th>Functions</th><th>Sequences</th><th>Indexes</th><th>Index/Table</th>'
\qecho '</tr></thead><tbody>'

WITH obj AS (
  SELECT
    n.nspname AS schema_name,
    COUNT(CASE WHEN c.relkind = 'r' THEN 1 END) AS tables_cnt,
    COUNT(CASE WHEN c.relkind = 'v' THEN 1 END) AS views_cnt,
    COUNT(CASE WHEN c.relkind = 'm' THEN 1 END) AS matviews_cnt,
    (SELECT COUNT(*) FROM pg_proc p WHERE p.pronamespace = n.oid) AS functions_cnt,
    COUNT(CASE WHEN c.relkind = 'S' THEN 1 END) AS sequences_cnt,
    COUNT(CASE WHEN c.relkind = 'i' THEN 1 END) AS indexes_cnt
  FROM pg_namespace n
  LEFT JOIN pg_class c ON c.relnamespace = n.oid
  WHERE n.nspname NOT IN ('pg_toast','pg_catalog','information_schema')
    AND n.nspname NOT LIKE 'pg_temp%'
  GROUP BY n.nspname, n.oid
  HAVING COUNT(c.oid) > 0
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || schema_name || '</td>' ||
      '<td class="num">' || tables_cnt || '</td>' ||
      '<td class="num">' || views_cnt || '</td>' ||
      '<td class="num">' || matviews_cnt || '</td>' ||
      '<td class="num">' || functions_cnt || '</td>' ||
      '<td class="num">' || sequences_cnt || '</td>' ||
      '<td class="num">' || indexes_cnt || '</td>' ||
      '<td class="num ' || CASE WHEN tables_cnt > 0 AND indexes_cnt::numeric / tables_cnt > 10 THEN 'warn' ELSE 'good' END || '">' ||
      CASE WHEN tables_cnt = 0 THEN 'N/A' ELSE round(indexes_cnt::numeric / tables_cnt,2)::text END ||
      '</td></tr>',
      E'\n' ORDER BY tables_cnt DESC, indexes_cnt DESC
    ),
    '<tr><td colspan="8" class="table-empty">No object inventory available</td></tr>'
  )
FROM obj;

\qecho '</tbody></table></div></div>'

-- S01.6 Primary Workload Database Detection
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Primary Workload Database Detection</div>'

WITH dbs AS (
  SELECT
    datname,
    pg_database_size(datname)::numeric AS size_bytes,
    (xact_commit + xact_rollback)::numeric AS xacts_total,
    temp_bytes::numeric AS temp_bytes,
    temp_files,
    numbackends,
    deadlocks,
    blks_hit,
    blks_read
  FROM pg_stat_database
  WHERE datname NOT IN ('template0','template1')
), scored AS (
  SELECT
    datname,
    size_bytes,
    xacts_total,
    temp_bytes,
    temp_files,
    numbackends,
    deadlocks,
    blks_hit,
    blks_read,
    COALESCE(xacts_total / NULLIF(MAX(xacts_total) OVER (),0),0) AS xact_score,
    COALESCE(size_bytes / NULLIF(MAX(size_bytes) OVER (),0),0) AS size_score,
    COALESCE(temp_bytes / NULLIF(MAX(temp_bytes) OVER (),0),0) AS temp_score
  FROM dbs
), ranked AS (
  SELECT
    *,
    (0.50 * xact_score + 0.30 * size_score + 0.20 * temp_score) AS workload_score,
    ROW_NUMBER() OVER (ORDER BY (0.50 * xact_score + 0.30 * size_score + 0.20 * temp_score) DESC, datname) AS rn
  FROM scored
)
SELECT
  '<div class="finding info"><div class="finding-header">' ||
  '<span class="finding-title">Primary workload database: ' ||
  replace(replace(replace(replace(replace(datname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</span><span class="severity-pill pill-info">ANCHOR</span></div>' ||
  '<div class="finding-body">Detection weighted by transactions (50%), size (30%), and temp spill volume (20%). Use this DB as the default tuning anchor.</div></div>'
FROM ranked
WHERE rn = 1;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Database</th><th>Score</th><th>Size</th><th>Transactions</th><th>Temp</th><th>Connections</th><th>Cache Hit%</th>'
\qecho '</tr></thead><tbody>'

WITH dbs AS (
  SELECT
    datname,
    pg_database_size(datname)::numeric AS size_bytes,
    (xact_commit + xact_rollback)::numeric AS xacts_total,
    temp_bytes::numeric AS temp_bytes,
    temp_files,
    numbackends,
    deadlocks,
    blks_hit,
    blks_read
  FROM pg_stat_database
  WHERE datname NOT IN ('template0','template1')
), scored AS (
  SELECT
    datname,
    size_bytes,
    xacts_total,
    temp_bytes,
    temp_files,
    numbackends,
    deadlocks,
    blks_hit,
    blks_read,
    COALESCE(xacts_total / NULLIF(MAX(xacts_total) OVER (),0),0) AS xact_score,
    COALESCE(size_bytes / NULLIF(MAX(size_bytes) OVER (),0),0) AS size_score,
    COALESCE(temp_bytes / NULLIF(MAX(temp_bytes) OVER (),0),0) AS temp_score
  FROM dbs
), ranked AS (
  SELECT
    *,
    (0.50 * xact_score + 0.30 * size_score + 0.20 * temp_score) AS workload_score,
    ROW_NUMBER() OVER (ORDER BY (0.50 * xact_score + 0.30 * size_score + 0.20 * temp_score) DESC, datname) AS rn
  FROM scored
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || datname || CASE WHEN rn = 1 THEN ' <span class="severity-pill pill-info">PRIMARY</span>' ELSE '' END || '</td>' ||
      '<td class="num">' || round(workload_score * 100,1) || '</td>' ||
      '<td class="num">' || pg_size_pretty(size_bytes::bigint) || '</td>' ||
      '<td class="num">' || to_char(xacts_total::bigint, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || pg_size_pretty(temp_bytes::bigint) || '</td>' ||
      '<td class="num">' || numbackends || '</td>' ||
      '<td class="num ' || CASE WHEN (blks_hit + blks_read) > 0 AND (blks_hit * 100.0 / (blks_hit + blks_read)) < 90 THEN 'warn' ELSE 'good' END || '">' ||
      CASE WHEN blks_hit + blks_read = 0 THEN 'N/A' ELSE round(blks_hit * 100.0 / (blks_hit + blks_read),1)::text || '%' END ||
      '</td></tr>',
      E'\n' ORDER BY workload_score DESC
    ),
    '<tr><td colspan="7" class="table-empty">No database workload data available</td></tr>'
  )
FROM ranked;

\qecho '</tbody></table></div></div>'

-- S01.7 Baseline Health Flags
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Baseline Health Flags (Green / Yellow / Red)</div>'

WITH d AS (
  SELECT
    blks_hit,
    blks_read,
    temp_files,
    temp_bytes,
    deadlocks,
    xact_commit,
    xact_rollback,
    tup_returned,
    tup_fetched,
    tup_inserted,
    tup_updated,
    tup_deleted,
    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::numeric AS uptime_seconds
  FROM pg_stat_database
  WHERE datname = current_database()
), rel AS (
  SELECT
    SUM(CASE WHEN c.relkind IN ('r','p','m') THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS table_bytes,
    SUM(CASE WHEN c.relkind = 'i' THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS index_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
), eval AS (
  SELECT
    CASE
      WHEN blks_hit + blks_read = 0 THEN 'GREEN'
      WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 85 THEN 'RED'
      WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 92 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS cache_flag,
    CASE
      WHEN temp_bytes > 8::numeric*1024*1024*1024 THEN 'RED'
      WHEN temp_bytes > 1::numeric*1024*1024*1024 OR temp_files > 500 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS temp_flag,
    CASE
      WHEN deadlocks > 0 THEN 'RED'
      ELSE 'GREEN'
    END AS deadlock_flag,
    CASE
      WHEN (xact_rollback * 100.0 / NULLIF(xact_commit + xact_rollback,0)) > 5 THEN 'RED'
      WHEN (xact_rollback * 100.0 / NULLIF(xact_commit + xact_rollback,0)) > 1 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS rollback_flag,
    CASE
      WHEN COALESCE(index_bytes / NULLIF(table_bytes,0),0) > 1.5 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS index_ratio_flag
  FROM d CROSS JOIN rel
), counts AS (
  SELECT
    COUNT(*) FILTER (WHERE f = 'GREEN') AS green_cnt,
    COUNT(*) FILTER (WHERE f = 'YELLOW') AS yellow_cnt,
    COUNT(*) FILTER (WHERE f = 'RED') AS red_cnt
  FROM eval,
  LATERAL (VALUES (cache_flag), (temp_flag), (deadlock_flag), (rollback_flag), (index_ratio_flag)) x(f)
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card good"><div class="card-label">Green</div><div class="card-value">' || green_cnt || '</div></div>' ||
  '<div class="card warning"><div class="card-label">Yellow</div><div class="card-value">' || yellow_cnt || '</div></div>' ||
  '<div class="card critical"><div class="card-label">Red</div><div class="card-value">' || red_cnt || '</div></div>' ||
  '<div class="card ' || CASE WHEN red_cnt > 0 THEN 'critical' WHEN yellow_cnt > 1 THEN 'warning' ELSE 'good' END || '"><div class="card-label">Overall</div><div class="card-value">' ||
  CASE WHEN red_cnt > 0 THEN 'RED' WHEN yellow_cnt > 1 THEN 'YELLOW' ELSE 'GREEN' END ||
  '</div></div>' ||
  '</div>'
FROM counts;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Flag Area</th><th>Status</th><th>Rule</th>'
\qecho '</tr></thead><tbody>'

WITH d AS (
  SELECT
    blks_hit,
    blks_read,
    temp_files,
    temp_bytes,
    deadlocks,
    xact_commit,
    xact_rollback
  FROM pg_stat_database
  WHERE datname = current_database()
), rel AS (
  SELECT
    SUM(CASE WHEN c.relkind IN ('r','p','m') THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS table_bytes,
    SUM(CASE WHEN c.relkind = 'i' THEN pg_relation_size(c.oid) ELSE 0 END)::numeric AS index_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
), rowset AS (
  SELECT
    'Cache behavior' AS area,
    CASE
      WHEN blks_hit + blks_read = 0 THEN 'GREEN'
      WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 85 THEN 'RED'
      WHEN (blks_hit * 100.0 / (blks_hit + blks_read)) < 92 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS status,
    'RED if hit ratio <85%; YELLOW if 85-92%' AS rule
  FROM d
  UNION ALL
  SELECT
    'Temp spill pressure',
    CASE
      WHEN temp_bytes > 8::numeric*1024*1024*1024 THEN 'RED'
      WHEN temp_bytes > 1::numeric*1024*1024*1024 OR temp_files > 500 THEN 'YELLOW'
      ELSE 'GREEN'
    END,
    'RED if temp >8GB; YELLOW if temp >1GB or temp files >500'
  FROM d
  UNION ALL
  SELECT
    'Deadlocks',
    CASE WHEN deadlocks > 0 THEN 'RED' ELSE 'GREEN' END,
    'RED if deadlocks > 0'
  FROM d
  UNION ALL
  SELECT
    'Rollback ratio',
    CASE
      WHEN (xact_rollback * 100.0 / NULLIF(xact_commit + xact_rollback,0)) > 5 THEN 'RED'
      WHEN (xact_rollback * 100.0 / NULLIF(xact_commit + xact_rollback,0)) > 1 THEN 'YELLOW'
      ELSE 'GREEN'
    END,
    'RED if rollback >5%; YELLOW if 1-5%'
  FROM d
  UNION ALL
  SELECT
    'Index-to-table ratio',
    CASE WHEN COALESCE(index_bytes / NULLIF(table_bytes,0),0) > 1.5 THEN 'YELLOW' ELSE 'GREEN' END,
    'YELLOW if index bytes exceed 150% of table bytes'
  FROM rel
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || area || '</td><td><span class="severity-pill ' ||
      CASE status WHEN 'RED' THEN 'pill-critical' WHEN 'YELLOW' THEN 'pill-warning' ELSE 'pill-good' END || '">' || status || '</span></td><td>' || rule || '</td></tr>',
      E'\n' ORDER BY area
    ),
    '<tr><td colspan="3" class="table-empty">No baseline flags available</td></tr>'
  )
FROM rowset;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S02: TOP SQL ANALYSIS
-- =============================================================================
\qecho '<div class="section" id="s02">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Top SQL Analysis</div>'
\qecho '    <div class="section-desc">Query-level diagnostics from pg_stat_statements: execution, planning overhead, I/O behavior, spill pressure, workload attribution, regression deltas, and triage leaderboard.</div>'
\qecho '  </div>'
\qecho '</div>'

-- Capability detection (read-only safe)
SELECT
  CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
            AND to_regclass('pg_stat_statements') IS NOT NULL
       THEN 'on' ELSE 'off' END AS s02_has_pgss,
  CASE
    WHEN to_regclass('pg_stat_statements') IS NULL THEN 'off'
    WHEN EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = 'pg_stat_statements'::regclass
        AND attname = 'total_plan_time'
        AND NOT attisdropped
    ) THEN 'on' ELSE 'off'
  END AS s02_has_plan,
  CASE
    WHEN to_regclass('pg_stat_statements') IS NULL THEN 'off'
    WHEN EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = 'pg_stat_statements'::regclass
        AND attname = 'wal_bytes'
        AND NOT attisdropped
    ) THEN 'on' ELSE 'off'
  END AS s02_has_wal,
  CASE
    WHEN to_regclass('pg_stat_statements') IS NULL THEN 'off'
    WHEN EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = 'pg_stat_statements'::regclass
        AND attname = 'blk_read_time'
        AND NOT attisdropped
    )
    AND EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = 'pg_stat_statements'::regclass
        AND attname = 'blk_write_time'
        AND NOT attisdropped
    ) THEN 'on' ELSE 'off'
  END AS s02_has_io_time,
  CASE WHEN COALESCE(current_setting('track_io_timing', true), 'off') = 'on' THEN 'on' ELSE 'off' END AS s02_track_io,
  CASE WHEN COALESCE(current_setting('shared_preload_libraries', true), '') ILIKE '%pg_stat_statements%' THEN 'on' ELSE 'off' END AS s02_preloaded,
  CASE
    WHEN to_regclass('pg_stat_statements_info') IS NOT NULL
    THEN COALESCE(NULLIF(
      (xpath('/row/stats_reset/text()', query_to_xml(
        'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
        false, true, ''
      )))[1]::text,
      ''
    ), '')
    ELSE ''
  END AS s02_stats_reset,
  CASE
    WHEN to_regclass('pg_stat_statements_info') IS NOT NULL
     AND NULLIF((xpath('/row/stats_reset/text()', query_to_xml(
           'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
           false, true, ''
         )))[1]::text, '') IS NOT NULL
    THEN round(
      extract(epoch FROM (
        clock_timestamp() -
        NULLIF((xpath('/row/stats_reset/text()', query_to_xml(
          'SELECT stats_reset::text AS stats_reset FROM pg_stat_statements_info LIMIT 1',
          false, true, ''
        )))[1]::text, '')::timestamptz
      )) / 3600.0,
      2
    )::text
    ELSE ''
  END AS s02_window_hours,
  CASE
    WHEN to_regclass('pg360_history.sql_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_history.sql_snapshot', 'SELECT')
    THEN 'on' ELSE 'off'
  END AS s02_has_history_snapshot,
  CASE
    WHEN to_regclass('pg360_history.sql_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_history.sql_snapshot', 'SELECT')
    THEN 'history'
    WHEN to_regclass('pg360_runtime.s02_pgss_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_runtime.s02_pgss_snapshot', 'SELECT')
    THEN 'legacy'
    ELSE 'off'
  END AS s02_snapshot_source,
  CASE
    WHEN (
      to_regclass('pg360_history.sql_snapshot') IS NOT NULL
      AND has_table_privilege(current_user, 'pg360_history.sql_snapshot', 'SELECT')
    ) OR (
      to_regclass('pg360_runtime.s02_pgss_snapshot') IS NOT NULL
      AND has_table_privilege(current_user, 'pg360_runtime.s02_pgss_snapshot', 'SELECT')
    )
    THEN 'on' ELSE 'off'
  END AS s02_has_snapshot
\gset

\if :s02_has_pgss
WITH s AS (
  SELECT
    COUNT(*)::bigint AS total_rows,
    COUNT(*) FILTER (
      WHERE query NOT ILIKE '%pg360%'
        AND query NOT ILIKE '%pg_stat_statements%'
        AND query NOT ILIKE 'BEGIN%'
        AND query NOT ILIKE 'COMMIT%'
        AND query NOT ILIKE 'SET %'
    )::bigint AS strict_rows
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
)
SELECT
  total_rows::text AS s02_total_rows,
  strict_rows::text AS s02_strict_rows,
  CASE WHEN strict_rows = 0 AND total_rows > 0 THEN 'on' ELSE 'off' END AS s02_relax_pgss_filter
FROM s
\gset
\else
\set s02_total_rows 0
\set s02_strict_rows 0
\set s02_relax_pgss_filter off
\endif

\if :s02_has_io_time
\set s02_io_cols 'blk_read_time::double precision AS blk_read_time, blk_write_time::double precision AS blk_write_time,'
\else
\set s02_io_cols 'NULL::double precision AS blk_read_time, NULL::double precision AS blk_write_time,'
\endif

\if :s02_has_plan
\set s02_plan_cols 'total_plan_time::double precision AS total_plan_time, mean_plan_time::double precision AS mean_plan_time, stddev_plan_time::double precision AS stddev_plan_time,'
\else
\set s02_plan_cols 'NULL::double precision AS total_plan_time, NULL::double precision AS mean_plan_time, NULL::double precision AS stddev_plan_time,'
\endif

\if :s02_has_wal
\set s02_wal_cols 'wal_bytes::numeric AS wal_bytes, wal_records::numeric AS wal_records, wal_fpi::numeric AS wal_fpi,'
\else
\set s02_wal_cols 'NULL::numeric AS wal_bytes, NULL::numeric AS wal_records, NULL::numeric AS wal_fpi,'
\endif

-- S02.1 Prerequisites and measurement window
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Prerequisites and Measurement Window</div>'

SELECT
  CASE
    WHEN :'s02_has_pgss' <> 'on' THEN
      '<div class="finding critical"><div class="finding-header">' ||
      '<span class="finding-title">pg_stat_statements extension is not available</span>' ||
      '<span class="severity-pill pill-critical">BLOCKED</span></div>' ||
      '<div class="finding-body">Install and preload pg_stat_statements to enable SQL diagnostics in S02.</div></div>'
    WHEN :'s02_preloaded' <> 'on' THEN
      '<div class="finding high"><div class="finding-header">' ||
      '<span class="finding-title">pg_stat_statements is not preloaded at startup</span>' ||
      '<span class="severity-pill pill-high">ACTION</span></div>' ||
      '<div class="finding-body">Set shared_preload_libraries = ''pg_stat_statements'' and restart PostgreSQL.</div></div>'
    WHEN COALESCE(NULLIF(:'s02_window_hours',''),'0')::numeric > 0
      AND COALESCE(NULLIF(:'s02_window_hours',''),'0')::numeric < 24 THEN
      '<div class="finding high"><div class="finding-header">' ||
      '<span class="finding-title">Short measurement window after stats reset</span>' ||
      '<span class="severity-pill pill-warning">CONTEXT</span></div>' ||
      '<div class="finding-body">Metrics reflect approximately <strong>' || :'s02_window_hours' ||
      ' hours</strong> since stats reset (' || COALESCE(NULLIF(:'s02_stats_reset',''),'unknown') || ').</div></div>'
    ELSE
      '<div class="finding good"><div class="finding-header">' ||
      '<span class="finding-title">diagnostics prerequisites are available</span>' ||
      '<span class="severity-pill pill-good">READY</span></div>' ||
      '<div class="finding-body">Measurement window start: ' || COALESCE(NULLIF(:'s02_stats_reset',''),'unknown') || '.</div></div>'
  END;

\if :s02_has_pgss
\if :s02_relax_pgss_filter
SELECT
  '<div class="finding info"><div class="finding-header"><span class="finding-title">Strict SQL hygiene filter produced no rows; fallback mode enabled</span><span class="severity-pill pill-info">FALLBACK</span></div><div class="finding-body">Captured rows in pg_stat_statements: ' ||
  :'s02_total_rows' || '. Rows after strict filter: ' || :'s02_strict_rows' ||
  '. S02 now includes pg_stat_statements-tagged rows so this section is not blank. Run representative application workload for cleaner ranking.</div></div>';
\endif
\endif

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Status</th><th>Details</th>'
\qecho '</tr></thead><tbody>'

WITH checks AS (
  SELECT 1 AS ord, 'pg_stat_statements installed' AS check_name,
         CASE WHEN :'s02_has_pgss' = 'on' THEN 'PASS' ELSE 'FAIL' END AS status,
         CASE WHEN :'s02_has_pgss' = 'on' THEN 'Extension detected in current database.' ELSE 'Install extension for SQL-level diagnostics.' END AS details
  UNION ALL
  SELECT 2, 'pg_stat_statements preloaded',
         CASE WHEN :'s02_preloaded' = 'on' THEN 'PASS' ELSE 'FAIL' END,
         CASE WHEN :'s02_preloaded' = 'on' THEN 'shared_preload_libraries includes pg_stat_statements.' ELSE 'Requires restart after preload configuration.' END
  UNION ALL
  SELECT 3, 'track_io_timing',
         CASE WHEN :'s02_track_io' = 'on' THEN 'PASS' ELSE 'WARN' END,
         CASE WHEN :'s02_track_io' = 'on' THEN 'I/O latency attribution enabled.' ELSE 'Enable track_io_timing for precise IO-bound classification.' END
  UNION ALL
  SELECT 4, 'Planning-time columns',
         CASE WHEN :'s02_has_plan' = 'on' THEN 'PASS' ELSE 'INFO' END,
         CASE WHEN :'s02_has_plan' = 'on' THEN 'total_plan_time / mean_plan_time available.' ELSE 'Planning split unavailable in this pg_stat_statements version.' END
  UNION ALL
  SELECT 5, 'WAL columns',
         CASE WHEN :'s02_has_wal' = 'on' THEN 'PASS' ELSE 'INFO' END,
         CASE WHEN :'s02_has_wal' = 'on' THEN 'wal_bytes / wal_records available.' ELSE 'WAL write-amplification metrics unavailable.' END
  UNION ALL
  SELECT 6, 'Stats reset horizon',
         CASE
           WHEN COALESCE(NULLIF(:'s02_window_hours',''),'') = '' THEN 'INFO'
           WHEN COALESCE(NULLIF(:'s02_window_hours',''),'0')::numeric < 24 THEN 'WARN'
           ELSE 'PASS'
         END,
         CASE
           WHEN COALESCE(NULLIF(:'s02_window_hours',''),'') = '' THEN 'stats_reset not visible'
           ELSE 'Window is ' || :'s02_window_hours' || ' hours since reset.'
         END
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || check_name || '</td><td><span class="severity-pill ' ||
      CASE status
        WHEN 'PASS' THEN 'pill-good'
        WHEN 'FAIL' THEN 'pill-critical'
        WHEN 'WARN' THEN 'pill-warning'
        ELSE 'pill-info'
      END || '">' || status || '</span></td><td>' || details || '</td></tr>',
      E'\n' ORDER BY ord
    ),
    '<tr><td colspan="3" class="table-empty">No prerequisite checks available</td></tr>'
  )
FROM checks;

\qecho '</tbody></table></div></div>'

\if :s02_has_pgss

-- S02.2 Top SQL by total execution time + resource class
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top SQL by Total Execution Time</div>'
\qecho '<input class="table-search" type="text" placeholder="Filter queries..." data-table-target="t02_2">'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360" id="t02_2"><thead><tr>'
\qecho '<th>#</th><th>Query (normalized)</th><th>Calls</th><th>Total Exec (ms)</th><th>Mean (ms)</th><th>Stddev (ms)</th><th>Rows/call</th><th>Cache Hit%</th><th>Temp Blks</th><th>Resource Class</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    COALESCE(queryid::text, md5(query)) AS queryid_text,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    stddev_exec_time::double precision AS stddev_exec_time,
    max_exec_time::double precision AS max_exec_time,
    rows::numeric AS rows,
    shared_blks_hit::bigint AS shared_blks_hit,
    shared_blks_read::bigint AS shared_blks_read,
    shared_blks_dirtied::bigint AS shared_blks_dirtied,
    shared_blks_written::bigint AS shared_blks_written,
    temp_blks_read::bigint AS temp_blks_read,
    temp_blks_written::bigint AS temp_blks_written,
    :s02_io_cols
    :s02_wal_cols
    :s02_plan_cols
    dbid::oid AS dbid,
    userid::oid AS userid,
    md5(query || '|' || userid::text || '|' || dbid::text) AS fingerprint
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
    AND query NOT ILIKE 'BEGIN%'
    AND query NOT ILIKE 'COMMIT%'
    AND query NOT ILIKE 'SET %'
), q AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY total_exec_time DESC) AS rn,
    query_text,
    calls,
    total_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows / NULLIF(calls,0) AS rows_per_call,
    (shared_blks_hit::numeric / NULLIF(shared_blks_hit + shared_blks_read, 0) * 100) AS cache_hit_pct,
    temp_blks_read + temp_blks_written AS temp_blks,
    CASE
      WHEN (temp_blks_read + temp_blks_written) > GREATEST(100, calls * 10) THEN 'Spill-bound'
      WHEN (COALESCE(blk_read_time, 0) + COALESCE(blk_write_time, 0)) > total_exec_time * 0.35 THEN 'IO-bound'
      WHEN shared_blks_read > shared_blks_hit * 0.5 AND (shared_blks_read + shared_blks_hit) > 1000 THEN 'IO-bound'
      WHEN total_exec_time > 0
       AND (COALESCE(blk_read_time, 0) + COALESCE(blk_write_time, 0)) < total_exec_time * 0.1
       AND (temp_blks_read + temp_blks_written) = 0 THEN 'CPU-bound'
      ELSE 'Mixed'
    END AS resource_class
  FROM base
  ORDER BY total_exec_time DESC
  LIMIT 25
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || rn || '</td>' ||
      '<td title="' ||
      replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text, E'''[^'']*''', '''?''', 'g'), '\\b\\d+\\.?\\d*\\b', '?', 'g'), 480),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '">' ||
      replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text, E'''[^'']*''', '''?''', 'g'), '\\b\\d+\\.?\\d*\\b', '?', 'g'), 120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(calls, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN total_exec_time > 600000 THEN 'crit' WHEN total_exec_time > 120000 THEN 'warn' ELSE '' END || '">' || to_char(round(total_exec_time::numeric,1), 'FM999,999,990.0') || '</td>' ||
      '<td class="num ' || CASE WHEN mean_exec_time > 1000 THEN 'crit' WHEN mean_exec_time > 100 THEN 'warn' ELSE '' END || '">' || to_char(round(mean_exec_time::numeric,2), 'FM999,990.00') || '</td>' ||
      '<td class="num">' || to_char(round(stddev_exec_time::numeric,2), 'FM999,990.00') || '</td>' ||
      '<td class="num">' || COALESCE(to_char(round(rows_per_call::numeric,2), 'FM999,990.00'), 'N/A') || '</td>' ||
      '<td class="num ' || CASE WHEN cache_hit_pct < 80 THEN 'crit' WHEN cache_hit_pct < 95 THEN 'warn' ELSE 'good' END || '">' || COALESCE(round(cache_hit_pct::numeric,1)::text, 'N/A') || '%</td>' ||
      '<td class="num ' || CASE WHEN temp_blks > 0 THEN 'warn' ELSE '' END || '">' || to_char(temp_blks, 'FM999,999,999') || '</td>' ||
      '<td><span class="severity-pill ' ||
      CASE resource_class
        WHEN 'IO-bound' THEN 'pill-warning'
        WHEN 'Spill-bound' THEN 'pill-warning'
        WHEN 'CPU-bound' THEN 'pill-info'
        ELSE 'pill-good'
      END || '">' || resource_class || '</span></td>' ||
      '</tr>', E'\n' ORDER BY rn
    ),
    '<tr><td colspan="10" class="table-empty">No pg_stat_statements rows available for this database</td></tr>'
  )
FROM q;

\qecho '</tbody></table></div></div>'

-- S02.3 Top SQL by calls + % load + p95 approx
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top SQL by Calls</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>#</th><th>Query (normalized)</th><th>Calls</th><th>% of Calls</th><th>% of Total Time</th><th>Mean (ms)</th><th>Approx p95 (ms)</th><th>Stddev (ms)</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    stddev_exec_time::double precision AS stddev_exec_time
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
    AND query NOT ILIKE 'BEGIN%'
    AND query NOT ILIKE 'COMMIT%'
    AND query NOT ILIKE 'SET %'
), tot AS (
  SELECT COALESCE(SUM(calls),0)::numeric AS total_calls,
         COALESCE(SUM(total_exec_time),0)::numeric AS total_exec
  FROM base
), q AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY b.calls DESC) AS rn,
    b.query_text,
    b.calls,
    b.total_exec_time,
    b.mean_exec_time,
    b.stddev_exec_time,
    b.calls * 100.0 / NULLIF(t.total_calls,0) AS pct_calls,
    b.total_exec_time * 100.0 / NULLIF(t.total_exec,0) AS pct_total_time,
    GREATEST(b.mean_exec_time + (1.645 * COALESCE(b.stddev_exec_time,0)), 0) AS approx_p95_ms
  FROM base b
  CROSS JOIN tot t
  ORDER BY b.calls DESC
  LIMIT 25
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || rn || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN calls > 50000 THEN 'warn' ELSE '' END || '">' || to_char(calls, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || COALESCE(round(pct_calls::numeric,2)::text,'N/A') || '%</td>' ||
      '<td class="num">' || COALESCE(round(pct_total_time::numeric,2)::text,'N/A') || '%</td>' ||
      '<td class="num">' || to_char(round(mean_exec_time::numeric,2), 'FM999,990.00') || '</td>' ||
      '<td class="num ' || CASE WHEN approx_p95_ms > mean_exec_time * 4 THEN 'warn' ELSE '' END || '">' || to_char(round(approx_p95_ms::numeric,2), 'FM999,990.00') || '</td>' ||
      '<td class="num">' || to_char(round(stddev_exec_time::numeric,2), 'FM999,990.00') || '</td>' ||
      '</tr>', E'\n' ORDER BY rn
    ),
    '<tr><td colspan="8" class="table-empty">No high-call SQL captured</td></tr>'
  )
FROM q;

\qecho '</tbody></table></div></div>'

-- S02.3a Resource attribution using pg_stat_statements
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Query Resource Attribution</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>#</th><th>Query (normalized)</th><th>Calls</th><th>% Exec Time</th><th>% CPU Proxy</th><th>% I/O Time</th><th>Temp Spill</th><th>Dominant Resource</th>'
\qecho '</tr></thead><tbody>'

WITH raw AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    :s02_io_cols
    temp_blks_written::bigint AS temp_blks_written,
    dbid
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
    AND query NOT ILIKE 'BEGIN%'
    AND query NOT ILIKE 'COMMIT%'
    AND query NOT ILIKE 'SET %'
), base AS (
  SELECT
    query_text,
    calls,
    total_exec_time,
    blk_read_time,
    blk_write_time,
    temp_blks_written,
    GREATEST(
      total_exec_time - (COALESCE(blk_read_time, 0) + COALESCE(blk_write_time, 0)),
      0
    ) AS cpu_proxy_time,
    (COALESCE(blk_read_time, 0) + COALESCE(blk_write_time, 0)) AS io_time,
    (COALESCE(temp_blks_written, 0)::bigint * current_setting('block_size')::bigint) AS temp_bytes_written
  FROM raw
), totals AS (
  SELECT
    SUM(total_exec_time) AS total_exec_time_all,
    SUM(io_time) AS total_io_time_all,
    SUM(cpu_proxy_time) AS total_cpu_proxy_time_all,
    SUM(temp_bytes_written) AS total_temp_bytes_all
  FROM base
), q AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY b.total_exec_time DESC) AS rn,
    b.query_text,
    b.calls,
    b.total_exec_time,
    b.cpu_proxy_time,
    b.io_time,
    b.temp_bytes_written,
    round((100.0 * b.total_exec_time / NULLIF(t.total_exec_time_all, 0))::numeric, 2) AS pct_total_exec_time,
    round((100.0 * b.cpu_proxy_time / NULLIF(t.total_cpu_proxy_time_all, 0))::numeric, 2) AS pct_cpu_proxy_time,
    round((100.0 * b.io_time / NULLIF(t.total_io_time_all, 0))::numeric, 2) AS pct_io_time,
    CASE
      WHEN b.temp_bytes_written >= 1024::bigint * 1024 * 1024 THEN 'Memory spill heavy'
      WHEN b.io_time > b.cpu_proxy_time AND b.io_time > 0 THEN 'I/O heavy'
      WHEN b.cpu_proxy_time >= b.io_time THEN 'CPU heavy'
      ELSE 'Mixed'
    END AS dominant_resource_proxy
  FROM base b
  CROSS JOIN totals t
  ORDER BY b.total_exec_time DESC
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td class="num">' || rn || '</td>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(calls, 'FM999,999,999') || '</td>' ||
    '<td class="num">' || COALESCE(to_char(pct_total_exec_time, 'FM999,990.00'), 'N/A') || '%</td>' ||
    '<td class="num">' || COALESCE(to_char(pct_cpu_proxy_time, 'FM999,990.00'), 'N/A') || '%</td>' ||
    '<td class="num">' || COALESCE(to_char(pct_io_time, 'FM999,990.00'), 'N/A') || '%</td>' ||
    '<td class="num ' || CASE WHEN temp_bytes_written > 0 THEN 'warn' ELSE '' END || '">' || pg_size_pretty(temp_bytes_written) || '</td>' ||
    '<td>' || dominant_resource_proxy || '</td>' ||
    '</tr>',
    E'\n' ORDER BY rn
  ),
  '<tr><td colspan="8" class="table-empty">No query resource attribution available from current pg_stat_statements sample</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.4 Planning vs execution split
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Planning vs Execution Overhead</div>'

\if :s02_has_plan
SELECT '<div class="finding good"><div class="finding-header"><span class="finding-title">Planning-time analysis enabled</span><span class="severity-pill pill-good">READY</span></div><div class="finding-body">Ranking queries by planner overhead (total_plan_time / total_exec_time).</div></div>';
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Planning-time fields unavailable</span><span class="severity-pill pill-info">N/A</span></div><div class="finding-body">This pg_stat_statements version does not expose total_plan_time. Planning split omitted.</div></div>';
\endif

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>#</th><th>Query (normalized)</th><th>Calls</th><th>Total Plan (ms)</th><th>Total Exec (ms)</th><th>Plan/Exec %</th><th>Mean Plan (ms)</th><th>Mean Exec (ms)</th>'
\qecho '</tr></thead><tbody>'

\if :s02_has_plan
WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    total_plan_time::double precision AS total_plan_time,
    mean_plan_time::double precision AS mean_plan_time
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY total_plan_time DESC) AS rn,
    query_text,
    calls,
    total_plan_time,
    total_exec_time,
    mean_plan_time,
    mean_exec_time,
    total_plan_time * 100.0 / NULLIF(total_exec_time,0) AS plan_ratio_pct
  FROM base
  WHERE COALESCE(total_plan_time,0) > 0
  ORDER BY total_plan_time DESC
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td class="num">' || rn || '</td>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(round(total_plan_time::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num">' || to_char(round(total_exec_time::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num ' || CASE WHEN plan_ratio_pct > 40 THEN 'crit' WHEN plan_ratio_pct > 15 THEN 'warn' ELSE 'good' END || '">' || COALESCE(round(plan_ratio_pct::numeric,2)::text,'N/A') || '%</td>' ||
    '<td class="num">' || to_char(round(mean_plan_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(round(mean_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '</tr>', E'\n' ORDER BY rn
  ),
  '<tr><td colspan="8" class="table-empty">No planner-heavy SQL identified</td></tr>'
) FROM q;
\else
SELECT '<tr><td colspan="8" class="table-empty">Planning columns unavailable in this PostgreSQL version</td></tr>';
\endif

\qecho '</tbody></table></div></div>'

-- S02.5 Row-efficiency and wasted-work signals
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Rows Efficiency &amp; Wasted-Work</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>#</th><th>Query (normalized)</th><th>Total Exec (ms)</th><th>Calls</th><th>Rows</th><th>Rows/call</th><th>ms/row</th><th>Shared Read/row</th><th>Signal</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    rows::numeric AS rows,
    shared_blks_read::numeric AS shared_blks_read
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY total_exec_time DESC) AS rn,
    query_text,
    total_exec_time,
    calls,
    rows,
    rows / NULLIF(calls,0) AS rows_per_call,
    total_exec_time / NULLIF(rows,0) AS ms_per_row,
    shared_blks_read / NULLIF(rows,0) AS blks_per_row,
    CASE
      WHEN rows = 0 AND total_exec_time > 10000 THEN 'High time with zero rows returned'
      WHEN total_exec_time > 60000 AND COALESCE(rows / NULLIF(calls,0),0) < 2 THEN 'High elapsed time for low returned rows'
      WHEN COALESCE(shared_blks_read / NULLIF(rows,0),0) > 50 THEN 'Many blocks read per returned row'
      ELSE 'Review predicate/index selectivity'
    END AS signal
  FROM base
  WHERE total_exec_time > 5000
  ORDER BY total_exec_time DESC
  LIMIT 25
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td class="num">' || rn || '</td>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),110),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(round(total_exec_time::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(COALESCE(rows,0),'FM999,999,999') || '</td>' ||
    '<td class="num">' || COALESCE(to_char(round(rows_per_call::numeric,2),'FM999,990.00'),'N/A') || '</td>' ||
    '<td class="num ' || CASE WHEN COALESCE(ms_per_row,0) > 10 THEN 'warn' ELSE '' END || '">' || COALESCE(to_char(round(ms_per_row::numeric,3),'FM999,990.000'),'N/A') || '</td>' ||
    '<td class="num ' || CASE WHEN COALESCE(blks_per_row,0) > 20 THEN 'warn' ELSE '' END || '">' || COALESCE(to_char(round(blks_per_row::numeric,2),'FM999,990.00'),'N/A') || '</td>' ||
    '<td>' || signal || '</td>' ||
    '</tr>', E'\n' ORDER BY rn
  ),
  '<tr><td colspan="9" class="table-empty">No row-efficiency signals detected in current sample</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.6 High variability with sample guard
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">High Variability Queries (calls >= 20)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Query (normalized)</th><th>Calls</th><th>Mean (ms)</th><th>Stddev (ms)</th><th>Stddev/Mean</th><th>Likely Cause</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    mean_exec_time::double precision AS mean_exec_time,
    stddev_exec_time::double precision AS stddev_exec_time,
    total_exec_time::double precision AS total_exec_time,
    temp_blks_read::bigint AS temp_blks_read,
    temp_blks_written::bigint AS temp_blks_written,
    shared_blks_hit::bigint AS shared_blks_hit,
    shared_blks_read::bigint AS shared_blks_read,
    :s02_plan_cols
    userid::oid AS userid
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    query_text,
    calls,
    mean_exec_time,
    stddev_exec_time,
    stddev_exec_time / NULLIF(mean_exec_time,0) AS var_ratio,
    CASE
      WHEN (temp_blks_read + temp_blks_written) > GREATEST(200, calls * 5) THEN 'Spill sensitivity (sort/hash memory pressure)'
      WHEN shared_blks_read > shared_blks_hit * 0.4 THEN 'Cache/data-locality variability'
      WHEN COALESCE(total_plan_time,0) > total_exec_time * 0.2 THEN 'Planner overhead and query-shape churn'
      ELSE 'Parameter selectivity or stale statistics drift'
    END AS likely_cause
  FROM base
  WHERE calls >= 20
    AND mean_exec_time > 0
    AND stddev_exec_time / NULLIF(mean_exec_time,0) > 2
  ORDER BY stddev_exec_time / NULLIF(mean_exec_time,0) DESC
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),110),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(round(mean_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num warn">' || to_char(round(stddev_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num crit">' || to_char(round(var_ratio::numeric,2),'FM999,990.00') || 'x</td>' ||
    '<td>' || likely_cause || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="6" class="table-empty">No high-variability SQL above sample threshold</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.7 Temp I/O and spill diagnostics
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Temp I/O and Spill Diagnostics</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Query (normalized)</th><th>Calls</th><th>Temp Read Blks</th><th>Temp Written Blks</th><th>Mean (ms)</th><th>Action Focus</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    temp_blks_read::bigint AS temp_blks_read,
    temp_blks_written::bigint AS temp_blks_written,
    mean_exec_time::double precision AS mean_exec_time
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    query_text,
    calls,
    temp_blks_read,
    temp_blks_written,
    mean_exec_time,
    CASE
      WHEN temp_blks_written > GREATEST(10000, calls * 10) THEN 'Reduce sort/hash footprint; review join order and projected columns'
      WHEN query_text ILIKE '%ORDER BY%' AND query_text ILIKE '%LIMIT%' THEN 'Evaluate index for ORDER BY + LIMIT pushdown'
      WHEN query_text ILIKE '%GROUP BY%' THEN 'Review aggregation path; consider pre-aggregation/materialized view'
      ELSE 'Tune work_mem per role/workload scope, not globally'
    END AS action_focus
  FROM base
  WHERE (temp_blks_read + temp_blks_written) > 0
  ORDER BY (temp_blks_read + temp_blks_written) DESC
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),110),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num warn">' || to_char(temp_blks_read,'FM999,999,999') || '</td>' ||
    '<td class="num warn">' || to_char(temp_blks_written,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(round(mean_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td>' || action_focus || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="6" class="table-empty">No temp I/O spills detected</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.8 Over-calling / chatty query patterns
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Over-calling and Chatty Access Patterns</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Query (normalized)</th><th>Calls</th><th>Mean (ms)</th><th>Total (ms)</th><th>Rows/call</th><th>Pattern</th><th>Suggested Action</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    mean_exec_time::double precision AS mean_exec_time,
    total_exec_time::double precision AS total_exec_time,
    rows::numeric AS rows,
    :s02_plan_cols
    userid::oid AS userid
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    query_text,
    calls,
    mean_exec_time,
    total_exec_time,
    rows / NULLIF(calls,0) AS rows_per_call,
    CASE
      WHEN query_text ILIKE '%LIMIT%' AND query_text ILIKE '%OFFSET%' AND calls > 5000 THEN 'Offset pagination loop'
      WHEN calls > 20000 AND mean_exec_time < 5 AND rows / NULLIF(calls,0) <= 1.5 THEN 'N+1 / single-row loop'
      WHEN calls > 20000 AND COALESCE(total_plan_time,0) > total_exec_time * 0.2 THEN 'High planning churn (likely unprepared statements)'
      ELSE 'High-frequency query shape'
    END AS pattern,
    CASE
      WHEN query_text ILIKE '%LIMIT%' AND query_text ILIKE '%OFFSET%' AND calls > 5000 THEN 'Adopt keyset pagination and supporting composite index'
      WHEN calls > 20000 AND mean_exec_time < 5 AND rows / NULLIF(calls,0) <= 1.5 THEN 'Batch lookups or eager loading to reduce round trips'
      WHEN calls > 20000 AND COALESCE(total_plan_time,0) > total_exec_time * 0.2 THEN 'Use prepared statements and stable SQL shapes'
      ELSE 'Validate app-side cache and call frequency'
    END AS suggested_action
  FROM base
  WHERE calls > 8000
  ORDER BY calls DESC
  LIMIT 20
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),105),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num warn">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(round(mean_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(round(total_exec_time::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num">' || COALESCE(to_char(round(rows_per_call::numeric,2),'FM999,990.00'),'N/A') || '</td>' ||
    '<td>' || pattern || '</td>' ||
    '<td>' || suggested_action || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="7" class="table-empty">No high-frequency over-calling patterns above threshold</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.9 Application attribution with unknown ratio
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload Attribution by Application Name</div>'

SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Total Sessions</div><div class="card-value">' || COUNT(*) FILTER (WHERE pid <> pg_backend_pid()) || '</div></div>' ||
  '<div class="card"><div class="card-label">Distinct application_name</div><div class="card-value">' || COUNT(DISTINCT COALESCE(NULLIF(application_name,''), '(unknown)')) FILTER (WHERE pid <> pg_backend_pid()) || '</div></div>' ||
  '<div class="card ' ||
    CASE
      WHEN COUNT(*) FILTER (WHERE pid <> pg_backend_pid()) = 0 THEN 'good'
      WHEN (COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND COALESCE(NULLIF(application_name,''), '(unknown)') = '(unknown)') * 100.0 /
            NULLIF(COUNT(*) FILTER (WHERE pid <> pg_backend_pid()),0)) > 20 THEN 'warning'
      ELSE 'good'
    END ||
  '"><div class="card-label">Unknown app_name %</div><div class="card-value">' ||
  COALESCE(round(
    COUNT(*) FILTER (WHERE pid <> pg_backend_pid() AND COALESCE(NULLIF(application_name,''), '(unknown)') = '(unknown)') * 100.0 /
    NULLIF(COUNT(*) FILTER (WHERE pid <> pg_backend_pid()),0), 1
  )::text, '0') || '%</div></div>' ||
  '</div>'
FROM pg_stat_activity
WHERE datname = current_database();

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Application</th><th>Total Sessions</th><th>Active</th><th>Waiting Active</th><th>Longest Active Age</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

WITH s AS (
  SELECT
    COALESCE(NULLIF(application_name,''), '(unknown)') AS app_name,
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE state = 'active') AS active_sessions,
    COUNT(*) FILTER (WHERE state = 'active' AND wait_event IS NOT NULL) AS waiting_active,
    MAX(EXTRACT(EPOCH FROM (clock_timestamp() - query_start))) FILTER (WHERE state = 'active') AS longest_active_secs
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
  GROUP BY COALESCE(NULLIF(application_name,''), '(unknown)')
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(app_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || total_sessions || '</td>' ||
    '<td class="num">' || active_sessions || '</td>' ||
    '<td class="num ' || CASE WHEN waiting_active > 0 THEN 'warn' ELSE '' END || '">' || waiting_active || '</td>' ||
    '<td class="num">' || COALESCE(to_char(INTERVAL '1 second' * longest_active_secs::int, 'HH24:MI:SS'),'00:00:00') || '</td>' ||
    '<td>' ||
    CASE
      WHEN app_name = '(unknown)' THEN 'Set application_name in connection string/pooler for attribution fidelity'
      WHEN waiting_active > 0 THEN 'Investigate lock/wait events for this app cohort'
      ELSE 'Attribution signal is healthy'
    END || '</td>' ||
    '</tr>', E'\n' ORDER BY total_sessions DESC
  ),
  '<tr><td colspan="6" class="table-empty">No application sessions visible</td></tr>'
) FROM s;

\qecho '</tbody></table></div></div>'

-- S02.10 Regression vs latest stored snapshot (read-only best effort)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Regressions Since Previous Snapshot</div>'

\if :s02_has_snapshot
\if :s02_has_history_snapshot
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Delta compared with latest repository capture</span><span class="severity-pill pill-info">DELTA</span></div><div class="finding-body">Uses pg360_history.sql_snapshot as the preferred baseline source.</div></div>';
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Delta compared with latest stored snapshot</span><span class="severity-pill pill-info">DELTA</span></div><div class="finding-body">Uses pg360_runtime.s02_pgss_snapshot as a legacy baseline source.</div></div>';
\endif
\else
SELECT '<div class="finding info"><div class="finding-header"><span class="finding-title">Snapshot source unavailable in read-only mode</span><span class="severity-pill pill-info">BASELINE</span></div><div class="finding-body">Create snapshot table outside this read-only script to enable run-to-run deltas.</div></div>';
\endif

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Query (normalized)</th><th>Δ Total Exec (ms)</th><th>Δ Mean Exec (ms)</th><th>Δ Calls</th><th>Δ Shared Reads</th><th>Δ Temp Written</th>'
\qecho '</tr></thead><tbody>'

\if :s02_has_snapshot
\if :s02_has_history_snapshot
WITH base AS (
  SELECT
    md5(query || '|' || userid::text || '|' || dbid::text) AS fingerprint,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    shared_blks_read::bigint AS shared_blks_read,
    temp_blks_written::bigint AS temp_blks_written
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), prev AS (
  SELECT
    s.fingerprint,
    s.query_text,
    s.calls,
    s.total_exec_time,
    s.mean_exec_time,
    s.shared_blks_read,
    s.temp_blks_written
  FROM pg360_history.sql_snapshot s
  JOIN pg360_history.run_snapshot r ON r.run_id = s.run_id
  WHERE s.dbname = current_database()
    AND r.captured_at = (
      SELECT max(r2.captured_at)
      FROM pg360_history.run_snapshot r2
      WHERE r2.dbname = current_database()
    )
), d AS (
  SELECT
    b.query_text,
    b.total_exec_time - COALESCE(p.total_exec_time,0) AS delta_total_exec,
    b.mean_exec_time - COALESCE(p.mean_exec_time,0) AS delta_mean_exec,
    b.calls - COALESCE(p.calls,0) AS delta_calls,
    b.shared_blks_read - COALESCE(p.shared_blks_read,0) AS delta_shared_reads,
    b.temp_blks_written - COALESCE(p.temp_blks_written,0) AS delta_temp_written
  FROM base b
  LEFT JOIN prev p USING (fingerprint)
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),115),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num ' || CASE WHEN delta_total_exec > 0 THEN 'warn' ELSE 'good' END || '">' || to_char(round(delta_total_exec::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num ' || CASE WHEN delta_mean_exec > 0 THEN 'warn' ELSE 'good' END || '">' || to_char(round(delta_mean_exec::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(delta_calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(delta_shared_reads,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(delta_temp_written,'FM999,999,999') || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="6" class="table-empty">No delta rows available from repository capture</td></tr>'
)
FROM (
  SELECT * FROM d ORDER BY delta_total_exec DESC LIMIT 10
) r;
\else
WITH base AS (
  SELECT
    md5(query || '|' || userid::text || '|' || dbid::text) AS fingerprint,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    shared_blks_read::bigint AS shared_blks_read,
    temp_blks_written::bigint AS temp_blks_written
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), prev AS (
  SELECT
    fingerprint,
    query_text,
    calls,
    total_exec_time,
    mean_exec_time,
    shared_blks_read,
    temp_blks_written
  FROM pg360_runtime.s02_pgss_snapshot
  WHERE dbname = current_database()
    AND captured_at = (
      SELECT max(captured_at)
      FROM pg360_runtime.s02_pgss_snapshot
      WHERE dbname = current_database()
    )
), d AS (
  SELECT
    b.query_text,
    b.total_exec_time - COALESCE(p.total_exec_time,0) AS delta_total_exec,
    b.mean_exec_time - COALESCE(p.mean_exec_time,0) AS delta_mean_exec,
    b.calls - COALESCE(p.calls,0) AS delta_calls,
    b.shared_blks_read - COALESCE(p.shared_blks_read,0) AS delta_shared_reads,
    b.temp_blks_written - COALESCE(p.temp_blks_written,0) AS delta_temp_written
  FROM base b
  LEFT JOIN prev p USING (fingerprint)
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(regexp_replace(query_text,E'''[^'']*''','''?''','g'),'\\b\\d+\\.?\\d*\\b','?','g'),115),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num ' || CASE WHEN delta_total_exec > 0 THEN 'warn' ELSE 'good' END || '">' || to_char(round(delta_total_exec::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num ' || CASE WHEN delta_mean_exec > 0 THEN 'warn' ELSE 'good' END || '">' || to_char(round(delta_mean_exec::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(delta_calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(delta_shared_reads,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(delta_temp_written,'FM999,999,999') || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="6" class="table-empty">No delta rows available from stored snapshot</td></tr>'
)
FROM (
  SELECT * FROM d ORDER BY delta_total_exec DESC LIMIT 10
) r;
\endif
\else
SELECT '<tr><td colspan="6" class="table-empty">Stored snapshot table not available for delta comparison</td></tr>';
\endif

\qecho '</tbody></table></div></div>'

-- S02.11 Unified leaderboard
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Unified SQL Leaderboard (Triage View)</div>'
\qecho '<input class="table-search" type="text" placeholder="Filter leaderboard..." data-table-target="t02_11">'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360" id="t02_11"><thead><tr>'
\qecho '<th>QueryID</th><th>User</th><th>Calls</th><th>Total (ms)</th><th>Mean (ms)</th><th>Stddev (ms)</th><th>Shared Hit/Read</th><th>Temp Blks</th><th>WAL Bytes</th><th>Resource</th><th>Flags</th>'
\qecho '</tr></thead><tbody>'

WITH base AS (
  SELECT
    COALESCE(queryid::text, md5(query)) AS queryid_text,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    stddev_exec_time::double precision AS stddev_exec_time,
    shared_blks_hit::bigint AS shared_blks_hit,
    shared_blks_read::bigint AS shared_blks_read,
    temp_blks_read::bigint AS temp_blks_read,
    temp_blks_written::bigint AS temp_blks_written,
    :s02_io_cols
    :s02_wal_cols
    :s02_plan_cols
    userid::oid AS userid
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), q AS (
  SELECT
    b.queryid_text,
    CASE
      WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes')
       AND r.rolname = current_user
      THEN :'pg360_redaction_token'
      ELSE COALESCE(r.rolname, '(unknown)')
    END AS user_name,
    b.calls,
    b.total_exec_time,
    b.mean_exec_time,
    b.stddev_exec_time,
    b.shared_blks_hit,
    b.shared_blks_read,
    b.temp_blks_read + b.temp_blks_written AS temp_blks,
    b.wal_bytes,
    CASE
      WHEN (b.temp_blks_read + b.temp_blks_written) > GREATEST(100, b.calls * 10) THEN 'Spill-bound'
      WHEN (COALESCE(b.blk_read_time,0) + COALESCE(b.blk_write_time,0)) > b.total_exec_time * 0.35 THEN 'IO-bound'
      WHEN b.shared_blks_read > b.shared_blks_hit * 0.5 AND (b.shared_blks_read + b.shared_blks_hit) > 1000 THEN 'IO-bound'
      WHEN b.total_exec_time > 0
       AND (COALESCE(b.blk_read_time,0) + COALESCE(b.blk_write_time,0)) < b.total_exec_time * 0.1
       AND (b.temp_blks_read + b.temp_blks_written) = 0 THEN 'CPU-bound'
      ELSE 'Mixed'
    END AS resource_class,
    CASE WHEN b.stddev_exec_time / NULLIF(b.mean_exec_time,0) > 2 AND b.calls >= 20 THEN 'VARIABILITY' ELSE NULL END AS f_var,
    CASE WHEN (b.temp_blks_read + b.temp_blks_written) > 0 THEN 'SPILL' ELSE NULL END AS f_spill,
    CASE WHEN b.calls > 20000 THEN 'OVERCALL' ELSE NULL END AS f_overcall,
    CASE WHEN COALESCE(b.wal_bytes,0) / NULLIF(b.calls,0) > 32768 THEN 'WAL_AMP' ELSE NULL END AS f_wal
  FROM base b
  LEFT JOIN pg_roles r ON r.oid = b.userid
  ORDER BY b.total_exec_time DESC
  LIMIT 30
)
SELECT COALESCE(
  string_agg(
    '<tr>' ||
    '<td>' || replace(replace(replace(replace(replace(COALESCE(queryid_text,'n/a'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td>' || replace(replace(replace(replace(replace(user_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
    '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
    '<td class="num">' || to_char(round(total_exec_time::numeric,1),'FM999,999,990.0') || '</td>' ||
    '<td class="num">' || to_char(round(mean_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(round(stddev_exec_time::numeric,2),'FM999,990.00') || '</td>' ||
    '<td class="num">' || to_char(shared_blks_hit,'FM999,999,999') || ' / ' || to_char(shared_blks_read,'FM999,999,999') || '</td>' ||
    '<td class="num ' || CASE WHEN temp_blks > 0 THEN 'warn' ELSE '' END || '">' || to_char(temp_blks,'FM999,999,999') || '</td>' ||
    '<td class="num">' || COALESCE(pg_size_pretty(wal_bytes::bigint), 'N/A') || '</td>' ||
    '<td><span class="severity-pill ' ||
    CASE resource_class
      WHEN 'IO-bound' THEN 'pill-warning'
      WHEN 'Spill-bound' THEN 'pill-warning'
      WHEN 'CPU-bound' THEN 'pill-info'
      ELSE 'pill-good'
    END || '">' || resource_class || '</span></td>' ||
    '<td>' || COALESCE(f_var || ' ','') || COALESCE(f_spill || ' ','') || COALESCE(f_overcall || ' ','') || COALESCE(f_wal || ' ','') || '</td>' ||
    '</tr>', E'\n'
  ),
  '<tr><td colspan="11" class="table-empty">No SQL rows available for leaderboard</td></tr>'
) FROM q;

\qecho '</tbody></table></div></div>'

-- S02.12 Consolidated action queue
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Consolidated Tuning Actions</div>'

WITH base AS (
  SELECT
    calls::bigint AS calls,
    total_exec_time::double precision AS total_exec_time,
    mean_exec_time::double precision AS mean_exec_time,
    stddev_exec_time::double precision AS stddev_exec_time,
    temp_blks_read::bigint AS temp_blks_read,
    temp_blks_written::bigint AS temp_blks_written,
    shared_blks_hit::bigint AS shared_blks_hit,
    shared_blks_read::bigint AS shared_blks_read,
    :s02_io_cols
    :s02_plan_cols
    userid::oid AS userid
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND (:'s02_relax_pgss_filter' = 'on' OR query NOT ILIKE '%pg_stat_statements%')
), stats AS (
  SELECT
    COUNT(*) FILTER (WHERE (temp_blks_read + temp_blks_written) > GREATEST(100, calls * 10)) AS spill_bound_cnt,
    COUNT(*) FILTER (WHERE (COALESCE(blk_read_time,0) + COALESCE(blk_write_time,0)) > total_exec_time * 0.35
                     OR (shared_blks_read > shared_blks_hit * 0.5 AND (shared_blks_read + shared_blks_hit) > 1000)) AS io_bound_cnt,
    COUNT(*) FILTER (WHERE calls > 20000) AS overcall_cnt,
    COUNT(*) FILTER (WHERE COALESCE(total_plan_time,0) > total_exec_time * 0.2) AS planning_heavy_cnt,
    COUNT(*) FILTER (WHERE stddev_exec_time / NULLIF(mean_exec_time,0) > 2 AND calls >= 20) AS variability_cnt
  FROM base
)
SELECT
  '<div class="finding ' || CASE WHEN spill_bound_cnt + io_bound_cnt + overcall_cnt + planning_heavy_cnt + variability_cnt > 0 THEN 'high' ELSE 'good' END || '">' ||
  '<div class="finding-header"><span class="finding-title">Top SQL Action Queue</span>' ||
  '<span class="severity-pill ' || CASE WHEN spill_bound_cnt + io_bound_cnt + overcall_cnt + planning_heavy_cnt + variability_cnt > 0 THEN 'pill-warning">ACTION' ELSE 'pill-good">OK' END || '</span></div>' ||
  '<div class="finding-body">' ||
  'Spill-bound queries: <strong>' || spill_bound_cnt || '</strong>; ' ||
  'I/O-bound queries: <strong>' || io_bound_cnt || '</strong>; ' ||
  'Over-calling patterns: <strong>' || overcall_cnt || '</strong>; ' ||
  'Planner-heavy queries: <strong>' || planning_heavy_cnt || '</strong>; ' ||
  'High-variability queries: <strong>' || variability_cnt || '</strong>.' ||
  '<br>Execution order: (1) address regression deltas, (2) remove over-calling, (3) reduce spill queries, (4) tune I/O plans, (5) stabilize high-variance SQL with EXPLAIN and fresh statistics.' ||
  '</div></div>'
FROM stats;

\else

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top SQL Diagnostics</div>'
SELECT '<div class="finding critical"><div class="finding-header"><span class="finding-title">skipped: pg_stat_statements is unavailable</span><span class="severity-pill pill-critical">BLOCKED</span></div><div class="finding-body">Enable extension and restart with shared_preload_libraries to populate this module.</div></div>';
\qecho '</div>'

\endif

\qecho '</div>'
\qecho '</div>'

-- SECTION S03: WAIT EVENTS & SESSION ACTIVITY
-- =============================================================================
\qecho '<div class="section" id="s03">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Wait Events and Session Activity</div>'
\qecho '    <div class="section-desc">Session wait behavior, sampling window, aging analysis, idle-in-transaction risk, prepared transaction posture, and immediate operational actions.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S03.1 Observability confidence
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Diagnostic Confidence</div>'

SELECT
  '<div class="finding ' ||
  CASE
    WHEN COALESCE(current_setting('track_activities', true), 'off') = 'on' THEN 'good'
    ELSE 'high'
  END ||
  '"><div class="finding-header">' ||
  '<span class="finding-title">Session activity visibility posture</span>' ||
  '<span class="severity-pill ' ||
  CASE
    WHEN COALESCE(current_setting('track_activities', true), 'off') = 'on' THEN 'pill-good">HIGH'
    ELSE 'pill-warning">MEDIUM'
  END ||
  '</span></div><div class="finding-body">' ||
  'track_activities=' || COALESCE(current_setting('track_activities', true), 'unknown') ||
  ', track_io_timing=' || COALESCE(current_setting('track_io_timing', true), 'unknown') ||
  '. Result confidence is reduced if sampling window is short or instrumentation is disabled.' ||
  '</div></div>';

\qecho '</div>'

-- S03.2 Sampling window with chronic signal (3 snapshots over 30 seconds)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Session Sampling Window</div>'

WITH s0 AS MATERIALIZED (
  SELECT
    clock_timestamp() AS sample_ts,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active') AS active_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event IS NOT NULL) AS waiting_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction') AS idle_in_tx,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock') AS lock_waits,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'IO') AS io_waits
), pause1 AS MATERIALIZED (
  SELECT pg_sleep(15) FROM s0
), s1 AS MATERIALIZED (
  SELECT
    clock_timestamp() AS sample_ts,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active') AS active_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event IS NOT NULL) AS waiting_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction') AS idle_in_tx,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock') AS lock_waits,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'IO') AS io_waits
  FROM pause1
), pause2 AS MATERIALIZED (
  SELECT pg_sleep(15) FROM s1
), s2 AS MATERIALIZED (
  SELECT
    clock_timestamp() AS sample_ts,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active') AS active_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event IS NOT NULL) AS waiting_sessions,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction') AS idle_in_tx,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock') AS lock_waits,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'IO') AS io_waits
  FROM pause2
), samples AS (
  SELECT 1 AS ord, 'T+0s' AS sample_name, * FROM s0
  UNION ALL
  SELECT 2, 'T+15s', * FROM s1
  UNION ALL
  SELECT 3, 'T+30s', * FROM s2
), agg AS (
  SELECT
    max(waiting_sessions) AS max_waiting,
    min(waiting_sessions) AS min_waiting,
    max(lock_waits) AS max_lock_waits,
    max(idle_in_tx) AS max_idle_in_tx
  FROM samples
)
SELECT
  '<div class="finding ' ||
  CASE
    WHEN max_waiting >= 3 OR max_lock_waits > 0 OR max_idle_in_tx > 0 THEN 'high'
    ELSE 'good'
  END ||
  '"><div class="finding-header">' ||
  '<span class="finding-title">Current activity profile over sampling window</span>' ||
  '<span class="severity-pill ' ||
  CASE
    WHEN max_waiting >= 3 OR max_lock_waits > 0 OR max_idle_in_tx > 0 THEN 'pill-warning">ATTENTION'
    ELSE 'pill-good">STABLE'
  END ||
  '</span></div><div class="finding-body">' ||
  CASE
    WHEN max_waiting >= 3 AND (max_waiting - min_waiting) <= 1
      THEN 'Wait pressure appears persistent across all snapshots. '
    WHEN max_waiting >= 3
      THEN 'Wait pressure is present and variable across snapshots. '
    ELSE 'No sustained wait pressure observed during sampling window. '
  END ||
  'Lock waits peak=' || max_lock_waits || ', idle-in-transaction peak=' || max_idle_in_tx || '. ' ||
  'Use this as the baseline for S04 lock triage and S09 connection hygiene.' ||
  '</div></div>' ||
  '<div class="table-wrap"><table class="pg360"><thead><tr>' ||
  '<th>Sample</th><th>Captured At</th><th>Active</th><th>Waiting</th><th>Idle In Tx</th><th>Lock Waits</th><th>I/O Waits</th>' ||
  '</tr></thead><tbody>' ||
  COALESCE(
    (
      SELECT string_agg(
        '<tr>' ||
        '<td>' || sample_name || '</td>' ||
        '<td class="num">' || to_char(sample_ts, 'HH24:MI:SS') || '</td>' ||
        '<td class="num">' || active_sessions || '</td>' ||
        '<td class="num ' || CASE WHEN waiting_sessions > 0 THEN 'warn' ELSE 'good' END || '">' || waiting_sessions || '</td>' ||
        '<td class="num ' || CASE WHEN idle_in_tx > 0 THEN 'warn' ELSE 'good' END || '">' || idle_in_tx || '</td>' ||
        '<td class="num ' || CASE WHEN lock_waits > 0 THEN 'crit' ELSE 'good' END || '">' || lock_waits || '</td>' ||
        '<td class="num ' || CASE WHEN io_waits > 0 THEN 'warn' ELSE 'good' END || '">' || io_waits || '</td>' ||
        '</tr>',
        E'\n' ORDER BY ord
      )
      FROM samples
    ),
    '<tr><td colspan="7" class="table-empty">No sessions available for sampling</td></tr>'
  ) ||
  '</tbody></table></div>'
FROM agg;

\qecho '</div>'

-- S03.3 Session aging and long-running activity
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Session Aging and Runtime Distribution</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>User</th><th>State</th><th>State Age</th><th>Transaction Age</th><th>Query Age</th><th>Wait Type/Event</th><th>Application</th><th>Client (masked)</th><th>Query (normalized)</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(state,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN state_age_secs > 600 THEN 'warn' ELSE '' END || '">' || to_char((interval '1 second' * state_age_secs), 'HH24:MI:SS') || '</td>' ||
      '<td class="num ' || CASE WHEN xact_age_secs > 600 THEN 'warn' ELSE '' END || '">' || COALESCE(to_char((interval '1 second' * xact_age_secs), 'HH24:MI:SS'),'00:00:00') || '</td>' ||
      '<td class="num ' || CASE WHEN query_age_secs > 300 THEN 'crit' WHEN query_age_secs > 60 THEN 'warn' ELSE '' END || '">' || COALESCE(to_char((interval '1 second' * query_age_secs), 'HH24:MI:SS'),'00:00:00') || '</td>' ||
      '<td>' || COALESCE(replace(replace(wait_event_type,'<','&lt;'),'>','&gt;'),'') ||
      CASE WHEN wait_event IS NOT NULL THEN '/' || replace(replace(wait_event,'<','&lt;'),'>','&gt;') ELSE '' END ||
      '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(application_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' ||
      CASE WHEN client_addr IS NULL THEN 'local'
           ELSE regexp_replace(host(client_addr), '(\\d+)\\.(\\d+)\\.\\d+\\.\\d+', '\\1.\\2.x.x')
      END || '</td>' ||
      '<td>' ||
      replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(query,''), E'''[^'']*''', '''?''', 'g'), 120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY query_age_secs DESC NULLS LAST
    ),
    '<tr><td colspan="10" class="table-empty">No session activity rows visible</td></tr>'
  )
FROM (
  SELECT
    pid,
    usename,
    state,
    wait_event_type,
    wait_event,
    application_name,
    client_addr,
    query,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(state_change, backend_start)))::bigint AS state_age_secs,
    EXTRACT(EPOCH FROM (clock_timestamp() - xact_start))::bigint AS xact_age_secs,
    EXTRACT(EPOCH FROM (clock_timestamp() - query_start))::bigint AS query_age_secs
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
) s;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S03.4 Wait event type concentration
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Wait Event Group Concentration</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Wait Group</th><th>Sessions</th><th>% of Waiting Sessions</th><th>Technical Interpretation</th>'
\qecho '</tr></thead><tbody>'

WITH waits AS (
  SELECT
    COALESCE(wait_event_type, 'Running/Client') AS wait_group,
    COUNT(*) AS cnt
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
    AND (state = 'active' OR wait_event IS NOT NULL)
  GROUP BY COALESCE(wait_event_type, 'Running/Client')
), totals AS (
  SELECT COALESCE(SUM(cnt),0)::numeric AS total_cnt FROM waits
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || wait_group || '</td>' ||
      '<td class="num">' || cnt || '</td>' ||
      '<td class="num">' || COALESCE(round(cnt * 100.0 / NULLIF(total_cnt,0),1)::text,'0') || '%</td>' ||
      '<td>' ||
      CASE
        WHEN wait_group = 'Lock' THEN 'Lock acquisition delays; investigate blocker tree in S04'
        WHEN wait_group = 'IO' THEN 'Storage latency or cache miss pressure; correlate with S07 and S02'
        WHEN wait_group = 'LWLock' THEN 'Shared-memory latch contention; review high-concurrency hotspots'
        WHEN wait_group = 'Client' THEN 'Client side pacing or network backpressure'
        WHEN wait_group = 'Timeout' THEN 'Timeout-triggered waits; validate timeout settings and retries'
        WHEN wait_group = 'IPC' THEN 'Inter-process sync waits; inspect parallel or maintenance operations'
        ELSE 'Mixed or running workload; validate with sampling trend'
      END ||
      '</td></tr>',
      E'\n' ORDER BY cnt DESC
    ),
    '<tr><td colspan="4" class="table-empty">No wait-group activity detected</td></tr>'
  )
FROM waits, totals;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S03.5 Idle in transaction risk scoring
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Idle In Transaction Risk Scoring</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>User</th><th>Idle Age</th><th>Blocked Sessions</th><th>backend_xmin Age</th><th>Risk Score</th><th>Risk Interpretation</th><th>Last Query</th>'
\qecho '</tr></thead><tbody>'

WITH idle_tx AS (
  SELECT
    a.pid,
    a.usename,
    a.query,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(a.state_change, a.xact_start, a.backend_start)))::numeric AS idle_secs,
    CASE WHEN a.backend_xmin IS NOT NULL THEN age(a.backend_xmin)::numeric ELSE NULL END AS backend_xmin_age,
    (
      SELECT COUNT(*)
      FROM pg_stat_activity b
      WHERE a.pid = ANY(pg_blocking_pids(b.pid))
    )::numeric AS blocked_sessions
  FROM pg_stat_activity a
  WHERE a.datname = current_database()
    AND a.pid <> pg_backend_pid()
    AND a.state = 'idle in transaction'
), scored AS (
  SELECT
    pid,
    usename,
    query,
    idle_secs,
    blocked_sessions,
    backend_xmin_age,
    LEAST(100,
      (idle_secs / 60.0) * 2.0 +
      (blocked_sessions * 20.0) +
      CASE
        WHEN COALESCE(backend_xmin_age,0) > 100000000 THEN 25
        WHEN COALESCE(backend_xmin_age,0) > 50000000 THEN 12
        ELSE 0
      END
    )::numeric(10,1) AS risk_score
  FROM idle_tx
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char((interval '1 second' * idle_secs::bigint), 'HH24:MI:SS') || '</td>' ||
      '<td class="num ' || CASE WHEN blocked_sessions > 0 THEN 'crit' ELSE 'good' END || '">' || blocked_sessions || '</td>' ||
      '<td class="num">' || COALESCE(to_char(backend_xmin_age, 'FM999,999,999'), 'N/A') || '</td>' ||
      '<td class="num ' || CASE WHEN risk_score >= 70 THEN 'crit' WHEN risk_score >= 35 THEN 'warn' ELSE 'good' END || '">' || risk_score || '</td>' ||
      '<td>' ||
      CASE
        WHEN risk_score >= 70 THEN 'High: terminate or force commit/rollback immediately'
        WHEN risk_score >= 35 THEN 'Medium: resolve session before vacuum debt increases'
        ELSE 'Low: monitor and enforce transaction timeout policy'
      END ||
      '</td>' ||
      '<td>' ||
      replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(query,''), E'''[^'']*''', '''?''', 'g'), 120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      E'\n' ORDER BY risk_score DESC
    ),
    '<tr><td colspan="8" class="table-empty">No idle-in-transaction sessions detected</td></tr>'
  )
FROM scored;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S03.6 Prepared transactions
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Prepared Transactions (2PC) Risk Check</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>GID</th><th>Prepared At</th><th>Age</th><th>Owner</th><th>Database</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(gid,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(prepared, 'YYYY-MM-DD HH24:MI:SS') || '</td>' ||
      '<td class="num ' || CASE WHEN clock_timestamp() - prepared > interval '30 min' THEN 'crit' WHEN clock_timestamp() - prepared > interval '5 min' THEN 'warn' ELSE '' END || '">' || to_char(clock_timestamp() - prepared, 'HH24:MI:SS') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(owner,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(database,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN clock_timestamp() - prepared > interval '30 min' THEN 'High: investigate XA coordinator and finish COMMIT PREPARED/ROLLBACK PREPARED'
                    ELSE 'Monitor' END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY prepared
    ),
    '<tr><td colspan="6" class="table-empty">No prepared transactions in this database</td></tr>'
  )
FROM pg_prepared_xacts
WHERE database = current_database();

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S03.7 Root cause summary and operational actions
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Root Cause Summary</div>'

WITH signals AS (
  SELECT 1 AS ord, 'Lock wait pressure' AS signal,
         COUNT(*) FILTER (WHERE wait_event_type = 'Lock' AND state = 'active')::numeric AS score,
         'Prioritize S04 blocker tree; remove highest blast-radius blocker first.' AS action
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
  UNION ALL
  SELECT 2, 'Idle in transaction backlog',
         COUNT(*) FILTER (WHERE state = 'idle in transaction')::numeric,
         'Enforce idle_in_transaction_session_timeout and terminate stale idle transactions.'
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
  UNION ALL
  SELECT 3, 'I/O wait concentration',
         COUNT(*) FILTER (WHERE wait_event_type = 'IO' AND state = 'active')::numeric,
         'Correlate with S07 cache and S02 IO-bound SQL; prioritize high-read hot spots.'
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
  UNION ALL
  SELECT 4, 'Prepared transaction exposure',
         COUNT(*)::numeric,
         'Clear orphaned prepared transactions and validate 2PC coordinator health.'
  FROM pg_prepared_xacts
  WHERE database = current_database()
  UNION ALL
  SELECT 5, 'Long-running active sessions',
         COUNT(*) FILTER (WHERE state = 'active' AND query_start < clock_timestamp() - interval '5 minutes')::numeric,
         'Review execution plans and lock dependencies for long-running active SQL.'
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND pid <> pg_backend_pid()
), ranked AS (
  SELECT *
  FROM signals
  ORDER BY score DESC, ord
  LIMIT 3
)
SELECT
  '<div class="finding ' || CASE WHEN COALESCE((SELECT max(score) FROM ranked),0) > 0 THEN 'high' ELSE 'good' END || '">' ||
  '<div class="finding-header"><span class="finding-title">Top suspected root causes from current session profile</span>' ||
  '<span class="severity-pill ' || CASE WHEN COALESCE((SELECT max(score) FROM ranked),0) > 0 THEN 'pill-warning">ACTION' ELSE 'pill-good">OK' END || '</span></div>' ||
  '<div class="finding-body"><ol>' ||
  COALESCE(
    (
      SELECT string_agg(
        '<li><strong>' || signal || '</strong> (count=' || score || '). ' || action || '</li>',
        '' ORDER BY score DESC, ord
      )
      FROM ranked
    ),
    '<li>No immediate root-cause pressure detected in session activity.</li>'
  ) ||
  '</ol>' ||
  '<div><strong>Fix:</strong> execute blocker/idle cleanup from S04 and this section.' ||
  ' <strong>Verify:</strong> rerun S03 and confirm waiting/idle-in-tx counts decrease.' ||
  ' <strong>Rollback:</strong> if cancellation impacts workload, stop termination and revert timeout changes.</div>' ||
  '</div></div>';

\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S04: LOCK ANALYSIS
-- =============================================================================
\qecho '<div class="section" id="s04">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Lock Analysis</div>'
\qecho '    <div class="section-desc">Blocking hierarchy, blast radius, DDL lock exposure, advisory lock posture, timeout configuration, and ranked mitigation actions.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S04.1 Blocking tree and blast radius
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Blocking Tree with Blast Radius</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Rank</th><th>Blocking PID</th><th>Blocking User</th><th>Blocked Sessions</th><th>Max Wait Age</th><th>Blocker Xact Age</th><th>Blocking Query (normalized)</th><th>Mitigation Priority</th>'
\qecho '</tr></thead><tbody>'

WITH pairs AS (
  SELECT
    blocked.pid AS blocked_pid,
    blocker.pid AS blocker_pid,
    blocker.usename AS blocker_user,
    blocker.query AS blocker_query,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(blocked.query_start, blocked.state_change, blocked.xact_start, blocked.backend_start)))::numeric AS blocked_wait_secs,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(blocker.xact_start, blocker.query_start, blocker.backend_start)))::numeric AS blocker_xact_secs
  FROM pg_stat_activity blocked
  JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) bp(blocker_pid) ON true
  JOIN pg_stat_activity blocker ON blocker.pid = bp.blocker_pid
  WHERE blocked.datname = current_database()
), agg AS (
  SELECT
    blocker_pid,
    max(blocker_user) AS blocker_user,
    max(blocker_query) AS blocker_query,
    COUNT(*) AS blocked_sessions,
    max(blocked_wait_secs) AS max_wait_secs,
    max(blocker_xact_secs) AS blocker_xact_secs
  FROM pairs
  GROUP BY blocker_pid
), ranked AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY blocked_sessions DESC, max_wait_secs DESC) AS rnk,
    *
  FROM agg
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || rnk || '</td>' ||
      '<td class="num crit">' || blocker_pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND blocker_user = current_user THEN :'pg360_redaction_token' ELSE blocker_user END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN blocked_sessions >= 3 THEN 'crit' WHEN blocked_sessions = 2 THEN 'warn' ELSE '' END || '">' || blocked_sessions || '</td>' ||
      '<td class="num">' || to_char((interval '1 second' * max_wait_secs::bigint), 'HH24:MI:SS') || '</td>' ||
      '<td class="num">' || to_char((interval '1 second' * blocker_xact_secs::bigint), 'HH24:MI:SS') || '</td>' ||
      '<td>' ||
      replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(regexp_replace(blocker_query, E'\\s+', ' ', 'g'),''), E'''[^'']*''', '''?''', 'g'), 130),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td>' ||
      CASE
        WHEN blocked_sessions >= 3 THEN 'Immediate candidate for cancellation/termination after owner validation'
        WHEN blocked_sessions = 2 THEN 'High priority; coordinate with application owner'
        ELSE 'Monitor and clear if wait age keeps rising'
      END ||
      '</td></tr>',
      E'\n' ORDER BY rnk
    ),
    '<tr><td colspan="8" class="table-empty">No blocking tree detected at capture time</td></tr>'
  )
FROM ranked;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S04.2 Blocking details by blocked session
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Blocked Session Detail</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Blocked PID</th><th>Blocked User</th><th>Wait Age</th><th>Blocking PID</th><th>Blocking Query</th><th>Blocked Query</th><th>Immediate Action</th>'
\qecho '</tr></thead><tbody>'

WITH pairs AS (
  SELECT
    blocked.pid AS blocked_pid,
    blocked.usename AS blocked_user,
    blocked.query AS blocked_query,
    blocker.pid AS blocker_pid,
    blocker.query AS blocker_query,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(blocked.query_start, blocked.state_change, blocked.xact_start, blocked.backend_start)))::numeric AS wait_secs
  FROM pg_stat_activity blocked
  JOIN LATERAL unnest(pg_blocking_pids(blocked.pid)) bp(blocker_pid) ON true
  JOIN pg_stat_activity blocker ON blocker.pid = bp.blocker_pid
  WHERE blocked.datname = current_database()
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num crit">' || blocked_pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND blocked_user = current_user THEN :'pg360_redaction_token' ELSE blocked_user END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char((interval '1 second' * wait_secs::bigint), 'HH24:MI:SS') || '</td>' ||
      '<td class="num warn">' || blocker_pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(regexp_replace(blocker_query, E'\\s+', ' ', 'g'),''), E'''[^'']*''', '''?''', 'g'), 100),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(regexp_replace(blocked_query, E'\\s+', ' ', 'g'),''), E'''[^'']*''', '''?''', 'g'), 100),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">SELECT pg_cancel_backend(' || blocker_pid || ');</td>' ||
      '</tr>',
      E'\n' ORDER BY wait_secs DESC
    ),
    '<tr><td colspan="7" class="table-empty">No blocked sessions detected</td></tr>'
  )
FROM pairs;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S04.3 AccessExclusiveLock and DDL lock detection
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">AccessExclusiveLock and DDL Lock Exposure</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>User</th><th>Schema</th><th>Relation</th><th>Granted</th><th>Mode</th><th>Xact Age</th><th>Query (normalized)</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

WITH ddl_locks AS (
  SELECT
    l.pid,
    a.usename,
    n.nspname AS schema_name,
    c.relname AS relation_name,
    l.granted,
    l.mode,
    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(a.xact_start, a.query_start, a.backend_start)))::numeric AS xact_age_secs,
    a.query
  FROM pg_locks l
  LEFT JOIN pg_class c ON c.oid = l.relation
  LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.mode = 'AccessExclusiveLock'
    AND a.datname = current_database()
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(schema_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(relation_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN granted THEN 'warn' ELSE 'crit' END || '">' || CASE WHEN granted THEN 'YES' ELSE 'WAITING' END || '</td>' ||
      '<td>' || mode || '</td>' ||
      '<td class="num ' || CASE WHEN xact_age_secs > 300 THEN 'crit' WHEN xact_age_secs > 60 THEN 'warn' ELSE '' END || '">' || to_char((interval '1 second' * xact_age_secs::bigint), 'HH24:MI:SS') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(regexp_replace(query, E'\\s+', ' ', 'g'),''), E'''[^'']*''', '''?''', 'g'), 130),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN granted THEN 'Blocks all conflicting table access; validate maintenance window'
                     ELSE 'Pending DDL lock; likely blocked by open transactions' END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY granted, xact_age_secs DESC
    ),
    '<tr><td colspan="9" class="table-empty">No AccessExclusiveLock holders or waiters detected</td></tr>'
  )
FROM ddl_locks;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S04.4 Advisory lock summary
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Advisory Lock Summary</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>User</th><th>Granted</th><th>Waiting</th><th>ClassID</th><th>ObjID</th><th>VirtualXID</th><th>Query (normalized)</th>'
\qecho '</tr></thead><tbody>'

WITH adv AS (
  SELECT
    l.pid,
    a.usename,
    COUNT(*) FILTER (WHERE l.granted) AS granted_cnt,
    COUNT(*) FILTER (WHERE NOT l.granted) AS waiting_cnt,
    max(l.classid) AS classid,
    max(l.objid) AS objid,
    max(l.virtualxid) AS virtualxid,
    max(a.query) AS query_text
  FROM pg_locks l
  LEFT JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.locktype = 'advisory'
    AND a.datname = current_database()
  GROUP BY l.pid, a.usename
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || granted_cnt || '</td>' ||
      '<td class="num ' || CASE WHEN waiting_cnt > 0 THEN 'warn' ELSE 'good' END || '">' || waiting_cnt || '</td>' ||
      '<td class="num">' || COALESCE(classid::text,'') || '</td>' ||
      '<td class="num">' || COALESCE(objid::text,'') || '</td>' ||
      '<td>' || COALESCE(virtualxid,'') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(left(regexp_replace(COALESCE(regexp_replace(query_text, E'\\s+', ' ', 'g'),''), E'''[^'']*''', '''?''', 'g'), 120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY waiting_cnt DESC, granted_cnt DESC
    ),
    '<tr><td colspan="8" class="table-empty">No advisory locks detected</td></tr>'
  )
FROM adv;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S04.5 Lock timeout posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Lock Timeout and Logging Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current Value</th><th>Technical Interpretation</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

WITH p AS (
  SELECT
    max(setting) FILTER (WHERE name = 'lock_timeout') AS lock_timeout,
    max(setting) FILTER (WHERE name = 'deadlock_timeout') AS deadlock_timeout,
    max(setting) FILTER (WHERE name = 'log_lock_waits') AS log_lock_waits,
    max(setting) FILTER (WHERE name = 'statement_timeout') AS statement_timeout
  FROM pg_settings
)
SELECT
  '<tr><td>lock_timeout</td><td>' || lock_timeout || '</td><td>' ||
  CASE WHEN lock_timeout IN ('0','0ms') THEN 'No lock wait cap; sessions can block indefinitely.' ELSE 'Lock waits are capped by timeout.' END ||
  '</td><td>' || CASE WHEN lock_timeout IN ('0','0ms') THEN 'Set lock_timeout per role/workload for DDL-sensitive workloads.' ELSE 'Validate timeout against workload SLOs.' END || '</td></tr>' ||
  '<tr><td>deadlock_timeout</td><td>' || deadlock_timeout || '</td><td>Time before deadlock detector runs and lock wait is logged.</td><td>1s is typical for faster deadlock diagnostics.</td></tr>' ||
  '<tr><td>log_lock_waits</td><td>' || log_lock_waits || '</td><td>' || CASE WHEN log_lock_waits = 'on' THEN 'Lock waits are logged for forensics.' ELSE 'Lock waits are not logged; root-cause traceability reduced.' END || '</td><td>' || CASE WHEN log_lock_waits = 'on' THEN 'Keep enabled.' ELSE 'Enable for production diagnostics.' END || '</td></tr>' ||
  '<tr><td>statement_timeout</td><td>' || statement_timeout || '</td><td>Global execution timeout guardrail.</td><td>Use role-specific values; avoid one-size global cutoff.</td></tr>'
FROM p;

\qecho '</tbody></table></div>'
\qecho '</div>'

-- S04.6 Ranked immediate remediation actions
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Mitigation Actions</div>'

WITH blockers AS (
  SELECT COUNT(*)::numeric AS blocker_edges
  FROM pg_stat_activity b
  WHERE cardinality(pg_blocking_pids(b.pid)) > 0
    AND b.datname = current_database()
), idle_tx AS (
  SELECT COUNT(*)::numeric AS idle_tx_cnt
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND state = 'idle in transaction'
    AND pid <> pg_backend_pid()
), ddl AS (
  SELECT COUNT(*)::numeric AS ddl_lock_cnt
  FROM pg_locks l
  JOIN pg_stat_activity a ON a.pid = l.pid
  WHERE l.mode = 'AccessExclusiveLock'
    AND a.datname = current_database()
)
SELECT
  '<div class="finding ' || CASE WHEN blocker_edges + idle_tx_cnt + ddl_lock_cnt > 0 THEN 'high' ELSE 'good' END || '">' ||
  '<div class="finding-header"><span class="finding-title">Immediate lock mitigation queue</span>' ||
  '<span class="severity-pill ' || CASE WHEN blocker_edges + idle_tx_cnt + ddl_lock_cnt > 0 THEN 'pill-warning">ACTION' ELSE 'pill-good">OK' END || '</span></div>' ||
  '<div class="finding-body">' ||
  '<strong>1. Blocking chains:</strong> ' || blocker_edges || ' blocking edges detected. '
  || 'Fix: cancel lowest-risk blocker first. Verify: blocked edge count drops to zero. Rollback: stop cancellations and coordinate controlled maintenance.<br>' ||
  '<strong>2. Idle in transaction:</strong> ' || idle_tx_cnt || ' sessions. '
  || 'Fix: terminate stale idle sessions and enforce timeout policy. Verify: idle-in-transaction count and S03 risk score drop. Rollback: relax timeout if business transaction flow breaks.<br>' ||
  '<strong>3. AccessExclusiveLock exposure:</strong> ' || ddl_lock_cnt || ' sessions. '
  || 'Fix: move DDL to maintenance windows or use online alternatives. Verify: no AccessExclusiveLock held during peak. Rollback: cancel migration batch and revert DDL deployment.' ||
  '</div></div>'
FROM blockers, idle_tx, ddl;

\qecho '</div>'
\qecho '</div>'
-- =============================================================================
\qecho '<div class="section" id="s05">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Table Health &amp; Bloat</div>'
\qecho '    <div class="section-desc">Dead tuples, bloat, autovacuum health, XID wraparound risk, sequences, temp tables, triggers.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S05.1 Table health overview
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top Tables by Dead Tuples</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Size</th><th>Live Rows</th>'
\qecho '<th>Dead Rows</th><th>Dead%</th><th>Last Autovacuum</th><th>Last Autoanalyze</th><th>Seq Scans</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' || CASE WHEN n_dead_tup > 100000 THEN 'crit' WHEN n_dead_tup > 10000 THEN 'warn' ELSE '' END || '">' ||
  to_char(n_dead_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' || CASE WHEN dead_pct > 20 THEN 'crit' WHEN dead_pct > 10 THEN 'warn' ELSE 'good' END || '">' ||
  round(dead_pct::numeric,1) || '%</td>' ||
  '<td>' || COALESCE(to_char(last_autovacuum,'YYYY-MM-DD HH24:MI'),'<span class="warn">Never</span>') || '</td>' ||
  '<td>' || COALESCE(to_char(last_autoanalyze,'YYYY-MM-DD HH24:MI'),'<span class="warn">Never</span>') || '</td>' ||
  '<td class="num ' || CASE WHEN seq_scan > 1000 THEN 'warn' ELSE '' END || '">' || to_char(seq_scan,'FM999,999') || '</td>' ||
  '</tr>'
FROM (
  SELECT *,
    n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100 AS dead_pct
  FROM pg_stat_user_tables
  WHERE n_live_tup + n_dead_tup > 0
) t
ORDER BY dead_pct DESC NULLS LAST
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S05.2 XID wraparound risk
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">XID Wraparound Risk</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>XID Age</th><th>Risk Level</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN xid_age > 1500000000 THEN 'crit' WHEN xid_age > 750000000 THEN 'warn' ELSE '' END || '">' ||
  to_char(xid_age,'FM999,999,999') || '</td>' ||
  '<td><span class="severity-pill ' ||
  CASE WHEN xid_age > 1500000000 THEN 'pill-critical"> CRITICAL'
       WHEN xid_age > 750000000  THEN 'pill-high"> HIGH'
       WHEN xid_age > 200000000  THEN 'pill-medium"> MEDIUM'
       ELSE 'pill-good"> OK' END ||
  '</span></td>' ||
  '<td>' ||
  CASE WHEN xid_age > 750000000
       THEN 'VACUUM FREEZE ' || relname || ' immediately'
       ELSE 'Monitor' END ||
  '</td></tr>'
FROM (
  SELECT n.nspname, c.relname,
    age(c.relfrozenxid) AS xid_age
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r','m')
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
) t
ORDER BY xid_age DESC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S05.3 Sequence sync check (CRITICAL post-migration)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Sequence Synchronization Check</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Sequence</th><th>Last Value</th><th>Table</th><th>Column</th><th>Max Value in Table</th><th>Status</th><th>Fix Script</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(seq_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || COALESCE(to_char(last_value,'FM999,999,999,999,999,999'),'NULL') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(tbl,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(col,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      CASE
        WHEN max_value IS NULL THEN '<td class="num warn">N/A (check manually)</td>'
        ELSE '<td class="num ' ||
             CASE WHEN status = 'CRITICAL' THEN 'crit' ELSE '' END ||
             '">' || to_char(max_value,'FM999,999,999,999,999,999') || '</td>'
      END ||
      '<td>' ||
      CASE status
        WHEN 'CRITICAL' THEN '<span class="severity-pill pill-critical">OUT_OF_SYNC</span>'
        WHEN 'OK' THEN '<span class="severity-pill pill-good">OK</span>'
        ELSE '<span class="severity-pill pill-info">VERIFY</span>'
      END ||
      '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(fix_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      ''
    ),
    '<tr><td colspan="7" class="table-empty"> No sequence ownership metadata found for validation.</td></tr>'
  )
FROM (
  SELECT
    seq_name,
    last_value,
    tbl,
    col,
    max_value,
    status,
    CASE
      WHEN tbl <> '' AND col <> '' THEN
        format(
          'SELECT setval(%L, COALESCE((SELECT MAX(%I) FROM %s), 0), true);',
          seq_name, col, tbl
        )
      ELSE
        '-- Manual verification required: sequence has no direct OWNED BY mapping.'
    END AS fix_sql
  FROM (
    SELECT
      seq_name,
      last_value,
      COALESCE(tbl,'') AS tbl,
      COALESCE(col,'') AS col,
      max_value,
      CASE
        WHEN tbl IS NULL OR col IS NULL THEN 'VERIFY'
        WHEN max_value IS NULL THEN 'VERIFY'
        WHEN last_value < max_value THEN 'CRITICAL'
        ELSE 'OK'
      END AS status
    FROM (
      SELECT
        s.schemaname || '.' || s.sequencename AS seq_name,
        s.last_value::numeric AS last_value,
        owner.table_fqn AS tbl,
        owner.column_name AS col,
        CASE
          WHEN owner.table_fqn IS NOT NULL AND owner.column_name IS NOT NULL THEN
            (
              SELECT
                CASE
                  WHEN max_txt ~ '^-?[0-9]+(\.[0-9]+)?$' THEN max_txt::numeric
                  ELSE NULL::numeric
                END
              FROM (
                SELECT NULLIF(
                  (
                    xpath(
                      '/row/max/text()',
                      query_to_xml(
                        format(
                          'SELECT max(%I)::text AS max FROM %s',
                          owner.column_name,
                          owner.table_fqn
                        ),
                        false,
                        true,
                        ''
                      )
                    )
                  )[1]::text,
                  ''
                ) AS max_txt
              ) max_probe
            )
          ELSE NULL::numeric
        END AS max_value
      FROM pg_sequences s
      LEFT JOIN LATERAL (
        SELECT
          format('%I.%I', n.nspname, c.relname) AS table_fqn,
          a.attname AS column_name
        FROM pg_depend d
        JOIN pg_class c ON c.oid = d.refobjid AND c.relkind IN ('r','p')
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a
          ON a.attrelid = c.oid
         AND a.attnum = d.refobjsubid
         AND a.attnum > 0
         AND NOT a.attisdropped
        WHERE d.classid = 'pg_class'::regclass
          AND d.refclassid = 'pg_class'::regclass
          AND d.objid = to_regclass(format('%I.%I', s.schemaname, s.sequencename))
          AND d.deptype IN ('a','i')
        ORDER BY CASE WHEN d.deptype = 'i' THEN 0 ELSE 1 END
        LIMIT 1
      ) owner ON TRUE
      WHERE s.last_value IS NOT NULL
        AND s.schemaname NOT IN ('pg_catalog','information_schema')
    ) seq_owner
  ) seq_eval
  ORDER BY
    CASE status
      WHEN 'CRITICAL' THEN 1
      WHEN 'VERIFY' THEN 2
      ELSE 3
    END,
    COALESCE(max_value - last_value, 0) DESC,
    last_value DESC
  LIMIT 30
) seq_data;

\qecho '</tbody></table></div></div>'

-- S05.4 Trigger inventory (Oracle migration: trigger explosion)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Trigger Inventory</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Trigger Name</th><th>Timing</th><th>Events</th><th>Orientation</th><th>Enabled</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(trigger_schema,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(event_object_table,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(trigger_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || action_timing || '</td>' ||
  '<td>' || event_manipulation || '</td>' ||
  '<td>' || action_orientation || '</td>' ||
  '<td class="good"></td>' ||
  '</tr>'
FROM information_schema.triggers
WHERE trigger_schema NOT IN ('pg_catalog','information_schema')
ORDER BY trigger_schema, event_object_table, trigger_name
LIMIT 100;

\qecho '</tbody></table></div></div>'

-- S05.5 Tables without primary keys
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables Without Primary Keys</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Row Count (approx)</th><th>Size</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(c.reltuples::bigint,'FM999,999,999') || '</td>' ||
      '<td class="num">' || pg_size_pretty(pg_total_relation_size(c.oid)) || '</td>' ||
      '<td class="warn">No PK  logical replication, row identification issues</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> All tables have primary keys</td></tr>'
  )
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
AND NOT EXISTS (
  SELECT 1 FROM pg_constraint pc
  WHERE pc.conrelid = c.oid AND pc.contype = 'p'
)
;

\qecho '</tbody></table></div></div>'
-- S05.6 Freeze age and wraparound countdown
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Freeze Age and Wraparound Countdown</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>relfrozenxid Age</th><th>Remaining to Freeze Max Age</th><th>Approx Days Remaining</th><th>Priority</th>'
\qecho '</tr></thead><tbody>'

WITH cfg AS (
  SELECT current_setting('autovacuum_freeze_max_age')::numeric AS freeze_max_age
), tx_rate AS (
  SELECT
    GREATEST(
      (SUM(xact_commit + xact_rollback)::numeric) /
      NULLIF(EXTRACT(EPOCH FROM (clock_timestamp() - pg_postmaster_start_time())), 0),
      0.001
    ) AS tx_per_sec
  FROM pg_stat_database
), t AS (
  SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    age(c.relfrozenxid)::numeric AS xid_age,
    cfg.freeze_max_age,
    tx_rate.tx_per_sec
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN cfg
  CROSS JOIN tx_rate
  WHERE c.relkind IN ('r','m')
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schema_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(table_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN xid_age > freeze_max_age * 0.9 THEN 'crit' WHEN xid_age > freeze_max_age * 0.7 THEN 'warn' ELSE '' END || '">' || to_char(xid_age, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(GREATEST(freeze_max_age - xid_age, 0), 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(round((GREATEST(freeze_max_age - xid_age,0) / NULLIF(tx_per_sec * 86400.0,0))::numeric, 2), 'FM999,990.00') || '</td>' ||
      '<td>' ||
      CASE
        WHEN xid_age > freeze_max_age * 0.9 THEN 'High: schedule VACUUM (FREEZE) immediately'
        WHEN xid_age > freeze_max_age * 0.7 THEN 'Medium: prioritize in next maintenance window'
        ELSE 'Monitor'
      END ||
      '</td></tr>',
      E'\n' ORDER BY xid_age DESC
    ),
    '<tr><td colspan="6" class="table-empty">No user tables found for freeze-age assessment</td></tr>'
  )
FROM (
  SELECT * FROM t ORDER BY xid_age DESC LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S05.7 Autovacuum effectiveness
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Autovacuum Effectiveness and Backlog</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Dead Tuples</th><th>Dead %</th><th>Last Autovacuum</th><th>Autovacuum Runs</th><th>Assessment</th><th>Suggested Adjustment</th>'
\qecho '</tr></thead><tbody>'

WITH t AS (
  SELECT
    schemaname,
    relname,
    n_live_tup,
    n_dead_tup,
    COALESCE(last_autovacuum, last_vacuum) AS last_vacuum_seen,
    autovacuum_count,
    (n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup,0) * 100.0) AS dead_pct
  FROM pg_stat_user_tables
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN n_dead_tup > 500000 THEN 'crit' WHEN n_dead_tup > 100000 THEN 'warn' ELSE '' END || '">' || to_char(n_dead_tup, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN dead_pct > 20 THEN 'crit' WHEN dead_pct > 10 THEN 'warn' ELSE 'good' END || '">' || COALESCE(round(dead_pct::numeric,2)::text, '0') || '%</td>' ||
      '<td>' || COALESCE(to_char(last_vacuum_seen, 'YYYY-MM-DD HH24:MI'), '<span class="warn">Never</span>') || '</td>' ||
      '<td class="num">' || autovacuum_count || '</td>' ||
      '<td>' ||
      CASE
        WHEN dead_pct > 20 AND (last_vacuum_seen IS NULL OR last_vacuum_seen < clock_timestamp() - interval '2 days')
          THEN 'Backlog: dead tuples high and vacuum lagging'
        WHEN dead_pct > 10 THEN 'Moderate vacuum debt'
        ELSE 'Vacuum appears effective'
      END ||
      '</td>' ||
      '<td>' ||
      CASE
        WHEN dead_pct > 20 THEN 'Lower table-level autovacuum_vacuum_scale_factor; increase autovacuum cadence'
        WHEN dead_pct > 10 THEN 'Increase analyze frequency and monitor churn'
        ELSE 'No immediate tuning change required'
      END ||
      '</td></tr>',
      E'\n' ORDER BY dead_pct DESC NULLS LAST
    ),
    '<tr><td colspan="8" class="table-empty">No autovacuum statistics available</td></tr>'
  )
FROM (
  SELECT * FROM t
  WHERE n_live_tup + n_dead_tup > 0
  ORDER BY dead_pct DESC NULLS LAST
  LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S05.8 Table churn profile
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Table Churn Rate Profile (Insert/Update/Delete)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Inserts</th><th>Updates</th><th>Deletes</th><th>Total DML</th><th>Update Bias</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(n_tup_ins, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(n_tup_upd, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(n_tup_del, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char((n_tup_ins + n_tup_upd + n_tup_del), 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN n_tup_upd > (n_tup_ins + n_tup_del) THEN 'warn' ELSE '' END || '">' ||
      COALESCE(round((n_tup_upd * 100.0 / NULLIF(n_tup_ins + n_tup_upd + n_tup_del,0))::numeric,1)::text,'0') || '%</td>' ||
      '<td>' ||
      CASE
        WHEN n_tup_upd > (n_tup_ins + n_tup_del) THEN 'Update-heavy: monitor HOT ratio and fillfactor'
        WHEN n_tup_del > n_tup_ins THEN 'Delete-heavy: monitor vacuum debt and free-space reuse'
        ELSE 'Balanced or insert-heavy churn'
      END ||
      '</td></tr>',
      E'\n' ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
    ),
    '<tr><td colspan="8" class="table-empty">No churn statistics available</td></tr>'
  )
FROM (
  SELECT *
  FROM pg_stat_user_tables
  WHERE (n_tup_ins + n_tup_upd + n_tup_del) > 0
  ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
  LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S05.9 Action queue
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Table-Health Actions</div>'

WITH stats AS (
  SELECT
    COUNT(*) FILTER (WHERE n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup,0) > 0.2) AS high_dead_ratio_tables,
    COUNT(*) FILTER (WHERE age(relfrozenxid) > current_setting('autovacuum_freeze_max_age')::numeric * 0.8) AS high_freeze_age_tables,
    COUNT(*) FILTER (WHERE n_tup_upd > (n_tup_ins + n_tup_del)) AS update_heavy_tables
FROM pg_stat_user_tables s
  JOIN pg_class c ON c.oid = s.relid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
)
SELECT
  '<div class="finding ' || CASE WHEN high_dead_ratio_tables + high_freeze_age_tables > 0 THEN 'high' ELSE 'good' END || '">' ||
  '<div class="finding-header"><span class="finding-title">Table health remediation queue</span>' ||
  '<span class="severity-pill ' || CASE WHEN high_dead_ratio_tables + high_freeze_age_tables > 0 THEN 'pill-warning">ACTION' ELSE 'pill-good">OK' END || '</span></div>' ||
  '<div class="finding-body">' ||
  'High dead-ratio tables: <strong>' || high_dead_ratio_tables || '</strong>; ' ||
  'High freeze-age tables: <strong>' || high_freeze_age_tables || '</strong>; ' ||
  'Update-heavy tables: <strong>' || update_heavy_tables || '</strong>.' ||
  '<br><strong>Fix:</strong> apply per-table autovacuum tuning and schedule VACUUM (FREEZE) for highest-age tables.' ||
  ' <strong>Verify:</strong> rerun S05 and confirm dead ratio / freeze age trend down.' ||
  ' <strong>Rollback:</strong> revert aggressive table-level autovacuum settings if write latency regresses.' ||
  '</div></div>'
FROM stats;

\qecho '</div>'
\qecho '</div>'
\qecho '<div class="section" id="s06">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Index Health &amp; Missing Index Suggestions</div>'
\qecho '    <div class="section-desc">Unused indexes, duplicate indexes, bloated indexes, and AI-driven missing index suggestions based on query patterns.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S06.1 Unused indexes (save write overhead by dropping)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Unused Non-Unique Indexes Since Last Stats Reset</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Index</th><th>Size</th><th>Scans</th><th>Type</th><th>Review Script</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(s.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(indexrelname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num warn">' || pg_size_pretty(pg_relation_size(s.indexrelid)) || '</td>' ||
  '<td class="num crit">0</td>' ||
  '<td>' || COALESCE(replace(replace(am.amname,'<','&lt;'),'>','&gt;'),'') || '</td>' ||
  '<td class="code-block">-- Review before drop: ' ||
  replace(replace(replace(replace(replace(
    format('DROP INDEX CONCURRENTLY %I.%I;', schemaname, indexrelname)
  ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td>' ||
  '</tr>'
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
LEFT JOIN pg_class ic ON ic.oid = s.indexrelid
LEFT JOIN pg_am am ON am.oid = ic.relam
WHERE s.idx_scan = 0
AND NOT i.indisprimary
AND NOT i.indisunique
AND NOT COALESCE(i.indisexclusion, false)
AND pg_relation_size(s.indexrelid) > 65536  -- ignore tiny indexes
AND NOT EXISTS (
  SELECT 1
  FROM pg_constraint con
  WHERE con.contype = 'f'
    AND con.conrelid = i.indrelid
    AND array_length(con.conkey, 1) IS NOT NULL
    AND (
      SELECT array_agg(k.attnum ORDER BY k.ord)
      FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k(attnum, ord)
      WHERE k.ord <= array_length(con.conkey, 1)
    ) = con.conkey
)
ORDER BY pg_relation_size(s.indexrelid) DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S06.2 Duplicate / redundant indexes
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Duplicate &amp; Redundant Indexes</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Table</th><th>Index 1</th><th>Index 2</th><th>Columns</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(t.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(i1.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(i2.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        array_to_string(ARRAY(
          SELECT a.attname FROM pg_attribute a
          WHERE a.attrelid = ix1.indrelid
          AND a.attnum = ANY(ix1.indkey)
          ORDER BY array_position(ix1.indkey, a.attnum)
        )::text[], ', '),
        '&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td class="warn">Consider dropping ' ||
      replace(replace(replace(replace(replace(i2.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      ' (subset of ' ||
      replace(replace(replace(replace(replace(i1.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      ')</td></tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No obvious duplicate indexes detected</td></tr>'
  )
FROM pg_index ix1
JOIN pg_index ix2 ON ix1.indrelid = ix2.indrelid
  AND ix1.indexrelid <> ix2.indexrelid
  AND (ix1.indkey::int[] @> ix2.indkey::int[])
  AND NOT ix2.indisprimary
JOIN pg_class i1 ON i1.oid = ix1.indexrelid
JOIN pg_class i2 ON i2.oid = ix2.indexrelid
JOIN pg_class t  ON t.oid  = ix1.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema')
AND ix1.indisvalid AND ix2.indisvalid;

\qecho '</tbody></table></div></div>'

-- S06.3 Missing FK indexes
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">FKs Without Supporting Indexes</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Table</th><th>FK Column(s)</th><th>References</th><th>Suggested Index (DBA review required)</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(src_table,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="warn">' || replace(replace(replace(replace(replace(fk_col,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(ref_table,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(fix_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> All foreign keys have supporting indexes</td></tr>'
  )
FROM (
  SELECT
    n.nspname AS src_schema,
    c.relname AS src_relname,
    n.nspname||'.'||c.relname AS src_table,
    a.attname AS fk_col,
    rn.nspname||'.'||rc.relname AS ref_table,
    format(
      'CREATE INDEX CONCURRENTLY %I ON %I.%I (%I);',
      'idx_fk_' || substr(md5(n.nspname || '.' || c.relname || ':' || a.attname), 1, 12),
      n.nspname,
      c.relname,
      a.attname
    ) AS fix_sql
  FROM pg_constraint con
  JOIN pg_class c ON c.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
  JOIN pg_class rc ON rc.oid = con.confrelid
  JOIN pg_namespace rn ON rn.oid = rc.relnamespace
  WHERE con.contype = 'f'
  AND n.nspname NOT IN ('pg_catalog','information_schema')
  AND NOT EXISTS (
    SELECT 1 FROM pg_index i
    JOIN pg_attribute ia ON ia.attrelid = i.indrelid AND ia.attnum = ANY(i.indkey)
    WHERE i.indrelid = con.conrelid
    AND ia.attname = a.attname
  )
) fk_missing;

\qecho '</tbody></table></div></div>'

-- S06.4 High sequential scan tables  missing index suggestion
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables With High Sequential Scans</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Seq Scans</th><th>Avg Rows/Scan</th><th>Table Size</th><th>Index Scans</th><th>Verdict</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN seq_scan > 10000 THEN 'crit' WHEN seq_scan > 1000 THEN 'warn' ELSE '' END || '">' ||
  to_char(seq_scan,'FM999,999') || '</td>' ||
  '<td class="num">' || to_char(avg_rows,'FM999,999') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || to_char(COALESCE(idx_scan,0),'FM999,999') || '</td>' ||
  '<td class="' ||
  CASE
    WHEN avg_rows > 10000 AND seq_scan > 100 THEN 'crit"> Likely missing index on filter column'
    WHEN avg_rows > 1000  AND seq_scan > 500 THEN 'warn"> Possible missing index'
    WHEN pg_total_relation_size(relid) < 1048576 THEN 'good"> Small table  seq scan OK'
    ELSE '">Monitor'
  END ||
  '</td></tr>'
FROM (
  SELECT *,
    seq_tup_read::numeric / NULLIF(seq_scan,0) AS avg_rows
  FROM pg_stat_user_tables
  WHERE seq_scan > 50
) t
ORDER BY seq_scan * avg_rows DESC NULLS LAST
LIMIT 25;

\qecho '</tbody></table></div></div>'

-- S06.5 Invalid indexes
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Invalid Indexes</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Index</th><th>Fix Script</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(t.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="crit">' || replace(replace(replace(replace(replace(i.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format('DROP INDEX CONCURRENTLY %I.%I; -- Then recreate', n.nspname, i.relname)
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> No invalid indexes found</td></tr>'
  )
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE NOT ix.indisvalid
AND n.nspname NOT IN ('pg_catalog','information_schema');

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Index Write-Cost Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Indexes</th><th>Total DML</th><th>Table Size</th><th>Write-Cost Posture</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

WITH idx_counts AS (
  SELECT
    schemaname,
    tablename,
    COUNT(*)::numeric AS index_count
  FROM pg_indexes
  WHERE schemaname NOT IN ('pg_catalog','information_schema')
  GROUP BY schemaname, tablename
), dml AS (
  SELECT
    s.schemaname,
    s.relname AS tablename,
    COALESCE(i.index_count, 0) AS index_count,
    (s.n_tup_ins + s.n_tup_upd + s.n_tup_del)::numeric AS total_dml,
    pg_total_relation_size(s.relid) AS table_bytes
  FROM pg_stat_user_tables s
  LEFT JOIN idx_counts i
    ON i.schemaname = s.schemaname
   AND i.tablename = s.relname
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(tablename,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' || CASE WHEN index_count >= 10 THEN 'warn' ELSE '' END || '">' || index_count || '</td>' ||
      '<td class="num">' || to_char(total_dml, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || pg_size_pretty(table_bytes) || '</td>' ||
      '<td>' ||
      CASE
        WHEN total_dml > 500000 AND index_count >= 10 THEN 'High write amplification risk'
        WHEN total_dml > 100000 AND index_count >= 6 THEN 'Moderate write-cost pressure'
        ELSE 'Balanced'
      END ||
      '</td>' ||
      '<td>' ||
      CASE
        WHEN total_dml > 500000 AND index_count >= 10 THEN 'Review low-value/unused indexes before adding new ones'
        WHEN total_dml > 100000 AND index_count >= 6 THEN 'Validate index ROI for write-heavy workload'
        ELSE 'No immediate index-count reduction needed'
      END ||
      '</td></tr>',
      E'\n' ORDER BY total_dml DESC
    ),
    '<tr><td colspan="7" class="table-empty">No DML-heavy tables detected</td></tr>'
  )
FROM (
  SELECT *
  FROM dml
  WHERE total_dml > 0
  ORDER BY total_dml DESC
  LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S06.7 Index-only scan potential
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Index-Only Scan Potential</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Index</th><th>Idx Scans</th><th>Idx Tuples Read</th><th>Heap Fetches</th><th>Heap Fetch Ratio</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

WITH idx AS (
  SELECT
    s.schemaname,
    t.relname AS tablename,
    s.indexrelname,
    s.idx_scan::numeric AS idx_scan,
    s.idx_tup_read::numeric AS idx_tup_read,
    s.idx_tup_fetch::numeric AS idx_tup_fetch,
    s.idx_tup_fetch / NULLIF(s.idx_tup_read, 0) AS heap_fetch_ratio
  FROM pg_stat_user_indexes s
  JOIN pg_class t ON t.oid = s.relid
  WHERE s.idx_scan > 100
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(tablename,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(indexrelname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(idx_scan, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(idx_tup_read, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(idx_tup_fetch, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN heap_fetch_ratio > 0.9 THEN 'warn' ELSE 'good' END || '">' || COALESCE(round(heap_fetch_ratio::numeric,2)::text,'0') || '</td>' ||
      '<td>' ||
      CASE
        WHEN heap_fetch_ratio > 0.9 THEN 'Potential INCLUDE candidate if SELECT list columns are missing'
        WHEN heap_fetch_ratio > 0.6 THEN 'Moderate heap fetch pressure'
        ELSE 'Index-only behavior already reasonable'
      END ||
      '</td></tr>',
      E'\n' ORDER BY heap_fetch_ratio DESC NULLS LAST
    ),
    '<tr><td colspan="8" class="table-empty">No index scan telemetry for index-only analysis</td></tr>'
  )
FROM (
  SELECT * FROM idx ORDER BY heap_fetch_ratio DESC NULLS LAST LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S06.8 FK index gaps with benefit/risk and verify template
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Foreign Key Index Gaps</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Child Table</th><th>FK Columns</th><th>Estimated Benefit</th><th>Write-Cost Risk</th><th>Create Script</th><th>Verify Plan</th>'
\qecho '</tr></thead><tbody>'

WITH fk AS (
  SELECT
    c.oid AS child_oid,
    n.nspname AS child_schema,
    c.relname AS child_table,
    con.conname,
    con.conkey,
    pg_get_constraintdef(con.oid) AS condef
  FROM pg_constraint con
  JOIN pg_class c ON c.oid = con.conrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE con.contype = 'f'
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
), fk_cols AS (
  SELECT
    fk.*,
    ARRAY(
      SELECT att.attname
      FROM unnest(fk.conkey) WITH ORDINALITY AS k(attnum, ord)
      JOIN pg_attribute att
        ON att.attrelid = fk.child_oid
       AND att.attnum = k.attnum
      ORDER BY k.ord
    ) AS col_names
  FROM fk
), missing AS (
  SELECT
    f.child_schema,
    f.child_table,
    f.col_names,
    s.n_tup_ins + s.n_tup_upd + s.n_tup_del AS total_dml,
    format(
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS %I ON %I.%I (%s);',
      'idx_fk_' || substr(md5(f.child_schema || '.' || f.child_table || ':' || array_to_string(f.col_names, ',')), 1, 12),
      f.child_schema,
      f.child_table,
      array_to_string(ARRAY(SELECT format('%I', c) FROM unnest(f.col_names) AS c), ', ')
    ) AS create_sql,
    format(
      'EXPLAIN (ANALYZE, BUFFERS) SELECT 1 FROM %I.%I WHERE %s;',
      f.child_schema,
      f.child_table,
      array_to_string(ARRAY(SELECT format('%I = ?', c) FROM unnest(f.col_names) AS c), ' AND ')
    ) AS verify_sql
  FROM fk_cols f
  LEFT JOIN pg_stat_user_tables s
    ON s.schemaname = f.child_schema
   AND s.relname = f.child_table
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_index i
    WHERE i.indrelid = f.child_oid
      AND i.indisvalid
      AND (
        SELECT array_agg(att.attname ORDER BY k.ord)
        FROM unnest(i.indkey[1:array_length(f.conkey,1)]) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute att
          ON att.attrelid = f.child_oid
         AND att.attnum = k.attnum
      ) = f.col_names
  )
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(child_schema || '.' || child_table,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(array_to_string(col_names, ', '),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' ||
      CASE
        WHEN COALESCE(total_dml,0) > 500000 THEN '<span class="severity-pill pill-critical">HIGH</span>'
        WHEN COALESCE(total_dml,0) > 100000 THEN '<span class="severity-pill pill-warning">MEDIUM</span>'
        ELSE '<span class="severity-pill pill-info">LOW</span>'
      END ||
      '</td>' ||
      '<td>' ||
      CASE
        WHEN COALESCE(total_dml,0) > 500000 THEN 'High on write-heavy table'
        WHEN COALESCE(total_dml,0) > 100000 THEN 'Moderate'
        ELSE 'Low'
      END ||
      '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(create_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(verify_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY COALESCE(total_dml,0) DESC
    ),
    '<tr><td colspan="6" class="table-empty">No missing FK support indexes detected</td></tr>'
  )
FROM missing;

\qecho '</tbody></table></div></div>'

-- S06.9 Version-aware planner and index advice
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">SQL Telemetry &amp; Version Insights</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Observed</th><th>Status</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'
WITH v AS (
  SELECT current_setting('server_version_num')::int AS vnum
), amcheck AS (
  SELECT
    EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'amcheck') AS amcheck_available,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'amcheck') AS amcheck_installed
), gin_check AS (
  SELECT to_regprocedure('gin_index_check(regclass)') IS NOT NULL AS gin_index_check_available
)
SELECT
  '<tr><td>Server version</td><td>' || (vnum / 10000) || '.' || CASE WHEN vnum >= 100000 THEN (vnum % 10000) ELSE ((vnum / 100) % 100) END || '</td><td class="good">INFO</td><td>' ||
  CASE WHEN vnum >= 180000 THEN 'Planner can use PostgreSQL 18 skip scan behavior where applicable.' ELSE 'Skip scan is not available on this branch; multicolumn index recommendations should assume classic left-prefix rules.' END ||
  '</td></tr>' ||
  '<tr><td>PG18 skip scan awareness</td><td>' || CASE WHEN vnum >= 180000 THEN 'yes' ELSE 'no' END || '</td><td class="' ||
  CASE WHEN vnum >= 180000 THEN 'good">ENABLED' ELSE 'warn">N/A' END || '</td><td>' ||
  CASE WHEN vnum >= 180000 THEN 'Review low-leading-column selectivity findings carefully before adding duplicate single-column indexes.' ELSE 'On pre-18 branches, left-prefix access rules remain stricter.' END ||
  '</td></tr>' ||
  '<tr><td>amcheck availability</td><td>' || CASE WHEN amcheck_available THEN 'available' ELSE 'missing' END || ' / installed=' || CASE WHEN amcheck_installed THEN 'yes' ELSE 'no' END || '</td><td class="' ||
  CASE WHEN amcheck_installed THEN 'good">READY' WHEN amcheck_available THEN 'warn">OPTIONAL' ELSE 'warn">LIMITED' END || '</td><td>' ||
  CASE WHEN amcheck_installed THEN 'Index and heap corruption checks can be added to maintenance workflows.' WHEN amcheck_available THEN 'amcheck is available but not installed; consider enabling it for integrity verification.' ELSE 'amcheck extension is not available in this environment.' END ||
  '</td></tr>' ||
  '<tr><td>GIN integrity check support</td><td>' || CASE WHEN gin_index_check_available THEN 'yes' ELSE 'no' END || '</td><td class="' ||
  CASE WHEN gin_index_check_available THEN 'good">READY' ELSE 'warn">N/A' END || '</td><td>' ||
  CASE WHEN gin_index_check_available THEN 'GIN index validation functions are available for deeper integrity checks.' ELSE 'GIN-specific integrity checks are not exposed on this branch / extension set.' END ||
  '</td></tr>'
FROM v, amcheck, gin_check;
\qecho '</tbody></table></div></div>'

-- S06.9 Index readiness status
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Index Readiness Status</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Index</th><th>indisvalid</th><th>indisready</th><th>Recommended Action</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(t.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN NOT ix.indisvalid OR NOT ix.indisready THEN 'crit' ELSE '' END || '">' || replace(replace(replace(replace(replace(i.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN ix.indisvalid THEN 'true' ELSE 'false' END || '</td>' ||
      '<td>' || CASE WHEN ix.indisready THEN 'true' ELSE 'false' END || '</td>' ||
      '<td>' ||
      CASE
        WHEN NOT ix.indisready THEN 'Index build incomplete; drop and recreate concurrently'
        WHEN NOT ix.indisvalid THEN 'Index invalid; rebuild with CREATE INDEX CONCURRENTLY'
        ELSE 'No action'
      END ||
      '</td></tr>',
      E'\n'
    ),
    '<tr><td colspan="6" class="table-empty">All indexes are valid and ready</td></tr>'
  )
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
  AND (NOT ix.indisvalid OR NOT ix.indisready);

\qecho '</tbody></table></div></div>'

-- S06.10 Action queue
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Index Remediation Queue</div>'

WITH stats AS (
  SELECT
    (SELECT COUNT(*) FROM pg_stat_user_indexes s JOIN pg_index i ON i.indexrelid = s.indexrelid WHERE s.idx_scan = 0 AND NOT i.indisprimary AND NOT i.indisunique) AS unused_idx_cnt,
    (SELECT COUNT(*) FROM pg_index WHERE NOT indisvalid OR NOT indisready) AS invalid_or_unready_cnt
)
SELECT
  '<div class="finding ' || CASE WHEN unused_idx_cnt + invalid_or_unready_cnt > 0 THEN 'high' ELSE 'good' END || '">' ||
  '<div class="finding-header"><span class="finding-title">Index remediation priorities</span>' ||
  '<span class="severity-pill ' || CASE WHEN unused_idx_cnt + invalid_or_unready_cnt > 0 THEN 'pill-warning">ACTION' ELSE 'pill-good">OK' END || '</span></div>' ||
  '<div class="finding-body">' ||
  'Unused index candidates: <strong>' || unused_idx_cnt || '</strong>; ' ||
  'Invalid or unready indexes: <strong>' || invalid_or_unready_cnt || '</strong>.' ||
  '<br><strong>Fix:</strong> rebuild invalid/unready indexes first, then evaluate unused indexes with workload confirmation.' ||
  ' <strong>Verify:</strong> rerun S06 and confirm readiness flags clear and plan quality improves in EXPLAIN.' ||
  ' <strong>Rollback:</strong> if regression occurs after index changes, recreate dropped indexes from captured DDL metadata.' ||
  '</div></div>'
FROM stats;

\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S06: INDEX HEALTH & MISSING INDEX SUGGESTIONS
-- SECTION S07: BUFFER CACHE & I/O
-- =============================================================================
\qecho '<div class="section" id="s07">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Buffer Cache &amp; I/O</div>'
\qecho '    <div class="section-desc">Buffer utilization, checkpoint health, I/O patterns. Requires pg_buffercache extension for detailed view.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S07.1 BGWriter & checkpoint stats
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">BGWriter &amp; Checkpoint Statistics</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Checkpoints Timed</td><td class="num">' || to_char(c.num_timed,'FM999,999') || '</td><td class="good"> Scheduled</td></tr>' ||
  '<tr><td>Checkpoints Requested (forced)</td><td class="num ' || CASE WHEN c.num_requested > c.num_timed * 0.1 THEN 'crit' ELSE 'good' END || '">' ||
  to_char(c.num_requested,'FM999,999') || '</td><td class="' || CASE WHEN c.num_requested > 100 THEN 'warn"> High  increase max_wal_size' ELSE 'good"> OK' END || '</td></tr>' ||
  '<tr><td>Write Time (ms)</td><td class="num">' || round(c.write_time::numeric,0) || '</td><td></td></tr>' ||
  '<tr><td>Sync Time (ms)</td><td class="num">' || round(c.sync_time::numeric,0) || '</td><td></td></tr>' ||
  '<tr><td>Buffers Checkpointer</td><td class="num">' || to_char(c.buffers_written,'FM999,999,999') || '</td><td></td></tr>' ||
  '<tr><td>Buffers Clean (bgwriter)</td><td class="num">' || to_char(b.buffers_clean,'FM999,999,999') || '</td><td></td></tr>' ||
  '<tr><td>Buffers Allocated</td><td class="num">' || to_char(b.buffers_alloc,'FM999,999,999') || '</td><td></td></tr>' ||
  '<tr><td>Max Written Clean</td><td class="num ' || CASE WHEN b.maxwritten_clean > 0 THEN 'warn' ELSE 'good' END || '">' ||
  to_char(b.maxwritten_clean,'FM999,999') || '</td><td class="' || CASE WHEN b.maxwritten_clean > 1000 THEN 'warn"> bgwriter_lru_maxpages too low' ELSE 'good"> OK' END || '</td></tr>'
FROM pg_stat_bgwriter b
CROSS JOIN pg_stat_checkpointer c;

\qecho '</tbody></table></div></div>'

-- S07.2 Table & Index I/O cache hit ratios
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Table-Level Cache Hit Ratios (Bottom 20)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Heap Hit%</th><th>Index Hit%</th><th>Toast Hit%</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN heap_hit < 80 THEN 'crit' WHEN heap_hit < 95 THEN 'warn' ELSE 'good' END || '">' ||
  round(heap_hit::numeric,1) || '%</td>' ||
  '<td class="num ' || CASE WHEN idx_hit < 80 THEN 'crit' WHEN idx_hit < 95 THEN 'warn' ELSE 'good' END || '">' ||
  round(idx_hit::numeric,1) || '%</td>' ||
  '<td class="num">' || COALESCE(round(toast_hit::numeric,1)||'%','N/A') || '</td>' ||
  '</tr>'
FROM (
  SELECT
    s.schemaname,
    s.relname,
    100.0 * io.heap_blks_hit / NULLIF(io.heap_blks_hit + io.heap_blks_read, 0) AS heap_hit,
    100.0 * io.idx_blks_hit  / NULLIF(io.idx_blks_hit  + io.idx_blks_read,  0) AS idx_hit,
    100.0 * io.toast_blks_hit/ NULLIF(io.toast_blks_hit+ io.toast_blks_read, 0) AS toast_hit
  FROM pg_statio_user_tables io
  JOIN pg_stat_user_tables s ON s.relid = io.relid
  WHERE io.heap_blks_hit + io.heap_blks_read > 0
) t
ORDER BY heap_hit ASC NULLS LAST
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S07.3 Checkpoint pressure indicators
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Checkpoint Pressure Indicators</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Forced Checkpoint Ratio</td><td class="num ' ||
  CASE WHEN forced_ratio > 20 THEN 'crit' WHEN forced_ratio > 10 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(forced_ratio::numeric,2)::text,'0') || '%</td><td class="' ||
  CASE WHEN forced_ratio > 20 THEN 'crit">High pressure. Raise max_wal_size and review WAL spikes.'
       WHEN forced_ratio > 10 THEN 'warn">Moderate pressure. Track with S08 WAL rate.'
       ELSE 'good">Stable checkpoint cadence.' END || '</td></tr>' ||
  '<tr><td>Checkpointer Write Share</td><td class="num ' ||
  CASE WHEN checkpointer_share > 80 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(checkpointer_share::numeric,2)::text,'0') || '%</td><td class="' ||
  CASE WHEN checkpointer_share > 80 THEN 'warn">Most writes happen at checkpoints. Smooth write load.'
       ELSE 'good">Write activity is balanced with bgwriter.' END || '</td></tr>' ||
  '<tr><td>Avg Checkpoint Write Time</td><td class="num ' ||
  CASE WHEN write_ms_per_cp > 20000 THEN 'crit' WHEN write_ms_per_cp > 8000 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(write_ms_per_cp::numeric,0)::text,'0') || ' ms</td><td class="' ||
  CASE WHEN write_ms_per_cp > 20000 THEN 'crit">High checkpoint write latency.'
       WHEN write_ms_per_cp > 8000 THEN 'warn">Monitor storage throughput and checkpoint tuning.'
       ELSE 'good">Checkpoint write latency is acceptable.' END || '</td></tr>' ||
  '<tr><td>Avg Checkpoint Sync Time</td><td class="num ' ||
  CASE WHEN sync_ms_per_cp > 5000 THEN 'crit' WHEN sync_ms_per_cp > 1500 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(sync_ms_per_cp::numeric,0)::text,'0') || ' ms</td><td class="' ||
  CASE WHEN sync_ms_per_cp > 5000 THEN 'crit">High fsync latency during checkpoints.'
       WHEN sync_ms_per_cp > 1500 THEN 'warn">Sync latency elevated. Validate I/O queue depth.'
       ELSE 'good">Sync phase is healthy.' END || '</td></tr>'
FROM (
  SELECT
    100.0 * c.num_requested / NULLIF(c.num_timed + c.num_requested, 0) AS forced_ratio,
    100.0 * c.buffers_written / NULLIF(c.buffers_written + b.buffers_clean, 0) AS checkpointer_share,
    c.write_time / NULLIF(c.num_done, 0) AS write_ms_per_cp,
    c.sync_time / NULLIF(c.num_done, 0) AS sync_ms_per_cp
  FROM pg_stat_checkpointer c
  CROSS JOIN pg_stat_bgwriter b
) m;

\qecho '</tbody></table></div></div>'

-- S07.3a Background writer pressure classification
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Background Write Pressure Classification</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Requested Checkpoint %</td><td class="num ' ||
  CASE WHEN requested_checkpoint_pct > 10 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(requested_checkpoint_pct::numeric, 2)::text, '0') || '%</td><td class="' ||
  CASE
    WHEN requested_checkpoint_pct > 10 THEN 'warn">Forced checkpoints are noticeable; review max_wal_size and write burst patterns.'
    ELSE 'good">Checkpoint request rate is stable.'
  END || '</td></tr>' ||
  '<tr><td>BGWriter Maxwritten Events</td><td class="num ' ||
  CASE WHEN maxwritten_clean > 0 THEN 'warn' ELSE 'good' END || '">' ||
  to_char(maxwritten_clean, 'FM999,999,999') || '</td><td class="' ||
  CASE
    WHEN maxwritten_clean > 1000 THEN 'warn">bgwriter is hitting write limits often; validate bgwriter_lru_maxpages and dirty-page churn.'
    WHEN maxwritten_clean > 0 THEN 'warn">Some bgwriter write throttling observed.'
    ELSE 'good">No bgwriter write-throttle signal.'
  END || '</td></tr>' ||
  '<tr><td>Pressure Label</td><td>' || pressure_label || '</td><td class="' ||
  CASE
    WHEN pressure_label = 'CHECKPOINT_PRESSURE_HIGH' THEN 'crit">Write bursts are checkpoint-driven.'
    WHEN pressure_label = 'BGWRITER_MAXWRITTEN_EVENTS' THEN 'warn">Background writer is frequently capped.'
    ELSE 'good">No dominant write-pressure label.'
  END || '</td></tr>' ||
  '<tr><td>Checkpointer stats reset</td><td>' || COALESCE(to_char(checkpointer_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A') || '</td><td>Window start for checkpoint pressure interpretation.</td></tr>' ||
  '<tr><td>BGWriter stats reset</td><td>' || COALESCE(to_char(bgwriter_stats_reset, 'YYYY-MM-DD HH24:MI:SS TZ'), 'N/A') || '</td><td>Window start for bgwriter write pressure interpretation.</td></tr>'
FROM (
  SELECT
    cp.num_timed,
    cp.num_requested,
    round(
      CASE WHEN cp.num_timed + cp.num_requested = 0 THEN 0
           ELSE 100.0 * cp.num_requested::numeric / (cp.num_timed + cp.num_requested)
      END,
      2
    ) AS requested_checkpoint_pct,
    bg.maxwritten_clean,
    CASE
      WHEN cp.num_requested > cp.num_timed THEN 'CHECKPOINT_PRESSURE_HIGH'
      WHEN bg.maxwritten_clean > 0 THEN 'BGWRITER_MAXWRITTEN_EVENTS'
      ELSE 'STABLE'
    END AS pressure_label,
    cp.stats_reset AS checkpointer_stats_reset,
    bg.stats_reset AS bgwriter_stats_reset
  FROM pg_stat_checkpointer cp
  CROSS JOIN pg_stat_bgwriter bg
) s;

\qecho '</tbody></table></div></div>'

-- S07.4 Top relations by physical reads
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top Relations by Physical Reads</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Relation</th><th>Heap Reads</th><th>Index Reads</th><th>Total Reads</th><th>Heap Hit%</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(heap_blks_read, 'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(idx_blks_read, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN total_read > 100000 THEN 'crit' WHEN total_read > 10000 THEN 'warn' ELSE '' END || '">' ||
      to_char(total_read, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN heap_hit < 90 THEN 'warn' ELSE 'good' END || '">' ||
      COALESCE(round(heap_hit::numeric,1)::text,'0') || '%</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="6" class="table-empty"> No physical read activity captured since stats reset</td></tr>'
  )
FROM (
  SELECT
    s.schemaname,
    s.relname,
    io.heap_blks_read,
    io.idx_blks_read,
    io.heap_blks_read + io.idx_blks_read AS total_read,
    100.0 * io.heap_blks_hit / NULLIF(io.heap_blks_hit + io.heap_blks_read, 0) AS heap_hit
  FROM pg_statio_user_tables io
  JOIN pg_stat_user_tables s ON s.relid = io.relid
  WHERE io.heap_blks_read + io.idx_blks_read > 0
  ORDER BY io.heap_blks_read + io.idx_blks_read DESC
  LIMIT 20
) t;

\qecho '</tbody></table></div></div>'

-- S07.5 Buffer residency view (pg_buffercache)
SELECT
  CASE
    WHEN to_regclass('pg_buffercache') IS NOT NULL
     AND has_table_privilege(current_user, 'pg_buffercache', 'SELECT')
    THEN 'on' ELSE 'off'
  END AS s07_has_buffercache
\gset

\if :s07_has_buffercache
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Buffer Residency by Object (pg_buffercache)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Object</th><th>Kind</th><th>Cached Buffers</th><th>Cache Share%</th><th>Dirty Buffers</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || relkind || '</td>' ||
      '<td class="num">' || to_char(buffer_pages, 'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN cache_share > 10 THEN 'warn' ELSE '' END || '">' ||
      round(cache_share::numeric,2) || '%</td>' ||
      '<td class="num ' || CASE WHEN dirty_pages > 10000 THEN 'warn' ELSE '' END || '">' ||
      to_char(dirty_pages, 'FM999,999,999') || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="6" class="table-empty"> pg_buffercache is present but no user-object buffers were sampled</td></tr>'
  )
FROM (
  SELECT
    n.nspname,
    c.relname,
    CASE c.relkind
      WHEN 'r' THEN 'table'
      WHEN 'i' THEN 'index'
      WHEN 'm' THEN 'matview'
      ELSE c.relkind::text
    END AS relkind,
    COUNT(*) AS buffer_pages,
    COUNT(*) FILTER (WHERE b.isdirty) AS dirty_pages,
    COUNT(*) * 100.0 / NULLIF(current_setting('shared_buffers')::numeric, 0) AS cache_share
  FROM pg_buffercache b
  JOIN pg_database d
    ON d.oid = b.reldatabase
  JOIN pg_class c
    ON c.relfilenode = b.relfilenode
  JOIN pg_namespace n
    ON n.oid = c.relnamespace
  WHERE d.datname = current_database()
    AND c.relkind IN ('r','i','m')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  GROUP BY n.nspname, c.relname, c.relkind
  ORDER BY COUNT(*) DESC
  LIMIT 20
) bc;

\qecho '</tbody></table></div></div>'
\else
SELECT
  '<div class="finding info"><div class="finding-header">' ||
  '' ||
  '<span class="finding-title">pg_buffercache not available for detailed cache residency</span>' ||
  '<span class="severity-pill pill-info">INFO</span></div>' ||
  '<div class="finding-body">Install extension pg_buffercache and grant SELECT to the report role for object-level cache analysis.</div></div>';
\endif
\qecho '</div>'

-- =============================================================================
-- SECTION S08: WAL & REPLICATION
-- =============================================================================
\qecho '<div class="section" id="s08">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">WAL &amp; Replication</div>'
\qecho '    <div class="section-desc">Replication lag, slot health, WAL generation, archive status.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S08.1 Replication status
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Replication Status</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Application</th><th>Client (masked)</th><th>State</th><th>Sent LSN</th><th>Write LSN</th><th>Flush LSN</th><th>Replay LSN</th><th>Lag</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(application_name,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      -- SECURITY: Mask client IP
      '<td>' || CASE WHEN client_addr IS NULL THEN 'local'
                     ELSE regexp_replace(host(client_addr),'(\d+)\.(\d+)\.\d+\.\d+','\1.\2.x.x')
                END || '</td>' ||
      '<td class="good">' || replace(replace(COALESCE(state,''),'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td class="num">' || COALESCE(sent_lsn::text,'') || '</td>' ||
      '<td class="num">' || COALESCE(write_lsn::text,'') || '</td>' ||
      '<td class="num">' || COALESCE(flush_lsn::text,'') || '</td>' ||
      '<td class="num">' || COALESCE(replay_lsn::text,'') || '</td>' ||
      '<td class="num ' || CASE WHEN write_lag > interval '30s' OR flush_lag > interval '30s' THEN 'crit' WHEN write_lag > interval '5s' THEN 'warn' ELSE 'good' END || '">' ||
      COALESCE(to_char(GREATEST(write_lag,flush_lag,replay_lag),'HH24:MI:SS'),'') || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="8" class="table-empty"> No streaming replication configured (standalone or primary with no standbys)</td></tr>'
  )
FROM pg_stat_replication;

\qecho '</tbody></table></div></div>'

-- S08.2 Replication slots (bloat risk)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Replication Slots</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Slot Name</th><th>Type</th><th>Active</th><th>WAL Retained</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(slot_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || slot_type || '</td>' ||
      '<td class="' || CASE WHEN active THEN 'good"> Active' ELSE 'crit"> INACTIVE' END || '</td>' ||
      '<td class="num ' || CASE WHEN wal_retained > 1073741824 THEN 'crit' WHEN wal_retained > 104857600 THEN 'warn' ELSE 'good' END || '">' ||
      pg_size_pretty(wal_retained) || '</td>' ||
      '<td class="' ||
      CASE WHEN NOT active AND wal_retained > 1073741824 THEN 'crit"> CRITICAL: Drop inactive slot'
           WHEN NOT active THEN 'warn"> Inactive slot  monitor'
           ELSE 'good"> OK' END ||
      '</td></tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No replication slots</td></tr>'
  )
FROM (
  SELECT *,
    CASE
      WHEN confirmed_flush_lsn IS NOT NULL
      THEN pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
      ELSE 0
    END AS wal_retained
  FROM pg_replication_slots
) rs;

\qecho '</tbody></table></div></div>'

-- S08.3 Replication slot safety window
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Replication Slot Safety Window</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Slot Name</th><th>WAL Status</th><th>Safe WAL Size</th><th>Inactive Since</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(slot_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' ||
      CASE WHEN wal_status IN ('lost','unreserved') THEN 'crit"> ' ELSE '"> ' END ||
      COALESCE(wal_status, 'unknown') || '</td>' ||
      '<td class="num ' ||
      CASE WHEN safe_wal_size IS NULL THEN 'warn'
           WHEN safe_wal_size < 1073741824 THEN 'warn'
           ELSE 'good' END || '">' ||
      COALESCE(pg_size_pretty(safe_wal_size), 'N/A') || '</td>' ||
      '<td>' || COALESCE(to_char(inactive_since,'YYYY-MM-DD HH24:MI'),'active') || '</td>' ||
      '<td class="' ||
      CASE
        WHEN wal_status IN ('lost','unreserved') THEN 'crit">Slot is at risk. Advance or drop slot urgently.'
        WHEN NOT active AND inactive_since IS NOT NULL AND inactive_since < now() - interval '6 hours' THEN 'warn">Inactive slot retains WAL. Validate consumer.'
        ELSE 'good">Slot state is stable.'
      END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No replication slots present</td></tr>'
  )
FROM pg_replication_slots;

\qecho '</tbody></table></div></div>'

-- S08.4 WAL generation rate
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">WAL Generation Rate</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>WAL Generated (since reset)</td><td class="num">' || pg_size_pretty(wal_bytes::bigint) || '</td><td></td></tr>' ||
  '<tr><td>WAL Throughput</td><td class="num ' ||
  CASE WHEN wal_mb_per_sec > 20 THEN 'crit' WHEN wal_mb_per_sec > 5 THEN 'warn' ELSE 'good' END || '">' ||
  round(wal_mb_per_sec::numeric,2) || ' MB/s</td><td class="' ||
  CASE WHEN wal_mb_per_sec > 20 THEN 'crit">High sustained WAL rate. Validate checkpoints, HOT ratio, and write amplification.'
       WHEN wal_mb_per_sec > 5 THEN 'warn">Moderate WAL rate. Track with S07 and S19.'
       ELSE 'good">WAL generation is in normal range.' END || '</td></tr>' ||
  '<tr><td>Full Page Images Ratio</td><td class="num ' ||
  CASE WHEN fpi_ratio > 15 THEN 'warn' ELSE 'good' END || '">' ||
  round(fpi_ratio::numeric,2) || '%</td><td class="' ||
  CASE WHEN fpi_ratio > 15 THEN 'warn">Elevated FPI ratio. Review checkpoint frequency.'
       ELSE 'good">FPI ratio is normal.' END || '</td></tr>' ||
  '<tr><td>Stats Window</td><td class="num">' || window_text || '</td><td></td></tr>'
FROM (
  SELECT
    w.wal_bytes,
    w.wal_fpi,
    COALESCE(100.0 * w.wal_fpi / NULLIF(w.wal_records,0), 0) AS fpi_ratio,
    COALESCE(w.wal_bytes / NULLIF(extract(epoch FROM (clock_timestamp() - w.stats_reset)),0) / 1024 / 1024, 0) AS wal_mb_per_sec,
    CASE
      WHEN w.stats_reset IS NULL THEN 'unknown'
      ELSE round(extract(epoch FROM (clock_timestamp() - w.stats_reset)) / 3600.0, 2)::text || ' hours'
    END AS window_text
  FROM pg_stat_wal w
) x;

\qecho '</tbody></table></div></div>'

-- S08.5 Backup verification and incremental backup readiness
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Backup &amp; Incremental Readiness</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Observed</th><th>Status</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'
WITH cfg AS (
  SELECT
    current_setting('server_version_num')::int AS vnum,
    COALESCE(current_setting('summarize_wal', true), '(unsupported)') AS summarize_wal,
    COALESCE(current_setting('wal_summary_keep_time', true), '(unsupported)') AS wal_summary_keep_time,
    COALESCE(current_setting('archive_mode', true), '(unsupported)') AS archive_mode,
    COALESCE(current_setting('archive_command', true), '(unset)') AS archive_command
), arch AS (
  SELECT
    COALESCE(archived_count, 0) AS archived_count,
    COALESCE(failed_count, 0) AS failed_count
  FROM pg_stat_archiver
), summary AS (
  SELECT
    CASE
      WHEN to_regclass('pg_catalog.pg_available_wal_summaries') IS NOT NULL
      THEN NULLIF(
        (
          xpath(
            '/row/c/text()',
            query_to_xml(
              'SELECT count(*)::text AS c FROM pg_catalog.pg_available_wal_summaries',
              false,
              true,
              ''
            )
          )
        )[1]::text,
        ''
      )::int
      ELSE NULL
    END AS wal_summary_files
)
SELECT
  '<tr><td>Archive mode</td><td>' || archive_mode || '</td><td class="' ||
  CASE WHEN archive_mode IN ('on','always') THEN 'good">OK' ELSE 'warn">WARN' END ||
  '</td><td>' ||
  CASE WHEN archive_mode IN ('on','always') THEN 'Archive mode is enabled for PITR / backup workflows.' ELSE 'Archive mode is off; PITR and some backup validation workflows are limited.' END ||
  '</td></tr>' ||
  '<tr><td>Archive command</td><td>' || replace(replace(replace(replace(replace(left(archive_command, 120),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td><td class="' ||
  CASE WHEN archive_command IN ('', '(disabled)', '(unset)') THEN 'warn">WARN' ELSE 'good">OK' END ||
  '</td><td>' ||
  CASE WHEN archive_command IN ('', '(disabled)', '(unset)') THEN 'Archive command is not clearly configured.' ELSE 'Archive command is present; validate failures below and test restore separately.' END ||
  '</td></tr>' ||
  '<tr><td>Archiver failure count</td><td>' || failed_count || '</td><td class="' ||
  CASE WHEN failed_count > 0 THEN 'warn">WARN' ELSE 'good">OK' END ||
  '</td><td>' ||
  CASE WHEN failed_count > 0 THEN 'Archive failures exist. Backup-chain confidence is reduced until resolved.' ELSE 'No archive failures recorded in stats.' END ||
  '</td></tr>' ||
  '<tr><td>WAL summarization</td><td>' || summarize_wal || '</td><td class="' ||
  CASE
    WHEN summarize_wal = '(unsupported)' THEN 'warn">N/A'
    WHEN summarize_wal = 'on' THEN 'good">OK'
    ELSE 'warn">WARN'
  END || '</td><td>' ||
  CASE
    WHEN summarize_wal = '(unsupported)' THEN 'This branch does not expose summarize_wal.'
    WHEN summarize_wal = 'on' THEN 'Incremental backup prerequisites are partially in place.'
    ELSE 'Incremental backup readiness is weaker without WAL summaries.'
  END || '</td></tr>' ||
  '<tr><td>WAL summary retention</td><td>' || wal_summary_keep_time || '</td><td class="' ||
  CASE
    WHEN wal_summary_keep_time = '(unsupported)' THEN 'warn">N/A'
    WHEN wal_summary_keep_time IN ('0', '0min', '0s') THEN 'warn">WARN'
    ELSE 'good">OK'
  END || '</td><td>' ||
  CASE
    WHEN wal_summary_keep_time = '(unsupported)' THEN 'This branch does not expose wal_summary_keep_time.'
    WHEN wal_summary_keep_time IN ('0', '0min', '0s') THEN 'WAL summaries may be removed immediately; incremental backup window is fragile.'
    ELSE 'Retention window exists for incremental backup workflows.'
  END || '</td></tr>' ||
  '<tr><td>Available WAL summaries</td><td>' || COALESCE(wal_summary_files::text, 'unknown') || '</td><td class="' ||
  CASE
    WHEN wal_summary_files IS NULL THEN 'warn">LIMITED'
    WHEN wal_summary_files > 0 THEN 'good">OK'
    ELSE 'warn">WARN'
  END || '</td><td>' ||
  CASE
    WHEN wal_summary_files IS NULL THEN 'Cannot inspect WAL summary inventory on this branch / visibility envelope.'
    WHEN wal_summary_files > 0 THEN 'WAL summary files exist; incremental backup chain can be validated further.'
    ELSE 'No WAL summaries are currently visible.'
  END || '</td></tr>' ||
  '<tr><td>Backup verification discipline</td><td>External process</td><td class="warn">VERIFY</td><td>PG360 cannot prove that pg_verifybackup and test restores are part of your runbook. Treat this as an explicit operational check.</td></tr>'
FROM cfg, arch, summary;
\qecho '</tbody></table></div></div>'

-- S08.6 Archive subsystem health
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Archive Subsystem Health</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Archived WAL Count</td><td class="num">' || to_char(archived_count,'FM999,999,999') || '</td><td></td></tr>' ||
  '<tr><td>Archive Failure Count</td><td class="num ' ||
  CASE WHEN failed_count > 0 THEN 'warn' ELSE 'good' END || '">' || to_char(failed_count,'FM999,999,999') ||
  '</td><td class="' || CASE WHEN failed_count > 0 THEN 'warn">Investigate archive_command failures and storage availability.'
                             ELSE 'good">No archive failures recorded.' END || '</td></tr>' ||
  '<tr><td>Failure Ratio</td><td class="num ' ||
  CASE WHEN fail_ratio > 1 THEN 'crit' WHEN fail_ratio > 0.1 THEN 'warn' ELSE 'good' END || '">' ||
  COALESCE(round(fail_ratio::numeric,3)::text, '0') || '%</td><td class="' ||
  CASE WHEN fail_ratio > 1 THEN 'crit">Archive reliability risk. PITR confidence is reduced.'
       WHEN fail_ratio > 0.1 THEN 'warn">Archive reliability needs review.'
       ELSE 'good">Archive reliability is healthy.' END || '</td></tr>' ||
  '<tr><td>Last Archived WAL</td><td>' || COALESCE(last_archived_wal,'N/A') || '</td><td>' ||
  COALESCE(to_char(last_archived_time,'YYYY-MM-DD HH24:MI'),'N/A') || '</td></tr>'
FROM (
  SELECT
    a.archived_count,
    a.failed_count,
    a.last_archived_wal,
    a.last_archived_time,
    100.0 * a.failed_count / NULLIF(a.archived_count + a.failed_count, 0) AS fail_ratio
  FROM pg_stat_archiver a
) s;

\qecho '</tbody></table></div></div>'

-- S08.7 Logical replication failover readiness
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Logical Replication Failover Readiness</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Observed</th><th>Status</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'
WITH cfg AS (
  SELECT
    current_setting('server_version_num')::int AS vnum,
    COALESCE(current_setting('sync_replication_slots', true), '(unsupported)') AS sync_replication_slots,
    COALESCE(current_setting('synchronized_standby_slots', true), '(unsupported)') AS synchronized_standby_slots
), subs AS (
  SELECT
    CASE WHEN to_regclass('pg_catalog.pg_subscription') IS NOT NULL THEN (SELECT COUNT(*)::int FROM pg_subscription) ELSE NULL END AS subscription_count,
    CASE WHEN to_regclass('pg_catalog.pg_subscription') IS NOT NULL AND EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='pg_catalog' AND table_name='pg_subscription' AND column_name='subfailover'
    ) THEN (SELECT COUNT(*)::int FROM pg_subscription WHERE subfailover) ELSE NULL END AS failover_subscriptions
), slots AS (
  SELECT
    COUNT(*) FILTER (WHERE slot_type = 'logical')::int AS logical_slots,
    COUNT(*) FILTER (WHERE slot_type = 'logical' AND NOT active)::int AS inactive_logical_slots
  FROM pg_replication_slots
)
SELECT
  '<tr><td>Logical subscriptions</td><td>' || COALESCE(subscription_count::text, 'unknown') || '</td><td class="' ||
  CASE
    WHEN subscription_count IS NULL THEN 'warn">LIMITED'
    WHEN subscription_count > 0 THEN 'good">PRESENT'
    ELSE 'good">NONE'
  END || '</td><td>' ||
  CASE
    WHEN subscription_count IS NULL THEN 'Cannot inspect pg_subscription with current branch / visibility.'
    WHEN subscription_count > 0 THEN 'Logical replication is configured; failover readiness should be reviewed.'
    ELSE 'No logical subscriptions detected.'
  END || '</td></tr>' ||
  '<tr><td>Failover-enabled subscriptions</td><td>' || COALESCE(failover_subscriptions::text, 'unknown') || '</td><td class="' ||
  CASE
    WHEN failover_subscriptions IS NULL THEN 'warn">LIMITED'
    WHEN failover_subscriptions > 0 THEN 'good">READY'
    WHEN COALESCE(subscription_count,0) > 0 THEN 'warn">REVIEW'
    ELSE 'good">N/A'
  END || '</td><td>' ||
  CASE
    WHEN failover_subscriptions IS NULL THEN 'Branch or visibility does not expose subscription failover metadata.'
    WHEN failover_subscriptions > 0 THEN 'At least one subscription is marked for failover-aware behavior.'
    WHEN COALESCE(subscription_count,0) > 0 THEN 'Subscriptions exist but none are marked failover-aware.'
    ELSE 'No subscriptions to assess.'
  END || '</td></tr>' ||
  '<tr><td>sync_replication_slots</td><td>' || sync_replication_slots || '</td><td class="' ||
  CASE
    WHEN sync_replication_slots = '(unsupported)' THEN 'warn">N/A'
    WHEN sync_replication_slots = 'on' THEN 'good">OK'
    ELSE 'warn">WARN'
  END || '</td><td>' ||
  CASE
    WHEN sync_replication_slots = '(unsupported)' THEN 'Branch does not expose sync_replication_slots.'
    WHEN sync_replication_slots = 'on' THEN 'Standby slot synchronization is enabled.'
    ELSE 'Standby slot synchronization is off; failover-ready logical replication may be incomplete.'
  END || '</td></tr>' ||
  '<tr><td>synchronized_standby_slots</td><td>' || synchronized_standby_slots || '</td><td class="' ||
  CASE
    WHEN synchronized_standby_slots = '(unsupported)' THEN 'warn">N/A'
    WHEN synchronized_standby_slots IN ('', '(none)') THEN 'warn">WARN'
    ELSE 'good">OK'
  END || '</td><td>' ||
  CASE
    WHEN synchronized_standby_slots = '(unsupported)' THEN 'Branch does not expose synchronized_standby_slots.'
    WHEN synchronized_standby_slots IN ('', '(none)') THEN 'No synchronized standby slots are declared.'
    ELSE 'Synchronized standby slots are configured.'
  END || '</td></tr>' ||
  '<tr><td>Logical replication slots</td><td>' || logical_slots || ' total / ' || inactive_logical_slots || ' inactive</td><td class="' ||
  CASE WHEN inactive_logical_slots > 0 THEN 'warn">WARN' ELSE 'good">OK' END ||
  '</td><td>' ||
  CASE WHEN inactive_logical_slots > 0 THEN 'Inactive logical slots can hold WAL and complicate failover readiness.' ELSE 'Logical slot activity looks stable.' END ||
  '</td></tr>'
FROM cfg, subs, slots;
\qecho '</tbody></table></div></div>'

-- S08.8 Synchronous replication posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Synchronous Replication Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>synchronous_commit</td><td>' || synchronous_commit || '</td><td class="' ||
  CASE WHEN synchronous_commit IN ('on','remote_write','remote_apply') THEN 'good">Synchronous durability requested.'
       ELSE 'warn">Asynchronous commit mode. Better latency, lower durability guarantees.' END || '</td></tr>' ||
  '<tr><td>synchronous_standby_names</td><td>' || sync_names || '</td><td class="' ||
  CASE WHEN sync_names IN ('', 'off') THEN 'warn">No explicit synchronous standby list.'
       ELSE 'good">Synchronous standby policy is configured.' END || '</td></tr>' ||
  '<tr><td>Connected Standbys</td><td class="num">' || standby_count || '</td><td></td></tr>' ||
  '<tr><td>Standbys in sync state</td><td class="num ' ||
  CASE WHEN sync_count = 0 AND synchronous_commit IN ('on','remote_write','remote_apply') THEN 'warn' ELSE 'good' END || '">' ||
  sync_count || '</td><td class="' ||
  CASE WHEN sync_count = 0 AND synchronous_commit IN ('on','remote_write','remote_apply') THEN 'warn">No standby currently in sync state.'
       ELSE 'good">Synchronous state is observable.' END || '</td></tr>'
FROM (
  SELECT
    current_setting('synchronous_commit') AS synchronous_commit,
    COALESCE(NULLIF(current_setting('synchronous_standby_names', true), ''), 'off') AS sync_names,
    (SELECT COUNT(*) FROM pg_stat_replication) AS standby_count,
    (SELECT COUNT(*) FROM pg_stat_replication WHERE sync_state = 'sync') AS sync_count
) r;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S09: CONNECTIONS & POOLING
-- =============================================================================
\qecho '<div class="section" id="s09">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Connections &amp; Pooling</div>'
\qecho '    <div class="section-desc">Connection utilization, age distribution, pooler recommendations.</div>'
\qecho '  </div>'
\qecho '</div>'

SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN used_pct > 80 THEN 'critical' WHEN used_pct > 60 THEN 'warning' ELSE 'good' END || '">' ||
  '<div class="card-label">Connections Used / Max</div>' ||
  '<div class="card-value">' || active_conn || ' / ' || max_conn || '</div>' ||
  '<div class="card-sub">' || round(used_pct::numeric,1) || '% utilized' ||
  CASE WHEN used_pct > 80 THEN '  Add pgBouncer!' ELSE '' END ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Connections by State</div>' ||
  '<div class="card-value">' ||
  'Active: ' || active_c || ' | Idle: ' || idle_c || ' | IdleXact: ' || idle_tx_c ||
  '</div></div>' ||
  '</div>'
FROM (
  SELECT
    COUNT(*) AS active_conn,
    (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS max_conn,
    COUNT(*) * 100.0 / (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS used_pct,
    COUNT(*) FILTER (WHERE state = 'active') AS active_c,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_c,
    COUNT(*) FILTER (WHERE state LIKE 'idle in%') AS idle_tx_c
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
) c;

-- S09.1 Connections by user/db/app
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Connections by Application / User</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Application</th><th>User</th><th>Database</th><th>Count</th><th>Active</th><th>Idle</th><th>Idle In Xact</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(COALESCE(application_name,'(unknown)'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(COALESCE(datname,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || total || '</td>' ||
  '<td class="num good">' || active || '</td>' ||
  '<td class="num">' || idle || '</td>' ||
  '<td class="num ' || CASE WHEN idle_xact > 3 THEN 'warn' ELSE '' END || '">' || idle_xact || '</td>' ||
  '</tr>'
FROM (
  SELECT
    application_name, usename, datname,
    COUNT(*)                                       AS total,
    COUNT(*) FILTER (WHERE state = 'active')       AS active,
    COUNT(*) FILTER (WHERE state = 'idle')         AS idle,
    COUNT(*) FILTER (WHERE state LIKE 'idle in%')  AS idle_xact
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
  GROUP BY application_name, usename, datname
) t
ORDER BY total DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S09.2 pgBouncer missing recommendation
SELECT
  CASE
    WHEN total_conn > 100 AND pgb_exists = 0
    THEN '<div class="finding high"><div class="finding-header">' ||
         '' ||
         '<span class="finding-title">No Connection Pooler Detected (' || total_conn || ' connections)</span>' ||
         '<span class="severity-pill pill-high">HIGH</span></div>' ||
         '<div class="finding-body">Each PostgreSQL connection uses 5-10MB RAM and has overhead. ' ||
         'With ' || total_conn || ' connections, consider deploying pgBouncer in transaction mode.</div>' ||
         '<div class="fix-label">RECOMMENDATION</div>' ||
         '<div class="finding-fix">-- Deploy pgBouncer in front of PostgreSQL&#10;' ||
         '-- pgbouncer.ini:&#10;' ||
         '-- pool_mode = transaction&#10;' ||
         '-- max_client_conn = 1000&#10;' ||
         '-- default_pool_size = 20</div></div>'
    ELSE ''
  END
FROM (
  SELECT COUNT(*) AS total_conn,
    (SELECT COUNT(*) FROM pg_stat_activity WHERE application_name ILIKE '%pgbouncer%') AS pgb_exists
  FROM pg_stat_activity WHERE pid <> pg_backend_pid()
) c;

-- S09.3 Connection churn and age distribution
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Connection Churn &amp; Session Age</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>New Sessions (last 1 hour)</td><td class="num ' ||
  CASE WHEN new_last_hour > 500 THEN 'crit' WHEN new_last_hour > 150 THEN 'warn' ELSE 'good' END || '">' ||
  to_char(new_last_hour, 'FM999,999,999') || '</td><td class="' ||
  CASE WHEN new_last_hour > 500 THEN 'crit">High churn. Validate pooler configuration and app reconnect loops.'
       WHEN new_last_hour > 150 THEN 'warn">Moderate churn. Pooling may reduce connection overhead.'
       ELSE 'good">Connection churn is low.' END || '</td></tr>' ||
  '<tr><td>Sessions Older Than 24h</td><td class="num ' ||
  CASE WHEN older_24h > 20 THEN 'warn' ELSE 'good' END || '">' || older_24h || '</td><td></td></tr>' ||
  '<tr><td>Idle In Transaction Sessions</td><td class="num ' ||
  CASE WHEN idle_in_xact > 0 THEN 'warn' ELSE 'good' END || '">' || idle_in_xact || '</td><td class="' ||
  CASE WHEN idle_in_xact > 0 THEN 'warn">Open idle transactions block vacuum and can increase bloat.'
       ELSE 'good">No idle-in-transaction sessions.' END || '</td></tr>' ||
  '<tr><td>Empty application_name Sessions</td><td class="num ' ||
  CASE WHEN unknown_app_pct > 30 THEN 'warn' ELSE 'good' END || '">' ||
  round(unknown_app_pct::numeric,1) || '%</td><td class="' ||
  CASE WHEN unknown_app_pct > 30 THEN 'warn">Set application_name in client connections for better attribution.'
       ELSE 'good">Application attribution coverage is good.' END || '</td></tr>'
FROM (
  SELECT
    COUNT(*) FILTER (WHERE backend_start >= now() - interval '1 hour') AS new_last_hour,
    COUNT(*) FILTER (WHERE backend_start <= now() - interval '24 hours') AS older_24h,
    COUNT(*) FILTER (WHERE state LIKE 'idle in transaction%') AS idle_in_xact,
    100.0 * COUNT(*) FILTER (WHERE COALESCE(application_name, '') = '') / NULLIF(COUNT(*), 0) AS unknown_app_pct
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
) x;

\qecho '</tbody></table></div></div>'

-- S09.4 Long-lived and idle-in-transaction sessions
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Long-Lived &amp; Idle-In-Tx Sessions</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>User</th><th>Application</th><th>State</th><th>Session Age</th><th>Transaction Age</th><th>Wait Event</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(application_name,'(unknown)'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN state LIKE 'idle in transaction%' THEN 'warn"> ' ELSE '"> ' END || COALESCE(state,'') || '</td>' ||
      '<td class="num">' || to_char(session_age, 'DD "d" HH24:MI:SS') || '</td>' ||
      '<td class="num ' || CASE WHEN xact_age > interval '15 minutes' THEN 'warn' ELSE '' END || '">' ||
      COALESCE(to_char(xact_age, 'DD "d" HH24:MI:SS'), '-') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(wait_event_type || ':' || wait_event, ''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="7" class="table-empty"> No long-lived or idle-in-transaction sessions requiring action</td></tr>'
  )
FROM (
  SELECT
    pid,
    usename,
    application_name,
    state,
    clock_timestamp() - backend_start AS session_age,
    CASE WHEN xact_start IS NOT NULL THEN clock_timestamp() - xact_start ELSE NULL END AS xact_age,
    wait_event_type,
    wait_event
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
    AND (
      backend_start <= now() - interval '4 hours'
      OR (state LIKE 'idle in transaction%' AND state_change <= now() - interval '5 minutes')
    )
  ORDER BY (clock_timestamp() - backend_start) DESC
  LIMIT 20
) s;

\qecho '</tbody></table></div></div>'

-- S09.5 Connection usage by role
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Connection Usage by Role</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Role</th><th>Connections</th><th>Share of Active Connections</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,'(unknown)'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || conn_count || '</td>' ||
      '<td class="num ' || CASE WHEN conn_share > 60 THEN 'warn' ELSE '' END || '">' || round(conn_share::numeric,1) || '%</td>' ||
      '<td class="' || CASE WHEN conn_share > 60 THEN 'warn">Single role dominates connections. Review pool partitioning.'
                          ELSE 'good">Distribution is acceptable.' END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> No role-level connection activity found</td></tr>'
  )
FROM (
  SELECT
    usename,
    COUNT(*) AS conn_count,
    100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid()), 0) AS conn_share
  FROM pg_stat_activity
  WHERE pid <> pg_backend_pid()
  GROUP BY usename
  ORDER BY COUNT(*) DESC
  LIMIT 20
) r;

\qecho '</tbody></table></div></div>'

-- S09.6 Pooler mode advisory
SELECT
  CASE
    WHEN pgbouncer_conn > 0
    THEN '<div class="finding good"><div class="finding-header">' ||
         '<span class="finding-title">Connection pooler detected</span><span class="severity-pill pill-good">OK</span></div>' ||
         '<div class="finding-body">pgBouncer signatures are visible. Validate pool mode: transaction pooling for OLTP, session pooling only when required by session state.</div></div>'
    ELSE '<div class="finding warn"><div class="finding-header">' ||
         '<span class="finding-title">Pooler not detected in session metadata</span><span class="severity-pill pill-warn">MEDIUM</span></div>' ||
         '<div class="finding-body">If connection churn or utilization is high, deploy pgBouncer and cap direct application connections.</div></div>'
  END
FROM (
  SELECT COUNT(*) AS pgbouncer_conn
  FROM pg_stat_activity
  WHERE application_name ILIKE '%pgbouncer%'
) p;

\qecho '</div>'

-- =============================================================================
-- SECTION S10: VACUUM & MAINTENANCE
-- =============================================================================
\qecho '<div class="section" id="s10">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Vacuum &amp; Maintenance</div>'
\qecho '    <div class="section-desc">Autovacuum health, pending vacuum/analyze, wraparound risk.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S10.1 Tables needing vacuum
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables Most in Need of Vacuum</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Table</th><th>Dead Tuples</th><th>Live Tuples</th><th>Dead%</th>'
\qecho '<th>Last Autovacuum</th><th>Modifications Since Analyze</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname||'.'||relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num crit">' || to_char(n_dead_tup,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' || CASE WHEN dead_pct > 20 THEN 'crit' WHEN dead_pct > 10 THEN 'warn' ELSE 'good' END || '">' ||
  round(dead_pct::numeric,1) || '%</td>' ||
  '<td>' || COALESCE(to_char(last_autovacuum,'YYYY-MM-DD HH24:MI'),'<span class="warn">Never</span>') || '</td>' ||
  '<td class="num ' || CASE WHEN n_mod_since_analyze > 100000 THEN 'warn' ELSE '' END || '">' ||
  to_char(n_mod_since_analyze,'FM999,999,999') || '</td>' ||
  '</tr>'
FROM (
  SELECT *,
    n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup,0) * 100 AS dead_pct
  FROM pg_stat_user_tables
  WHERE n_dead_tup > 1000
) t
ORDER BY n_dead_tup DESC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S10.2 Vacuum debt score
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Vacuum Debt Score</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Table</th><th>Dead Tuples</th><th>Vacuum Threshold</th><th>Debt Score</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(table_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(n_dead_tup,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(vac_threshold::bigint,'FM999,999,999') || '</td>' ||
      '<td class="num ' ||
      CASE WHEN debt_score >= 2 THEN 'crit' WHEN debt_score >= 1 THEN 'warn' ELSE 'good' END || '">' ||
      round(debt_score::numeric,2) || '</td>' ||
      '<td class="' ||
      CASE WHEN debt_score >= 2 THEN 'crit">Autovacuum is behind for this table.'
           WHEN debt_score >= 1 THEN 'warn">Near autovacuum trigger threshold.'
           ELSE 'good">Vacuum debt is controlled.' END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No tables currently near vacuum threshold</td></tr>'
  )
FROM (
  SELECT
    s.schemaname || '.' || s.relname AS table_name,
    s.n_dead_tup,
    (cfg.vacuum_threshold + cfg.vacuum_scale_factor * GREATEST(s.n_live_tup, 0))::numeric AS vac_threshold,
    s.n_dead_tup::numeric / NULLIF((cfg.vacuum_threshold + cfg.vacuum_scale_factor * GREATEST(s.n_live_tup, 0))::numeric, 0) AS debt_score
  FROM pg_stat_user_tables s
  CROSS JOIN (
    SELECT
      current_setting('autovacuum_vacuum_threshold')::numeric AS vacuum_threshold,
      current_setting('autovacuum_vacuum_scale_factor')::numeric AS vacuum_scale_factor
  ) cfg
  WHERE s.n_live_tup > 0
    AND s.n_dead_tup > 0
  ORDER BY s.n_dead_tup::numeric / NULLIF((cfg.vacuum_threshold + cfg.vacuum_scale_factor * GREATEST(s.n_live_tup, 0))::numeric, 0) DESC
  LIMIT 20
) d
WHERE debt_score >= 0.5;

\qecho '</tbody></table></div></div>'

-- S10.3 Wraparound risk summary
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Transaction ID Wraparound Risk</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Scope</th><th>Current Age</th><th>% of Freeze Limit</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Database frozen XID age</td><td class="num">' || db_age || '</td><td class="num ' ||
  CASE WHEN db_pct > 80 THEN 'crit' WHEN db_pct > 60 THEN 'warn' ELSE 'good' END || '">' ||
  round(db_pct::numeric,2) || '%</td><td class="' ||
  CASE WHEN db_pct > 80 THEN 'crit">Critical. Immediate vacuum planning required.'
       WHEN db_pct > 60 THEN 'warn">Elevated. Increase autovacuum throughput.'
       ELSE 'good">Wraparound age is controlled.' END || '</td></tr>' ||
  '<tr><td>Oldest table frozen XID age</td><td class="num">' || max_rel_age || '</td><td class="num ' ||
  CASE WHEN rel_pct > 80 THEN 'crit' WHEN rel_pct > 60 THEN 'warn' ELSE 'good' END || '">' ||
  round(rel_pct::numeric,2) || '%</td><td class="' ||
  CASE WHEN rel_pct > 80 THEN 'crit">Table-level freeze debt is high.'
       WHEN rel_pct > 60 THEN 'warn">Review top freeze-age tables below.'
       ELSE 'good">Table freeze age is healthy.' END || '</td></tr>'
FROM (
  SELECT
    age(d.datfrozenxid) AS db_age,
    100.0 * age(d.datfrozenxid) / NULLIF(current_setting('autovacuum_freeze_max_age')::numeric, 0) AS db_pct,
    COALESCE((SELECT max(age(c.relfrozenxid)) FROM pg_class c WHERE c.relkind = 'r'), 0) AS max_rel_age,
    100.0 * COALESCE((SELECT max(age(c.relfrozenxid)) FROM pg_class c WHERE c.relkind = 'r'), 0) /
      NULLIF(current_setting('autovacuum_freeze_max_age')::numeric, 0) AS rel_pct
  FROM pg_database d
  WHERE d.datname = current_database()
) w;

\qecho '</tbody></table></div></div>'

-- S10.4 Tables with autovacuum disabled
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables with autovacuum_enabled = off</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Reloptions</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(array_to_string(c.reloptions, ', '),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td class="crit">Autovacuum disabled. Table can accumulate dead tuples and freeze debt.</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> No tables with autovacuum disabled</td></tr>'
  )
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND EXISTS (
    SELECT 1
    FROM unnest(COALESCE(c.reloptions, ARRAY[]::text[])) AS opt
    WHERE opt ILIKE 'autovacuum_enabled=off'
  );

\qecho '</tbody></table></div></div>'

-- S10.5 Autovacuum parameter posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Autovacuum Parameter Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || name || '</td><td class="num">' || setting || COALESCE(' ' || unit, '') || '</td><td class="' ||
      CASE
        WHEN name = 'autovacuum_max_workers' AND setting::int < 3 THEN 'warn">Low for write-heavy systems.'
        WHEN name = 'autovacuum_naptime' AND setting::int > 60 THEN 'warn">Slow revisit interval can delay cleanup.'
        WHEN name = 'autovacuum_vacuum_scale_factor' AND setting::numeric > 0.2 THEN 'warn">High scale factor for large tables.'
        WHEN name = 'autovacuum_analyze_scale_factor' AND setting::numeric > 0.1 THEN 'warn">Analyze may lag on large tables.'
        WHEN name = 'autovacuum_freeze_max_age' AND setting::int < 100000000 THEN 'warn">Unusually low. Verify freeze cadence.'
        ELSE 'good">Within common operational range.'
      END || '</td></tr>',
      ''
      ORDER BY name
    ),
    '<tr><td colspan="3" class="table-empty"> Autovacuum parameters unavailable</td></tr>'
  )
FROM pg_settings
WHERE name IN (
  'autovacuum',
  'autovacuum_max_workers',
  'autovacuum_naptime',
  'autovacuum_vacuum_scale_factor',
  'autovacuum_vacuum_threshold',
  'autovacuum_analyze_scale_factor',
  'autovacuum_analyze_threshold',
  'autovacuum_freeze_max_age'
);

\qecho '</tbody></table></div></div>'

-- S10.6 Progress reporting coverage
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Maintenance Progress Reporting</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>View / Operation</th><th>Status</th><th>Current Activity</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'
WITH progress AS (
  SELECT
    CASE WHEN to_regclass('pg_catalog.pg_stat_progress_vacuum') IS NOT NULL THEN (SELECT COUNT(*)::int FROM pg_stat_progress_vacuum) ELSE NULL END AS vacuum_cnt,
    CASE WHEN to_regclass('pg_catalog.pg_stat_progress_create_index') IS NOT NULL THEN (SELECT COUNT(*)::int FROM pg_stat_progress_create_index) ELSE NULL END AS create_index_cnt,
    CASE WHEN to_regclass('pg_catalog.pg_stat_progress_analyze') IS NOT NULL THEN (SELECT COUNT(*)::int FROM pg_stat_progress_analyze) ELSE NULL END AS analyze_cnt,
    CASE WHEN to_regclass('pg_catalog.pg_stat_progress_basebackup') IS NOT NULL THEN (SELECT COUNT(*)::int FROM pg_stat_progress_basebackup) ELSE NULL END AS basebackup_cnt
)
SELECT
  '<tr><td>VACUUM progress</td><td class="' ||
  CASE WHEN vacuum_cnt IS NULL THEN 'warn">LIMITED' WHEN vacuum_cnt > 0 THEN 'good">ACTIVE' ELSE 'good">AVAILABLE' END ||
  '</td><td>' || COALESCE(vacuum_cnt::text, 'unknown') || '</td><td>' ||
  CASE WHEN vacuum_cnt IS NULL THEN 'View unavailable on this branch / visibility envelope.' WHEN vacuum_cnt > 0 THEN 'Active vacuum operations are visible.' ELSE 'View is present; no active vacuum currently.' END ||
  '</td></tr>' ||
  '<tr><td>CREATE INDEX progress</td><td class="' ||
  CASE WHEN create_index_cnt IS NULL THEN 'warn">LIMITED' WHEN create_index_cnt > 0 THEN 'warn">ACTIVE' ELSE 'good">AVAILABLE' END ||
  '</td><td>' || COALESCE(create_index_cnt::text, 'unknown') || '</td><td>' ||
  CASE WHEN create_index_cnt IS NULL THEN 'View unavailable on this branch / visibility envelope.' WHEN create_index_cnt > 0 THEN 'Index builds are currently running; track maintenance windows here.' ELSE 'View is present; no active index build currently.' END ||
  '</td></tr>' ||
  '<tr><td>ANALYZE progress</td><td class="' ||
  CASE WHEN analyze_cnt IS NULL THEN 'warn">LIMITED' WHEN analyze_cnt > 0 THEN 'good">ACTIVE' ELSE 'good">AVAILABLE' END ||
  '</td><td>' || COALESCE(analyze_cnt::text, 'unknown') || '</td><td>' ||
  CASE WHEN analyze_cnt IS NULL THEN 'View unavailable on this branch / visibility envelope.' WHEN analyze_cnt > 0 THEN 'Analyze activity is visible right now.' ELSE 'View is present; no active analyze currently.' END ||
  '</td></tr>' ||
  '<tr><td>BASEBACKUP progress</td><td class="' ||
  CASE WHEN basebackup_cnt IS NULL THEN 'warn">LIMITED' WHEN basebackup_cnt > 0 THEN 'warn">ACTIVE' ELSE 'good">AVAILABLE' END ||
  '</td><td>' || COALESCE(basebackup_cnt::text, 'unknown') || '</td><td>' ||
  CASE WHEN basebackup_cnt IS NULL THEN 'View unavailable on this branch / visibility envelope.' WHEN basebackup_cnt > 0 THEN 'A base backup is active; monitor duration and throughput.' ELSE 'View is present; no active base backup currently.' END ||
  '</td></tr>'
FROM progress;
\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S11: WORKLOAD PROFILE & CONFIGURATION TUNING
-- =============================================================================
\qecho '<div class="section" id="s11">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Workload Profile &amp; Configuration Tuning</div>'
\qecho '    <div class="section-desc">Detected workload type with tailored configuration recommendations.</div>'
\qecho '  </div>'
\qecho '</div>'

-- Detect workload and generate recommendations
SELECT
  '<div class="finding ' ||
  CASE workload
    WHEN 'WRITE-HEAVY' THEN 'high'
    WHEN 'ANALYTICAL'  THEN 'high'
    ELSE 'info'
  END ||
  '">' ||
  '<div class="finding-header">' ||
  '' ||
  '<span class="finding-title">Detected Workload: ' || workload || '</span>' ||
  '<span class="severity-pill pill-info">' || workload || '</span>' ||
  '</div>' ||
  '<div class="finding-body">' ||
  description ||
  '</div></div>' ||

  '<div class="table-wrap"><table class="pg360"><thead><tr>' ||
  '<th>Parameter</th><th>Current Value</th><th>Recommended Value</th><th>Impact</th>' ||
  '</tr></thead><tbody>' ||
  recommendations ||
  '</tbody></table></div>'

FROM (
  WITH w AS (
    SELECT
      CASE
        WHEN (tup_fetched::numeric / NULLIF(tup_inserted+tup_updated+tup_deleted,0)) > 10
          THEN 'READ-HEAVY'
        WHEN (tup_inserted+tup_updated+tup_deleted)::numeric / NULLIF(tup_fetched,0) > 3
          THEN 'WRITE-HEAVY'
        WHEN temp_files > 100 THEN 'ANALYTICAL'
        ELSE 'MIXED-OLTP'
      END AS workload,
      tup_fetched, tup_inserted, tup_updated, tup_deleted, temp_files
    FROM pg_stat_database WHERE datname = current_database()
  ),
  current_settings AS (
    SELECT
      MAX(CASE WHEN name='shared_buffers' THEN setting END) AS shared_buffers,
      MAX(CASE WHEN name='work_mem' THEN setting END) AS work_mem,
      MAX(CASE WHEN name='checkpoint_completion_target' THEN setting END) AS cct,
      MAX(CASE WHEN name='max_wal_size' THEN setting END) AS max_wal_size,
      MAX(CASE WHEN name='autovacuum_max_workers' THEN setting END) AS av_workers,
      MAX(CASE WHEN name='max_parallel_workers_per_gather' THEN setting END) AS par_workers
    FROM pg_settings
    WHERE name IN ('shared_buffers','work_mem','checkpoint_completion_target','max_wal_size','autovacuum_max_workers','max_parallel_workers_per_gather')
  )
  SELECT
    w.workload,
    CASE w.workload
      WHEN 'READ-HEAVY'  THEN 'High read/write ratio detected. Optimize for buffer cache, effective_cache_size, and read parallelism.'
      WHEN 'WRITE-HEAVY' THEN 'High write throughput detected. Optimize WAL, checkpoints, and autovacuum aggressiveness.'
      WHEN 'ANALYTICAL'  THEN 'Significant temp file spills detected. Queries need more work_mem and parallel workers.'
      ELSE 'Balanced OLTP workload. Standard tuning applies.'
    END AS description,
    (
      '<tr><td>shared_buffers</td><td class="num">' || pg_size_pretty(cs.shared_buffers::bigint * 8192) ||
      '</td><td class="good">' || CASE w.workload WHEN 'READ-HEAVY' THEN '30-40% RAM' ELSE '25% RAM' END ||
      '</td><td>Primary data cache</td></tr>' ||

      '<tr><td>work_mem</td><td class="num">' || pg_size_pretty(cs.work_mem::bigint * 1024) ||
      '</td><td class="' || CASE WHEN cs.work_mem::int < 4096 THEN 'crit' ELSE 'warn' END || '">' ||
      CASE w.workload WHEN 'ANALYTICAL' THEN '256MB - 1GB' WHEN 'WRITE-HEAVY' THEN '8-16MB' ELSE '32-64MB' END ||
      '</td><td>Sort &amp; hash memory (per operation)</td></tr>' ||

      '<tr><td>checkpoint_completion_target</td><td class="num">' || cs.cct ||
      '</td><td class="' || CASE WHEN cs.cct::numeric < 0.7 THEN 'warn' ELSE 'good' END || '">' ||
      CASE w.workload WHEN 'WRITE-HEAVY' THEN '0.9' ELSE '0.7-0.9' END ||
      '</td><td>Spread checkpoint I/O</td></tr>' ||

      '<tr><td>autovacuum_max_workers</td><td class="num">' || cs.av_workers ||
      '</td><td class="' || CASE WHEN cs.av_workers::int < 3 THEN 'warn' ELSE 'good' END || '">' ||
      CASE w.workload WHEN 'WRITE-HEAVY' THEN '6-10' ELSE '3-5' END ||
      '</td><td>Parallel vacuum workers</td></tr>' ||

      '<tr><td>max_parallel_workers_per_gather</td><td class="num">' || cs.par_workers ||
      '</td><td class="' || CASE WHEN cs.par_workers::int < 2 AND w.workload = 'ANALYTICAL' THEN 'crit' ELSE 'good' END || '">' ||
      CASE w.workload WHEN 'ANALYTICAL' THEN '4-8' WHEN 'WRITE-HEAVY' THEN '0-1' ELSE '2-4' END ||
      '</td><td>Parallel query workers</td></tr>'
    ) AS recommendations
  FROM w, current_settings cs
) config_analysis;

-- S11.2 Workload evidence summary
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload Evidence Summary</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Read/Write Tuple Ratio</td><td class="num ' ||
  CASE WHEN rw_ratio > 10 THEN 'good' WHEN rw_ratio < 1 THEN 'warn' ELSE '' END || '">' ||
  round(rw_ratio::numeric,2) || '</td><td>' ||
  CASE WHEN rw_ratio > 10 THEN 'Read-dominant workload.'
       WHEN rw_ratio < 1 THEN 'Write-dominant workload.'
       ELSE 'Mixed transactional pattern.' END || '</td></tr>' ||
  '<tr><td>Rollback Ratio</td><td class="num ' ||
  CASE WHEN rollback_pct > 5 THEN 'warn' ELSE 'good' END || '">' ||
  round(rollback_pct::numeric,2) || '%</td><td>' ||
  CASE WHEN rollback_pct > 5 THEN 'Elevated rollback activity. Review application retry behavior and constraint errors.'
       ELSE 'Rollback ratio is stable.' END || '</td></tr>' ||
  '<tr><td>Temp Spill per Transaction</td><td class="num ' ||
  CASE WHEN temp_kb_per_xact > 512 THEN 'warn' ELSE 'good' END || '">' ||
  round(temp_kb_per_xact::numeric,1) || ' KB</td><td>' ||
  CASE WHEN temp_kb_per_xact > 512 THEN 'Spill-heavy profile. Review S02 temp I/O and work_mem.'
       ELSE 'Temp spill pressure is limited.' END || '</td></tr>' ||
  '<tr><td>Buffer Cache Hit Ratio</td><td class="num ' ||
  CASE WHEN cache_hit_pct < 95 THEN 'warn' ELSE 'good' END || '">' ||
  round(cache_hit_pct::numeric,2) || '%</td><td>' ||
  CASE WHEN cache_hit_pct < 95 THEN 'Lower cache hit ratio. Correlate with S07 physical read hot set.'
       ELSE 'Cache hit ratio is healthy.' END || '</td></tr>' ||
  '<tr><td>Forced Checkpoint Ratio</td><td class="num ' ||
  CASE WHEN forced_ckpt_pct > 15 THEN 'warn' ELSE 'good' END || '">' ||
  round(forced_ckpt_pct::numeric,2) || '%</td><td>' ||
  CASE WHEN forced_ckpt_pct > 15 THEN 'Checkpoint pressure detected. Tune max_wal_size and checkpoint cadence.'
       ELSE 'Checkpoint pressure is low.' END || '</td></tr>' ||
  '<tr><td>WAL Throughput</td><td class="num ' ||
  CASE WHEN wal_mb_per_sec > 20 THEN 'crit' WHEN wal_mb_per_sec > 5 THEN 'warn' ELSE 'good' END || '">' ||
  round(wal_mb_per_sec::numeric,2) || ' MB/s</td><td>' ||
  CASE WHEN wal_mb_per_sec > 20 THEN 'High WAL generation. Validate HOT ratio, index write overhead, and checkpoint pressure.'
       WHEN wal_mb_per_sec > 5 THEN 'Moderate WAL generation. Track trend with S08.'
       ELSE 'WAL generation is controlled.' END || '</td></tr>'
FROM (
  SELECT
    d.tup_fetched::numeric / NULLIF((d.tup_inserted + d.tup_updated + d.tup_deleted)::numeric, 0) AS rw_ratio,
    100.0 * d.xact_rollback / NULLIF(d.xact_commit + d.xact_rollback, 0) AS rollback_pct,
    d.temp_bytes / NULLIF((d.xact_commit + d.xact_rollback)::numeric, 0) / 1024 AS temp_kb_per_xact,
    100.0 * d.blks_hit / NULLIF(d.blks_hit + d.blks_read, 0) AS cache_hit_pct,
    100.0 * c.num_requested / NULLIF(c.num_timed + c.num_requested, 0) AS forced_ckpt_pct,
    w.wal_bytes / NULLIF(extract(epoch FROM (clock_timestamp() - w.stats_reset)), 0) / 1024 / 1024 AS wal_mb_per_sec
  FROM pg_stat_database d
  CROSS JOIN pg_stat_checkpointer c
  CROSS JOIN pg_stat_wal w
  WHERE d.datname = current_database()
) e;

\qecho '</tbody></table></div></div>'

-- S11.3 Parameter action matrix
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Parameter Action Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Direction</th><th>Reason</th><th>Change Safety</th><th>Evidence Source</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || param || '</td>' ||
      '<td>' || direction || '</td>' ||
      '<td>' || reason || '</td>' ||
      '<td class="' || CASE WHEN safety = 'SAFE NOW' THEN 'good">SAFE NOW' ELSE 'warn">LOAD TEST FIRST' END || '</td>' ||
      '<td>' || source_module || '</td>' ||
      '</tr>',
      ''
      ORDER BY sort_order
    ),
    '<tr><td colspan="5" class="table-empty"> No parameter recommendations produced</td></tr>'
  )
FROM (
  SELECT 1 AS sort_order, 'log_lock_waits' AS param, 'ensure = on' AS direction,
         'Low risk observability improvement for lock diagnostics.' AS reason,
         'SAFE NOW' AS safety, 'S03, S04' AS source_module
  UNION ALL
  SELECT 2, 'log_min_duration_statement', 'set threshold per SLA',
         'Captures slow SQL for regression analysis without full statement logging.',
         'SAFE NOW', 'S02'
  UNION ALL
  SELECT 3, 'autovacuum_max_workers', 'increase if vacuum debt is high',
         'Reduces dead tuple backlog and freeze debt when S10 debt score is elevated.',
         CASE WHEN current_setting('autovacuum_max_workers')::int < 3 THEN 'LOAD TEST FIRST' ELSE 'SAFE NOW' END,
         'S10'
  UNION ALL
  SELECT 4, 'autovacuum_vacuum_scale_factor', 'decrease for large write-heavy tables',
         'Triggers vacuum earlier on large relations; can increase background I/O.',
         'LOAD TEST FIRST', 'S10'
  UNION ALL
  SELECT 5, 'max_wal_size', 'increase when forced checkpoints are frequent',
         'Reduces checkpoint storms and write bursts.',
         'LOAD TEST FIRST', 'S07, S08'
  UNION ALL
  SELECT 6, 'checkpoint_completion_target', 'move toward 0.9 for write-heavy patterns',
         'Spreads checkpoint writes; too aggressive can affect latency if I/O is constrained.',
         'LOAD TEST FIRST', 'S07, S08'
  UNION ALL
  SELECT 7, 'work_mem', 'raise selectively for spill-heavy workloads',
         'Global increases can cause memory pressure under concurrency.',
         'LOAD TEST FIRST', 'S02, S11'
  UNION ALL
  SELECT 8, 'effective_cache_size', 'align to OS cache + shared_buffers',
         'Improves planner decisions for index usage.',
         'SAFE NOW', 'S07, S11'
) actions;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S12: SECURITY AUDIT
-- =============================================================================
\qecho '<div class="section" id="s12">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Security Audit</div>'
\qecho '    <div class="section-desc">Role privileges, superusers, public schema exposure, SSL, RLS gaps. Password hashes are NEVER shown.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S12.1 Superuser accounts
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Superuser Accounts</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Role</th><th>Can Login</th><th>Connection Limit</th><th>Valid Until</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td class="crit">' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND rolname = current_user THEN :'pg360_redaction_token' ELSE rolname END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="' || CASE WHEN rolcanlogin THEN 'crit"> YES  Can login as superuser' ELSE 'good">No (role only)' END || '</td>' ||
  '<td class="num">' || CASE WHEN rolconnlimit = -1 THEN 'Unlimited' ELSE rolconnlimit::text END || '</td>' ||
  '<td>' || COALESCE(to_char(rolvaliduntil,'YYYY-MM-DD'),'No expiry') || '</td>' ||
  '<td class="' ||
  CASE WHEN rolcanlogin THEN 'crit"> Active superuser login  restrict to maintenance only'
       ELSE 'warn"> Superuser role  ensure needed' END ||
  '</td></tr>'
FROM pg_roles
WHERE rolsuper = true
ORDER BY rolcanlogin DESC, rolname;

\qecho '</tbody></table></div></div>'

-- S12.2 Roles with login (all active users)  SECURITY: NO password shown
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Login Roles</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Role</th><th>Superuser</th><th>Create DB</th><th>Replication</th><th>Bypass RLS</th><th>Valid Until</th><th>Password</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND rolname = current_user THEN :'pg360_redaction_token' ELSE rolname END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="' || CASE WHEN rolsuper THEN 'crit"> YES' ELSE 'good">No' END || '</td>' ||
  '<td class="' || CASE WHEN rolcreatedb THEN 'warn">Yes' ELSE '">No' END || '</td>' ||
  '<td class="' || CASE WHEN rolreplication THEN 'warn">Yes' ELSE '">No' END || '</td>' ||
  '<td class="' || CASE WHEN rolbypassrls THEN 'crit"> YES' ELSE '">No' END || '</td>' ||
  '<td>' || COALESCE(to_char(rolvaliduntil,'YYYY-MM-DD'),'No expiry') || '</td>' ||
  -- SECURITY: NEVER show password hash
  '<td class="warn">***REDACTED***</td>' ||
  '</tr>'
FROM pg_roles
WHERE rolcanlogin = true
ORDER BY rolsuper DESC, rolname;

\qecho '</tbody></table></div></div>'

-- S12.3 Security definer functions without search_path (privilege escalation risk)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">SECURITY DEFINER search_path Risk</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Function</th><th>Owner</th><th>Risk</th><th>Fix</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(p.proname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND r.rolname = current_user THEN :'pg360_redaction_token' ELSE r.rolname END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="crit">Attacker can inject functions into search_path</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format('ALTER FUNCTION %s SET search_path = %I, pg_catalog;', p.oid::regprocedure, n.nspname)
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td></tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No vulnerable SECURITY DEFINER functions found</td></tr>'
  )
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_roles r ON r.oid = p.proowner
WHERE p.prosecdef = true
AND NOT (p.proconfig @> ARRAY['search_path=pg_catalog'])
AND NOT (p.proconfig::text ILIKE '%search_path%')
AND n.nspname NOT IN ('pg_catalog','information_schema');

\qecho '</tbody></table></div></div>'

-- S12.4 Public schema privilege check
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Public Schema Privileges</div>'
\qecho '</div>'

SELECT
  CASE
    WHEN has_schema_privilege('public','public','CREATE')
    THEN '<div class="finding high"><div class="finding-header">' ||
         '' ||
         '<span class="finding-title">PUBLIC role has CREATE on public schema</span>' ||
         '<span class="severity-pill pill-high">HIGH</span></div>' ||
         '<div class="finding-body">Any user can create objects in the public schema. ' ||
         'This is the default in PostgreSQL &lt; 15 but is a security risk.</div>' ||
         '<div class="fix-label">FIX</div>' ||
         '<div class="finding-fix">REVOKE CREATE ON SCHEMA public FROM PUBLIC;</div>' ||
         '</div>'
    ELSE '<div class="finding good"><div class="finding-header">' ||
         '' ||
         '<span class="finding-title">PUBLIC role does NOT have CREATE on public schema</span>' ||
         '<span class="severity-pill pill-good">OK</span></div>' ||
         '</div>'
  END;

-- S12.5 Default privilege drift
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">ALTER DEFAULT PRIVILEGES Drift</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Owner Role</th><th>Schema</th><th>Object Type</th><th>Default ACL</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND pg_get_userbyid(d.defaclrole) = current_user THEN :'pg360_redaction_token' ELSE pg_get_userbyid(d.defaclrole) END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(n.nspname,'(all schemas)'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE d.defaclobjtype
                  WHEN 'r' THEN 'tables'
                  WHEN 'S' THEN 'sequences'
                  WHEN 'f' THEN 'functions'
                  WHEN 'T' THEN 'types'
                  WHEN 'n' THEN 'schemas'
                  ELSE d.defaclobjtype::text
                END || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(array_to_string(d.defaclacl, ', '),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="warn">Review least-privilege posture for future objects.</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No custom ALTER DEFAULT PRIVILEGES entries found</td></tr>'
  )
FROM pg_default_acl d
LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace;

\qecho '</tbody></table></div></div>'

-- S12.6 Role inheritance and admin option exposure
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Role Membership and ADMIN OPTION Exposure</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Member Role</th><th>Granted Role</th><th>Admin Option</th><th>Inheritance</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND member_name = current_user THEN :'pg360_redaction_token' ELSE member_name END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND granted_name = current_user THEN :'pg360_redaction_token' ELSE granted_name END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN admin_option THEN 'warn">YES' ELSE 'good">No' END || '</td>' ||
      '<td>' || CASE WHEN member_inherits THEN 'YES' ELSE 'No' END || '</td>' ||
      '<td class="' ||
      CASE WHEN admin_option THEN 'warn">Member can re-grant this role. Validate delegated privilege model.'
           WHEN member_inherits THEN 'good">Inherited privileges apply at login.'
           ELSE '">Requires SET ROLE to activate privileges.' END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="5" class="table-empty"> No explicit role memberships found</td></tr>'
  )
FROM (
  SELECT
    m.admin_option,
    r_member.rolname AS member_name,
    r_member.rolinherit AS member_inherits,
    r_role.rolname AS granted_name
  FROM pg_auth_members m
  JOIN pg_roles r_member ON r_member.oid = m.member
  JOIN pg_roles r_role ON r_role.oid = m.roleid
  ORDER BY m.admin_option DESC, r_member.rolname, r_role.rolname
) x;

\qecho '</tbody></table></div></div>'

-- S12.7 Non-SSL active connections
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Active Connections Without SSL</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>User</th><th>Database</th><th>Application</th><th>Client (masked)</th><th>SSL</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND a.usename = current_user THEN :'pg360_redaction_token' ELSE a.usename END,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(a.datname,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(COALESCE(a.application_name,'(unknown)'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN a.client_addr IS NULL THEN 'local'
                     ELSE regexp_replace(host(a.client_addr),'(\d+)\.(\d+)\.\d+\.\d+','\1.\2.x.x')
                END || '</td>' ||
      '<td class="crit">No</td>' ||
      '<td class="crit">Remote session not using SSL.</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="6" class="table-empty"> No active non-SSL remote sessions</td></tr>'
  )
FROM pg_stat_activity a
LEFT JOIN pg_stat_ssl ssl ON ssl.pid = a.pid
WHERE a.pid <> pg_backend_pid()
  AND a.client_addr IS NOT NULL
  AND COALESCE(ssl.ssl, false) = false;

\qecho '</tbody></table></div></div>'

-- S12.8 Authentication and audit logging posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Authentication and Audit Logging Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || name || '</td><td>' || setting || COALESCE(' ' || unit, '') || '</td><td class="' ||
      CASE
        WHEN name = 'password_encryption' AND setting <> 'scram-sha-256' THEN 'warn">Prefer scram-sha-256 for password auth.'
        WHEN name = 'ssl' AND setting <> 'on' THEN 'crit">SSL is disabled at server level.'
        WHEN name = 'log_connections' AND setting <> 'on' THEN 'warn">Enable connection logging for incident investigations.'
        WHEN name = 'log_disconnections' AND setting <> 'on' THEN 'warn">Enable disconnect logging to track session churn.'
        WHEN name = 'log_lock_waits' AND setting <> 'on' THEN 'warn">Enable lock wait logging for blocking diagnostics.'
        WHEN name = 'log_min_duration_statement' AND setting = '-1' THEN 'warn">No slow-query logging threshold is set.'
        ELSE 'good">Configuration is aligned.' END || '</td></tr>',
      ''
      ORDER BY name
    ),
    '<tr><td colspan="3" class="table-empty"> Security posture parameters unavailable</td></tr>'
  )
FROM pg_settings
WHERE name IN (
  'password_encryption',
  'ssl',
  'log_connections',
  'log_disconnections',
  'log_lock_waits',
  'log_min_duration_statement'
);

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S13: PARTITIONING HEALTH
-- =============================================================================
\qecho '<div class="section" id="s13">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Partitioning Health</div>'
\qecho '    <div class="section-desc">Declarative partition tables, partition counts, missing default partitions.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Partitioned Tables Overview</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Parent Table</th><th>Strategy</th><th>Partition Count</th>'
\qecho '<th>Total Size</th><th>Default Partition</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE p.partstrat WHEN 'r' THEN 'RANGE' WHEN 'l' THEN 'LIST' WHEN 'h' THEN 'HASH' ELSE p.partstrat::text END || '</td>' ||
      '<td class="num">' || part_count || '</td>' ||
      '<td class="num">' || pg_size_pretty(pg_total_relation_size(c.oid)) || '</td>' ||
      '<td class="' ||
      CASE WHEN has_default THEN 'good"> Yes' ELSE 'warn"> Missing  new rows will error' END ||
      '</td></tr>',
      ''
    ),
    '<tr><td colspan="6" class="table-empty"> No partitioned tables found</td></tr>'
  )
FROM pg_partitioned_table p
JOIN pg_class c ON c.oid = p.partrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN (
  SELECT inhparent, COUNT(*) AS part_count,
    bool_or(pg_get_expr(pc.relpartbound, pc.oid) = 'DEFAULT') AS has_default
  FROM pg_inherits
  JOIN pg_class pc ON pc.oid = pg_inherits.inhrelid
  GROUP BY inhparent
) parts ON parts.inhparent = c.oid
WHERE n.nspname NOT IN ('pg_catalog','information_schema');

\qecho '</tbody></table></div></div>'

-- S13.2 Partition count pressure
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Partition Count Pressure</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parent Table</th><th>Partitions</th><th>Total Size</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(parent_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' ||
      CASE WHEN part_count > 500 THEN 'crit' WHEN part_count > 200 THEN 'warn' ELSE 'good' END || '">' ||
      part_count || '</td>' ||
      '<td class="num">' || pg_size_pretty(total_size) || '</td>' ||
      '<td class="' ||
      CASE WHEN part_count > 500 THEN 'crit">High partition count can increase planner overhead.'
           WHEN part_count > 200 THEN 'warn">Review partition retention and pruning effectiveness.'
           ELSE 'good">Partition count is within a manageable range.' END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> No partition count pressure signals</td></tr>'
  )
FROM (
  SELECT
    n.nspname || '.' || c.relname AS parent_name,
    COUNT(i.inhrelid) AS part_count,
    pg_total_relation_size(c.oid) AS total_size
  FROM pg_partitioned_table p
  JOIN pg_class c ON c.oid = p.partrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_inherits i ON i.inhparent = c.oid
  WHERE n.nspname NOT IN ('pg_catalog','information_schema')
  GROUP BY n.nspname, c.relname, c.oid
  ORDER BY COUNT(i.inhrelid) DESC
  LIMIT 20
) x;

\qecho '</tbody></table></div></div>'

-- S13.3 Child partitions without supporting indexes
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Child Partitions Missing Indexes</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parent Table</th><th>Total Partitions</th><th>Partitions Without Index</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(parent_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || total_parts || '</td>' ||
      '<td class="num ' || CASE WHEN no_index_parts > 0 THEN 'crit' ELSE 'good' END || '">' || no_index_parts || '</td>' ||
      '<td class="' || CASE WHEN no_index_parts > 0 THEN 'crit">Some child partitions have no index coverage.'
                          ELSE 'good">All child partitions have at least one index.' END || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="4" class="table-empty"> No partitioned tables found</td></tr>'
  )
FROM (
  WITH part_children AS (
    SELECT inhparent AS parent_oid, inhrelid AS child_oid
    FROM pg_inherits
  ),
  child_index_count AS (
    SELECT indrelid AS relid, COUNT(*) AS idx_count
    FROM pg_index
    WHERE indisvalid AND indisready
    GROUP BY indrelid
  )
  SELECT
    pn.nspname || '.' || pc.relname AS parent_name,
    COUNT(*) AS total_parts,
    COUNT(*) FILTER (WHERE COALESCE(ci.idx_count, 0) = 0) AS no_index_parts
  FROM part_children ch
  JOIN pg_class pc ON pc.oid = ch.parent_oid
  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
  LEFT JOIN child_index_count ci ON ci.relid = ch.child_oid
  WHERE pn.nspname NOT IN ('pg_catalog','information_schema')
  GROUP BY pn.nspname, pc.relname
  ORDER BY COUNT(*) FILTER (WHERE COALESCE(ci.idx_count, 0) = 0) DESC, COUNT(*) DESC
  LIMIT 20
) p;

\qecho '</tbody></table></div></div>'

-- S13.4 Partition maintenance automation signals
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Partition Maintenance Automation Signals</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Assessment</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>pg_partman extension</td><td>' || partman_installed || '</td><td class="' ||
  CASE WHEN partman_installed = 'installed' THEN 'good">Automated partition maintenance tooling available.'
       ELSE 'warn">No built-in partition automation detected.' END || '</td></tr>' ||
  '<tr><td>Partitioned tables count</td><td class="num">' || partitioned_tables || '</td><td></td></tr>' ||
  '<tr><td>Parents missing default partition</td><td class="num ' ||
  CASE WHEN parents_missing_default > 0 THEN 'warn' ELSE 'good' END || '">' || parents_missing_default || '</td><td class="' ||
  CASE WHEN parents_missing_default > 0 THEN 'warn">Inserts may fail for out-of-range keys.'
       ELSE 'good">Default partition coverage is present for all parents.' END || '</td></tr>'
FROM (
  SELECT
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_partman') THEN 'installed' ELSE 'not installed' END AS partman_installed,
    (SELECT COUNT(*) FROM pg_partitioned_table) AS partitioned_tables,
    (SELECT COUNT(*)
     FROM pg_partitioned_table p
     JOIN pg_class c ON c.oid = p.partrelid
     JOIN pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN (
       SELECT inhparent, bool_or(pg_get_expr(pc.relpartbound, pc.oid) = 'DEFAULT') AS has_default
       FROM pg_inherits
       JOIN pg_class pc ON pc.oid = pg_inherits.inhrelid
       GROUP BY inhparent
     ) x ON x.inhparent = c.oid
     WHERE n.nspname NOT IN ('pg_catalog','information_schema')
       AND COALESCE(x.has_default, false) = false) AS parents_missing_default
) m;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S15: DATA QUALITY CHECKS
-- =============================================================================
\qecho '<div class="section" id="s15">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Data Quality Checks</div>'
\qecho '    <div class="section-desc">Constraint gaps, CHECK constraint coverage, FK health. No data values are read  only metadata.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S15.1 NOT NULL constraint coverage
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables With Low NOT NULL Coverage</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Total Columns</th><th>NOT NULL Columns</th><th>Coverage%</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || total_cols || '</td>' ||
  '<td class="num">' || notnull_cols || '</td>' ||
  '<td class="num ' ||
  CASE WHEN notnull_pct < 50 THEN 'warn' WHEN notnull_pct < 30 THEN 'crit' ELSE '' END || '">' ||
  round(notnull_pct::numeric,0) || '%</td>' ||
  '</tr>'
FROM (
  SELECT
    a.attrelid,
    COUNT(*)                                  AS total_cols,
    COUNT(*) FILTER (WHERE a.attnotnull)      AS notnull_cols,
    COUNT(*) FILTER (WHERE a.attnotnull) * 100.0 / COUNT(*) AS notnull_pct
  FROM pg_attribute a
  WHERE a.attnum > 0 AND NOT a.attisdropped
  GROUP BY a.attrelid
) col_stats
JOIN pg_class c ON c.oid = col_stats.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
AND n.nspname NOT IN ('pg_catalog','information_schema')
AND notnull_pct < 60
ORDER BY notnull_pct ASC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S15.2 Check constraint coverage
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables Without CHECK Constraints</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Rows (approx)</th><th>Has CHECK?</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || to_char(c.reltuples::bigint,'FM999,999,999') || '</td>' ||
  '<td class="warn">No CHECK constraints  data integrity relies entirely on application</td>' ||
  '</tr>'
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
AND n.nspname NOT IN ('pg_catalog','information_schema')
AND c.reltuples > 1000
AND NOT EXISTS (
  SELECT 1 FROM pg_constraint con
  WHERE con.conrelid = c.oid AND con.contype = 'c'
)
ORDER BY c.reltuples DESC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S15.3 NOT VALID constraints backlog
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">NOT VALID Constraints Backlog</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Constraint</th><th>Type</th><th>Validated</th><th>Fix</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname || '.' || c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(con.conname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' ||
      CASE con.contype
        WHEN 'f' THEN 'FOREIGN KEY'
        WHEN 'c' THEN 'CHECK'
        WHEN 'u' THEN 'UNIQUE'
        ELSE con.contype::text
      END || '</td>' ||
      '<td class="' || CASE WHEN con.convalidated THEN 'good">Yes' ELSE 'warn">No' END || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format('ALTER TABLE %I.%I VALIDATE CONSTRAINT %I;', n.nspname, c.relname, con.conname)
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY n.nspname, c.relname, con.conname
    ),
    '<tr><td colspan="5" class="table-empty">No NOT VALID constraints detected.</td></tr>'
  )
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT con.convalidated
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast');

\qecho '</tbody></table></div></div>'

-- S15.4 FK orphan-risk indicators (metadata-only)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">FK Orphan-Risk Indicators (Metadata-Only)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>FK Constraint</th><th>Child Table</th><th>Parent Table</th><th>Validated</th><th>Supporting Index</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(con.conname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(child_ns.nspname || '.' || child.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(parent_ns.nspname || '.' || parent.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN con.convalidated THEN 'good">Yes' ELSE 'warn">No' END || '</td>' ||
      '<td class="' || CASE WHEN has_fk_index THEN 'good">Yes' ELSE 'warn">No' END || '</td>' ||
      '<td class="' ||
      CASE
        WHEN NOT con.convalidated AND NOT has_fk_index THEN 'crit">Highest risk: unvalidated FK + no child index'
        WHEN NOT con.convalidated THEN 'warn">Validate FK to enforce integrity'
        WHEN NOT has_fk_index THEN 'warn">Add child-side FK index for DML stability'
        ELSE 'good">No immediate metadata risk'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY child_ns.nspname, child.relname
    ),
    '<tr><td colspan="6" class="table-empty">No foreign key constraints found.</td></tr>'
  )
FROM (
  SELECT
    con.*,
    EXISTS (
      SELECT 1
      FROM pg_index i
      JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
      WHERE i.indrelid = con.conrelid
        AND i.indkey[0] = k.attnum
    ) AS has_fk_index
  FROM pg_constraint con
  WHERE con.contype = 'f'
) con
JOIN pg_class child ON child.oid = con.conrelid
JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
JOIN pg_class parent ON parent.oid = con.confrelid
JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
WHERE child_ns.nspname NOT IN ('pg_catalog','information_schema','pg_toast');

\qecho '</tbody></table></div></div>'

-- S15.5 Candidate key coverage gaps
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Candidate Key Coverage Gaps</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Column</th><th>Data Type</th><th>Rows (approx)</th><th>Signal</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(table_schema || '.' || table_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(column_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(data_type,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(reltuples::bigint,'FM999,999,999') || '</td>' ||
      '<td class="warn">ID-like column lacks UNIQUE/PK coverage; verify business key intent.</td>' ||
      '</tr>',
      E'\n' ORDER BY reltuples DESC
    ),
    '<tr><td colspan="5" class="table-empty">No obvious candidate-key coverage gaps found.</td></tr>'
  )
FROM (
  SELECT
    c.table_schema,
    c.table_name,
    c.column_name,
    c.data_type,
    cls.reltuples
  FROM information_schema.columns c
  JOIN pg_class cls ON cls.relname = c.table_name
  JOIN pg_namespace ns ON ns.oid = cls.relnamespace AND ns.nspname = c.table_schema
  WHERE c.table_schema NOT IN ('pg_catalog','information_schema')
    AND c.column_name ~* '(id|code|number|key)$'
    AND cls.relkind = 'r'
    AND cls.reltuples > 1000
    AND NOT EXISTS (
      SELECT 1
      FROM pg_constraint con
      JOIN pg_attribute a ON a.attrelid = con.conrelid
      WHERE con.conrelid = cls.oid
        AND con.contype IN ('p','u')
        AND a.attnum = ANY(con.conkey)
        AND a.attname = c.column_name
    )
) gaps
LIMIT 40;

\qecho '</tbody></table></div></div>'

-- S15.6 Sensitive table RLS handoff to security module
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Sensitive Table RLS Coverage Handoff (to S25)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Sensitivity Signal</th><th>RLS Enabled</th><th>Next Action</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(c.nspname || '.' || c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || signal || '</td>' ||
      '<td class="' || CASE WHEN c.relrowsecurity THEN 'good">Yes' ELSE 'warn">No' END || '</td>' ||
      '<td class="' ||
      CASE WHEN c.relrowsecurity THEN 'good">Continue policy validation in S25'
           ELSE 'warn">Review RLS requirement in S25.1 for tenant/data isolation'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY c.relname
    ),
    '<tr><td colspan="4" class="table-empty">No sensitive-name table signals detected.</td></tr>'
  )
FROM (
  SELECT c.oid, n.nspname, c.relname, c.relrowsecurity,
         'Name pattern (user/account/payment/order/transaction)'::text AS signal
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND c.relname ~* '(user|account|payment|order|transaction|customer|tenant)'
) c;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S16: CAPACITY & GROWTH PROJECTIONS
-- =============================================================================
\qecho '<div class="section" id="s16">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Capacity &amp; Growth Projections</div>'
\qecho '    <div class="section-desc">Insert rate-based growth projections, WAL generation, index overhead.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top Tables by Insert Rate (growth leaders)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Current Size</th><th>Total Inserts</th>'
\qecho '<th>Updates</th><th>Deletes</th><th>Insert Rate Pattern</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || to_char(n_tup_ins,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_tup_upd,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_tup_del,'FM999,999,999') || '</td>' ||
  '<td class="' ||
  CASE
    WHEN n_tup_ins > n_tup_del * 10 THEN 'warn">Growing fast  monitor capacity'
    WHEN n_tup_ins > 0 AND n_tup_del > n_tup_ins * 0.9 THEN 'good">Queue-like pattern'
    ELSE '">Stable'
  END ||
  '</td></tr>'
FROM pg_stat_user_tables
WHERE n_tup_ins > 0
ORDER BY n_tup_ins DESC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S16.2 Index-to-data ratio (over-indexed tables)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Index Overhead Ratio (indexes &gt; 2x table size = over-indexed)</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Table Size</th><th>Index Size</th><th>Ratio</th><th>Index Count</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_relation_size(c.oid)) || '</td>' ||
  '<td class="num warn">' || pg_size_pretty(idx_size) || '</td>' ||
  '<td class="num ' || CASE WHEN idx_ratio > 3 THEN 'crit' WHEN idx_ratio > 2 THEN 'warn' ELSE '' END || '">' ||
  round(idx_ratio::numeric,1) || 'x</td>' ||
  '<td class="num">' || idx_count || '</td>' ||
  '</tr>'
FROM (
  SELECT
    t.oid,
    SUM(pg_relation_size(i.indexrelid)) AS idx_size,
    COUNT(i.indexrelid) AS idx_count,
    SUM(pg_relation_size(i.indexrelid))::numeric / NULLIF(pg_relation_size(t.oid),0) AS idx_ratio
  FROM pg_class t
  JOIN pg_index i ON i.indrelid = t.oid
  GROUP BY t.oid
) idx_stats
JOIN pg_class c ON c.oid = idx_stats.oid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE idx_ratio > 1.5
AND pg_relation_size(c.oid) > 1048576
AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY idx_ratio DESC
LIMIT 20;

\qecho '</tbody></table></div></div>'

-- S16.3 Projection confidence for baseline capacity
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Projection Confidence</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed</th><th>Status</th><th>Meaning</th>'
\qecho '</tr></thead><tbody>'

WITH c AS (
  SELECT
    COALESCE((SELECT now() - stats_reset FROM pg_stat_database WHERE datname = current_database()), interval '365 days') AS stats_window,
    COALESCE((SELECT SUM(n_tup_ins + n_tup_upd + n_tup_del) FROM pg_stat_user_tables), 0) AS activity_rows
)
SELECT
  '<tr><td>Stats window since reset</td><td>' || replace(replace(replace(replace(replace(stats_window::text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td><td class="' ||
  CASE WHEN stats_window >= interval '3 days' THEN 'good">High confidence'
       WHEN stats_window >= interval '12 hours' THEN 'warn">Medium confidence'
       ELSE 'crit">Low confidence' END ||
  '</td><td>Longer windows reduce noise in trend-based decisions.</td></tr>' ||
  '<tr><td>Tuple activity in window</td><td class="num">' || to_char(activity_rows,'FM999,999,999') || '</td><td class="' ||
  CASE WHEN activity_rows > 500000 THEN 'good">Representative'
       WHEN activity_rows > 50000 THEN 'warn">Moderate'
       ELSE 'crit">Sparse data' END ||
  '</td><td>Low activity can overfit short-term spikes.</td></tr>'
FROM c;

\qecho '</tbody></table></div></div>'

-- S16.4 Growth decomposition: heap vs index pressure
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Growth Decomposition</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Heap Size</th><th>Index Size</th><th>Index/Heap Ratio</th><th>Net Row Growth</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || pg_size_pretty(heap_bytes) || '</td>' ||
      '<td class="num">' || pg_size_pretty(index_bytes) || '</td>' ||
      '<td class="num ' || CASE WHEN idx_heap_ratio > 2 THEN 'warn' ELSE 'good' END || '">' || round(idx_heap_ratio::numeric,2) || 'x</td>' ||
      '<td class="num">' || to_char(net_growth_rows,'FM999,999,999') || '</td>' ||
      '<td class="' ||
      CASE
        WHEN idx_heap_ratio > 2.5 THEN 'warn">Review duplicate/unused indexes and covering-index scope'
        WHEN net_growth_rows > 1000000 THEN 'warn">Plan retention/partition strategy for sustained growth'
        ELSE 'good">No immediate structural action'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY net_growth_rows DESC, idx_heap_ratio DESC
    ),
    '<tr><td colspan="6" class="table-empty">No user tables available for decomposition output.</td></tr>'
  )
FROM (
  SELECT
    st.schemaname,
    st.relname,
    pg_relation_size(st.relid) AS heap_bytes,
    pg_indexes_size(st.relid) AS index_bytes,
    pg_indexes_size(st.relid)::numeric / NULLIF(pg_relation_size(st.relid),0) AS idx_heap_ratio,
    (st.n_tup_ins - st.n_tup_del) AS net_growth_rows
  FROM pg_stat_user_tables st
  WHERE pg_relation_size(st.relid) > 0
) d
LIMIT 30;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S17: HA & DISASTER RECOVERY READINESS
-- =============================================================================
\qecho '<div class="section" id="s17">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">HA &amp; Disaster Recovery Readiness</div>'
\qecho '    <div class="section-desc">Archive mode, WAL level, backup readiness, PITR capability.</div>'
\qecho '  </div>'
\qecho '</div>'

SELECT
  '<div class="table-wrap"><table class="pg360"><thead><tr>' ||
  '<th>Check</th><th>Current Setting</th><th>Required for PITR</th><th>Status</th>' ||
  '</tr></thead><tbody>' ||

  '<tr><td>archive_mode</td><td class="num">' || archive_mode || '</td><td>on</td><td class="' ||
  CASE WHEN archive_mode = 'on' THEN 'good"> Enabled' ELSE 'crit"> Disabled  no WAL archiving' END || '</td></tr>' ||

  '<tr><td>wal_level</td><td class="num">' || wal_level || '</td><td>replica or logical</td><td class="' ||
  CASE WHEN wal_level IN ('replica','logical') THEN 'good"> OK' ELSE 'crit"> Insufficient  no PITR possible' END || '</td></tr>' ||

  '<tr><td>archive_command</td><td class="num">' ||
  CASE WHEN archive_cmd = '' OR archive_cmd IS NULL THEN '<span class="crit">Not set</span>'
       ELSE replace(replace(replace(replace(replace(left(archive_cmd,50),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
  END ||
  '</td><td>Set to backup command</td><td class="' ||
  CASE WHEN archive_cmd IS NOT NULL AND archive_cmd <> '' THEN 'good"> Configured' ELSE 'crit"> No archive command' END || '</td></tr>' ||

  '<tr><td>max_wal_senders</td><td class="num">' || max_wal_senders || '</td><td>&gt; 0</td><td class="' ||
  CASE WHEN max_wal_senders::int > 0 THEN 'good"> OK' ELSE 'warn"> No streaming replication configured' END || '</td></tr>' ||

  '</tbody></table></div>'

FROM (
  SELECT
    MAX(CASE WHEN name='archive_mode' THEN setting END) AS archive_mode,
    MAX(CASE WHEN name='wal_level' THEN setting END) AS wal_level,
    MAX(CASE WHEN name='archive_command' THEN setting END) AS archive_cmd,
    MAX(CASE WHEN name='max_wal_senders' THEN setting END) AS max_wal_senders
  FROM pg_settings
  WHERE name IN ('archive_mode','wal_level','archive_command','max_wal_senders')
) ha_settings;

-- S17.0 Backup and PITR configuration health matrix
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Backup &amp; PITR Health Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current Value</th><th>Recommended</th><th>Status</th><th>Why it Matters</th>'
\qecho '</tr></thead><tbody>'
WITH expected(name, recommended, details, ord) AS (
  VALUES
    ('archive_mode', 'on/always', 'WAL archiving must be enabled for PITR.', 1),
    ('archive_command', 'non-empty', 'Archive command must successfully ship WAL files.', 2),
    ('archive_library', 'non-empty (optional alternative)', 'Alternative to archive_command in newer releases.', 3),
    ('wal_level', 'replica/logical', 'At least replica is required for replication and PITR.', 4),
    ('max_wal_senders', '>= 2', 'Support base backup and replicas without blocking one another.', 5),
    ('max_replication_slots', '>= 2', 'Slots improve WAL retention guarantees for replicas/consumers.', 6),
    ('wal_keep_size', '> 0 or use replication slots', 'Extra WAL retention safety net.', 7),
    ('hot_standby', 'on (for standby)', 'Required to serve read traffic on standby.', 8),
    ('restore_command', 'set on restore/standby', 'Required when replaying archived WAL during restore.', 9)
), settings AS (
  SELECT
    e.ord,
    e.name,
    e.recommended,
    e.details,
    s.setting,
    s.source
  FROM expected e
  LEFT JOIN pg_settings s
    ON s.name = e.name
), checks AS (
  SELECT
    ord,
    name AS parameter_name,
    COALESCE(setting, '(not available on this version)') AS current_value,
    recommended,
    CASE
      WHEN name = 'archive_mode' AND setting IN ('on', 'always') THEN 'OK'
      WHEN name = 'archive_command'
           AND COALESCE((SELECT setting FROM settings WHERE name = 'archive_mode'), 'off') IN ('on', 'always')
           AND COALESCE(trim(setting), '') <> ''
           AND trim(setting) <> '(disabled)' THEN 'OK'
      WHEN name = 'archive_library'
           AND COALESCE((SELECT setting FROM settings WHERE name = 'archive_mode'), 'off') IN ('on', 'always')
           AND COALESCE(trim(setting), '') <> '' THEN 'OK'
      WHEN name = 'wal_level' AND setting IN ('replica', 'logical') THEN 'OK'
      WHEN name = 'max_wal_senders' AND setting ~ '^[0-9]+$' AND setting::int >= 2 THEN 'OK'
      WHEN name = 'max_replication_slots' AND setting ~ '^[0-9]+$' AND setting::int >= 2 THEN 'OK'
      WHEN name = 'wal_keep_size' AND setting ~ '^[0-9]+$' AND setting::int > 0 THEN 'OK'
      WHEN name = 'hot_standby' AND pg_is_in_recovery() AND setting = 'on' THEN 'OK'
      WHEN name = 'hot_standby' AND NOT pg_is_in_recovery() THEN 'INFO'
      WHEN name = 'restore_command' AND NOT pg_is_in_recovery() THEN 'INFO'
      WHEN name = 'restore_command' AND pg_is_in_recovery() AND COALESCE(trim(setting), '') <> '' THEN 'OK'
      ELSE 'GAP'
    END AS status,
    details AS why_it_matters
  FROM settings
)
SELECT COALESCE(
  string_agg(
    '<tr><td>' || parameter_name || '</td><td>' ||
    replace(replace(replace(replace(replace(current_value,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
    '</td><td>' || recommended || '</td><td class="' ||
    CASE status
      WHEN 'OK' THEN 'good">OK'
      WHEN 'GAP' THEN 'warn">GAP'
      ELSE '">INFO'
    END || '</td><td>' || why_it_matters || '</td></tr>',
    E'\n' ORDER BY ord
  ),
  '<tr><td colspan="5" class="table-empty">Backup and PITR configuration health unavailable</td></tr>'
) FROM checks;
\qecho '</tbody></table></div></div>'

-- S17.1 Archive and PITR signal quality
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Archive and PITR Signal Quality</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Status</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Last archived WAL time</td><td>' || COALESCE(to_char(last_archived_time,'YYYY-MM-DD HH24:MI:SS'),'N/A') ||
  '</td><td class="' || CASE WHEN last_archived_time IS NULL THEN 'warn">Unknown' ELSE 'good">Present' END ||
  '</td><td>Shows whether archiving pipeline has emitted recent files.</td></tr>' ||
  '<tr><td>Archive failure count</td><td class="num">' || failed_count || '</td><td class="' ||
  CASE WHEN failed_count > 0 THEN 'warn">Review' ELSE 'good">Clean' END ||
  '</td><td>Non-zero failures reduce PITR confidence and should be investigated.</td></tr>' ||
  '<tr><td>restore_command</td><td>' || COALESCE(NULLIF(restore_command,''),'(empty)') || '</td><td class="' ||
  CASE WHEN restore_command = '' THEN 'warn">Not set' ELSE 'good">Configured' END ||
  '</td><td>Required on recovery nodes for WAL replay from archive.</td></tr>' ||
  '<tr><td>full_page_writes</td><td>' || full_page_writes || '</td><td class="' ||
  CASE WHEN full_page_writes = 'on' THEN 'good">On' ELSE 'crit">Off' END ||
  '</td><td>Must be ON to ensure crash-safe recovery semantics.</td></tr>'
FROM (
  SELECT
    a.last_archived_time,
    a.failed_count,
    current_setting('restore_command', true) AS restore_command,
    current_setting('full_page_writes', true) AS full_page_writes
  FROM pg_stat_archiver a
) pitr;

\qecho '</tbody></table></div></div>'

-- S17.2 Failover readiness and RPO posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Failover Readiness &amp; RPO Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Check</th><th>Value</th><th>Status</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>Standby count</td><td class="num">' || standby_count || '</td><td class="' ||
  CASE WHEN standby_count > 0 THEN 'good">Redundant' ELSE 'warn">Single-node' END ||
  '</td><td>' || CASE WHEN standby_count > 0 THEN 'Validate failover runbook and replica lag alerting.'
                     ELSE 'Provision standby for HA targets.' END || '</td></tr>' ||
  '<tr><td>Worst replay lag</td><td class="num">' || COALESCE(worst_replay_lag,'N/A') || '</td><td class="' ||
  CASE WHEN lag_seconds > 60 THEN 'warn">Elevated' ELSE 'good">Acceptable' END ||
  '</td><td>Align lag budget with business RPO objective.</td></tr>' ||
  '<tr><td>synchronous_commit</td><td>' || synchronous_commit || '</td><td class="' ||
  CASE WHEN synchronous_commit IN ('on','remote_write','remote_apply') THEN 'good">Durability-favoring'
       ELSE 'warn">Latency-favoring' END ||
  '</td><td>Confirm this matches loss tolerance in failover scenarios.</td></tr>' ||
  '<tr><td>synchronous_standby_names</td><td>' || sync_names || '</td><td class="' ||
  CASE WHEN sync_names IN ('', 'off') THEN 'warn">Not enforced' ELSE 'good">Configured' END ||
  '</td><td>Explicit synchronous quorum improves deterministic RPO.</td></tr>'
FROM (
  SELECT
    current_setting('synchronous_commit') AS synchronous_commit,
    COALESCE(NULLIF(current_setting('synchronous_standby_names', true),''), 'off') AS sync_names,
    (SELECT COUNT(*) FROM pg_stat_replication) AS standby_count,
    (SELECT COALESCE(MAX(EXTRACT(epoch FROM replay_lag)),0) FROM pg_stat_replication) AS lag_seconds,
    (SELECT to_char(MAX(replay_lag),'HH24:MI:SS') FROM pg_stat_replication) AS worst_replay_lag
) ha;

\qecho '</tbody></table></div></div>'

-- S17.3 Operational DR checklist (non-DB dependencies)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Repl Evidence for RPO Discussion</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Node Role</th><th>Endpoint / Application</th><th>State</th><th>LSN or Replay Signal</th><th>Lag / Gap</th><th>RPO Status</th>'
\qecho '</tr></thead><tbody>'

SELECT CASE WHEN pg_is_in_recovery() THEN 'on' ELSE 'off' END AS s17_is_standby \gset

\if :s17_is_standby
SELECT
  '<tr><td>standby</td><td>' || replace(replace(replace(replace(replace(current_database(),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td><td>recovery</td><td>' ||
  COALESCE(pg_last_wal_receive_lsn()::text, 'N/A') || ' -> ' || COALESCE(pg_last_wal_replay_lsn()::text, 'N/A') ||
  '</td><td>' ||
  COALESCE(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::text, 'N/A') || ' bytes / ' ||
  COALESCE(round(extract(epoch FROM (clock_timestamp() - pg_last_xact_replay_timestamp()))::numeric, 2)::text, 'N/A') || ' sec' ||
  '</td><td class="' ||
  CASE
    WHEN pg_last_xact_replay_timestamp() IS NULL THEN 'warn">NO_REPLAY_EVIDENCE'
    WHEN clock_timestamp() - pg_last_xact_replay_timestamp() > interval '60 seconds' THEN 'warn">RPO_RISK_GT_60S'
    ELSE 'good">RPO_OK_LE_60S'
  END || '</td></tr>';
\else
SELECT COALESCE(
  string_agg(
    '<tr><td>primary</td><td>' || COALESCE(replace(replace(replace(replace(replace(application_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;'), '(unknown)') ||
    '</td><td>' || COALESCE(state, '(unknown)') || ' / ' || COALESCE(sync_state, '(unknown)') ||
    '</td><td>' || COALESCE(sent_lsn::text, 'N/A') || ' -> ' || COALESCE(replay_lsn::text, 'N/A') ||
    '</td><td>' || COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn)::text, 'N/A') || ' bytes / ' ||
    COALESCE(extract(epoch FROM replay_lag)::numeric(18,2)::text, '0.00') || ' sec' ||
    '</td><td class="' ||
    CASE
      WHEN state <> 'streaming' THEN 'warn">NOT_STREAMING'
      WHEN COALESCE(extract(epoch FROM replay_lag), 0) > 60 THEN 'warn">RPO_RISK_GT_60S'
      ELSE 'good">RPO_OK_LE_60S'
    END || '</td></tr>',
    E'\n' ORDER BY replay_lag DESC NULLS LAST, pg_wal_lsn_diff(sent_lsn, replay_lsn) DESC NULLS LAST
  ),
  '<tr><td colspan="6" class="table-empty">No streaming replication rows found on the primary.</td></tr>'
) FROM pg_stat_replication;
\endif

\qecho '</tbody></table></div></div>'

-- S17.3 Operational DR checklist (non-DB dependencies)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Operational DR Checklist</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Dependency</th><th>Database Signal</th><th>Recommended Validation</th>'
\qecho '</tr></thead><tbody>'
\qecho '<tr><td>Backup restore drill</td><td class="warn">Not directly observable from SQL metadata</td><td>Run scheduled restore test to staging and capture restore duration (RTO).</td></tr>'
\qecho '<tr><td>Application failover endpoint</td><td class="warn">Outside PostgreSQL scope</td><td>Verify DNS/connection-string failover procedure and automation.</td></tr>'
\qecho '<tr><td>Replication slot retention guardrails</td><td class="good">Visible in S08 slot diagnostics</td><td>Set alerting on inactive slots and retained WAL thresholds.</td></tr>'
\qecho '<tr><td>Recovery documentation</td><td class="warn">Not stored in catalog</td><td>Maintain versioned runbook with owner, last test date, and rollback steps.</td></tr>'
\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- CLOSE HTML
-- =============================================================================


-- =============================================================================
-- SECTION S18: EXECUTIVE HEALTH SCORE
-- =============================================================================
\qecho '<div class="section" id="s18">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Executive Health Score</div>'
\qecho '    <div class="section-desc">Aggregated health across all dimensions. Red items require immediate attention.</div>'
\qecho '  </div>'
\qecho '</div>'

-- Workload detection
SELECT
  '<div class="card-grid">' ||
  '<div class="card"><div class="card-label">Database</div><div class="card-value">' ||
  replace(replace(replace(replace(replace(current_database(),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">PostgreSQL Version</div><div class="card-value">' ||
  replace(replace(split_part(version(),' ',2),'<','&lt;'),'>','&gt;') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Uptime</div><div class="card-value">' ||
  to_char(now() - pg_postmaster_start_time(), 'DD"d" HH24"h"') ||
  '</div></div>' ||
  '<div class="card"><div class="card-label">Database Size</div><div class="card-value">' ||
  pg_size_pretty(pg_database_size(current_database())) ||
  '</div></div>' ||
  '</div>';

-- Workload profile detection
SELECT
  '<div class="finding info">' ||
  '<div class="finding-header">' ||
  '' ||
  '<span class="finding-title">Detected Workload Profile</span>' ||
  '<span class="severity-pill pill-info">' ||
  CASE
    WHEN (d.tup_fetched::numeric / NULLIF(d.tup_inserted + d.tup_updated + d.tup_deleted, 0)) > 10
      THEN 'READ-HEAVY'
    WHEN (d.tup_inserted + d.tup_updated + d.tup_deleted)::numeric / NULLIF(d.tup_fetched, 0) > 3
      THEN 'WRITE-HEAVY'
    WHEN d.temp_files > 100
      THEN 'ANALYTICAL'
    ELSE 'MIXED OLTP'
  END ||
  '</span></div>' ||
  '<div class="finding-body">' ||
  'Reads: ' || to_char(d.tup_fetched, 'FM999,999,999') ||
  ' | Writes: ' || to_char(d.tup_inserted + d.tup_updated + d.tup_deleted, 'FM999,999,999') ||
  ' | Temp Spills: ' || d.temp_files ||
  ' | Cache Hit: ' || ROUND(
    d.blks_hit::numeric / NULLIF(d.blks_hit + d.blks_read, 0) * 100, 2
  ) || '%' ||
  '</div></div>'
FROM pg_stat_database d
WHERE d.datname = current_database();

-- Critical findings summary
SELECT
  '<div class="card-grid">' ||
  '<div class="card critical"><div class="card-label"> Sequences Out of Sync</div>' ||
  '<div class="card-value">' ||
  COUNT(*)::text ||
  '</div><div class="card-sub">Risk: PK violation on next insert</div></div></div>'
FROM (
  SELECT s.schemaname, s.sequencename
  FROM pg_sequences s
  JOIN pg_depend d ON d.objid = (
    ('"' || s.schemaname || '"."' || s.sequencename || '"')::regclass
  )
  JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
  JOIN (
    SELECT n.nspname, c.relname,
           (SELECT MAX(a2.attnum) FROM pg_attribute a2 WHERE a2.attrelid = c.oid AND a2.attnotnull) AS maxattnum
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
  ) t ON t.nspname = s.schemaname
  WHERE s.last_value IS NOT NULL
) seq_check;

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Category Score Breakdown</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Category</th><th>Score (0-100)</th><th>Weight</th><th>Weighted Contribution</th><th>Evidence Basis</th>'
\qecho '</tr></thead><tbody>'

WITH metrics AS (
  SELECT
    COALESCE((SELECT age(datfrozenxid)::numeric / 2000000000
              FROM pg_database WHERE datname = current_database()), 0) AS xid_pct,
    COALESCE((SELECT blks_hit::numeric / NULLIF(blks_hit + blks_read, 0)
              FROM pg_stat_database WHERE datname = current_database()), 0) AS cache_hit,
    COALESCE((SELECT temp_files::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS temp_files,
    COALESCE((SELECT deadlocks::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS deadlocks,
    COALESCE((SELECT SUM(n_dead_tup)::numeric / NULLIF(SUM(n_live_tup + n_dead_tup), 0)
              FROM pg_stat_user_tables), 0) AS dead_ratio,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'autovacuum'), 'on') AS autovacuum_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'ssl'), 'off') AS ssl_setting,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_stat_replication), 0) AS standby_count,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_replication_slots WHERE NOT active), 0) AS inactive_slots,
    COALESCE((SELECT COUNT(*)::numeric FROM information_schema.role_table_grants WHERE grantee = 'PUBLIC'), 0) AS public_grants
),
categories AS (
  SELECT
    'Stability'::text AS category,
    25::numeric AS weight,
    GREATEST(0, LEAST(100,
      100 - LEAST(xid_pct * 120, 70) - LEAST(deadlocks * 5, 30)
    )) AS score,
    'XID age, deadlocks, write safety margin'::text AS evidence
  FROM metrics
  UNION ALL
  SELECT
    'Performance',
    25,
    GREATEST(0, LEAST(100,
      (CASE
         WHEN cache_hit >= 0.99 THEN 96
         WHEN cache_hit >= 0.95 THEN 86
         WHEN cache_hit >= 0.90 THEN 74
         ELSE 58
       END) - LEAST(temp_files / 500.0 * 20, 20)
    )),
    'Buffer hit ratio and temp-file pressure'
  FROM metrics
  UNION ALL
  SELECT
    'Maintenance',
    20,
    GREATEST(0, LEAST(100,
      (CASE WHEN autovacuum_setting = 'on' THEN 80 ELSE 30 END)
      - LEAST(dead_ratio * 300, 50)
      + CASE WHEN dead_ratio < 0.05 THEN 15 ELSE 0 END
    )),
    'Autovacuum posture and dead-tuple accumulation'
  FROM metrics
  UNION ALL
  SELECT
    'Resilience',
    15,
    GREATEST(0, LEAST(100,
      60 + LEAST(standby_count * 15, 30) - LEAST(inactive_slots * 20, 40)
    )),
    'Standby coverage and inactive slot risk'
  FROM metrics
  UNION ALL
  SELECT
    'Security',
    15,
    GREATEST(0, LEAST(100,
      (CASE WHEN ssl_setting = 'on' THEN 85 ELSE 40 END)
      - LEAST(public_grants * 3, 45)
    )),
    'Transport encryption and PUBLIC privilege surface'
  FROM metrics
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(category,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num ' ||
      CASE
        WHEN score >= 85 THEN 'good'
        WHEN score >= 65 THEN 'warn'
        ELSE 'crit'
      END || '">' || round(score,1) || '</td>' ||
      '<td class="num">' || round(weight,0) || '%</td>' ||
      '<td class="num">' || round(score * weight / 100.0,1) || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(evidence,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY weight DESC, category
    ),
    '<tr><td colspan="5" class="table-empty">Score components unavailable</td></tr>'
  )
FROM categories;

\qecho '</tbody></table></div>'

WITH metrics AS (
  SELECT
    COALESCE((SELECT age(datfrozenxid)::numeric / 2000000000
              FROM pg_database WHERE datname = current_database()), 0) AS xid_pct,
    COALESCE((SELECT blks_hit::numeric / NULLIF(blks_hit + blks_read, 0)
              FROM pg_stat_database WHERE datname = current_database()), 0) AS cache_hit,
    COALESCE((SELECT temp_files::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS temp_files,
    COALESCE((SELECT deadlocks::numeric
              FROM pg_stat_database WHERE datname = current_database()), 0) AS deadlocks,
    COALESCE((SELECT SUM(n_dead_tup)::numeric / NULLIF(SUM(n_live_tup + n_dead_tup), 0)
              FROM pg_stat_user_tables), 0) AS dead_ratio,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'autovacuum'), 'on') AS autovacuum_setting,
    COALESCE((SELECT setting FROM pg_settings WHERE name = 'ssl'), 'off') AS ssl_setting,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_stat_replication), 0) AS standby_count,
    COALESCE((SELECT COUNT(*)::numeric FROM pg_replication_slots WHERE NOT active), 0) AS inactive_slots,
    COALESCE((SELECT COUNT(*)::numeric FROM information_schema.role_table_grants WHERE grantee = 'PUBLIC'), 0) AS public_grants
),
categories AS (
  SELECT 25::numeric AS weight, GREATEST(0, LEAST(100, 100 - LEAST(xid_pct * 120, 70) - LEAST(deadlocks * 5, 30))) AS score FROM metrics
  UNION ALL
  SELECT 25, GREATEST(0, LEAST(100, (CASE WHEN cache_hit >= 0.99 THEN 96 WHEN cache_hit >= 0.95 THEN 86 WHEN cache_hit >= 0.90 THEN 74 ELSE 58 END) - LEAST(temp_files / 500.0 * 20, 20))) FROM metrics
  UNION ALL
  SELECT 20, GREATEST(0, LEAST(100, (CASE WHEN autovacuum_setting = 'on' THEN 80 ELSE 30 END) - LEAST(dead_ratio * 300, 50) + CASE WHEN dead_ratio < 0.05 THEN 15 ELSE 0 END)) FROM metrics
  UNION ALL
  SELECT 15, GREATEST(0, LEAST(100, 60 + LEAST(standby_count * 15, 30) - LEAST(inactive_slots * 20, 40))) FROM metrics
  UNION ALL
  SELECT 15, GREATEST(0, LEAST(100, (CASE WHEN ssl_setting = 'on' THEN 85 ELSE 40 END) - LEAST(public_grants * 3, 45))) FROM metrics
),
tot AS (
  SELECT round(SUM(score * weight) / NULLIF(SUM(weight), 0), 1) AS overall_score
  FROM categories
)
SELECT
  '<div class="finding ' ||
  CASE
    WHEN overall_score >= 85 THEN 'good'
    WHEN overall_score >= 70 THEN 'info'
    WHEN overall_score >= 55 THEN 'high'
    ELSE 'critical'
  END || '"><div class="finding-header">' ||
  '' ||
  '<span class="finding-title">Weighted Health Score: ' || overall_score || '/100</span>' ||
  '<span class="severity-pill ' ||
  CASE
    WHEN overall_score >= 85 THEN 'pill-good">Stable'
    WHEN overall_score >= 70 THEN 'pill-info">Needs Attention'
    WHEN overall_score >= 55 THEN 'pill-high">Elevated Risk'
    ELSE 'pill-critical">Critical Risk'
  END || '</span></div>' ||
  '<div class="finding-body">Scoring is fully transparent in the table above and can be recalculated after each remediation cycle.</div></div>'
FROM tot;
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Diagnostic Confidence</div>'

WITH readiness AS (
  SELECT
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS pgss_installed,
    current_setting('shared_preload_libraries', true) ILIKE '%pg_stat_statements%' AS pgss_preloaded,
    current_setting('track_io_timing', true) = 'on' AS track_io_timing_on,
    current_setting('track_counts', true) = 'on' AS track_counts_on,
    COALESCE((SELECT now() - stats_reset FROM pg_stat_database WHERE datname = current_database()), interval '365 days') AS db_stats_window
),
scored AS (
  SELECT *,
    (CASE WHEN pgss_installed THEN 1 ELSE 0 END) +
    (CASE WHEN pgss_preloaded THEN 1 ELSE 0 END) +
    (CASE WHEN track_io_timing_on THEN 1 ELSE 0 END) +
    (CASE WHEN track_counts_on THEN 1 ELSE 0 END) +
    (CASE WHEN db_stats_window >= interval '6 hours' THEN 1 ELSE 0 END) AS readiness_points
  FROM readiness
)
SELECT
  '<div class="finding ' ||
  CASE
    WHEN readiness_points >= 5 THEN 'good'
    WHEN readiness_points >= 3 THEN 'high'
    ELSE 'critical'
  END || '"><div class="finding-header">' ||
  '' ||
  '<span class="finding-title">Diagnostic Confidence</span>' ||
  '<span class="severity-pill ' ||
  CASE
    WHEN readiness_points >= 5 THEN 'pill-good">HIGH'
    WHEN readiness_points >= 3 THEN 'pill-medium">MEDIUM'
    ELSE 'pill-critical">LOW'
  END || '</span></div>' ||
  '<div class="finding-body">Stats measurement window: ' ||
  replace(replace(replace(replace(replace(db_stats_window::text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '. Confidence reflects extension/collector readiness needed by deeper modules.</div></div>'
FROM scored;

\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Prerequisite</th><th>Status</th><th>Operational Impact</th>'
\qecho '</tr></thead><tbody>'
WITH readiness AS (
  SELECT
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') AS pgss_installed,
    current_setting('shared_preload_libraries', true) ILIKE '%pg_stat_statements%' AS pgss_preloaded,
    current_setting('track_io_timing', true) = 'on' AS track_io_timing_on,
    current_setting('track_counts', true) = 'on' AS track_counts_on,
    COALESCE((SELECT now() - stats_reset FROM pg_stat_database WHERE datname = current_database()), interval '365 days') AS db_stats_window
)
SELECT
  '<tr><td>pg_stat_statements extension installed</td><td class="' ||
  CASE WHEN pgss_installed THEN 'good">Yes' ELSE 'crit">No' END ||
  '</td><td>' ||
  CASE WHEN pgss_installed THEN 'Top SQL and trendable workload diagnostics available.' ELSE 'S02 and SQL forensics quality is limited.' END ||
  '</td></tr>' ||
  '<tr><td>pg_stat_statements preloaded</td><td class="' ||
  CASE WHEN pgss_preloaded THEN 'good">Yes' ELSE 'warn">No/Unknown' END ||
  '</td><td>' ||
  CASE WHEN pgss_preloaded THEN 'Planner/SQL metrics are captured from startup.' ELSE 'Set shared_preload_libraries and restart for complete capture.' END ||
  '</td></tr>' ||
  '<tr><td>track_io_timing</td><td class="' ||
  CASE WHEN track_io_timing_on THEN 'good">On' ELSE 'warn">Off' END ||
  '</td><td>' ||
  CASE WHEN track_io_timing_on THEN 'I/O latency attribution is reliable.' ELSE 'Cannot separate CPU-bound vs IO-bound SQL accurately.' END ||
  '</td></tr>' ||
  '<tr><td>track_counts</td><td class="' ||
  CASE WHEN track_counts_on THEN 'good">On' ELSE 'crit">Off' END ||
  '</td><td>' ||
  CASE WHEN track_counts_on THEN 'Table/index statistics available.' ELSE 'Most PG360 health sections lose diagnostic signal.' END ||
  '</td></tr>' ||
  '<tr><td>Stats window since reset</td><td class="' ||
  CASE WHEN db_stats_window >= interval '6 hours' THEN 'good">' ELSE 'warn">' END ||
  replace(replace(replace(replace(replace(db_stats_window::text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td><td>' ||
  CASE WHEN db_stats_window >= interval '6 hours' THEN 'Sufficient time window for directional decisions.' ELSE 'Short window; treat rates and outliers as provisional.' END ||
  '</td></tr>'
FROM readiness;
\qecho '</tbody></table></div>'
\qecho '</div>'

\qecho '</div>'


-- =============================================================================
-- SECTION S19: HOT UPDATES, FILLFACTOR & WRITE AMPLIFICATION ADVISOR
-- =============================================================================
\qecho '<div class="section" id="s19">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">HOT Updates, Fillfactor &amp; Write Amplification</div>'
\qecho '    <div class="section-desc">HOT (Heap-Only Tuple) updates avoid index churn. Low HOT ratio + default fillfactor = unnecessary WAL and vacuum overhead.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding info"><div class="finding-header">'
\qecho '<span class="finding-title">Why HOT Updates Matter: PostgreSQL''s Most Underutilized Optimization</span>'
\qecho '<span class="severity-pill pill-info">EDUCATION</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>HOT Update:</strong> When an UPDATE modifies a row on the same page AND no indexed column changes,'
\qecho 'PostgreSQL can do a "Heap Only Tuple" update  updating just the heap page with no index changes.'
\qecho 'This is 5-10x cheaper than a normal update: no index entry added, no WAL for index pages.<br>'
\qecho '<strong>FILLFACTOR</strong> controls how full heap pages are packed: default is 100% (no free space).'
\qecho 'If you set FILLFACTOR=70, 30% of each page stays free for HOT updates on that same page.<br>'
\qecho '<strong>When to set FILLFACTOR &lt; 100:</strong> Tables with frequent UPDATEs to non-indexed columns.'
\qecho 'Cost: ~30% more storage. Benefit: 5-10x faster updates, less WAL, less vacuum pressure, better concurrency.'
\qecho '</div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">HOT Update Ratio by Table</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>HOT Updates</th><th>Total Updates</th>'
\qecho '<th>HOT Ratio %</th><th>Table Size</th><th>Fillfactor</th>'
\qecho '<th>Recommendation</th><th>Fix Script</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname||'.'||relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num good">' || to_char(n_tup_hot_upd,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_tup_upd,'FM999,999,999') || '</td>' ||
  '<td class="num ' ||
  CASE
    WHEN n_tup_upd = 0 THEN ''
    WHEN hot_ratio >= 90 THEN 'good'
    WHEN hot_ratio >= 50 THEN 'warn'
    ELSE 'crit'
  END || '">' ||
  CASE WHEN n_tup_upd = 0 THEN 'N/A' ELSE round(hot_ratio::numeric,1)::text || '%' END || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || COALESCE(fillfactor::text,'100') || '</td>' ||
  '<td class="' ||
  CASE
    WHEN n_tup_upd = 0 THEN '">No updates  N/A'
    WHEN hot_ratio >= 90 THEN 'good">Excellent  HOT updates working well'
    WHEN hot_ratio >= 50 THEN 'warn">Moderate  consider fillfactor=80'
    WHEN n_tup_upd > 10000 THEN 'crit">Low HOT ratio on high-update table  fillfactor tuning critical'
    ELSE 'warn">Low HOT  review if frequently updated'
  END || '</td>' ||
  '<td class="code-block">' ||
  CASE
    WHEN n_tup_upd > 1000 AND hot_ratio < 50
    THEN replace(replace(replace(replace(replace(
           format('ALTER TABLE %I.%I SET (fillfactor=75);', schemaname, relname)
         ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
    ELSE '-- No action needed'
  END || '</td></tr>'
FROM (
  SELECT
    t.relid, t.schemaname, t.relname,
    t.n_tup_upd, t.n_tup_hot_upd,
    CASE WHEN t.n_tup_upd > 0 THEN 100.0*t.n_tup_hot_upd/t.n_tup_upd ELSE 100 END AS hot_ratio,
    c.reloptions,
    (SELECT COALESCE(
      (regexp_match(array_to_string(c2.reloptions,','), 'fillfactor=(\d+)'))[1]::int,
      100)
     FROM pg_class c2 WHERE c2.oid = t.relid) AS fillfactor
  FROM pg_stat_user_tables t
  JOIN pg_class c ON c.oid = t.relid
  WHERE t.n_tup_upd > 0
) hot_stats
ORDER BY
  CASE WHEN n_tup_upd = 0 THEN 0 ELSE n_tup_upd * (1 - hot_ratio/100.0) END DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S19.2 Fillfactor candidate selection with verification and rollback guidance
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Fillfactor Candidate Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Update Volume</th><th>HOT Ratio%</th><th>Index Count</th><th>Current Fillfactor</th><th>Decision</th><th>Fix</th><th>Verify</th><th>Rollback</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(n_tup_upd,'FM999,999,999') || '</td>' ||
      '<td class="num ' ||
      CASE
        WHEN hot_ratio >= 80 THEN 'good'
        WHEN hot_ratio >= 50 THEN 'warn'
        ELSE 'crit'
      END || '">' || round(hot_ratio::numeric,1) || '</td>' ||
      '<td class="num">' || index_count || '</td>' ||
      '<td class="num">' || fillfactor || '</td>' ||
      '<td class="' ||
      CASE
        WHEN n_tup_upd >= 10000 AND hot_ratio < 60 AND fillfactor >= 90 THEN 'crit">Apply fillfactor=70-75 and monitor 48h'
        WHEN n_tup_upd >= 5000 AND hot_ratio < 80 THEN 'warn">Apply fillfactor=80 and monitor'
        WHEN hot_ratio >= 80 THEN 'good">No change required'
        ELSE '">Observe only'
      END || '</td>' ||
      '<td class="code-block">' ||
      CASE
        WHEN n_tup_upd >= 5000 AND hot_ratio < 80 THEN
          replace(replace(replace(replace(replace(
            format(
              'ALTER TABLE %I.%I SET (fillfactor=%s);',
              schemaname,
              relname,
              CASE WHEN hot_ratio < 60 THEN '75' ELSE '80' END
            )
          ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
        ELSE '-- No DDL'
      END || '</td>' ||
      '<td class="code-block">' ||
      'SELECT n_tup_upd,n_tup_hot_upd,ROUND(100.0*n_tup_hot_upd/NULLIF(n_tup_upd,0),2) AS hot_ratio FROM pg_stat_user_tables WHERE relid=' || relid || ';</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format('ALTER TABLE %I.%I RESET (fillfactor);', schemaname, relname)
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY n_tup_upd DESC
    ),
    '<tr><td colspan="9" class="table-empty">No update-heavy tables found for fillfactor tuning.</td></tr>'
  )
FROM (
  SELECT
    st.relid,
    st.schemaname,
    st.relname,
    st.n_tup_upd,
    CASE WHEN st.n_tup_upd > 0 THEN 100.0 * st.n_tup_hot_upd / st.n_tup_upd ELSE 100 END AS hot_ratio,
    COALESCE((regexp_match(array_to_string(c.reloptions,','), 'fillfactor=(\d+)'))[1]::int, 100) AS fillfactor,
    COALESCE((SELECT COUNT(*) FROM pg_index i WHERE i.indrelid = st.relid), 0) AS index_count
  FROM pg_stat_user_tables st
  JOIN pg_class c ON c.oid = st.relid
  WHERE st.n_tup_upd > 100
) cand;

\qecho '</tbody></table></div></div>'

-- S19.3 Write amplification posture linked to HOT and vacuum debt
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Write Amplification Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed Value</th><th>Interpretation</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

WITH wa AS (
  SELECT
    COALESCE(SUM(n_tup_upd),0) AS total_updates,
    COALESCE(SUM(n_tup_hot_upd),0) AS total_hot_updates,
    COALESCE(SUM(n_tup_upd - n_tup_hot_upd),0) AS non_hot_updates,
    COALESCE(SUM(n_dead_tup),0) AS total_dead_tuples,
    COALESCE(SUM(n_live_tup),0) AS total_live_tuples
  FROM pg_stat_user_tables
)
SELECT
  '<tr><td>Non-HOT updates</td><td class="num">' || to_char(non_hot_updates,'FM999,999,999') || '</td><td class="' ||
  CASE
    WHEN total_updates = 0 THEN 'good">No updates in stats window'
    WHEN non_hot_updates::numeric / NULLIF(total_updates,0) > 0.5 THEN 'crit">High write amplification from index churn'
    WHEN non_hot_updates::numeric / NULLIF(total_updates,0) > 0.2 THEN 'warn">Moderate write amplification'
    ELSE 'good">HOT coverage is healthy'
  END ||
  '</td><td>Use S19.2 to target fillfactor only on high-update, HOT-poor tables.</td></tr>' ||
  '<tr><td>Database HOT ratio</td><td class="num">' ||
  CASE WHEN total_updates = 0 THEN 'N/A'
       ELSE round(100.0 * total_hot_updates / NULLIF(total_updates,0),2)::text || '%' END ||
  '</td><td class="' ||
  CASE
    WHEN total_updates = 0 THEN 'good">No update workload in current window'
    WHEN (100.0 * total_hot_updates / NULLIF(total_updates,0)) >= 80 THEN 'good">Target met for update-heavy systems'
    WHEN (100.0 * total_hot_updates / NULLIF(total_updates,0)) >= 60 THEN 'warn">Below target, tune top offenders'
    ELSE 'crit">Material efficiency gap, prioritize remediation'
  END ||
  '</td><td>Goal: >80% HOT ratio for frequently updated tables.</td></tr>' ||
  '<tr><td>Dead tuple ratio</td><td class="num">' ||
  round(100.0 * total_dead_tuples::numeric / NULLIF(total_live_tuples + total_dead_tuples,0),2) || '%</td><td class="' ||
  CASE
    WHEN total_live_tuples + total_dead_tuples = 0 THEN 'good">No tuple statistics yet'
    WHEN total_dead_tuples::numeric / NULLIF(total_live_tuples + total_dead_tuples,0) > 0.2 THEN 'crit">Vacuum debt likely impacting performance'
    WHEN total_dead_tuples::numeric / NULLIF(total_live_tuples + total_dead_tuples,0) > 0.1 THEN 'warn">Watch autovacuum throughput'
    ELSE 'good">Vacuum posture acceptable'
  END ||
  '</td><td>Cross-check S21 vacuum urgency and table-level autovacuum settings.</td></tr>'
FROM wa;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S20: PLANNER STATISTICS QUALITY & ESTIMATION ERRORS
-- =============================================================================
\qecho '<div class="section" id="s20">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Planner Statistics Quality &amp; Estimation Errors</div>'
\qecho '    <div class="section-desc">Stale statistics cause wrong query plans. This section identifies tables where the planner is likely making bad row estimates.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding critical"><div class="finding-header">'
\qecho '<span class="finding-title">Why This Matters: A Bad Estimate = A Catastrophically Wrong Plan</span>'
\qecho '<span class="severity-pill pill-critical">PLAN QUALITY</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>The #1 cause of slow queries in PostgreSQL is bad planner row estimates.</strong><br>'
\qecho 'If the planner thinks a table has 100 rows but it actually has 1,000,000 rows, it will:'
\qecho '(1) Choose a Nested Loop join instead of a Hash Join  10,000x slower.'
\qecho '(2) Not use an index it should use. (3) Not allocate enough work_mem for sorts.<br>'
\qecho '<strong>Root Cause:</strong> ANALYZE not run after large data loads, or autovacuum falling behind.<br>'
\qecho '<strong>Root Fix:</strong> ANALYZE table; or increase autovacuum_analyze_scale_factor.'
\qecho 'For frequently updated tables: SET autovacuum_analyze_scale_factor TO 0.01 per table.'
\qecho '</div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tables With Stale Statistics</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Est. Row Count</th><th>Actual Live Rows</th>'
\qecho '<th>Estimate Error</th><th>Modifications Since Analyze</th><th>Last Analyze</th>'
\qecho '<th>Action</th><th>Fix Script</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname||'.'||relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || to_char(pg_reltuples::bigint,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' ||
  CASE
    WHEN pg_reltuples = 0 AND n_live_tup > 0 THEN 'crit'
    WHEN pg_reltuples > 0 AND ABS(1 - n_live_tup::numeric/NULLIF(pg_reltuples,0)) > 0.5 THEN 'crit'
    WHEN pg_reltuples > 0 AND ABS(1 - n_live_tup::numeric/NULLIF(pg_reltuples,0)) > 0.2 THEN 'warn'
    ELSE 'good'
  END || '">' ||
  CASE
    WHEN pg_reltuples = 0 THEN 'N/A (never analyzed)'
    ELSE round(ABS(1 - n_live_tup::numeric/NULLIF(pg_reltuples,0))*100,0)::text || '% off'
  END || '</td>' ||
  '<td class="num ' || CASE WHEN n_mod_since_analyze > 100000 THEN 'crit' WHEN n_mod_since_analyze > 10000 THEN 'warn' ELSE '' END || '">' ||
  to_char(n_mod_since_analyze,'FM999,999,999') || '</td>' ||
  '<td>' || COALESCE(to_char(last_analyze,'YYYY-MM-DD HH24:MI'), COALESCE(to_char(last_autoanalyze,'YYYY-MM-DD HH24:MI'),'<span class="crit">NEVER</span>')) || '</td>' ||
  '<td class="' ||
  CASE
    WHEN pg_reltuples = 0 AND n_live_tup > 1000 THEN 'crit"> Never analyzed  planner blind'
    WHEN ABS(1 - n_live_tup::numeric/NULLIF(pg_reltuples,0)) > 0.5 THEN 'crit"> 50%+ estimate error  bad plans likely'
    WHEN n_mod_since_analyze > 100000 THEN 'warn"> High modification rate  analyze soon'
    ELSE 'good">OK'
  END || '</td>' ||
  '<td class="code-block">' ||
  replace(replace(replace(replace(replace(
    format('ANALYZE VERBOSE %I.%I;', schemaname, relname)
  ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td></tr>'
FROM (
  SELECT
    s.schemaname, s.relname, s.relid,
    c.reltuples::numeric AS pg_reltuples,
    s.n_live_tup, s.n_mod_since_analyze,
    s.last_analyze, s.last_autoanalyze
  FROM pg_stat_user_tables s
  JOIN pg_class c ON c.oid = s.relid
  WHERE s.n_live_tup > 1000
    OR c.reltuples = 0
) stat_check
ORDER BY
  CASE WHEN pg_reltuples = 0 AND n_live_tup > 0 THEN 0
       ELSE ABS(1 - n_live_tup::numeric/NULLIF(pg_reltuples,0)) END DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S20.2 Tables with no extended statistics (correlation-blind planner)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Column Correlation Gaps</div>'
\qecho '<div class="finding high"><div class="finding-header">'
\qecho '<span class="finding-title">Multi-column WHERE clauses on correlated columns produce wrong estimates without extended stats</span>'
\qecho '<span class="severity-pill pill-high">PLAN QUALITY</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>Problem:</strong> When you write WHERE city=''NYC'' AND state=''NY'', the planner multiplies'
\qecho 'the individual selectivities: 5%  10% = 0.5%. But city and state are correlated!'
\qecho 'The actual selectivity might be 8%. This mis-estimate causes nested loops instead of hash joins.<br>'
\qecho '<strong>Oracle equivalent:</strong> Oracle''s adaptive query optimization handles this automatically.'
\qecho 'PostgreSQL requires manual CREATE STATISTICS.<br>'
\qecho '<strong>Fix:</strong> CREATE STATISTICS stat_city_state ON city, state FROM addresses;<br>'
\qecho 'Then: ANALYZE addresses;<br>'
\qecho '<strong>Verify:</strong> EXPLAIN shows actual vs estimated rows  estimate should be within 2x of actual.'
\qecho '</div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Existing Extended Stats</th><th>Column Count</th><th>Table Size</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname||'.'||c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN stat_count = 0 THEN 'warn' ELSE 'good' END || '">' || stat_count || '</td>' ||
  '<td class="num">' || col_count || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(c.oid)) || '</td>' ||
  '<td class="' ||
  CASE
    WHEN stat_count = 0 AND col_count > 5 AND pg_total_relation_size(c.oid) > 10485760
      THEN 'warn">Consider extended stats for multi-column WHERE queries'
    WHEN stat_count > 0
      THEN 'good">Has extended statistics'
    ELSE '">Low priority  small table'
  END || '</td></tr>'
FROM (
  SELECT
    c.oid, c.relname, n.nspname,
    COUNT(a.attnum) AS col_count,
    (SELECT COUNT(*) FROM pg_statistic_ext se WHERE se.stxrelid = c.oid) AS stat_count
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
  GROUP BY c.oid, c.relname, n.nspname
  HAVING pg_total_relation_size(c.oid) > 1048576
) stats_check
JOIN pg_class c ON c.oid = stats_check.oid
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY stat_count ASC, pg_total_relation_size(c.oid) DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S20.3 Extended statistics remediation queue
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Extended Statistics Queue</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Candidate Columns</th><th>Why</th><th>Fix</th><th>Verify</th><th>Rollback</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || replace(replace(replace(replace(replace(column_list,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>Multi-column predicates likely correlated; planner needs dependency/MCV stats.</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format(
          'CREATE STATISTICS %I (dependencies, mcv) ON %s FROM %I.%I; ANALYZE %I.%I;',
          stat_name,
          column_list,
          schemaname,
          relname,
          schemaname,
          relname
        )
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td class="code-block">EXPLAIN (ANALYZE, BUFFERS) -- compare estimated vs actual rows before/after.</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format('DROP STATISTICS IF EXISTS %I;', stat_name)
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY table_bytes DESC
    ),
    '<tr><td colspan="6" class="table-empty">No immediate extended-statistics candidates identified.</td></tr>'
  )
FROM (
  SELECT
    n.nspname AS schemaname,
    c.relname,
    pg_total_relation_size(c.oid) AS table_bytes,
    left('st_' || c.relname || '_' || substr(md5(string_agg(a.attname, ',' ORDER BY k.ordinality)), 1, 8), 63) AS stat_name,
    string_agg(quote_ident(a.attname), ', ' ORDER BY k.ordinality) AS column_list
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN LATERAL unnest(i.indkey[0:i.indnkeyatts-1]) WITH ORDINALITY AS k(attnum, ordinality) ON true
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum AND NOT a.attisdropped
  WHERE c.relkind = 'r'
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND i.indnkeyatts BETWEEN 2 AND 4
    AND pg_total_relation_size(c.oid) > 10485760
    AND NOT EXISTS (SELECT 1 FROM pg_statistic_ext se WHERE se.stxrelid = c.oid)
  GROUP BY n.nspname, c.relname, c.oid
  LIMIT 20
) q;

\qecho '</tbody></table></div></div>'

-- S20.4 Analyze policy tuning queue for volatile tables
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Analyze Policy Tuning Queue</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Rows (live)</th><th>n_mod_since_analyze</th><th>Change Ratio%</th><th>Recommendation</th><th>Fix</th><th>Verify</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(n_mod_since_analyze,'FM999,999,999') || '</td>' ||
      '<td class="num ' || CASE WHEN mod_ratio >= 20 THEN 'crit' WHEN mod_ratio >= 10 THEN 'warn' ELSE 'good' END || '">' ||
      round(mod_ratio::numeric,1) || '</td>' ||
      '<td class="' ||
      CASE
        WHEN mod_ratio >= 20 THEN 'crit">Aggressive analyze cadence needed'
        WHEN mod_ratio >= 10 THEN 'warn">Lower analyze scale factor recommended'
        ELSE 'good">Current cadence likely acceptable'
      END || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format(
          'ALTER TABLE %I.%I SET (autovacuum_analyze_scale_factor=%s, autovacuum_analyze_threshold=1000);',
          schemaname,
          relname,
          CASE WHEN mod_ratio >= 20 THEN '0.005' WHEN mod_ratio >= 10 THEN '0.01' ELSE '0.02' END
        )
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td class="code-block">SELECT n_mod_since_analyze,last_autoanalyze,last_analyze FROM pg_stat_user_tables WHERE relid=' || relid || ';</td>' ||
      '</tr>',
      E'\n' ORDER BY mod_ratio DESC, n_mod_since_analyze DESC
    ),
    '<tr><td colspan="7" class="table-empty">No high-modification analyze tuning candidates found.</td></tr>'
  )
FROM (
  SELECT
    relid,
    schemaname,
    relname,
    n_live_tup,
    n_mod_since_analyze,
    100.0 * n_mod_since_analyze::numeric / NULLIF(n_live_tup, 0) AS mod_ratio
  FROM pg_stat_user_tables
  WHERE n_live_tup >= 10000
    AND n_mod_since_analyze > 0
) q
WHERE mod_ratio >= 5;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S21: AUTOVACUUM FULL ADVISOR
-- =============================================================================
\qecho '<div class="section" id="s21">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Autovacuum Full Advisor</div>'
\qecho '    <div class="section-desc">Per-table vacuum thresholds, cost delay impact, wraparound countdown, and per-table tuning scripts.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding critical"><div class="finding-header">'
\qecho '<span class="finding-title">Autovacuum is the Most Misunderstood and Most Critical PostgreSQL Feature</span>'
\qecho '<span class="severity-pill pill-critical">CRITICAL KNOWLEDGE</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>What autovacuum does:</strong> (1) Reclaims dead tuple space. (2) Prevents XID wraparound.'
\qecho '(3) Updates statistics for the planner. Without it: table bloat, planner failures, database shutdown.<br>'
\qecho '<strong>The #1 mistake:</strong> The default vacuum threshold is 20% dead rows (autovacuum_vacuum_scale_factor=0.2).'
\qecho 'On a 100M row table, vacuum only triggers after 20M dead rows accumulate.'
\qecho 'By then your table is 30% larger, sequential scans are slower, and hot data is being evicted from cache.<br>'
\qecho '<strong>The fix:</strong> Set per-table storage parameters:'
\qecho 'ALTER TABLE big_table SET (autovacuum_vacuum_scale_factor=0.01, autovacuum_analyze_scale_factor=0.005);'
\qecho 'This triggers vacuum after 1% dead rows (1M rows) instead of 20% (20M rows).<br>'
\qecho '<strong>Cost delay:</strong> autovacuum_vacuum_cost_delay throttles vacuum speed to avoid I/O saturation.'
\qecho 'On modern SSDs, set to 2ms (from default 20ms) or 0 for very busy tables.'
\qecho '</div></div>'

-- S21.1 Autovacuum current configuration
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Autovacuum Global Configuration</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current</th><th>Recommended Range</th><th>Status</th><th>Impact if Wrong</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td class="num">' || param || '</td><td class="num">' || current_val || '</td><td class="num ' ||
  CASE WHEN status='OK' THEN 'good' WHEN status='WARN' THEN 'warn' ELSE 'crit' END ||
  '">' || recommended || '</td><td class="' ||
  CASE WHEN status='OK' THEN 'good"> OK' WHEN status='WARN' THEN 'warn"> Review' ELSE 'crit"> Action needed' END ||
  '</td><td>' || impact || '</td></tr>'
FROM (
  SELECT
    'autovacuum_vacuum_scale_factor' AS param,
    (SELECT setting FROM pg_settings WHERE name='autovacuum_vacuum_scale_factor') AS current_val,
    '0.010.05 (default 0.2 is too high for large tables)' AS recommended,
    CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name='autovacuum_vacuum_scale_factor') > 0.1 THEN 'WARN' ELSE 'OK' END AS status,
    'High value = vacuum too infrequent = table bloat on large tables' AS impact
  UNION ALL SELECT
    'autovacuum_analyze_scale_factor',
    (SELECT setting FROM pg_settings WHERE name='autovacuum_analyze_scale_factor'),
    '0.0050.02 (default 0.1 causes stale stats)',
    CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name='autovacuum_analyze_scale_factor') > 0.05 THEN 'WARN' ELSE 'OK' END,
    'High value = stats go stale = bad query plans'
  UNION ALL SELECT
    'autovacuum_max_workers',
    (SELECT setting FROM pg_settings WHERE name='autovacuum_max_workers'),
    '510 for busy systems (default 3 is often too low)',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='autovacuum_max_workers') < 4 THEN 'WARN' ELSE 'OK' END,
    'Too few workers = vacuum queue builds up = bloat'
  UNION ALL SELECT
    'autovacuum_vacuum_cost_delay',
    (SELECT setting FROM pg_settings WHERE name='autovacuum_vacuum_cost_delay') || 'ms',
    '2ms (SSD) or 20ms (HDD)  default 2ms in PG14+',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='autovacuum_vacuum_cost_delay') > 10 THEN 'WARN' ELSE 'OK' END,
    'High delay = vacuum runs too slowly = bloat accumulates faster than vacuum cleans'
  UNION ALL SELECT
    'autovacuum_vacuum_threshold',
    (SELECT setting FROM pg_settings WHERE name='autovacuum_vacuum_threshold'),
    '50100 (default 50  usually OK)',
    'OK',
    'Minimum dead rows before vacuum triggers (absolute, added to scale factor)'
  UNION ALL SELECT
    'autovacuum_vacuum_insert_scale_factor',
    (SELECT setting FROM pg_settings WHERE name='autovacuum_vacuum_insert_scale_factor'),
    '0.020.1 (vacuum after inserts to avoid visibility map misses)',
    'OK',
    'Too high = insert-heavy tables never vacuum = no visibility map = always seqscan'
) av_config;

\qecho '</tbody></table></div></div>'

-- S21.2 Per-table vacuum urgency matrix
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Per-Table Vacuum Urgency Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Size</th><th>Live Rows</th>'
\qecho '<th>Dead Rows</th><th>Dead%</th><th>Threshold (calc)</th>'
\qecho '<th>Over Threshold?</th><th>Last Vacuum</th><th>Last Analyze</th>'
\qecho '<th>Urgency</th><th>Recommended Per-Table Setting</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname||'.'||relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' || CASE WHEN n_dead_tup > threshold THEN 'crit' WHEN n_dead_tup > threshold*0.7 THEN 'warn' ELSE '' END || '">' ||
  to_char(n_dead_tup,'FM999,999,999') || '</td>' ||
  '<td class="num">' || round(dead_pct::numeric,1) || '%</td>' ||
  '<td class="num">' || to_char(threshold::bigint,'FM999,999,999') || '</td>' ||
  '<td class="' || CASE WHEN n_dead_tup > threshold THEN 'crit"> Over threshold' WHEN n_dead_tup > threshold*0.7 THEN 'warn"> Near threshold' ELSE 'good"> Under threshold' END || '</td>' ||
  '<td>' || COALESCE(to_char(last_autovacuum,'MM-DD HH24:MI'),COALESCE(to_char(last_vacuum,'MM-DD HH24:MI'),'<span class="crit">NEVER</span>')) || '</td>' ||
  '<td>' || COALESCE(to_char(last_autoanalyze,'MM-DD HH24:MI'),COALESCE(to_char(last_analyze,'MM-DD HH24:MI'),'<span class="crit">NEVER</span>')) || '</td>' ||
  '<td><span class="severity-pill ' ||
  CASE
    WHEN n_dead_tup > threshold AND n_live_tup > 1000000 THEN 'pill-critical"> CRITICAL'
    WHEN n_dead_tup > threshold THEN 'pill-high"> HIGH'
    WHEN n_dead_tup > threshold * 0.7 THEN 'pill-medium"> MEDIUM'
    ELSE 'pill-good"> OK'
  END || '</span></td>' ||
  '<td class="code-block">' ||
  CASE WHEN n_live_tup > 1000000
  THEN replace(replace(replace(replace(replace(
         format(
           'ALTER TABLE %I.%I SET (autovacuum_vacuum_scale_factor=0.01, autovacuum_analyze_scale_factor=0.005);',
           schemaname,
           relname
         )
       ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
  ELSE '-- Default settings sufficient'
  END || '</td></tr>'
FROM (
  SELECT
    s.*,
    n_dead_tup::numeric/(n_live_tup+n_dead_tup+1)*100 AS dead_pct,
    (
      (SELECT setting::numeric FROM pg_settings WHERE name='autovacuum_vacuum_threshold') +
      (SELECT setting::numeric FROM pg_settings WHERE name='autovacuum_vacuum_scale_factor') * n_live_tup
    ) AS threshold
  FROM pg_stat_user_tables s
  WHERE n_live_tup + n_dead_tup > 100
) t
ORDER BY
  CASE WHEN n_dead_tup > threshold THEN 0 ELSE 1 END,
  n_dead_tup DESC
LIMIT 40;

\qecho '</tbody></table></div></div>'

-- S21.3 Autovacuum activity right now
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Autovacuum Workers Currently Running</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>PID</th><th>Table</th><th>Phase</th><th>Heap Blks Total</th>'
\qecho '<th>Heap Blks Vacuumed</th><th>Progress%</th><th>Duration</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || a.pid || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(v.relid::regclass::text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(v.phase,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td class="num">' || v.heap_blks_total || '</td>' ||
      '<td class="num good">' || v.heap_blks_vacuumed || '</td>' ||
      '<td class="num">' || COALESCE(round(100.0*v.heap_blks_vacuumed/NULLIF(v.heap_blks_total,0),1)::text,'0') || '%</td>' ||
      '<td class="num">' || COALESCE(to_char(now()-a.query_start,'MI"m"SS"s"'),'') || '</td>' ||
      '</tr>',
      ''
    ),
    '<tr><td colspan="7" class="table-empty"> No autovacuum workers running at this moment</td></tr>'
  )
FROM pg_stat_progress_vacuum v
JOIN pg_stat_activity a ON a.pid = v.pid
WHERE v.phase IS NOT NULL;

\qecho '</tbody></table></div></div>'

-- S21.4 XID wraparound countdown per database AND table
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">XID Wraparound Countdown (Database Level)</div>'
\qecho '<div class="finding critical"><div class="finding-header">'
\qecho '<span class="finding-title">XID wraparound will cause PostgreSQL to refuse all writes and initiate emergency shutdown</span>'
\qecho '<span class="severity-pill pill-critical">OUTAGE RISK</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>How it works:</strong> PostgreSQL uses 32-bit transaction IDs (XIDs). After ~2.1 billion transactions,'
\qecho 'XIDs wrap around. Old data appears to be "in the future" and becomes invisible (data loss simulation).'
\qecho 'PostgreSQL will shut down at 40M XIDs before wrap to prevent this.<br>'
\qecho '<strong>Warning levels:</strong> age > 1.5B = CRITICAL (start emergency VACUUM FREEZE).'
\qecho 'age > 1B = HIGH (schedule VACUUM FREEZE this week). age > 500M = MEDIUM (monitor).<br>'
\qecho '<strong>Emergency fix:</strong> VACUUM FREEZE VERBOSE schema.large_table;<br>'
\qecho '<strong>Full database emergency:</strong> vacuumdb --all --freeze --analyze-in-stages<br>'
\qecho '<strong>Prevention:</strong> Ensure autovacuum is running and not falling behind. Monitor with this report weekly.'
\qecho '</div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Database</th><th>Datfrozenxid Age</th><th>% of 2B Limit</th>'
\qecho '<th>Est. Transactions Until Danger</th><th>Urgency</th><th>Emergency Fix</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(datname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN age(datfrozenxid) > 1500000000 THEN 'crit' WHEN age(datfrozenxid) > 750000000 THEN 'warn' ELSE '' END || '">' ||
  to_char(age(datfrozenxid),'FM999,999,999') || '</td>' ||
  '<td class="num">' || round(age(datfrozenxid)*100.0/2000000000,2) || '%</td>' ||
  '<td class="num">' || to_char((2000000000 - age(datfrozenxid))::bigint,'FM999,999,999') || '</td>' ||
  '<td><span class="severity-pill ' ||
  CASE
    WHEN age(datfrozenxid) > 1500000000 THEN 'pill-critical"> EMERGENCY'
    WHEN age(datfrozenxid) > 1000000000 THEN 'pill-high"> CRITICAL'
    WHEN age(datfrozenxid) > 500000000  THEN 'pill-medium"> MEDIUM'
    ELSE 'pill-good"> OK'
  END || '</span></td>' ||
  '<td class="code-block">' ||
  CASE WHEN age(datfrozenxid) > 500000000
  THEN replace(replace(replace(replace(replace(
         format('vacuumdb -d %s --freeze --analyze -j 4 -v', quote_literal(datname))
       ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
  ELSE '-- No action needed'
  END || '</td></tr>'
FROM pg_database
WHERE datname NOT IN ('template0','template1')
ORDER BY age(datfrozenxid) DESC;

\qecho '</tbody></table></div></div>'

-- S21.5 Table-level XID age  per table countdown
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Per-Table XID Age</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>XID Age</th><th>% of 2B Limit</th><th>Table Size</th>'
\qecho '<th>Last Freeze Vacuum</th><th>Urgency</th><th>Fix</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname||'.'||c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num ' || CASE WHEN age(c.relfrozenxid) > 1500000000 THEN 'crit' WHEN age(c.relfrozenxid) > 750000000 THEN 'warn' ELSE '' END || '">' ||
  to_char(age(c.relfrozenxid),'FM999,999,999') || '</td>' ||
  '<td class="num">' || round(age(c.relfrozenxid)*100.0/2000000000,2) || '%</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(c.oid)) || '</td>' ||
  '<td>' || COALESCE(to_char(s.last_autovacuum,'YYYY-MM-DD'),'<span class="warn">Unknown</span>') || '</td>' ||
  '<td><span class="severity-pill ' ||
  CASE
    WHEN age(c.relfrozenxid) > 1500000000 THEN 'pill-critical"> FREEZE NOW'
    WHEN age(c.relfrozenxid) > 750000000  THEN 'pill-high"> Freeze Soon'
    WHEN age(c.relfrozenxid) > 200000000  THEN 'pill-medium"> Monitor'
    ELSE 'pill-good"> OK'
  END || '</span></td>' ||
  '<td class="code-block">' ||
  CASE WHEN age(c.relfrozenxid) > 200000000
  THEN 'VACUUM FREEZE ANALYZE ' ||
       replace(replace(replace(replace(replace(n.nspname||'.'||c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || ';'
  ELSE '-- OK'
  END || '</td></tr>'
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind IN ('r','m')
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
ORDER BY age(c.relfrozenxid) DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S21.6 Safe application guardrails (fix / verify / rollback matrix)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Autovacuum Tuning Guardrails</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Recommended Table Settings</th><th>Do Not Apply If</th><th>Verify</th><th>Rollback</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format(
          'ALTER TABLE %I.%I SET (autovacuum_vacuum_scale_factor=%s, autovacuum_analyze_scale_factor=%s, autovacuum_vacuum_threshold=1000, autovacuum_analyze_threshold=1000);',
          schemaname,
          relname,
          CASE WHEN table_bytes >= 10737418240 THEN '0.005' ELSE '0.01' END,
          CASE WHEN table_bytes >= 10737418240 THEN '0.0025' ELSE '0.005' END
        )
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '<td class="' ||
      CASE
        WHEN table_bytes >= 53687091200 THEN 'warn">Table is >50GB during peak load window; schedule during low traffic.'
        WHEN n_dead_tup < 10000 THEN '">Table churn is low; default settings may already be sufficient.'
        ELSE 'good">Safe candidate for tuned autovacuum cadence.'
      END || '</td>' ||
      '<td class="code-block">SELECT n_dead_tup,last_autovacuum,last_autoanalyze FROM pg_stat_user_tables WHERE relid=' || relid || ';</td>' ||
      '<td class="code-block">' ||
      replace(replace(replace(replace(replace(
        format(
          'ALTER TABLE %I.%I RESET (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_threshold);',
          schemaname,
          relname
        )
      ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY table_bytes DESC, n_dead_tup DESC
    ),
    '<tr><td colspan="5" class="table-empty">No high-value per-table autovacuum tuning candidates found.</td></tr>'
  )
FROM (
  SELECT
    relid,
    schemaname,
    relname,
    n_dead_tup,
    pg_total_relation_size(relid) AS table_bytes
  FROM pg_stat_user_tables
  WHERE (n_live_tup > 100000 OR pg_total_relation_size(relid) > 5368709120)
    AND n_dead_tup > 5000
  ORDER BY n_dead_tup DESC
  LIMIT 25
) q;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S22: CONNECTION POOLING ADVISOR
-- =============================================================================
\qecho '<div class="section" id="s22">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Connection Pooling Advisor</div>'
\qecho '    <div class="section-desc">Connection utilization, idle waste, pooler detection, and PgBouncer setup recommendations.</div>'
\qecho '  </div>'
\qecho '</div>'

SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN total_conns::numeric/max_conns > 0.9 THEN 'critical' WHEN total_conns::numeric/max_conns > 0.7 THEN 'warning' ELSE 'good' END || '">' ||
  '<div class="card-label">Connection Utilization</div>' ||
  '<div class="card-value">' || round(100.0*total_conns/max_conns,1) || '%</div>' ||
  '<div class="card-sub">' || total_conns || ' of ' || max_conns || ' max</div></div>' ||

  '<div class="card ' || CASE WHEN idle_pct > 60 THEN 'warning' ELSE 'good' END || '">' ||
  '<div class="card-label">Idle Connection Waste</div>' ||
  '<div class="card-value">' || round(idle_pct,0) || '%</div>' ||
  '<div class="card-sub">' || idle_conns || ' idle connections</div></div>' ||

  '<div class="card ' || CASE WHEN idle_xact_conns > 5 THEN 'critical' WHEN idle_xact_conns > 0 THEN 'warning' ELSE 'good' END || '">' ||
  '<div class="card-label">Idle-In-Transaction</div>' ||
  '<div class="card-value">' || idle_xact_conns || '</div>' ||
  '<div class="card-sub">Holding open transactions</div></div>' ||

  '<div class="card"><div class="card-label">RAM Wasted on Idle</div>' ||
  '<div class="card-value">' || pg_size_pretty((idle_conns * 5 * 1024 * 1024)::bigint) || '</div>' ||
  '<div class="card-sub">~5MB per idle connection</div></div>' ||
  '</div>'
FROM (
  SELECT
    COUNT(*) AS total_conns,
    COUNT(*) FILTER (WHERE state='idle') AS idle_conns,
    COUNT(*) FILTER (WHERE state ILIKE '%idle in transaction%') AS idle_xact_conns,
    100.0*COUNT(*) FILTER (WHERE state='idle')/NULLIF(COUNT(*),0) AS idle_pct,
    (SELECT setting::int FROM pg_settings WHERE name='max_connections') AS max_conns
  FROM pg_stat_activity
  WHERE datname = current_database()
) conn_stats;

-- S22.1a Connection saturation and queue risk
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Connection Saturation and Queue Risk</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Effective Client Slots</th><th>Client Backends</th><th>Active</th><th>Idle</th><th>Idle in Tx</th><th>Utilization %</th><th>Risk Label</th><th>Action Hint</th>'
\qecho '</tr></thead><tbody>'

WITH cfg AS (
  SELECT
    current_setting('max_connections')::int AS max_connections,
    COALESCE(NULLIF(current_setting('superuser_reserved_connections', true), ''), '0')::int AS superuser_reserved_connections,
    COALESCE(NULLIF(current_setting('reserved_connections', true), ''), '0')::int AS reserved_connections
), act AS (
  SELECT
    count(*) FILTER (WHERE backend_type = 'client backend') AS client_backends,
    count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'active') AS active_client_backends,
    count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'idle') AS idle_client_backends,
    count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'idle in transaction') AS idle_in_tx_client_backends
  FROM pg_stat_activity
)
SELECT
  '<tr><td class="num">' || (max_connections - superuser_reserved_connections - reserved_connections) || '</td>' ||
  '<td class="num">' || client_backends || '</td>' ||
  '<td class="num">' || active_client_backends || '</td>' ||
  '<td class="num">' || idle_client_backends || '</td>' ||
  '<td class="num ' || CASE WHEN idle_in_tx_client_backends > 0 THEN 'warn' ELSE 'good' END || '">' || idle_in_tx_client_backends || '</td>' ||
  '<td class="num ' ||
  CASE
    WHEN client_backends >= (max_connections - superuser_reserved_connections - reserved_connections) THEN 'crit'
    WHEN client_backends >= (max_connections * 0.85) THEN 'warn'
    ELSE 'good'
  END || '">' ||
  round(
    CASE WHEN max_connections = 0 THEN 0
         ELSE 100.0 * client_backends::numeric / max_connections
    END,
    2
  ) || '%</td>' ||
  '<td class="' ||
  CASE
    WHEN client_backends >= (max_connections - superuser_reserved_connections - reserved_connections) THEN 'crit">SATURATED'
    WHEN client_backends >= (max_connections * 0.85) THEN 'warn">HIGH_UTILIZATION'
    WHEN idle_client_backends > active_client_backends * 2 THEN 'warn">IDLE_HEAVY_PATTERN'
    ELSE 'good">HEALTHY_HEADROOM'
  END || '</td><td>' ||
  CASE
    WHEN idle_in_tx_client_backends > 0 THEN 'Investigate idle-in-transaction sessions first; they hold locks and pins.'
    WHEN client_backends >= (max_connections * 0.85) THEN 'Validate pool queue depth and pool mode in PgBouncer or proxy layer.'
    ELSE 'No urgent saturation signal from PostgreSQL-side evidence.'
  END || '</td></tr>'
FROM cfg CROSS JOIN act;

\qecho '</tbody></table></div></div>'

-- Pooler detection
SELECT
  CASE
    WHEN EXISTS (
      SELECT 1 FROM pg_stat_activity
      WHERE application_name ILIKE '%pgbouncer%' OR application_name ILIKE '%pgpool%'
    )
    THEN '<div class="finding good"><div class="finding-header">' ||
         '<span class="finding-title">Connection pooler detected (PgBouncer or pgPool)</span>' ||
         '<span class="severity-pill pill-good">POOLER ACTIVE</span></div>' ||
         '<div class="finding-body">Good  a connection pooler is in use. ' ||
         'Verify it is using transaction pooling mode for maximum efficiency.</div></div>'
    WHEN (SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle') > 20
    THEN '<div class="finding critical"><div class="finding-header">' ||
         '<span class="finding-title">No connection pooler detected + high idle count  RAM waste and connection limit risk</span>' ||
         '<span class="severity-pill pill-critical">POOLER REQUIRED</span></div>' ||
         '<div class="finding-body">' ||
         '<strong>Root Cause:</strong> Without a pooler, each application thread/process holds a dedicated PostgreSQL backend.'
         || ' Even idle backends use ~5MB RAM and occupy a max_connections slot.<br>' ||
         '<strong>Impact:</strong> At 200 connections, ~1GB wasted on idle backends. Connection exhaustion under load.<br>' ||
         '<strong>Fix:</strong> Deploy PgBouncer in transaction pooling mode:<br>' ||
         '<pre class="finding-fix">' ||
         '# pgbouncer.ini\n' ||
         '[databases]\n' ||
         'mydb = host=127.0.0.1 port=5432 dbname=mydb\n\n' ||
         '[pgbouncer]\n' ||
         'pool_mode = transaction\n' ||
         'max_client_conn = 1000\n' ||
         'default_pool_size = 20\n' ||
         'reserve_pool_size = 5\n' ||
         'server_idle_timeout = 600\n' ||
         'listen_port = 6432\n' ||
         'auth_type = md5\n' ||
         '</pre>' ||
         '<strong>Result:</strong> 1000 app connections  20 PostgreSQL connections. 98% fewer connections.' ||
         '</div></div>'
    ELSE '<div class="finding info"><div class="finding-header">' ||
         '<span class="finding-title">No pooler detected. Low idle count  monitor as traffic grows</span>' ||
         '<span class="severity-pill pill-info">MONITOR</span></div></div>'
  END;

-- S22.2 Connection distribution by user/app/state
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Connection Distribution</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>User</th><th>Application</th><th>Active</th><th>Idle</th>'
\qecho '<th>Idle in Txn</th><th>Waiting</th><th>Oldest (sec)</th><th>Total</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(COALESCE(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND usename = current_user THEN :'pg360_redaction_token' ELSE usename END,'?'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(COALESCE(left(application_name,30),'?'),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num good">' || COUNT(*) FILTER (WHERE state='active') || '</td>' ||
  '<td class="num">' || COUNT(*) FILTER (WHERE state='idle') || '</td>' ||
  '<td class="num ' || CASE WHEN COUNT(*) FILTER (WHERE state ILIKE '%idle in transaction%') > 0 THEN 'crit' ELSE '' END || '">' ||
  COUNT(*) FILTER (WHERE state ILIKE '%idle in transaction%') || '</td>' ||
  '<td class="num ' || CASE WHEN COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state='active') > 0 THEN 'warn' ELSE '' END || '">' ||
  COUNT(*) FILTER (WHERE wait_event IS NOT NULL AND state='active') || '</td>' ||
  '<td class="num">' ||
  COALESCE(round(MAX(EXTRACT(EPOCH FROM (now()-backend_start)))::numeric,0)::text,'') || '</td>' ||
  '<td class="num">' || COUNT(*) || '</td>' ||
  '</tr>'
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
GROUP BY usename, application_name
ORDER BY COUNT(*) DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S22.3 Pool sizing calculator and pooling mode guidance
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Pool Sizing Calculator</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Metric</th><th>Observed</th><th>Recommendation</th><th>Why</th>'
\qecho '</tr></thead><tbody>'

WITH conn AS (
  SELECT
    COUNT(*) AS total_conns,
    COUNT(*) FILTER (WHERE state = 'active') AS active_conns,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_conns,
    COUNT(*) FILTER (WHERE state ILIKE '%idle in transaction%') AS idle_xact,
    ROUND(100.0 * COUNT(*) FILTER (WHERE state = 'idle') / NULLIF(COUNT(*),0), 1) AS idle_pct
  FROM pg_stat_activity
  WHERE datname = current_database()
),
env AS (
  SELECT
    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') AS max_connections,
    (SELECT setting::int FROM pg_settings WHERE name = 'max_worker_processes') AS max_worker_processes
),
calc AS (
  SELECT
    c.*,
    e.max_connections,
    e.max_worker_processes,
    LEAST(GREATEST(20, e.max_worker_processes * 4), GREATEST(20, e.max_connections - 10)) AS recommended_server_pool,
    LEAST(5000, LEAST(GREATEST(20, e.max_worker_processes * 4), GREATEST(20, e.max_connections - 10)) * 20) AS recommended_client_pool
  FROM conn c CROSS JOIN env e
)
SELECT
  '<tr><td>Current connection usage</td><td class="num">' || total_conns || ' / ' || max_connections || '</td><td class="num">Target steady-state server pool: ' || recommended_server_pool || '</td><td>Keep active server connections bounded; absorb client spikes in pooler.</td></tr>' ||
  '<tr><td>Idle connection ratio</td><td class="num">' || idle_pct || '%</td><td>' ||
  CASE
    WHEN idle_pct >= 40 THEN '<span class="warn">Use transaction pooling</span>'
    WHEN idle_pct >= 20 THEN '<span class="warn">Transaction pooling likely beneficial</span>'
    ELSE '<span class="good">Session pooling acceptable if app requires it</span>'
  END || '</td><td>High idle ratio indicates backend slot waste without pooling.</td></tr>' ||
  '<tr><td>Idle-in-transaction sessions</td><td class="num">' || idle_xact || '</td><td>' ||
  CASE
    WHEN idle_xact > 0 THEN '<span class="crit">Fix app transaction handling before aggressive pooling</span>'
    ELSE '<span class="good">No blocker for transaction pooling mode</span>'
  END || '</td><td>Idle transactions hold locks and can block vacuum.</td></tr>' ||
  '<tr><td>Suggested PgBouncer limits</td><td class="num">N/A</td><td class="code-block">default_pool_size=' || recommended_server_pool ||
  ', max_client_conn=' || recommended_client_pool || ', reserve_pool_size=' || GREATEST(5, recommended_server_pool / 5) ||
  '</td><td>Use as baseline and tune with observed queueing/latency.</td></tr>'
FROM calc;

\qecho '</tbody></table></div></div>'

-- S22.4 Application attribution completeness
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload Attribution Completeness</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed</th><th>Status</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

WITH app_cov AS (
  SELECT
    COUNT(*) AS total_sessions,
    COUNT(*) FILTER (WHERE COALESCE(NULLIF(application_name,''),'') = '') AS missing_app_name,
    COUNT(*) FILTER (WHERE COALESCE(NULLIF(application_name,''),'') <> '') AS tagged_sessions
  FROM pg_stat_activity
  WHERE datname = current_database()
)
SELECT
  '<tr><td>Sessions missing application_name</td><td class="num">' || missing_app_name || ' / ' || total_sessions || '</td><td class="' ||
  CASE
    WHEN total_sessions = 0 THEN 'good">No sessions'
    WHEN missing_app_name::numeric / total_sessions > 0.5 THEN 'crit">Poor attribution'
    WHEN missing_app_name::numeric / total_sessions > 0.2 THEN 'warn">Partial attribution'
    ELSE 'good">Healthy attribution'
  END || '</td><td>Set application_name in driver/pooler connection string for all services.</td></tr>' ||
  '<tr><td>Tagged sessions</td><td class="num">' || tagged_sessions || '</td><td class="good">Usable</td><td>Improves top SQL ownership and incident triage speed.</td></tr>'
FROM app_cov;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S23: FULL CONFIGURATION DEEP DIVE (40+ PARAMETERS)
-- =============================================================================
\qecho '<div class="section" id="s23">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Full Configuration Parameter Audit (40+ Critical Settings)</div>'
\qecho '    <div class="section-desc">Every critical postgresql.conf parameter vs recommended value with impact assessment.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding info"><div class="finding-header">'
\qecho '<span class="finding-title">Non-Default Parameters (settings changed from PostgreSQL defaults)</span>'
\qecho '<span class="severity-pill pill-info">CHANGED SETTINGS</span></div>'
\qecho '<div class="finding-body">'
\qecho 'These settings have been explicitly configured. Review to ensure they match current workload needs.'
\qecho 'Parameters set by configuration file are the intended overrides. Command-line overrides should be temporary only.'
\qecho '</div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current</th><th>Boot Default</th><th>Source</th><th>Notes</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td class="num">' || replace(replace(name,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td class="num ' || CASE WHEN source = 'command line' THEN 'warn' ELSE '' END || '">' ||
  replace(replace(CASE WHEN unit IS NOT NULL THEN setting||' '||unit ELSE setting END,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td class="num">' ||
  replace(replace(COALESCE(CASE WHEN unit IS NOT NULL THEN boot_val||' '||unit ELSE boot_val END,'N/A'),'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td class="' || CASE WHEN source='command line' THEN 'warn' WHEN source='configuration file' THEN '' ELSE '' END || '">' ||
  replace(replace(source,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td>' ||
  replace(replace(COALESCE(left(short_desc,80),''),'<','&lt;'),'>','&gt;') || '</td>' ||
  '</tr>'
FROM pg_settings
WHERE setting <> boot_val
  AND name NOT ILIKE '%password%'
  AND name NOT ILIKE '%secret%'
  AND name NOT ILIKE '%passphrase%'
  AND name NOT IN ('application_name','search_path','TimeZone','DateStyle','IntervalStyle','extra_float_digits','lc_messages','lc_monetary','lc_numeric','lc_time','client_encoding')
ORDER BY
  CASE source WHEN 'command line' THEN 0 WHEN 'configuration file' THEN 1 ELSE 2 END,
  name;

\qecho '</tbody></table></div>'

-- Full parameter recommendations
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Critical Parameter Recommendations</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Category</th><th>Parameter</th><th>Current Value</th>'
\qecho '<th>Recommended</th><th>Status</th><th>Why It Matters</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || category || '</td>' ||
  '<td class="num">' || param || '</td>' ||
  '<td class="num ' || CASE WHEN status = 'CRITICAL' THEN 'crit' WHEN status = 'WARNING' THEN 'warn' ELSE '' END || '">' || current_val || '</td>' ||
  '<td class="num good">' || recommended || '</td>' ||
  '<td><span class="severity-pill ' ||
  CASE status
    WHEN 'CRITICAL' THEN 'pill-critical"> CRITICAL'
    WHEN 'WARNING'  THEN 'pill-high"> WARNING'
    WHEN 'OK'       THEN 'pill-good"> OK'
    ELSE 'pill-info">INFO'
  END || '</span></td>' ||
  '<td>' || why || '</td></tr>'
FROM (

  SELECT 'Memory' AS category, 'shared_buffers' AS param,
    pg_size_pretty((SELECT setting::bigint * 8192 FROM pg_settings WHERE name='shared_buffers')) AS current_val,
    '25-40% of total RAM' AS recommended,
    CASE WHEN (SELECT setting::bigint FROM pg_settings WHERE name='shared_buffers') * 8192 < 134217728 THEN 'WARNING' ELSE 'OK' END AS status,
    'Primary data cache. Too small = excessive disk reads. Too large = OS cache crowded. Most impactful single setting.' AS why

  UNION ALL SELECT 'Memory', 'effective_cache_size',
    pg_size_pretty((SELECT setting::bigint * 8192 FROM pg_settings WHERE name='effective_cache_size')),
    '50-75% of total RAM',
    'OK',
    'Tells planner how much total cache (shared_buffers + OS) is available. Affects index vs seq scan decisions.'

  UNION ALL SELECT 'Memory', 'work_mem',
    pg_size_pretty((SELECT setting::bigint * 1024 FROM pg_settings WHERE name='work_mem')),
    'OLTP: 4-32MB | Analytics: 256MB-1GB',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='work_mem') < 2048 THEN 'WARNING' ELSE 'OK' END,
    'Per sort/hash operation. Low = temp file spills. High = OOM risk. Multiply by max_connections  parallel ops.'

  UNION ALL SELECT 'Memory', 'maintenance_work_mem',
    pg_size_pretty((SELECT setting::bigint * 1024 FROM pg_settings WHERE name='maintenance_work_mem')),
    '512MB-2GB (for VACUUM, CREATE INDEX)',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='maintenance_work_mem') < 65536 THEN 'WARNING' ELSE 'OK' END,
    'Used by VACUUM, CREATE INDEX, ALTER TABLE. More RAM = faster index builds and more efficient vacuum.'

  UNION ALL SELECT 'Memory', 'huge_pages',
    (SELECT setting FROM pg_settings WHERE name='huge_pages'),
    'try (if OS supports)',
    'OK',
    'Linux huge pages reduce TLB misses for large shared_buffers. Can give 5-15% performance improvement.'

  UNION ALL SELECT 'WAL', 'wal_level',
    (SELECT setting FROM pg_settings WHERE name='wal_level'),
    'replica or logical',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name='wal_level') = 'minimal' THEN 'CRITICAL' ELSE 'OK' END,
    'minimal = no PITR, no replication possible. replica = streaming replication. logical = logical replication.'

  UNION ALL SELECT 'WAL', 'wal_buffers',
    pg_size_pretty((SELECT setting::bigint * 8192 FROM pg_settings WHERE name='wal_buffers')),
    '32-64MB (or -1 for auto)',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='wal_buffers') < 512 THEN 'WARNING' ELSE 'OK' END,
    'WAL buffer in shared memory. Too small = frequent WAL flushes. -1 = auto-size to 1/32 of shared_buffers.'

  UNION ALL SELECT 'WAL', 'max_wal_size',
    pg_size_pretty((SELECT setting::bigint * 1048576 FROM pg_settings WHERE name='max_wal_size')),
    'Write-heavy: 4-16GB | Normal: 1-4GB',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='max_wal_size') < 1024 THEN 'WARNING' ELSE 'OK' END,
    'Max WAL kept before checkpoint forced. Too small = forced checkpoints = write stalls.'

  UNION ALL SELECT 'WAL', 'synchronous_commit',
    (SELECT setting FROM pg_settings WHERE name='synchronous_commit'),
    'on (for safety) or off (high perf)',
    'OK',
    'off = up to work_mem of transactions lost on crash. 2-3x write throughput gain. Use only for non-critical data.'

  UNION ALL SELECT 'Checkpoint', 'checkpoint_completion_target',
    (SELECT setting FROM pg_settings WHERE name='checkpoint_completion_target'),
    '0.9',
    CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name='checkpoint_completion_target') < 0.7 THEN 'WARNING' ELSE 'OK' END,
    'Spread checkpoint I/O over this fraction of checkpoint_timeout. Low = bursty writes. 0.9 = smooth I/O.'

  UNION ALL SELECT 'Planner', 'random_page_cost',
    (SELECT setting FROM pg_settings WHERE name='random_page_cost'),
    'SSD: 1.1 | HDD: 4.0 | NVMe: 1.0',
    CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name='random_page_cost') > 2.0 THEN 'WARNING' ELSE 'OK' END,
    'Cost of a random disk read vs seq read. Default 4.0 assumes HDD. SSD systems should use 1.1 or 1.0.'

  UNION ALL SELECT 'Planner', 'seq_page_cost',
    (SELECT setting FROM pg_settings WHERE name='seq_page_cost'),
    '1.0',
    'OK',
    'Baseline cost unit. Keep at 1.0. Adjust random_page_cost relative to this.'

  UNION ALL SELECT 'Planner', 'default_statistics_target',
    (SELECT setting FROM pg_settings WHERE name='default_statistics_target'),
    '100-500 (default 100, complex queries need more)',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='default_statistics_target') < 100 THEN 'WARNING' ELSE 'OK' END,
    'Number of histogram buckets for statistics. Higher = better estimates on complex queries. Cost: slightly slower ANALYZE.'

  UNION ALL SELECT 'Parallel', 'max_parallel_workers_per_gather',
    (SELECT setting FROM pg_settings WHERE name='max_parallel_workers_per_gather'),
    'OLTP: 2-4 | Analytics: 4-8',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='max_parallel_workers_per_gather') = 0 THEN 'WARNING' ELSE 'OK' END,
    '0 = parallel queries disabled. Even OLTP benefits from 2-4 workers for large table scans and sorts.'

  UNION ALL SELECT 'Parallel', 'max_parallel_workers',
    (SELECT setting FROM pg_settings WHERE name='max_parallel_workers'),
    'num_cpu_cores - 2',
    'OK',
    'Total parallel workers across all sessions. Should match or exceed max_parallel_workers_per_gather.'

  UNION ALL SELECT 'Parallel', 'parallel_leader_participation',
    (SELECT setting FROM pg_settings WHERE name='parallel_leader_participation'),
    'on',
    'OK',
    'Leader process helps with parallel work. on = more CPU utilized. off = leader only coordinates.'

  UNION ALL SELECT 'Logging', 'log_min_duration_statement',
    (SELECT setting FROM pg_settings WHERE name='log_min_duration_statement') || 'ms',
    '500-2000ms (log slow queries)',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='log_min_duration_statement') = -1 THEN 'WARNING' ELSE 'OK' END,
    '-1 = no slow query logging! Essential for troubleshooting. Set to 500-2000ms to catch slow queries.'

  UNION ALL SELECT 'Logging', 'log_checkpoints',
    (SELECT setting FROM pg_settings WHERE name='log_checkpoints'),
    'on',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_checkpoints') = 'off' THEN 'WARNING' ELSE 'OK' END,
    'off = you will not know when forced checkpoints happen. Checkpoints are key performance indicators.'

  UNION ALL SELECT 'Logging', 'log_lock_waits',
    (SELECT setting FROM pg_settings WHERE name='log_lock_waits'),
    'on',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_lock_waits') = 'off' THEN 'WARNING' ELSE 'OK' END,
    'off = lock waits are invisible. Set on to log any lock wait > deadlock_timeout. Critical for diagnosing contention.'

  UNION ALL SELECT 'Logging', 'track_io_timing',
    (SELECT setting FROM pg_settings WHERE name='track_io_timing'),
    'on',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name='track_io_timing') = 'off' THEN 'WARNING' ELSE 'OK' END,
    'off = no I/O timing in pg_stat_statements or EXPLAIN. Cannot identify I/O bottlenecks. Minimal overhead on modern systems.'

  UNION ALL SELECT 'Replication', 'max_replication_slots',
    (SELECT setting FROM pg_settings WHERE name='max_replication_slots'),
    '>= number of replicas + logical subs',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='max_replication_slots') = 0 THEN 'WARNING' ELSE 'OK' END,
    '0 = replication slots disabled. Must be > 0 for logical replication and most HA solutions.'

  UNION ALL SELECT 'Connections', 'max_connections',
    (SELECT setting FROM pg_settings WHERE name='max_connections'),
    'With pooler: 50-100 | Without: 200-500',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name='max_connections') > 500 THEN 'WARNING' ELSE 'OK' END,
    '>500 without pooler = excessive RAM overhead. Each connection = 5-10MB overhead.'

  UNION ALL SELECT 'Security', 'ssl',
    (SELECT setting FROM pg_settings WHERE name='ssl'),
    'on (in production)',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name='ssl') = 'off' THEN 'CRITICAL' ELSE 'OK' END,
    'off = all connections are unencrypted. Passwords sent in plaintext. Never acceptable in production.'

  UNION ALL SELECT 'Security', 'row_security',
    (SELECT setting FROM pg_settings WHERE name='row_security'),
    'on (if using RLS policies)',
    'OK',
    'Row Level Security. If you have RLS policies, ensure this is on. off = RLS policies are bypassed for superusers.'

) param_audit
ORDER BY
  CASE status WHEN 'CRITICAL' THEN 0 WHEN 'WARNING' THEN 1 ELSE 2 END,
  category, param;

\qecho '</tbody></table></div></div>'

-- S23.2 Parameter mutability and managed-service constraints
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Parameter Mutability &amp; Constraints</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Parameter</th><th>Current</th><th>Context</th><th>Pending Restart</th><th>Change Source</th><th>Operational Note</th>'
\qecho '</tr></thead><tbody>'

WITH platform AS (
  SELECT
    CASE
      WHEN current_setting('rds.extensions', true) IS NOT NULL
        OR current_setting('rds.force_ssl', true) IS NOT NULL THEN 'AWS RDS/Aurora'
      WHEN current_setting('azure.extensions', true) IS NOT NULL THEN 'Azure Database for PostgreSQL'
      WHEN version() ILIKE '%cloudsql%' THEN 'Cloud SQL'
      ELSE 'Self-managed / Unknown'
    END AS platform_name
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || replace(replace(name,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td class="num">' || replace(replace(CASE WHEN unit IS NOT NULL AND unit <> '' THEN setting || ' ' || unit ELSE setting END,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td>' || replace(replace(context,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td class="' || CASE WHEN pending_restart THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td>' ||
      replace(replace(
        CASE
          WHEN lower(:'pg360_redact_paths') IN ('on','true','1','yes')
          THEN replace(COALESCE(sourcefile, source), :'pg360_redact_path_prefix', :'pg360_redacted_path_token')
          ELSE COALESCE(sourcefile, source)
        END
      ,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td class="' ||
      CASE
        WHEN context = 'postmaster' THEN 'warn">Restart required; batch with maintenance window.'
        WHEN platform_name <> 'Self-managed / Unknown' AND source = 'default' THEN 'warn">May require parameter group change.'
        ELSE 'good">Can be tuned online or at reload level.'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY
        CASE context WHEN 'postmaster' THEN 0 WHEN 'sighup' THEN 1 ELSE 2 END,
        name
    ),
    '<tr><td colspan="6" class="table-empty">No parameters returned.</td></tr>'
  )
FROM pg_settings, platform
WHERE name IN (
  'shared_buffers','work_mem','maintenance_work_mem','effective_cache_size',
  'max_connections','max_worker_processes','max_parallel_workers','max_parallel_workers_per_gather',
  'wal_level','max_wal_size','checkpoint_timeout','checkpoint_completion_target',
  'autovacuum','autovacuum_vacuum_scale_factor','autovacuum_analyze_scale_factor',
  'track_io_timing','log_min_duration_statement','log_lock_waits','ssl'
);

\qecho '</tbody></table></div></div>'

-- S23.3 Workload-aligned tuning focus (linking settings to workload profile)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Workload-Aligned Tuning Focus</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Detected Profile</th><th>Primary Tuning Focus</th><th>Parameters to Prioritize</th><th>Why</th>'
\qecho '</tr></thead><tbody>'

WITH w AS (
  SELECT
    d.tup_fetched::numeric AS reads,
    (d.tup_inserted + d.tup_updated + d.tup_deleted)::numeric AS writes,
    d.temp_files::numeric AS temp_files
  FROM pg_stat_database d
  WHERE d.datname = current_database()
)
SELECT
  CASE
    WHEN reads > writes * 8 THEN
      '<tr><td class="good">Read-heavy OLTP</td><td>Planner and cache efficiency</td><td class="num">effective_cache_size, random_page_cost, shared_buffers, track_io_timing</td><td>Read paths dominate latency; plan quality and cache hit ratio drive throughput.</td></tr>'
    WHEN writes > reads * 2 THEN
      '<tr><td class="warn">Write-heavy OLTP</td><td>WAL, checkpoints, autovacuum</td><td class="num">max_wal_size, checkpoint_completion_target, autovacuum_*, wal_buffers, synchronous_commit</td><td>Write amplification and vacuum debt become primary stability risks.</td></tr>'
    WHEN temp_files > 100 THEN
      '<tr><td class="warn">Analytical / spill-prone</td><td>Memory and parallelism</td><td class="num">work_mem, maintenance_work_mem, max_parallel_workers_per_gather, temp_file_limit</td><td>Sort/hash spill pressure indicates memory tuning and query-shape review are required.</td></tr>'
    ELSE
      '<tr><td class="good">Mixed workload</td><td>Balanced posture</td><td class="num">shared_buffers, work_mem, autovacuum_*, log_min_duration_statement</td><td>Prioritize balanced safety + throughput with observability enabled.</td></tr>'
  END
FROM w;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S24: INDEX BLOAT ESTIMATION
-- =============================================================================
\qecho '<div class="section" id="s24">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Index Bloat Estimation</div>'
\qecho '    <div class="section-desc">BTree index bloat from deletes and updates. Bloated indexes waste cache and slow all queries on those tables.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding high"><div class="finding-header">'
\qecho '<span class="finding-title">Index Bloat: Invisible Tax on Every Query</span>'
\qecho '<span class="severity-pill pill-high">STORAGE & PERF</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>Root Cause:</strong> When rows are deleted or updated, the index entries become "dead pages."'
\qecho 'BTree indexes reuse space for same-key inserts but not for different keys. On write-heavy tables,'
\qecho 'indexes can become 50-300% larger than needed.<br>'
\qecho '<strong>Impact:</strong> Bloated indexes take more cache space, require more I/O to scan,'
\qecho 'and make every query on that table slower. An index with 60% bloat is literally 2.5x slower.<br>'
\qecho '<strong>Fix:</strong> REINDEX CONCURRENTLY index_name; (PG12+  non-blocking)<br>'
\qecho '<strong>Prevention:</strong> Set appropriate FILLFACTOR on high-churn indexes:'
\qecho 'CREATE INDEX idx ON table(col) WITH (fillfactor=70);<br>'
\qecho '<strong>Note:</strong> Bloat estimation below uses heuristics from pg_stats. Install pgstattuple for exact measurements.'
\qecho '</div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">BTree Index Bloat Estimation</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema</th><th>Table</th><th>Index</th>'
\qecho '<th>Index Size</th><th>Est. Bloat%</th><th>Est. Wasted Space</th>'
\qecho '<th>Verdict</th><th>Fix (non-blocking)</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(tablename,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' || replace(replace(replace(replace(replace(indexname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(index_size) || '</td>' ||
  '<td class="num ' || CASE WHEN bloat_pct > 50 THEN 'crit' WHEN bloat_pct > 30 THEN 'warn' ELSE '' END || '">' ||
  round(bloat_pct::numeric,0) || '%</td>' ||
  '<td class="num warn">' || pg_size_pretty(wasted_bytes) || '</td>' ||
  '<td class="' ||
  CASE
    WHEN bloat_pct > 50 THEN 'crit"> Severe bloat  REINDEX immediately'
    WHEN bloat_pct > 30 THEN 'warn"> Moderate bloat  schedule REINDEX'
    ELSE 'good"> Acceptable'
  END || '</td>' ||
  '<td class="code-block">' ||
  CASE WHEN bloat_pct > 30
  THEN replace(replace(replace(replace(replace(
         format('REINDEX INDEX CONCURRENTLY %I.%I;', schemaname, indexname)
       ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
  ELSE '-- OK'
  END || '</td></tr>'
FROM (
  SELECT
    si.schemaname,
    si.relname AS tablename,
    si.indexrelname AS indexname,
    pg_relation_size(si.indexrelid) AS index_size,
    CASE
      WHEN pg_relation_size(si.indexrelid) = 0 THEN 0
      WHEN si.idx_scan = 0 THEN 20  -- Conservative estimate for unused indexes
      ELSE GREATEST(0, LEAST(80,
        (100 - (
          COALESCE(
            (SELECT round(100.0 * c2.relpages / NULLIF(c2.relpages + GREATEST(0, c2.relpages * (1 - n.n_live_tup::numeric / NULLIF(n.n_live_tup + n.n_dead_tup, 0) * 10), 0), 0), 0)
             FROM pg_class c2
             JOIN pg_stat_user_tables n ON n.relid = c2.oid
             WHERE c2.oid = si.indexrelid),
            70
          )
        ))
      ))
    END AS bloat_pct,
    CASE WHEN pg_relation_size(si.indexrelid) > 0
    THEN (pg_relation_size(si.indexrelid) *
      GREATEST(0, LEAST(0.8,
        (1 - COALESCE((
          SELECT CASE WHEN c2.relpages = 0 THEN 0
          ELSE round(100.0 * c2.relpages / NULLIF(c2.relpages + GREATEST(0, c2.relpages * (1 - n2.n_live_tup::numeric/NULLIF(n2.n_live_tup+n2.n_dead_tup,0)*10),0),0),0)
          END / 100.0
          FROM pg_class c2
          JOIN pg_stat_user_tables n2 ON n2.relid = c2.oid
          WHERE c2.oid = si.indexrelid),
          0.3)
      ))))::bigint
    ELSE 0 END AS wasted_bytes
  FROM pg_stat_user_indexes si
  JOIN pg_class c ON c.oid = si.indexrelid
  WHERE pg_relation_size(si.indexrelid) > 1048576  -- only indexes > 1MB
) bloat_est
ORDER BY bloat_pct DESC, index_size DESC
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S24.2 Prioritized reindex queue (bloat x access frequency)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Reindex Priority Queue</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Index</th><th>Size</th><th>idx_scan</th><th>Estimated Waste</th><th>Priority Score</th><th>Action</th><th>Fix</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || indexname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || pg_size_pretty(index_size) || '</td>' ||
      '<td class="num">' || to_char(idx_scan,'FM999,999,999') || '</td>' ||
      '<td class="num">' || pg_size_pretty(wasted_bytes) || '</td>' ||
      '<td class="num ' ||
      CASE WHEN priority_score >= 5000 THEN 'crit' WHEN priority_score >= 1000 THEN 'warn' ELSE 'good' END || '">' ||
      round(priority_score::numeric,1) || '</td>' ||
      '<td class="' ||
      CASE
        WHEN priority_score >= 5000 THEN 'crit">Rebuild first (high ROI)'
        WHEN priority_score >= 1000 THEN 'warn">Schedule in maintenance window'
        ELSE 'good">Defer for now'
      END || '</td>' ||
      '<td class="code-block">' ||
      CASE
        WHEN priority_score >= 1000 THEN replace(replace(replace(replace(replace(
          format('REINDEX INDEX CONCURRENTLY %I.%I;', schemaname, indexname)
        ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
        ELSE '-- Leave index as-is'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY priority_score DESC
    ),
    '<tr><td colspan="7" class="table-empty">No candidate indexes for prioritized reindexing.</td></tr>'
  )
FROM (
  SELECT
    si.schemaname,
    si.indexrelname AS indexname,
    pg_relation_size(si.indexrelid) AS index_size,
    si.idx_scan,
    GREATEST(0, (pg_relation_size(si.indexrelid) * CASE WHEN si.idx_scan = 0 THEN 0.2 ELSE 0.35 END))::bigint AS wasted_bytes,
    (GREATEST(0, (pg_relation_size(si.indexrelid) * CASE WHEN si.idx_scan = 0 THEN 0.2 ELSE 0.35 END)) / 1048576.0) *
      LN(GREATEST(si.idx_scan, 1) + 10) AS priority_score
  FROM pg_stat_user_indexes si
  WHERE pg_relation_size(si.indexrelid) > 10485760
) q
WHERE wasted_bytes > 0;

\qecho '</tbody></table></div></div>'

-- S24.3 Rebuild vs leave criteria
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Rebuild vs Leave Decision Criteria</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Condition</th><th>Recommended Decision</th><th>Reason</th>'
\qecho '</tr></thead><tbody>'
\qecho '<tr><td>Large wasted space + frequently scanned index</td><td class="crit">Reindex now</td><td>Directly reduces read latency and cache churn.</td></tr>'
\qecho '<tr><td>Large wasted space + rarely used index</td><td class="warn">Consider drop before rebuild</td><td>If index has near-zero scans, drop may deliver higher ROI than rebuild.</td></tr>'
\qecho '<tr><td>Small bloat + high write table</td><td class="good">Leave for now</td><td>Frequent rebuilds can increase write overhead and maintenance load.</td></tr>'
\qecho '<tr><td>Estimated bloat only (no pgstattuple validation)</td><td class="warn">Validate first for critical objects</td><td>Use pgstattuple for exact bloat before high-impact maintenance.</td></tr>'
\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S25: SECURITY DEEP DIVE
-- =============================================================================
\qecho '<div class="section" id="s25">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Security &amp; Access Review</div>'
\qecho '    <div class="section-desc">RLS posture, role membership, grant exposure, SSL, and access hardening evidence for least-privilege review.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S25.1 RLS policy coverage
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Row-Level Security Policy Coverage</div>'
\qecho '<div class="finding high"><div class="finding-header">'
\qecho '<span class="finding-title">Tables with sensitive data but no RLS  any authenticated user can read all rows</span>'
\qecho '<span class="severity-pill pill-high">DATA ISOLATION</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>Root Cause:</strong> PostgreSQL tables are accessible to any role with SELECT privilege.'
\qecho 'Without RLS, user A can query user B''s private data.<br>'
\qecho '<strong>Oracle equivalent:</strong> Virtual Private Database (VPD) / Fine-Grained Auditing (FGA).<br>'
\qecho '<strong>Fix:</strong><br>'
\qecho 'ALTER TABLE orders ENABLE ROW LEVEL SECURITY;<br>'
\qecho 'CREATE POLICY user_orders ON orders FOR ALL TO app_role<br>'
\qecho '  USING (user_id = current_setting(''app.current_user_id'')::int);<br>'
\qecho 'REVOKE ALL ON orders FROM PUBLIC;<br>'
\qecho 'GRANT SELECT ON orders TO app_role;'
\qecho '</div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>RLS Enabled</th><th>Policy Count</th><th>Force Row Security</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(n.nspname||'.'||c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="' || CASE WHEN c.relrowsecurity THEN 'good"> Enabled' ELSE 'warn"> Disabled' END || '</td>' ||
  '<td class="num">' || policy_count || '</td>' ||
  '<td>' || CASE WHEN c.relforcerowsecurity THEN 'Yes (bypasses BYPASSRLS)' ELSE 'No' END || '</td>' ||
  '<td class="' ||
  CASE
    WHEN NOT c.relrowsecurity AND policy_count = 0
      THEN 'warn">No RLS  consider if row isolation needed'
    WHEN c.relrowsecurity AND policy_count = 0
      THEN 'crit">RLS enabled but NO POLICIES  all queries will return 0 rows!'
    WHEN c.relrowsecurity AND policy_count > 0
      THEN 'good">RLS configured'
    ELSE '">'
  END || '</td></tr>'
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN (
  SELECT polrelid, COUNT(*) AS policy_count
  FROM pg_policy
  GROUP BY polrelid
) pol ON pol.polrelid = c.oid
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
ORDER BY c.relrowsecurity DESC, policy_count DESC NULLS LAST
LIMIT 40;

\qecho '</tbody></table></div></div>'

-- S25.2 Extension security audit
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Extension Security Audit</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Extension</th><th>Version</th><th>Schema</th><th>Risk Level</th><th>Notes</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td class="num">' || replace(replace(e.extname,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td>' || replace(replace(e.extversion,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td>' || replace(replace(n.nspname,'<','&lt;'),'>','&gt;') || '</td>' ||
  '<td class="' ||
  CASE e.extname
    WHEN 'pg_stat_statements' THEN 'good">Low  monitoring only'
    WHEN 'pg_buffercache'     THEN 'good">Low  monitoring only'
    WHEN 'pg_cron'            THEN 'warn">Medium  can schedule arbitrary SQL'
    WHEN 'dblink'             THEN 'crit">High  can connect to other databases'
    WHEN 'postgres_fdw'       THEN 'warn">Medium  can access remote databases'
    WHEN 'file_fdw'           THEN 'crit">High  can read files from server filesystem'
    WHEN 'adminpack'          THEN 'crit">High  administrative functions, superuser required'
    WHEN 'pgcrypto'           THEN 'warn">Medium  encryption keys may be exposed in queries'
    WHEN 'uuid-ossp'          THEN 'good">Low  UUID generation only'
    WHEN 'hstore'             THEN 'good">Low  data type only'
    WHEN 'postgis'            THEN 'good">Low  spatial types only'
    WHEN 'plpgsql'            THEN 'good">Low  built-in procedural language'
    WHEN 'plpython3u'         THEN 'crit">Critical  untrusted Python, can execute OS commands'
    WHEN 'plperlu'            THEN 'crit">Critical  untrusted Perl, can execute OS commands'
    WHEN 'pltclu'             THEN 'crit">Critical  untrusted Tcl, can execute OS commands'
    ELSE 'warn">Review  security posture unknown'
  END || '</td>' ||
  '<td>' ||
  CASE e.extname
    WHEN 'dblink'      THEN 'Ensure only trusted users can execute dblink functions'
    WHEN 'file_fdw'    THEN 'Restrict to superuser only: REVOKE ALL ON FOREIGN SERVER FROM PUBLIC'
    WHEN 'plpython3u'  THEN 'Untrusted language  only superuser should create functions in plpython3u'
    WHEN 'plperlu'     THEN 'Untrusted language  only superuser should create functions in plperlu'
    WHEN 'pg_cron'     THEN 'Audit pg_cron.job table for scheduled queries'
    ELSE ''
  END || '</td></tr>'
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY
  CASE e.extname
    WHEN 'plpython3u' THEN 0 WHEN 'plperlu' THEN 0 WHEN 'file_fdw' THEN 0
    WHEN 'dblink' THEN 1 WHEN 'adminpack' THEN 1
    WHEN 'pg_cron' THEN 2 WHEN 'postgres_fdw' THEN 2
    ELSE 3
  END;

\qecho '</tbody></table></div></div>'

-- S25.3 Object privilege explosion
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Objects Accessible to PUBLIC</div>'
\qecho '<div class="finding high"><div class="finding-header">'
\qecho '<span class="finding-title">Objects granted to PUBLIC are accessible to every user in the database</span>'
\qecho '<span class="severity-pill pill-high">PRIVILEGE EXPOSURE</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>Risk:</strong> A newly created user with only CONNECT privilege can read these objects.'
\qecho 'This is especially dangerous in multi-tenant databases.<br>'
\qecho '<strong>Oracle equivalent:</strong> In Oracle, PUBLIC grants are equally dangerous and equally common.<br>'
\qecho '<strong>Fix:</strong> REVOKE ALL ON TABLE sensitive_table FROM PUBLIC; GRANT SELECT ON sensitive_table TO specific_role;'
\qecho '</div></div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Object</th><th>Type</th><th>PUBLIC Privileges</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname||'.'||c.relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' ||
      CASE c.relkind WHEN 'r' THEN 'Table' WHEN 'v' THEN 'View' WHEN 'm' THEN 'Mat View' WHEN 'f' THEN 'Foreign Table' ELSE c.relkind::text END ||
      '</td>' ||
      '<td class="warn">' ||
      array_to_string(ARRAY(
        SELECT CASE p.privilege_type
          WHEN 'SELECT' THEN 'SELECT' WHEN 'INSERT' THEN 'INSERT'
          WHEN 'UPDATE' THEN 'UPDATE' WHEN 'DELETE' THEN 'DELETE'
          ELSE p.privilege_type END
        FROM information_schema.role_table_grants p
        WHERE p.table_schema = n.nspname
          AND p.table_name = c.relname
          AND p.grantee = 'PUBLIC'
      ), ', ') || '</td>' ||
      '<td class="' ||
      CASE WHEN c.relkind = 'r' THEN 'crit">Any user can access this table' ELSE 'warn">Any user can access this view' END ||
      '</td></tr>',
      '' ORDER BY c.relkind, n.nspname, c.relname
    ),
    '<tr><td colspan="4" class="table-empty"> No objects found with explicit PUBLIC grants</td></tr>'
  )
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','m','f')
  AND n.nspname NOT IN ('pg_catalog','information_schema')
  AND EXISTS (
    SELECT 1 FROM information_schema.role_table_grants rg
    WHERE rg.grantee = 'PUBLIC'
      AND rg.table_schema = n.nspname
      AND rg.table_name = c.relname
  )
;

\qecho '</tbody></table></div></div>'

-- S25.4 Role capability concentration
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Elevated Role Capability Review</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Role</th><th>Can Login</th><th>Superuser</th><th>Create Role</th><th>Replication</th><th>Bypass RLS</th><th>Risk</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(CASE WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND rolname = current_user THEN :'pg360_redaction_token' ELSE rolname END,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || CASE WHEN rolcanlogin THEN 'Yes' ELSE 'No' END || '</td>' ||
      '<td class="' || CASE WHEN rolsuper THEN 'crit">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN rolcreaterole THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN rolreplication THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN rolbypassrls THEN 'crit">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' ||
      CASE
        WHEN rolsuper OR rolbypassrls THEN 'crit">Highest privilege surface'
        WHEN rolcreaterole OR rolreplication THEN 'warn">Elevated privilege; validate need'
        ELSE 'good">Standard role'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY (rolsuper OR rolbypassrls) DESC, rolcreaterole DESC, rolreplication DESC, rolname
    ),
    '<tr><td colspan="7" class="table-empty">No roles returned.</td></tr>'
  )
FROM pg_roles
WHERE rolname !~ '^pg_';

\qecho '</tbody></table></div></div>'

-- S25.5 Role membership graph
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Role Membership and Inherited Access</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Role</th><th>Can Login</th><th>Superuser</th><th>Create DB</th><th>Create Role</th><th>Member Of</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(role_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN can_login THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN is_superuser THEN 'crit">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN can_create_db THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td class="' || CASE WHEN can_create_role THEN 'warn">Yes' ELSE 'good">No' END || '</td>' ||
      '<td>' || COALESCE(replace(replace(replace(replace(replace(member_of_role,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;'),'Direct only') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY role_name, member_of_role
    ),
    '<tr><td colspan="6" class="table-empty">No role membership rows returned.</td></tr>'
  )
FROM (
  SELECT
    CASE
      WHEN lower(:'pg360_redact_user') IN ('on','true','1','yes') AND r.rolname = current_user
        THEN :'pg360_redaction_token'
      ELSE r.rolname
    END AS role_name,
    r.rolcanlogin AS can_login,
    r.rolsuper AS is_superuser,
    r.rolcreatedb AS can_create_db,
    r.rolcreaterole AS can_create_role,
    m.rolname AS member_of_role
  FROM pg_roles r
  LEFT JOIN pg_auth_members am ON am.member = r.oid
  LEFT JOIN pg_roles m ON m.oid = am.roleid
  ORDER BY r.rolname, m.rolname
) memberships;

\qecho '</tbody></table></div></div>'

-- S25.6 Grant exposure audit
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Grant Exposure Audit</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Object Type</th><th>Object</th><th>Grantee</th><th>Privilege</th><th>Grantable</th><th>Risk</th><th>Recommendation</th>'
\qecho '</tr></thead><tbody>'

WITH table_grants AS (
  SELECT
    'TABLE'::text AS object_type,
    tp.table_schema AS object_schema,
    tp.table_name AS object_name,
    tp.grantee,
    tp.privilege_type,
    tp.is_grantable,
    CASE
      WHEN tp.grantee = 'PUBLIC' THEN 'HIGH'
      WHEN tp.is_grantable = 'YES' AND tp.grantee <> pg_get_userbyid(c.relowner) THEN 'MEDIUM'
      ELSE 'LOW'
    END AS risk_level
  FROM information_schema.table_privileges tp
  JOIN pg_namespace n ON n.nspname = tp.table_schema
  JOIN pg_class c
    ON c.relnamespace = n.oid
   AND c.relname = tp.table_name
   AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  WHERE tp.table_schema !~ '^pg_'
    AND tp.table_schema <> 'information_schema'
),
routine_owner AS (
  SELECT
    n.nspname AS routine_schema,
    p.proname AS routine_name,
    array_agg(DISTINCT pg_get_userbyid(p.proowner)) AS owner_names
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname !~ '^pg_'
    AND n.nspname <> 'information_schema'
  GROUP BY n.nspname, p.proname
),
routine_grants AS (
  SELECT
    'ROUTINE'::text AS object_type,
    rp.routine_schema AS object_schema,
    rp.routine_name AS object_name,
    rp.grantee,
    rp.privilege_type,
    rp.is_grantable,
    CASE
      WHEN rp.grantee = 'PUBLIC' THEN 'HIGH'
      WHEN rp.is_grantable = 'YES'
       AND NOT (rp.grantee = ANY(COALESCE(ro.owner_names, ARRAY[]::text[]))) THEN 'MEDIUM'
      ELSE 'LOW'
    END AS risk_level
  FROM information_schema.routine_privileges rp
  LEFT JOIN routine_owner ro
    ON ro.routine_schema = rp.routine_schema
   AND ro.routine_name = rp.routine_name
  WHERE rp.routine_schema !~ '^pg_'
    AND rp.routine_schema <> 'information_schema'
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || object_type || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(object_schema || '.' || object_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(grantee,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || privilege_type || '</td>' ||
      '<td>' || is_grantable || '</td>' ||
      '<td class="' || CASE risk_level WHEN 'HIGH' THEN 'crit">High' WHEN 'MEDIUM' THEN 'warn">Medium' ELSE 'good">Low' END || '</td>' ||
      '<td>' ||
        CASE
          WHEN grantee = 'PUBLIC' THEN 'Consider REVOKE from PUBLIC and re-grant via named roles.'
          WHEN is_grantable = 'YES' THEN 'Review delegation chain and remove grant option if not required.'
          ELSE 'No immediate issue.'
        END ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY CASE risk_level WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 ELSE 2 END, object_schema, object_name, grantee, privilege_type
    ),
    '<tr><td colspan="7" class="table-empty">No medium or high-risk grant exposure found.</td></tr>'
  )
FROM (
  SELECT * FROM table_grants
  UNION ALL
  SELECT * FROM routine_grants
) x
WHERE risk_level <> 'LOW';

\qecho '</tbody></table></div></div>'

-- S25.7 SSL and authentication posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">SSL and Authentication Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Control</th><th>Observed Value</th><th>Status</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

WITH ssl_state AS (
  SELECT
    current_setting('ssl', true) AS ssl_on,
    current_setting('password_encryption', true) AS password_encryption,
    COALESCE((SELECT COUNT(*)
              FROM pg_stat_activity a
              LEFT JOIN pg_stat_ssl s ON s.pid = a.pid
              WHERE a.datname = current_database()
                AND a.pid <> pg_backend_pid()
                AND COALESCE(s.ssl, false) = false), 0) AS non_ssl_sessions
)
SELECT
  '<tr><td>ssl</td><td>' || ssl_on || '</td><td class="' ||
  CASE WHEN ssl_on = 'on' THEN 'good">Enabled' ELSE 'crit">Disabled' END ||
  '</td><td>' ||
  CASE WHEN ssl_on = 'on' THEN 'Keep enabled and enforce hostssl in pg_hba.' ELSE 'Enable TLS in server config and restart.' END ||
  '</td></tr>' ||
  '<tr><td>password_encryption</td><td>' || password_encryption || '</td><td class="' ||
  CASE WHEN password_encryption = 'scram-sha-256' THEN 'good">Strong' ELSE 'warn">Legacy/default' END ||
  '</td><td>' ||
  CASE WHEN password_encryption = 'scram-sha-256' THEN 'No change required.' ELSE 'Switch to scram-sha-256 and rotate credentials in waves.' END ||
  '</td></tr>' ||
  '<tr><td>Active non-SSL sessions</td><td class="num">' || non_ssl_sessions || '</td><td class="' ||
  CASE WHEN non_ssl_sessions = 0 THEN 'good">Compliant' WHEN non_ssl_sessions < 5 THEN 'warn">Partial' ELSE 'crit">Exposure' END ||
  '</td><td>Investigate clients/poolers not enforcing SSL and update connection strings.</td></tr>'
FROM ssl_state;

\qecho '</tbody></table></div></div>'

-- S25.8 Audit logging posture
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Audit Logging Posture</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Value</th><th>Status</th><th>Operational Guidance</th>'
\qecho '</tr></thead><tbody>'

WITH lg AS (
  SELECT
    current_setting('log_connections', true) AS log_connections,
    current_setting('log_disconnections', true) AS log_disconnections,
    current_setting('log_lock_waits', true) AS log_lock_waits,
    current_setting('log_min_duration_statement', true) AS log_min_duration_statement,
    current_setting('log_statement', true) AS log_statement,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgaudit') AS pgaudit_installed
)
SELECT
  '<tr><td>Connection logging</td><td>' || log_connections || '/' || log_disconnections || '</td><td class="' ||
  CASE WHEN log_connections = 'on' AND log_disconnections = 'on' THEN 'good">Enabled' ELSE 'warn">Partial' END ||
  '</td><td>Enable both to support access forensics and session lifecycle auditing.</td></tr>' ||
  '<tr><td>Lock wait logging</td><td>' || log_lock_waits || '</td><td class="' ||
  CASE WHEN log_lock_waits = 'on' THEN 'good">Enabled' ELSE 'warn">Disabled' END ||
  '</td><td>Keep on for incident triage of blocking chains.</td></tr>' ||
  '<tr><td>Slow query logging</td><td>' || log_min_duration_statement || '</td><td class="' ||
  CASE WHEN log_min_duration_statement = '-1' THEN 'warn">Disabled' ELSE 'good">Enabled' END ||
  '</td><td>Use bounded threshold (e.g., 500-2000ms) for performance forensics.</td></tr>' ||
  '<tr><td>Statement audit mode</td><td>' || log_statement || '</td><td class="' ||
  CASE WHEN log_statement IN ('all','mod') THEN 'warn">High volume' ELSE 'good">Controlled' END ||
  '</td><td>Prefer scoped audit controls to avoid log amplification.</td></tr>' ||
  '<tr><td>pgaudit extension</td><td>' || CASE WHEN pgaudit_installed THEN 'installed' ELSE 'not installed' END || '</td><td class="' ||
  CASE WHEN pgaudit_installed THEN 'good">Available' ELSE 'warn">Optional for regulated workloads' END ||
  '</td><td>Enable where compliance requires statement-class audit trails.</td></tr>'
FROM lg;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S26: CAPACITY & GROWTH PROJECTIONS (Enhanced)
-- =============================================================================
\qecho '<div class="section" id="s26">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Capacity &amp; Growth Projections (Enhanced)</div>'
\qecho '    <div class="section-desc">Table growth velocity, disk usage trends, tablespace free space, and projected capacity dates.</div>'
\qecho '  </div>'
\qecho '</div>'

-- S26.1 Table size with growth velocity
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Table Size &amp; Growth Velocity Analysis</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Current Size</th><th>Table Only</th><th>Index Size</th>'
\qecho '<th>Total Rows</th><th>Insert Rate</th><th>Delete Rate</th>'
\qecho '<th>Net Growth</th><th>Pattern</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(schemaname||'.'||relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_total_relation_size(relid)) || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_relation_size(relid)) || '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_indexes_size(relid)) || '</td>' ||
  '<td class="num">' || to_char(n_live_tup,'FM999,999,999') || '</td>' ||
  '<td class="num ' || CASE WHEN n_tup_ins > n_tup_del*2 THEN 'warn' ELSE '' END || '">' ||
  to_char(n_tup_ins,'FM999,999,999') || '</td>' ||
  '<td class="num">' || to_char(n_tup_del,'FM999,999,999') || '</td>' ||
  '<td class="num ' ||
  CASE
    WHEN n_tup_ins - n_tup_del > 1000000 THEN 'warn'
    WHEN n_tup_ins > 0 AND n_tup_del > n_tup_ins * 0.9 THEN 'good'
    ELSE ''
  END || '">' ||
  to_char(n_tup_ins - n_tup_del,'FM999,999,999') || '</td>' ||
  '<td class="' ||
  CASE
    WHEN n_tup_ins > 0 AND n_tup_del > n_tup_ins * 0.9 THEN 'good">Queue/log pattern  stable size'
    WHEN n_tup_ins > n_tup_del * 10 AND n_tup_ins > 100000 THEN 'warn">Growing fast  monitor capacity'
    WHEN n_tup_ins = 0 AND n_tup_del = 0 THEN '">Static reference table'
    WHEN n_tup_ins > 0 THEN '">Normal insert pattern'
    ELSE '">'
  END || '</td></tr>'
FROM pg_stat_user_tables
WHERE n_live_tup > 0 OR n_tup_ins > 0
ORDER BY pg_total_relation_size(relid) DESC NULLS LAST
LIMIT 30;

\qecho '</tbody></table></div></div>'

-- S26.2 Tablespace utilization
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Tablespace Size Summary</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Tablespace</th><th>Location</th><th>Total Size</th><th>Number of Objects</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td>' || replace(replace(replace(replace(replace(spc.spcname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
  '<td>' ||
  -- SECURITY: Mask exact path for security if configured
  COALESCE(replace(replace(pg_tablespace_location(spc.oid),'<','&lt;'),'>','&gt;'),'(default $PGDATA)') ||
  '</td>' ||
  '<td class="num">' || pg_size_pretty(pg_tablespace_size(spc.oid)) || '</td>' ||
  '<td class="num">' ||
  (SELECT COUNT(*) FROM pg_class c WHERE c.reltablespace = spc.oid OR (c.reltablespace = 0 AND spc.spcname = 'pg_default'))::text ||
  '</td></tr>'
FROM pg_tablespace spc
ORDER BY pg_tablespace_size(spc.oid) DESC NULLS LAST;

\qecho '</tbody></table></div></div>'

-- S26.3 Database total size with all related objects
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Complete Storage Breakdown</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Component</th><th>Count</th><th>Total Size</th><th>% of DB</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr><td>' || component || '</td>' ||
  '<td class="num">' || obj_count || '</td>' ||
  '<td class="num">' || pg_size_pretty(total_bytes) || '</td>' ||
  '<td class="num">' ||
  round(100.0*total_bytes/NULLIF((SELECT pg_database_size(current_database())),0),1)::text || '%</td></tr>'
FROM (
  SELECT 'User Tables (heap only)' AS component,
    COUNT(*)::text AS obj_count,
    SUM(pg_relation_size(c.oid)) AS total_bytes
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')

  UNION ALL SELECT 'Table Indexes',
    COUNT(*)::text,
    SUM(pg_relation_size(c.oid))
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind='i' AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')

  UNION ALL SELECT 'TOAST Tables',
    COUNT(*)::text,
    SUM(pg_relation_size(c.oid))
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind='t' AND n.nspname NOT IN ('pg_catalog','information_schema')

  UNION ALL SELECT 'Materialized Views',
    COUNT(*)::text,
    SUM(pg_relation_size(c.oid))
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind='m' AND n.nspname NOT IN ('pg_catalog','information_schema')

  UNION ALL SELECT 'System Catalog',
    COUNT(*)::text,
    SUM(pg_relation_size(c.oid))
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname IN ('pg_catalog','information_schema')
) storage_breakdown
ORDER BY total_bytes DESC NULLS LAST;

\qecho '</tbody></table></div></div>'

-- S26.4 Projection confidence and assumptions
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Projection Confidence and Assumptions</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Signal</th><th>Observed</th><th>Confidence Impact</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

WITH c AS (
  SELECT
    COALESCE((SELECT now() - stats_reset FROM pg_stat_database WHERE datname = current_database()), interval '365 days') AS stats_window,
    COALESCE((SELECT SUM(n_tup_ins + n_tup_upd + n_tup_del) FROM pg_stat_user_tables), 0) AS tuple_activity
)
SELECT
  '<tr><td>Stats window since reset</td><td>' || replace(replace(replace(replace(replace(stats_window::text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td><td class="' ||
  CASE
    WHEN stats_window >= interval '7 days' THEN 'good">High'
    WHEN stats_window >= interval '24 hours' THEN 'warn">Medium'
    ELSE 'crit">Low'
  END || '</td><td>Longer windows produce more stable growth rates.</td></tr>' ||
  '<tr><td>Tuple activity in window</td><td class="num">' || to_char(tuple_activity,'FM999,999,999') || '</td><td class="' ||
  CASE WHEN tuple_activity > 100000 THEN 'good">High' WHEN tuple_activity > 10000 THEN 'warn">Medium' ELSE 'crit">Low' END ||
  '</td><td>Low activity windows reduce forecast reliability.</td></tr>' ||
  '<tr><td>Model assumption</td><td>Linear growth</td><td class="warn">Assumed</td><td>Use forecast as directional planning input, not exact capacity date.</td></tr>'
FROM c;

\qecho '</tbody></table></div></div>'

-- S26.5 Top projected growth drivers (30-day estimate)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Top Growth Drivers</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Current Size</th><th>Estimated Daily Growth</th><th>Projected 30d Growth</th><th>Days to +50GB</th><th>Action</th>'
\qecho '</tr></thead><tbody>'

WITH w AS (
  SELECT COALESCE(EXTRACT(epoch FROM (now() - stats_reset)) / 86400.0, 1.0) AS days_window
  FROM pg_stat_database
  WHERE datname = current_database()
),
g AS (
  SELECT
    st.relid,
    st.schemaname,
    st.relname,
    pg_total_relation_size(st.relid) AS total_bytes,
    GREATEST(st.n_tup_ins - st.n_tup_del, 0)::numeric AS net_rows_window,
    CASE
      WHEN st.n_live_tup > 0 THEN pg_relation_size(st.relid)::numeric / st.n_live_tup
      ELSE NULL
    END AS bytes_per_live_row
  FROM pg_stat_user_tables st
  WHERE st.n_live_tup > 0
),
proj AS (
  SELECT
    g.*,
    w.days_window,
    (g.net_rows_window / NULLIF(w.days_window, 0)) * COALESCE(g.bytes_per_live_row, 0) AS est_daily_growth_bytes
  FROM g CROSS JOIN w
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schemaname || '.' || relname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || pg_size_pretty(total_bytes) || '</td>' ||
      '<td class="num">' || pg_size_pretty(GREATEST(est_daily_growth_bytes,0)::bigint) || '</td>' ||
      '<td class="num">' || pg_size_pretty(GREATEST(est_daily_growth_bytes * 30,0)::bigint) || '</td>' ||
      '<td class="num">' ||
      CASE
        WHEN est_daily_growth_bytes <= 0 THEN 'N/A'
        ELSE round((50.0 * 1024 * 1024 * 1024) / est_daily_growth_bytes, 1)::text
      END || '</td>' ||
      '<td class="' ||
      CASE
        WHEN est_daily_growth_bytes * 30 > 10.0 * 1024 * 1024 * 1024 THEN 'crit">Prioritize partition/retention strategy'
        WHEN est_daily_growth_bytes * 30 > 1.0 * 1024 * 1024 * 1024 THEN 'warn">Plan index/storage expansion'
        ELSE 'good">Monitor trend'
      END || '</td>' ||
      '</tr>',
      E'\n' ORDER BY est_daily_growth_bytes DESC
    ),
    '<tr><td colspan="6" class="table-empty">Insufficient table activity for directional growth projection.</td></tr>'
  )
FROM proj
WHERE est_daily_growth_bytes > 0
LIMIT 25;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S28: CONSULTING REMEDIATION ACTION PLAN
-- =============================================================================
\qecho '<div class="section" id="s28">'
\qecho '<div class="section-header">'
\qecho '  '
\qecho '  <div>'
\qecho '    <div class="section-title">Consulting Remediation Action Plan</div>'
\qecho '    <div class="section-desc">Prioritized action plan  what to fix now, this week, and this quarter. Each item includes root cause, business impact, fix script, and verification steps.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="finding critical"><div class="finding-header">'
\qecho '<span class="finding-title">How to Use This Remediation Plan</span>'
\qecho '<span class="severity-pill pill-info">CONSULTING GUIDE</span></div>'
\qecho '<div class="finding-body">'
\qecho '<strong>Priority 1 (TODAY):</strong> XID wraparound risk, inactive replication slots (disk full risk),'
\qecho 'sequences out of sync, invalid indexes, any SECURITY DEFINER without search_path.<br>'
\qecho '<strong>Priority 2 (THIS WEEK):</strong> Missing FK indexes, unused indexes on large tables,'
\qecho 'tables needing immediate vacuum, high-bloat tables blocking autovacuum.<br>'
\qecho '<strong>Priority 3 (THIS MONTH):</strong> Config tuning (shared_buffers, work_mem, checkpoint),'
\qecho 'planner statistics quality, extended statistics, fillfactor tuning, connection pooling.<br>'
\qecho '<strong>Priority 4 (THIS QUARTER):</strong> RLS policies,'
\qecho 'schema reorganization, archival partitioning strategy, index consolidation.<br>'
\qecho '<strong>Before any change:</strong> Test in staging, use CONCURRENTLY for index operations,'
\qecho 'have rollback plan, monitor for 24h after each change.'
\qecho '</div></div>'

-- Generate dynamic priority action list
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' || CASE WHEN critical_count > 0 THEN 'critical' ELSE 'good' END || '">' ||
  '<div class="card-label"> Critical Actions</div>' ||
  '<div class="card-value">' || critical_count || '</div>' ||
  '<div class="card-sub">Fix today  risk of outage</div></div>' ||

  '<div class="card ' || CASE WHEN high_count > 0 THEN 'warning' ELSE 'good' END || '">' ||
  '<div class="card-label"> High Priority</div>' ||
  '<div class="card-value">' || high_count || '</div>' ||
  '<div class="card-sub">Fix this week</div></div>' ||

  '<div class="card">' ||
  '<div class="card-label"> Medium Priority</div>' ||
  '<div class="card-value">' || medium_count || '</div>' ||
  '<div class="card-sub">Fix this month</div></div>' ||

  '<div class="card">' ||
  '<div class="card-label"> Improvements</div>' ||
  '<div class="card-value">' || improve_count || '</div>' ||
  '<div class="card-sub">Planned quarter work</div></div>' ||
  '</div>'
FROM (
  SELECT
    -- Critical
    (CASE WHEN (SELECT age(datfrozenxid) FROM pg_database WHERE datname=current_database()) > 1500000000 THEN 1 ELSE 0 END) +
    (SELECT COUNT(*) FROM pg_replication_slots WHERE NOT active)::int +
    (SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND NOT (p.proconfig::text ILIKE '%search_path%') AND n.nspname NOT IN ('pg_catalog','information_schema'))::int
    AS critical_count,

    -- High
    (SELECT COUNT(*) FROM pg_index WHERE NOT indisvalid)::int +
    (SELECT COUNT(*) FROM pg_stat_user_tables WHERE n_dead_tup > 100000)::int +
    (SELECT COUNT(*) FROM pg_stat_user_indexes WHERE idx_scan=0 AND pg_relation_size(indexrelid)>1048576)::int
    AS high_count,

    -- Medium
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_min_duration_statement') = '-1' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='track_io_timing') = 'off' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_checkpoints') = 'off' THEN 1 ELSE 0 END) +
    (CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_lock_waits') = 'off' THEN 1 ELSE 0 END)
    AS medium_count,

    -- Improvements
    5 AS improve_count
) action_counts;

-- Actionable findings table
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Priority</th><th>Finding ID</th><th>Issue</th>'
\qecho '<th>Root Cause</th><th>Business Impact</th><th>Immediate Action</th><th>Verification</th>'
\qecho '</tr></thead><tbody>'

-- XID wraparound
SELECT
  CASE WHEN age(datfrozenxid) > 1000000000
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-critical"> P1 TODAY</span></td>' ||
       '<td>S21.4</td>' ||
       '<td>XID Age: ' || to_char(age(datfrozenxid),'FM999,999,999') || ' (' || round(age(datfrozenxid)*100.0/2000000000,1)::text || '% of limit)</td>' ||
       '<td>Autovacuum not keeping up with transaction rate. Tables not being frozen.</td>' ||
       '<td class="crit">Database WILL shut down and refuse writes if XID reaches 2B. No warning before shutdown.</td>' ||
       '<td class="code-block">' ||
       replace(replace(replace(replace(replace(
         format('vacuumdb -d %s --freeze --analyze -j 4 -v', quote_literal(current_database()))
       ,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
       '</td>' ||
       '<td>Check: SELECT age(datfrozenxid) FROM pg_database;</td></tr>'
  ELSE ''
  END
FROM pg_database
WHERE datname = current_database();

-- Inactive replication slots
SELECT
  CASE WHEN cnt > 0
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-critical"> P1 TODAY</span></td>' ||
       '<td>S08.slot</td>' ||
       '<td>' || cnt || ' inactive replication slot(s)  WAL accumulating</td>' ||
       '<td>Replication consumer stopped or disconnected. Slot prevents WAL cleanup.</td>' ||
       '<td class="crit">pg_wal directory grows indefinitely  disk full  database crash.</td>' ||
       '<td class="code-block">'  ||
       '-- First verify slot is truly unused, then:<br>' ||
       '-- SELECT pg_drop_replication_slot(''slot_name'');</td>' ||
       '<td>Monitor: SELECT slot_name,active,pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)) FROM pg_replication_slots;</td></tr>'
  ELSE ''
  END
FROM (SELECT COUNT(*) AS cnt FROM pg_replication_slots WHERE NOT active) rs;

-- Invalid indexes
SELECT
  CASE WHEN cnt > 0
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-high"> P2 WEEK</span></td>' ||
       '<td>S06.5</td>' ||
       '<td>' || cnt || ' invalid index(es)  not being used but consuming space</td>' ||
       '<td>CREATE INDEX CONCURRENTLY was interrupted or failed. Index marked INVALID.</td>' ||
       '<td class="warn">Queries cannot use this index. Writes still update it (write overhead, no read benefit).</td>' ||
       '<td class="code-block">-- For each invalid index:<br>DROP INDEX CONCURRENTLY invalid_index_name;<br>-- Then recreate with CREATE INDEX CONCURRENTLY</td>' ||
       '<td>SELECT indexrelid::regclass FROM pg_index WHERE NOT indisvalid;</td></tr>'
  ELSE ''
  END
FROM (SELECT COUNT(*) AS cnt FROM pg_index WHERE NOT indisvalid) iv;

-- Tables never vacuumed with many dead tuples
SELECT
  CASE WHEN cnt > 0
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-high"> P2 WEEK</span></td>' ||
       '<td>S21.2</td>' ||
       '<td>' || cnt || ' table(s) with >50k dead tuples need immediate vacuum</td>' ||
       '<td>Autovacuum threshold too high, or autovacuum disabled/throttled too much.</td>' ||
       '<td class="warn">Table bloat  larger sequential scans  slower queries. Statistics stale  bad plans.</td>' ||
       '<td class="code-block">-- Run manually for top offenders:<br>VACUUM ANALYZE schema.table_name;<br>-- Then: Lower autovacuum_vacuum_scale_factor per table</td>' ||
       '<td>After: n_dead_tup should drop to near 0 in pg_stat_user_tables.</td></tr>'
  ELSE ''
  END
FROM (SELECT COUNT(*) AS cnt FROM pg_stat_user_tables WHERE n_dead_tup > 50000) dt;

-- Missing FK indexes
SELECT
  CASE WHEN cnt > 0
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-high"> P2 WEEK</span></td>' ||
       '<td>S06.3</td>' ||
       '<td>' || cnt || ' foreign key column(s) without supporting index</td>' ||
       '<td>Oracle auto-creates FK indexes. PostgreSQL does not. Migration tools miss this.</td>' ||
       '<td class="warn">Every DELETE/UPDATE on parent table  full sequential scan of child table. 100-1000x slower on large tables.</td>' ||
       '<td class="code-block">-- See Section S06.3 for exact CREATE INDEX CONCURRENTLY scripts per FK column</td>' ||
       '<td>After: idx_scan count for new index should increase. seq_scan should decrease.</td></tr>'
  ELSE ''
  END
FROM (
  SELECT COUNT(*) AS cnt
  FROM pg_constraint con
  JOIN pg_class c ON c.oid=con.conrelid
  JOIN pg_namespace n ON n.oid=c.relnamespace
  JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS x(attnum,ord) ON true
  WHERE con.contype='f'
    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND NOT EXISTS (
      SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indkey[0]=x.attnum
    )
) fk_miss;

-- Config: slow query logging off
SELECT
  CASE WHEN (SELECT setting FROM pg_settings WHERE name='log_min_duration_statement') = '-1'
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-medium"> P3 MONTH</span></td>' ||
       '<td>S23.1</td>' ||
       '<td>Slow query logging disabled (log_min_duration_statement=-1)</td>' ||
       '<td>Default configuration  slow queries are invisible in logs.</td>' ||
       '<td>Cannot diagnose performance incidents without slow query logs. Flying blind.</td>' ||
       '<td class="code-block">-- In postgresql.conf:<br>log_min_duration_statement = 1000 -- log queries > 1 second<br>-- Or per-session: SET log_min_duration_statement=500;</td>' ||
       '<td>After: Check pg_log for slow query entries. Confirm pg_stat_statements also enabled.</td></tr>'
  ELSE ''
  END;

-- Config: track_io_timing off
SELECT
  CASE WHEN (SELECT setting FROM pg_settings WHERE name='track_io_timing') = 'off'
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-medium"> P3 MONTH</span></td>' ||
       '<td>S23.1</td>' ||
       '<td>track_io_timing disabled  no I/O timing data in EXPLAIN or pg_stat_statements</td>' ||
       '<td>Performance feature off by default  must be explicitly enabled.</td>' ||
       '<td>Cannot identify I/O vs CPU bottlenecks. EXPLAIN BUFFERS shows block counts but not timing.</td>' ||
       '<td class="code-block">-- In postgresql.conf:<br>track_io_timing = on<br>-- Or per-session (no restart needed):<br>SET track_io_timing = on;</td>' ||
       '<td>After: EXPLAIN (ANALYZE, BUFFERS) shows I/O time. Confirm in pg_stat_statements.blk_read_time.</td></tr>'
  ELSE ''
  END;

-- Security: SSL off
SELECT
  CASE WHEN (SELECT setting FROM pg_settings WHERE name='ssl') = 'off'
  THEN '<tr>' ||
       '<td><span class="severity-pill pill-critical"> P1 TODAY</span></td>' ||
       '<td>S12.ssl</td>' ||
       '<td>SSL disabled  all connections transmit data in plaintext</td>' ||
       '<td>ssl=off in postgresql.conf. Default is off on some platforms.</td>' ||
       '<td class="crit">Passwords, queries, and all data transmitted in cleartext. Trivial to intercept on any network.</td>' ||
       '<td class="code-block">-- In postgresql.conf:<br>ssl = on<br>ssl_cert_file = ''server.crt''<br>ssl_key_file = ''server.key''<br>-- Requires PostgreSQL restart</td>' ||
       '<td>After: SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid();</td></tr>'
  ELSE ''
  END;

\qecho '<tr><td colspan="7">'
\qecho ' This action plan is auto-generated from live diagnostic data. Priorities are recalculated each time PG360 runs.'
\qecho 'Work through P1 items immediately, P2 within a week, P3 within a month.'
\qecho 'Always test changes in staging first. Use CONCURRENTLY variants for index operations on live systems.'
\qecho '</td></tr>'

\qecho '</tbody></table></div>'

-- S28.2 Runbook-grade remediation matrix (fix / verify / rollback / downtime)
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Runbook Matrix</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Priority</th><th>Module</th><th>Issue</th><th>Risk</th><th>Downtime Required</th><th>Fix</th><th>Verify</th><th>Rollback</th>'
\qecho '</tr></thead><tbody>'

WITH issues AS (
  SELECT
    'P1 TODAY'::text AS priority,
    'S21.4'::text AS module_id,
    'XID age nearing wraparound safety window'::text AS issue,
    (SELECT age(datfrozenxid) > 1000000000 FROM pg_database WHERE datname = current_database()) AS present,
    'Critical'::text AS risk,
    'No (online vacuum path)'::text AS downtime_required,
    'vacuumdb -d ' || quote_literal(current_database()) || ' --freeze --analyze -j 4 -v'::text AS fix_sql,
    'SELECT age(datfrozenxid) FROM pg_database WHERE datname=current_database();'::text AS verify_sql,
    'Restore previous scheduling; keep aggressive freeze for top aged tables only.'::text AS rollback_step
  UNION ALL
  SELECT
    'P1 TODAY','S08','Inactive replication slot retention risk',
    EXISTS (SELECT 1 FROM pg_replication_slots WHERE NOT active),
    'Critical','No',
    'SELECT pg_drop_replication_slot(''slot_name''); -- after consumer validation',
    'SELECT slot_name,active FROM pg_replication_slots;',
    'Recreate slot and reconnect consumer if dropped prematurely.'
  UNION ALL
  SELECT
    'P2 WEEK','S06','Invalid indexes requiring rebuild',
    EXISTS (SELECT 1 FROM pg_index WHERE NOT indisvalid),
    'High','No (CONCURRENTLY)',
    'DROP INDEX CONCURRENTLY idx; CREATE INDEX CONCURRENTLY idx ON ...;',
    'SELECT indexrelid::regclass, indisvalid FROM pg_index WHERE NOT indisvalid;',
    'Recreate original index definition and retain old name compatibility.'
  UNION ALL
  SELECT
    'P2 WEEK','S06','Missing foreign-key support indexes',
    EXISTS (
      SELECT 1
      FROM pg_constraint con
      JOIN pg_class c ON c.oid = con.conrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS x(attnum,ord) ON true
      WHERE con.contype='f'
        AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
        AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid=c.oid AND i.indkey[0]=x.attnum)
    ),
    'High','No (CONCURRENTLY)',
    'CREATE INDEX CONCURRENTLY ... ON child_table(fk_col);',
    'EXPLAIN on parent DELETE/UPDATE should stop showing child seq scans.',
    'DROP INDEX CONCURRENTLY created_idx_name;'
  UNION ALL
  SELECT
    'P3 MONTH','S23','Observability disabled (slow log and I/O timing)',
    (SELECT setting FROM pg_settings WHERE name='log_min_duration_statement') = '-1'
      OR (SELECT setting FROM pg_settings WHERE name='track_io_timing') = 'off',
    'Medium',
    'No (reload for most settings)',
    'ALTER SYSTEM SET log_min_duration_statement = 1000; ALTER SYSTEM SET track_io_timing = on; SELECT pg_reload_conf();',
    'SHOW log_min_duration_statement; SHOW track_io_timing;',
    'ALTER SYSTEM RESET log_min_duration_statement; ALTER SYSTEM RESET track_io_timing; SELECT pg_reload_conf();'
),
filtered AS (
  SELECT * FROM issues WHERE present
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td><span class="severity-pill ' ||
      CASE priority WHEN 'P1 TODAY' THEN 'pill-critical' WHEN 'P2 WEEK' THEN 'pill-high' ELSE 'pill-medium' END || '">' ||
      replace(priority,'&','&amp;') || '</span></td>' ||
      '<td>' || replace(replace(module_id,'<','&lt;'),'>','&gt;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(issue,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE risk WHEN 'Critical' THEN 'crit">Critical' WHEN 'High' THEN 'warn">High' ELSE 'good">Medium' END || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(downtime_required,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' || replace(replace(replace(replace(replace(fix_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="code-block">' || replace(replace(replace(replace(replace(verify_sql,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(rollback_step,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY
        CASE priority WHEN 'P1 TODAY' THEN 0 WHEN 'P2 WEEK' THEN 1 ELSE 2 END,
        module_id
    ),
    '<tr><td colspan="8" class="table-empty">No active remediation items met threshold for this run.</td></tr>'
  )
FROM filtered;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S29: EXTENSION INVENTORY
-- =============================================================================
\qecho '<div class="section" id="s29">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">S29</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Extension Inventory</div>'
\qecho '    <div class="section-desc">Installed extensions, monitoring extension readiness, and extension-specific security and observability guidance.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Extension &amp; Monitoring Snapshot</div>'

WITH ext_status AS (
  SELECT
    ext_name,
    is_required,
    is_recommended,
    EXISTS (SELECT 1 FROM pg_extension e WHERE e.extname = ext_name) AS installed
  FROM (
    VALUES
      ('pg_stat_statements', true,  false),
      ('pg_buffercache',    false, true),
      ('pgstattuple',       false, true),
      ('pg_prewarm',        false, false),
      ('pg_visibility',     false, false),
      ('pg_stat_kcache',    false, false)
  ) v(ext_name, is_required, is_recommended)
), agg AS (
  SELECT
    COUNT(*) FILTER (WHERE is_required) AS required_total,
    COUNT(*) FILTER (WHERE is_required AND installed) AS required_installed,
    COUNT(*) FILTER (WHERE is_recommended) AS recommended_total,
    COUNT(*) FILTER (WHERE is_recommended AND installed) AS recommended_installed,
    COUNT(*) FILTER (WHERE NOT is_required AND NOT is_recommended) AS optional_total,
    COUNT(*) FILTER (WHERE NOT is_required AND NOT is_recommended AND installed) AS optional_installed,
    COALESCE(current_setting('shared_preload_libraries', true), '') AS preload_libs,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement
  FROM ext_status
)
SELECT
  '<div class="card-grid">' ||
  '<div class="card ' ||
    CASE WHEN required_installed = required_total THEN 'good' ELSE 'critical' END ||
  '"><div class="card-label">Required Extensions</div><div class="card-value">' ||
    required_installed || '/' || required_total || '</div><div class="card-sub">pg_stat_statements baseline</div></div>' ||
  '<div class="card ' ||
    CASE WHEN recommended_installed = recommended_total THEN 'good' ELSE 'warning' END ||
  '"><div class="card-label">Recommended Extensions</div><div class="card-value">' ||
    recommended_installed || '/' || recommended_total || '</div><div class="card-sub">pg_buffercache + pgstattuple</div></div>' ||
  '<div class="card"><div class="card-label">Optional Extensions</div><div class="card-value">' ||
    optional_installed || '/' || optional_total || '</div><div class="card-sub">pg_prewarm, pg_visibility, pg_stat_kcache</div></div>' ||
  '<div class="card ' ||
    CASE WHEN preload_libs ILIKE '%pg_stat_statements%' THEN 'good' ELSE 'warning' END ||
  '"><div class="card-label">shared_preload_libraries</div><div class="card-value">' ||
    CASE WHEN preload_libs = '' THEN '(empty)' ELSE replace(replace(replace(replace(replace(preload_libs,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') END ||
  '</div></div>' ||
  '<div class="card ' ||
    CASE WHEN track_io_timing = 'on' THEN 'good' ELSE 'warning' END ||
  '"><div class="card-label">track_io_timing</div><div class="card-value">' ||
    replace(replace(replace(replace(replace(track_io_timing,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '<div class="card ' ||
    CASE WHEN log_min_duration_statement = '-1' THEN 'warning' ELSE 'good' END ||
  '"><div class="card-label">log_min_duration_statement</div><div class="card-value">' ||
    replace(replace(replace(replace(replace(log_min_duration_statement,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</div></div>' ||
  '</div>'
FROM agg;

WITH ext_status AS (
  SELECT
    ext_name,
    is_required,
    is_recommended,
    EXISTS (SELECT 1 FROM pg_extension e WHERE e.extname = ext_name) AS installed
  FROM (
    VALUES
      ('pg_stat_statements', true,  false),
      ('pg_buffercache',    false, true),
      ('pgstattuple',       false, true)
  ) v(ext_name, is_required, is_recommended)
), summary AS (
  SELECT
    COUNT(*) FILTER (WHERE is_required AND NOT installed) AS req_missing,
    COUNT(*) FILTER (WHERE is_recommended AND NOT installed) AS rec_missing,
    COALESCE(current_setting('track_io_timing', true), 'off') AS track_io_timing,
    COALESCE(current_setting('log_min_duration_statement', true), '-1') AS log_min_duration_statement
  FROM ext_status
)
SELECT
  '<div class="finding ' ||
  CASE
    WHEN req_missing > 0 THEN 'critical'
    WHEN rec_missing > 0 OR track_io_timing <> 'on' OR log_min_duration_statement = '-1' THEN 'high'
    ELSE 'good'
  END || '">' ||
  '<div class="finding-header"><span class="finding-title">Extension observability readiness</span>' ||
  '<span class="severity-pill ' ||
  CASE
    WHEN req_missing > 0 THEN 'pill-critical">BLOCKED'
    WHEN rec_missing > 0 OR track_io_timing <> 'on' OR log_min_duration_statement = '-1' THEN 'pill-high">ACTION'
    ELSE 'pill-good">READY'
  END || '</span></div>' ||
  '<div class="finding-body">Missing required extensions: <strong>' || req_missing ||
  '</strong>; missing recommended extensions: <strong>' || rec_missing ||
  '</strong>; track_io_timing=' || track_io_timing ||
  '; log_min_duration_statement=' || log_min_duration_statement ||
  '. Use this section to close observability gaps before deep tuning decisions.</div></div>'
FROM summary;

\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Installed Extensions Inventory</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Extension</th><th>Version</th><th>Schema</th><th>Owner</th><th>Use in PG360</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(e.extname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(e.extversion,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(n.nspname,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(pg_get_userbyid(e.extowner),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' ||
        CASE e.extname
          WHEN 'pg_stat_statements' THEN 'Primary SQL telemetry source'
          WHEN 'pg_buffercache' THEN 'Buffer residency validation'
          WHEN 'pgstattuple' THEN 'Exact bloat validation'
          WHEN 'pg_stat_kcache' THEN 'OS-level per-query attribution'
          WHEN 'pg_visibility' THEN 'Visibility map and vacuum evidence'
          ELSE 'General extension inventory only'
        END ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY e.extname
    ),
    '<tr><td colspan="5" class="table-empty">No installed extensions returned in this database.</td></tr>'
  )
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace;

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Recommended Extension Status</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Extension</th><th>Status</th><th>Purpose</th><th>Install Command</th><th>Priority</th>'
\qecho '</tr></thead><tbody>'

SELECT
  '<tr>' ||
  '<td class="num">' || ext_name || '</td>' ||
  '<td class="' || CASE WHEN is_installed THEN 'good"> Installed' ELSE 'warn"> Not installed' END || '</td>' ||
  '<td>' || purpose || '</td>' ||
  '<td class="code-block">' ||
  replace(replace(replace(replace(replace(install_cmd,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
  '</td>' ||
  '<td><span class="severity-pill ' || priority_class || '">' || priority_label || '</span></td>' ||
  '</tr>'
FROM (
  SELECT
    'pg_stat_statements' AS ext_name,
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements') AS is_installed,
    'Query performance tracking  most critical extension for performance diagnosis' AS purpose,
    E'shared_preload_libraries = ''pg_stat_statements'' (restart required)\nCREATE EXTENSION pg_stat_statements;' AS install_cmd,
    'pill-critical' AS priority_class, ' CRITICAL' AS priority_label
  UNION ALL SELECT
    'pg_buffercache',
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_buffercache'),
    'Inspect what is in shared_buffers  which tables/indexes are cached',
    'CREATE EXTENSION pg_buffercache;',
    'pill-medium', ' RECOMMENDED'
  UNION ALL SELECT
    'pgstattuple',
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pgstattuple'),
    'Exact table and index bloat measurement (vs heuristic estimates)',
    'CREATE EXTENSION pgstattuple;',
    'pill-medium', ' RECOMMENDED'
  UNION ALL SELECT
    'pg_prewarm',
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_prewarm'),
    'Pre-load hot tables into buffer cache after restart  reduce cold-start impact',
    'CREATE EXTENSION pg_prewarm;',
    'pill-info', ' OPTIONAL'
  UNION ALL SELECT
    'pg_visibility',
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_visibility'),
    'Inspect visibility map  identify tables needing VACUUM for sequential scan optimization',
    'CREATE EXTENSION pg_visibility;',
    'pill-info', ' OPTIONAL'
  UNION ALL SELECT
    'pg_stat_kcache',
    EXISTS (SELECT 1 FROM pg_extension WHERE extname='pg_stat_kcache'),
    'OS-level I/O and memory statistics per query (requires pg_stat_statements)',
    'CREATE EXTENSION pg_stat_kcache;',
    'pill-info', ' OPTIONAL'
) ext_list;

\qecho '</tbody></table></div></div>'

-- pg_stat_statements configuration advice
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Optimal pg_stat_statements Configuration</div>'
\qecho '<div class="code-block">'
\qecho '-- Add to postgresql.conf (requires restart for shared_preload_libraries):'
\qecho 'shared_preload_libraries = ''pg_stat_statements''  -- REQUIRED for PG11-'
\qecho ''
\qecho '-- Tune pg_stat_statements:'
\qecho 'pg_stat_statements.max = 10000          -- track top 10k unique query shapes (default 5000)'
\qecho 'pg_stat_statements.track = all          -- track all statements including nested (default top)'
\qecho 'pg_stat_statements.track_utility = on   -- track COPY, VACUUM, etc. (default off)'
\qecho 'pg_stat_statements.save = on            -- persist across restarts (default on)'
\qecho ''
\qecho '-- After restart, create extension:'
\qecho 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;'
\qecho ''
\qecho '-- Verify:'
\qecho 'SELECT calls, mean_exec_time, query FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 5;'
\qecho '</div>'
\qecho '</div>'

-- S29.3 Observability tiering
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Observability Tiers</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Tier</th><th>Required Components</th><th>Operational Outcome</th>'
\qecho '</tr></thead><tbody>'
\qecho '<tr><td class="good">Minimal</td><td>pg_stat_statements, log_min_duration_statement, log_lock_waits, track_counts</td><td>Baseline performance triage with SQL and lock visibility.</td></tr>'
\qecho '<tr><td class="warn">Recommended</td><td>Minimal + track_io_timing, pgstattuple, pg_buffercache, checkpoint logging</td><td>Actionable IO/plan diagnostics and bloat validation.</td></tr>'
\qecho '<tr><td class="warn">Advanced</td><td>Recommended + auto_explain, pgaudit (if compliance), pg_stat_kcache</td><td>Deep forensic coverage with per-query runtime behavior and audit trail.</td></tr>'
\qecho '</tbody></table></div></div>'

-- S29.4 auto_explain safe baseline
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">auto_explain Safe Baseline</div>'
\qecho '<div class="code-block">'
\qecho '-- postgresql.conf (restart required for shared_preload_libraries if missing auto_explain)'
\qecho 'shared_preload_libraries = ''pg_stat_statements,auto_explain'''
\qecho ''
\qecho '-- Keep overhead controlled:'
\qecho 'auto_explain.log_min_duration = ''1000ms'''
\qecho 'auto_explain.log_analyze = on'
\qecho 'auto_explain.log_buffers = on'
\qecho 'auto_explain.log_timing = off'
\qecho 'auto_explain.log_verbose = off'
\qecho 'auto_explain.log_nested_statements = off'
\qecho 'auto_explain.sample_rate = 0.1'
\qecho ''
\qecho '-- Verify:'
\qecho 'SHOW auto_explain.log_min_duration;'
\qecho 'SHOW auto_explain.sample_rate;'
\qecho '</div>'
\qecho '</div>'

-- S29.5 Read-only monitoring role template
\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Read-Only Monitoring Role Template</div>'
\qecho '<div class="code-block">'
\qecho '-- Create role for observability tools (Grafana/collectors):'
\qecho 'CREATE ROLE pg360_monitor LOGIN;'
\qecho 'GRANT pg_monitor TO pg360_monitor;'
\qecho ''
\qecho '-- Optional explicit grants for non-default schemas:'
\qecho 'GRANT USAGE ON SCHEMA public TO pg360_monitor;'
\qecho 'GRANT SELECT ON ALL TABLES IN SCHEMA public TO pg360_monitor;'
\qecho 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO pg360_monitor;'
\qecho ''
\qecho '-- Verification:'
\qecho 'SELECT has_role(''pg360_monitor'',''pg_monitor'',''member'');'
\qecho '</div>'
\qecho '</div>'

\qecho '</div>'

-- =============================================================================
-- SECTION S30: JOIN RISK DETECTION
-- =============================================================================
\qecho '<div class="section" id="s30">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">S30</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Join Risk Detection</div>'
\qecho '    <div class="section-desc">Heuristic join-column risk detection using scan pressure, join-heavy SQL fingerprints, and missing leftmost index coverage.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Join-Column Index Gaps</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Column</th><th>Est Rows</th><th>Seq Scan</th><th>Idx Scan</th><th>Rows Read</th><th>Distinctness</th><th>Suggested Review SQL</th>'
\qecho '</tr></thead><tbody>'

WITH table_pressure AS (
  SELECT
    st.relid,
    st.schemaname AS schema_name,
    st.relname AS table_name,
    st.seq_scan,
    st.idx_scan,
    st.seq_tup_read,
    c.reltuples::bigint AS est_rows
  FROM pg_stat_user_tables st
  JOIN pg_class c ON c.oid = st.relid
  WHERE c.reltuples >= 10000
    AND st.seq_tup_read >= 100000
    AND st.seq_scan >= st.idx_scan
),
joinish_columns AS (
  SELECT
    p.relid,
    p.schema_name,
    p.table_name,
    a.attnum,
    a.attname AS column_name,
    p.est_rows,
    p.seq_scan,
    p.idx_scan,
    p.seq_tup_read,
    s.n_distinct
  FROM table_pressure p
  JOIN pg_attribute a
    ON a.attrelid = p.relid
   AND a.attnum > 0
   AND NOT a.attisdropped
  LEFT JOIN pg_stats s
    ON s.schemaname = p.schema_name
   AND s.tablename = p.table_name
   AND s.attname = a.attname
  WHERE a.attname ~* '(^id$|_id$|_key$|_code$)'
),
missing AS (
  SELECT j.*
  FROM joinish_columns j
  WHERE NOT EXISTS (
    SELECT 1
    FROM pg_index i
    WHERE i.indrelid = j.relid
      AND i.indisvalid
      AND i.indisready
      AND (i.indkey::smallint[])[array_lower(i.indkey::smallint[], 1)] = j.attnum
  )
)
SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schema_name || '.' || table_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(column_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="num">' || to_char(est_rows,'FM999,999,999,999') || '</td>' ||
      '<td class="num">' || to_char(seq_scan,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(idx_scan,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(seq_tup_read,'FM999,999,999,999') || '</td>' ||
      '<td class="num">' || COALESCE(to_char(n_distinct,'FM999999990.00'),'n/a') || '</td>' ||
      '<td class="code-block">' ||
        replace(replace(replace(replace(replace(
          format(
            'CREATE INDEX CONCURRENTLY %I ON %I.%I (%I);',
            left('idx_' || table_name || '_' || column_name, 60),
            schema_name,
            table_name,
            column_name
          ),
        '&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY seq_tup_read DESC, est_rows DESC, schema_name, table_name, column_name
    ),
    '<tr><td colspan="8" class="table-empty">No join-column index gaps crossed the scan-pressure threshold in this run.</td></tr>'
  )
FROM missing;

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Join-Heavy SQL Fingerprints</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>QueryID</th><th>Calls</th><th>Total Exec ms</th><th>Mean Exec ms</th><th>Shared Reads</th><th>Temp Writes</th><th>Fingerprint</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || queryid::text || '</td>' ||
      '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(round(total_exec_time::numeric, 1),'FM999,999,999,990.0') || '</td>' ||
      '<td class="num">' || to_char(round(mean_exec_time::numeric, 2),'FM999,999,990.00') || '</td>' ||
      '<td class="num">' || to_char(shared_blks_read,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(temp_blks_written,'FM999,999,999') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(query_text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY total_exec_time DESC, calls DESC
    ),
    '<tr><td colspan="7" class="table-empty">No join-heavy fingerprints were found in pg_stat_statements for this run.</td></tr>'
  )
FROM (
  SELECT
    queryid,
    calls,
    total_exec_time,
    mean_exec_time,
    shared_blks_read,
    temp_blks_written,
    left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 180) AS query_text
  FROM pg_stat_statements
  WHERE lower(query) LIKE '% join %'
  ORDER BY total_exec_time DESC
  LIMIT 20
) q;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S31: PARALLEL QUERY EFFICIENCY
-- =============================================================================
\qecho '<div class="section" id="s31">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">S31</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">Parallel Query Efficiency</div>'
\qecho '    <div class="section-desc">Parallel planner settings, worker fulfillment, and query fingerprints that requested or launched parallel workers.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Parallel Planner &amp; Worker Settings</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Setting</th><th>Value</th><th>Status</th><th>Interpretation</th>'
\qecho '</tr></thead><tbody>'

WITH cfg AS (
  SELECT name, setting
  FROM pg_settings
  WHERE name IN (
    'max_parallel_workers',
    'max_parallel_workers_per_gather',
    'max_worker_processes',
    'parallel_leader_participation',
    'min_parallel_table_scan_size',
    'min_parallel_index_scan_size'
  )
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || name || '</td><td>' ||
      replace(replace(replace(replace(replace(setting,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td class="' ||
      CASE
        WHEN name = 'max_parallel_workers_per_gather' AND setting::int = 0 THEN 'warn">Disabled'
        WHEN name = 'max_parallel_workers' AND setting::int = 0 THEN 'warn">Disabled'
        WHEN name = 'parallel_leader_participation' AND setting = 'off' THEN 'warn">Leader excluded'
        ELSE 'good">Configured'
      END ||
      '</td><td>' ||
      CASE
        WHEN name = 'max_parallel_workers_per_gather' THEN 'Upper bound per parallel plan node.'
        WHEN name = 'max_parallel_workers' THEN 'Global worker budget across the cluster.'
        WHEN name = 'max_worker_processes' THEN 'Overall worker ceiling shared with background workers.'
        WHEN name = 'parallel_leader_participation' THEN 'Leader can contribute CPU when on.'
        WHEN name = 'min_parallel_table_scan_size' THEN 'Planner threshold before table scans consider parallelism.'
        WHEN name = 'min_parallel_index_scan_size' THEN 'Planner threshold before index scans consider parallelism.'
        ELSE 'Review in context.'
      END || '</td></tr>',
      E'\n' ORDER BY name
    ),
    '<tr><td colspan="4" class="table-empty">Parallel planner settings not exposed.</td></tr>'
  )
FROM cfg;

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">Queries Using Parallel Workers</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>QueryID</th><th>Calls</th><th>Workers Requested</th><th>Workers Launched</th><th>Fulfillment %</th><th>Total Exec ms</th><th>Fingerprint</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || queryid::text || '</td>' ||
      '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(parallel_workers_to_launch,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(parallel_workers_launched,'FM999,999,999') || '</td>' ||
      '<td class="' ||
        CASE
          WHEN parallel_workers_to_launch = 0 THEN '">n/a'
          WHEN parallel_workers_launched::numeric / NULLIF(parallel_workers_to_launch, 0) < 0.70 THEN 'warn">'
          ELSE 'good">'
        END ||
        COALESCE(to_char(round(100.0 * parallel_workers_launched::numeric / NULLIF(parallel_workers_to_launch, 0), 1), 'FM990.0') || '%', 'n/a') ||
      '</td>' ||
      '<td class="num">' || to_char(round(total_exec_time::numeric, 1),'FM999,999,999,990.0') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(query_text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY total_exec_time DESC, calls DESC
    ),
    '<tr><td colspan="7" class="table-empty">No query fingerprints recorded parallel worker activity in this run.</td></tr>'
  )
FROM (
  SELECT
    queryid,
    calls,
    parallel_workers_to_launch,
    parallel_workers_launched,
    total_exec_time,
    left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 180) AS query_text
  FROM pg_stat_statements
  WHERE parallel_workers_to_launch > 0
     OR parallel_workers_launched > 0
  ORDER BY total_exec_time DESC
  LIMIT 20
) q;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S32: JIT USAGE ANALYSIS
-- =============================================================================
\qecho '<div class="section" id="s32">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">S32</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">JIT Usage Analysis</div>'
\qecho '    <div class="section-desc">JIT enablement, cost thresholds, and query fingerprints where JIT compilation time may be material.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">JIT Settings Snapshot</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Setting</th><th>Value</th><th>Status</th><th>Operational Meaning</th>'
\qecho '</tr></thead><tbody>'

WITH cfg AS (
  SELECT name, setting
  FROM pg_settings
  WHERE name IN ('jit','jit_above_cost','jit_inline_above_cost','jit_optimize_above_cost')
)
SELECT
  COALESCE(
    string_agg(
      '<tr><td>' || name || '</td><td>' ||
      replace(replace(replace(replace(replace(setting,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') ||
      '</td><td class="' ||
      CASE
        WHEN name = 'jit' AND setting = 'off' THEN 'warn">Disabled'
        WHEN name = 'jit' AND setting = 'on' THEN 'good">Enabled'
        ELSE '">Review'
      END || '</td><td>' ||
      CASE
        WHEN name = 'jit' THEN 'Master switch for LLVM-based compilation of expensive query fragments.'
        WHEN name = 'jit_above_cost' THEN 'Planner cost threshold before JIT is considered.'
        WHEN name = 'jit_inline_above_cost' THEN 'Cost threshold before inlining is attempted.'
        WHEN name = 'jit_optimize_above_cost' THEN 'Cost threshold before aggressive LLVM optimization is attempted.'
        ELSE 'Review in context.'
      END || '</td></tr>',
      E'\n' ORDER BY name
    ),
    '<tr><td colspan="4" class="table-empty">JIT settings not exposed.</td></tr>'
  )
FROM cfg;

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">JIT-Active Query Fingerprints</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>QueryID</th><th>Calls</th><th>Total Exec ms</th><th>Total JIT ms</th><th>JIT % of Exec</th><th>Interpretation</th><th>Fingerprint</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || queryid::text || '</td>' ||
      '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(round(total_exec_time::numeric, 1),'FM999,999,999,990.0') || '</td>' ||
      '<td class="num">' || to_char(round(total_jit_ms::numeric, 1),'FM999,999,999,990.0') || '</td>' ||
      '<td class="' ||
        CASE
          WHEN total_exec_time <= 0 THEN '">n/a'
          WHEN total_jit_ms / total_exec_time > 0.30 THEN 'warn">'
          ELSE 'good">'
        END ||
        COALESCE(to_char(round(100.0 * total_jit_ms::numeric / NULLIF(total_exec_time::numeric, 0), 1), 'FM990.0') || '%', 'n/a') ||
      '</td>' ||
      '<td>' ||
        CASE
          WHEN total_exec_time > 0 AND total_jit_ms / total_exec_time > 0.30 THEN 'JIT overhead is material; validate that compile cost is justified.'
          WHEN total_jit_ms > 0 THEN 'JIT is active on an expensive query shape.'
          ELSE 'No JIT time recorded.'
        END ||
      '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(query_text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY total_jit_ms DESC, total_exec_time DESC
    ),
    '<tr><td colspan="7" class="table-empty">No JIT-active fingerprints were recorded in pg_stat_statements for this run.</td></tr>'
  )
FROM (
  SELECT
    queryid,
    calls,
    total_exec_time,
    (
      COALESCE(jit_generation_time, 0) +
      COALESCE(jit_inlining_time, 0) +
      COALESCE(jit_optimization_time, 0) +
      COALESCE(jit_emission_time, 0) +
      COALESCE(jit_deform_time, 0)
    ) AS total_jit_ms,
    left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 180) AS query_text
  FROM pg_stat_statements
  WHERE (
      COALESCE(jit_generation_time, 0) +
      COALESCE(jit_inlining_time, 0) +
      COALESCE(jit_optimization_time, 0) +
      COALESCE(jit_emission_time, 0) +
      COALESCE(jit_deform_time, 0)
    ) > 0
  ORDER BY total_jit_ms DESC
  LIMIT 20
) q;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

-- =============================================================================
-- SECTION S33: JSONB WORKLOAD DETECTION
-- =============================================================================
\qecho '<div class="section" id="s33">'
\qecho '<div class="section-header">'
\qecho '  <span class="section-id">S33</span>'
\qecho '  <div>'
\qecho '    <div class="section-title">JSONB Workload Detection</div>'
\qecho '    <div class="section-desc">JSON and array column inventory, JSON-heavy SQL fingerprints, and early indexability signals for semi-structured data.</div>'
\qecho '  </div>'
\qecho '</div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">JSON / JSONB Column Inventory</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>Schema.Table</th><th>Column</th><th>Data Type</th><th>GIN Coverage</th><th>Review Note</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td>' || replace(replace(replace(replace(replace(schema_name || '.' || table_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(column_name,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(data_type,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '<td class="' || CASE WHEN has_gin THEN 'good">Present' ELSE 'warn">Missing' END || '</td>' ||
      '<td>' ||
        CASE
          WHEN data_type IN ('json', 'jsonb') AND NOT has_gin THEN 'Review containment/path predicates before adding GIN.'
          WHEN data_type LIKE '%[]' AND NOT has_gin THEN 'Array predicates may benefit from GIN if membership tests are common.'
          ELSE 'Inventory only; validate with query patterns.'
        END ||
      '</td>' ||
      '</tr>',
      E'\n' ORDER BY schema_name, table_name, column_name
    ),
    '<tr><td colspan="5" class="table-empty">No JSON, JSONB, or array columns were found in this database.</td></tr>'
  )
FROM (
  SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    EXISTS (
      SELECT 1
      FROM pg_index i
      JOIN pg_class ic ON ic.oid = i.indexrelid
      JOIN pg_am am ON am.oid = ic.relam
      WHERE i.indrelid = c.oid
        AND i.indisvalid
        AND i.indisready
        AND am.amname = 'gin'
        AND a.attnum = ANY(i.indkey::smallint[])
    ) AS has_gin
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND n.nspname !~ '^pg_'
    AND n.nspname <> 'information_schema'
    AND (
      pg_catalog.format_type(a.atttypid, a.atttypmod) IN ('json', 'jsonb')
      OR pg_catalog.format_type(a.atttypid, a.atttypmod) LIKE '%[]'
    )
) x;

\qecho '</tbody></table></div></div>'

\qecho '<div class="subsection">'
\qecho '<div class="subsection-title">JSON-Heavy SQL Fingerprints</div>'
\qecho '<div class="table-wrap">'
\qecho '<table class="pg360"><thead><tr>'
\qecho '<th>QueryID</th><th>Calls</th><th>Total Exec ms</th><th>Mean Exec ms</th><th>Temp Writes</th><th>Fingerprint</th>'
\qecho '</tr></thead><tbody>'

SELECT
  COALESCE(
    string_agg(
      '<tr>' ||
      '<td class="num">' || queryid::text || '</td>' ||
      '<td class="num">' || to_char(calls,'FM999,999,999') || '</td>' ||
      '<td class="num">' || to_char(round(total_exec_time::numeric, 1),'FM999,999,999,990.0') || '</td>' ||
      '<td class="num">' || to_char(round(mean_exec_time::numeric, 2),'FM999,999,990.00') || '</td>' ||
      '<td class="num">' || to_char(temp_blks_written,'FM999,999,999') || '</td>' ||
      '<td>' || replace(replace(replace(replace(replace(query_text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') || '</td>' ||
      '</tr>',
      E'\n' ORDER BY total_exec_time DESC, calls DESC
    ),
    '<tr><td colspan="6" class="table-empty">No JSON-heavy query fingerprints were detected in pg_stat_statements for this run.</td></tr>'
  )
FROM (
  SELECT
    queryid,
    calls,
    total_exec_time,
    mean_exec_time,
    temp_blks_written,
    left(regexp_replace(query, '[[:space:]]+', ' ', 'g'), 180) AS query_text
  FROM pg_stat_statements
  WHERE lower(query) LIKE '%jsonb%'
     OR lower(query) LIKE '%::jsonb%'
     OR query LIKE '%->%'
     OR query LIKE '%->>%'
     OR query LIKE '%@>%'
     OR query LIKE '%#>>%'
  ORDER BY total_exec_time DESC
  LIMIT 20
) q;

\qecho '</tbody></table></div></div>'
\qecho '</div>'

\if false
\qecho '</div>'
\qecho '</div>'
\endif
\qecho '</div>'
\qecho '</div>'

-- =============================================================================
-- JAVASCRIPT
-- =============================================================================
\qecho '<script>'
\qecho '(function () {'
\qecho '  ''use strict'';'
\qecho ''
\qecho '  var DENSITY_STORAGE_KEY = ''pg360_density_mode'';'
\qecho '  var DENSITY_COMPACT = ''compact'';'
\qecho '  var DENSITY_COMFORTABLE = ''comfortable'';'
\qecho ''
\qecho '  function getStoredDensity() {'
\qecho '    try {'
\qecho '      return window.localStorage.getItem(DENSITY_STORAGE_KEY);'
\qecho '    } catch (err) {'
\qecho '      return null;'
\qecho '    }'
\qecho '  }'
\qecho ''
\qecho '  function setStoredDensity(mode) {'
\qecho '    try {'
\qecho '      window.localStorage.setItem(DENSITY_STORAGE_KEY, mode);'
\qecho '    } catch (err) {'
\qecho '      /* no-op when storage is unavailable */'
\qecho '    }'
\qecho '  }'
\qecho ''
\qecho '  function ensureNavActionsContainer() {'
\qecho '    var sections = document.getElementById(''section_nav'') || document.querySelector(''.sections'');'
\qecho '    if (!sections) {'
\qecho '      return null;'
\qecho '    }'
\qecho ''
\qecho '    var actions = sections.querySelector(''.nav-actions'');'
\qecho '    if (!actions) {'
\qecho '      actions = document.createElement(''div'');'
\qecho '      actions.className = ''nav-actions'';'
\qecho '      sections.appendChild(actions);'
\qecho '    }'
\qecho '    return actions;'
\qecho '  }'
\qecho ''
\qecho '  function applyDensity(mode) {'
\qecho '    var body = document.body;'
\qecho '    if (!body) {'
\qecho '      return;'
\qecho '    }'
\qecho '    var selected = mode === DENSITY_COMFORTABLE ? DENSITY_COMFORTABLE : DENSITY_COMPACT;'
\qecho '    body.classList.remove(''density-compact'', ''density-comfortable'');'
\qecho '    body.classList.add(selected === DENSITY_COMFORTABLE ? ''density-comfortable'' : ''density-compact'');'
\qecho '    setStoredDensity(selected);'
\qecho ''
\qecho '    var btn = document.getElementById(''density_toggle'');'
\qecho '    if (btn) {'
\qecho '      var compactActive = selected === DENSITY_COMPACT;'
\qecho '      btn.setAttribute(''aria-pressed'', compactActive ? ''true'' : ''false'');'
\qecho '      btn.textContent = compactActive ? ''Density: Compact'' : ''Density: Comfortable'';'
\qecho '    }'
\qecho '  }'
\qecho ''
\qecho '  function initDensityToggle() {'
\qecho '    var actions = ensureNavActionsContainer();'
\qecho '    if (!actions) {'
\qecho '      applyDensity(DENSITY_COMPACT);'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    if (!actions.querySelector(''#density_toggle'')) {'
\qecho '      var toggle = document.createElement(''button'');'
\qecho '      toggle.type = ''button'';'
\qecho '      toggle.id = ''density_toggle'';'
\qecho '      toggle.className = ''density-toggle'';'
\qecho '      toggle.textContent = ''Density: Compact'';'
\qecho '      toggle.addEventListener(''click'', function () {'
\qecho '        var current = document.body.classList.contains(''density-comfortable'') ? DENSITY_COMFORTABLE : DENSITY_COMPACT;'
\qecho '        applyDensity(current === DENSITY_COMPACT ? DENSITY_COMFORTABLE : DENSITY_COMPACT);'
\qecho '      });'
\qecho '      actions.appendChild(toggle);'
\qecho '    }'
\qecho ''
\qecho '    applyDensity(getStoredDensity() || DENSITY_COMPACT);'
\qecho '  }'
\qecho ''
\qecho '  function initSectionObserver() {'
\qecho '    var sections = document.querySelectorAll(''.section[id]'');'
\qecho '    var navItems = Array.prototype.filter.call(document.querySelectorAll(''.nav-item''), function (item) {'
\qecho '      var href = item.getAttribute(''href'') || '''';'
\qecho '      return href.charAt(0) === ''#'';'
\qecho '    });'
\qecho '    var chipItems = Array.prototype.filter.call(document.querySelectorAll(''.sections a''), function (item) {'
\qecho '      var href = item.getAttribute(''href'') || '''';'
\qecho '      return href.charAt(0) === ''#'';'
\qecho '    });'
\qecho ''
\qecho '    if (!sections.length || typeof IntersectionObserver === ''undefined'' || (!navItems.length && !chipItems.length)) {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    var observer = new IntersectionObserver(function (entries) {'
\qecho '      entries.forEach(function (entry) {'
\qecho '        if (!entry.isIntersecting) {'
\qecho '          return;'
\qecho '        }'
\qecho '        var targetHash = ''#'' + entry.target.id;'
\qecho ''
\qecho '        navItems.forEach(function (item) {'
\qecho '          item.classList.toggle(''active'', item.getAttribute(''href'') === targetHash);'
\qecho '        });'
\qecho ''
\qecho '        chipItems.forEach(function (item) {'
\qecho '          item.classList.toggle(''active'', item.getAttribute(''href'') === targetHash);'
\qecho '        });'
\qecho '      });'
\qecho '    }, { threshold: 0.28 });'
\qecho ''
\qecho '    sections.forEach(function (section) {'
\qecho '      observer.observe(section);'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function initBackLinks() {'
\qecho '    var sections = document.querySelectorAll(''.section[id]'');'
\qecho '    sections.forEach(function (section) {'
\qecho '      var header = section.querySelector(''.section-header'');'
\qecho '      if (!header || header.querySelector(''.section-back'')) {'
\qecho '        return;'
\qecho '      }'
\qecho '      var back = document.createElement(''a'');'
\qecho '      back.href = ''#report_index'';'
\qecho '      back.className = ''section-back'';'
\qecho '      back.textContent = ''Back to Topics'';'
\qecho '      header.appendChild(back);'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function getFocusContextFromHash() {'
\qecho '    var hash = window.location.hash || '''';'
\qecho '    if (!hash || hash === ''#report_index'') {'
\qecho '      return null;'
\qecho '    }'
\qecho ''
\qecho '    var match = hash.match(/^#([sm]\\d{2})$/i);'
\qecho '    if (match) {'
\qecho '      return { sectionId: match[1].toLowerCase(), targetId: match[1].toLowerCase() };'
\qecho '    }'
\qecho ''
\qecho '    var targetId = hash.replace(/^#/, '''');'
\qecho '    var subsectionMatch = targetId.match(/^([sm]\\d{2})_check_[a-z0-9_]+$/i);'
\qecho '    if (subsectionMatch) {'
\qecho '      return { sectionId: subsectionMatch[1].toLowerCase(), targetId: targetId };'
\qecho '    }'
\qecho ''
\qecho '    var target = document.getElementById(targetId);'
\qecho '    if (!target) {'
\qecho '      return null;'
\qecho '    }'
\qecho ''
\qecho '    var section = target.closest ? target.closest(''.section[id]'') : null;'
\qecho '    if (!section || !section.id) {'
\qecho '      return null;'
\qecho '    }'
\qecho ''
\qecho '    return { sectionId: section.id.toLowerCase(), targetId: targetId };'
\qecho '  }'
\qecho ''
\qecho '  function applySectionFocusMode() {'
\qecho '    var sections = document.querySelectorAll(''.section[id]'');'
\qecho '    if (!sections.length) {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    var focus = getFocusContextFromHash();'
\qecho '    var hero = document.querySelector(''.hero'');'
\qecho '    var topNav = document.querySelector(''.top-nav'');'
\qecho '    var reportIndexes = document.querySelectorAll(''.report-index'');'
\qecho '    var evidenceCatalogShell = document.getElementById(''evidence_catalog_shell'');'
\qecho '    var securityNotice = document.querySelector(''.security-notice'');'
\qecho '    var hasLandingIndex = reportIndexes.length || !!evidenceCatalogShell;'
\qecho ''
\qecho '    if (!focus) {'
\qecho '      document.body.classList.remove(''focus-mode'');'
\qecho '      if (hero) {'
\qecho '        hero.style.display = '''';'
\qecho '      }'
\qecho '      if (topNav) {'
\qecho '        topNav.style.display = '''';'
\qecho '      }'
\qecho '      if (hasLandingIndex) {'
\qecho '        // Single-file index mode: show the topic index until a section is selected.'
\qecho '        sections.forEach(function (section) {'
\qecho '          section.style.display = ''none'';'
\qecho '        });'
\qecho '        reportIndexes.forEach(function (node) {'
\qecho '          node.style.display = '''';'
\qecho '        });'
\qecho '        if (evidenceCatalogShell) {'
\qecho '          evidenceCatalogShell.style.display = '''';'
\qecho '        }'
\qecho '        if (securityNotice) {'
\qecho '          securityNotice.style.display = ''none'';'
\qecho '        }'
\qecho '      } else {'
\qecho '        // Standalone component pages (legacy sXX.html): keep content visible.'
\qecho '        sections.forEach(function (section) {'
\qecho '          section.style.display = '''';'
\qecho '        });'
\qecho '      }'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    document.body.classList.add(''focus-mode'');'
\qecho '    sections.forEach(function (section) {'
\qecho '      section.style.display = section.id.toLowerCase() === focus.sectionId ? '''' : ''none'';'
\qecho '    });'
\qecho '    if (hero) {'
\qecho '      hero.style.display = ''none'';'
\qecho '    }'
\qecho '    if (topNav) {'
\qecho '      topNav.style.display = ''none'';'
\qecho '    }'
\qecho '    if (reportIndexes.length) {'
\qecho '      reportIndexes.forEach(function (node) {'
\qecho '        node.style.display = ''none'';'
\qecho '      });'
\qecho '    }'
\qecho '    if (evidenceCatalogShell) {'
\qecho '      evidenceCatalogShell.style.display = ''none'';'
\qecho '    }'
\qecho '    if (securityNotice) {'
\qecho '      securityNotice.style.display = ''none'';'
\qecho '    }'
\qecho ''
\qecho '    if (focus.targetId) {'
\qecho '      window.requestAnimationFrame(function () {'
\qecho '        var target = document.getElementById(focus.targetId);'
\qecho '        if (!target && /_check_/i.test(focus.targetId)) {'
\qecho '          buildEvidenceCatalog();'
\qecho '          target = document.getElementById(focus.targetId);'
\qecho '        }'
\qecho '        if (target && target.scrollIntoView) {'
\qecho '          target.scrollIntoView({ behavior: ''smooth'', block: ''start'' });'
\qecho '          return;'
\qecho '        }'
\qecho '        var sectionTarget = document.getElementById(focus.sectionId);'
\qecho '        if (sectionTarget && sectionTarget.scrollIntoView) {'
\qecho '          sectionTarget.scrollIntoView({ behavior: ''smooth'', block: ''start'' });'
\qecho '        }'
\qecho '      });'
\qecho '    }'
\qecho '  }'
\qecho ''
\qecho '  function initSectionFocusMode() {'
\qecho '    applySectionFocusMode();'
\qecho ''
\qecho '    window.addEventListener(''hashchange'', function () {'
\qecho '      applySectionFocusMode();'
\qecho '    });'
\qecho ''
\qecho '    document.addEventListener(''click'', function (evt) {'
\qecho '      var back = evt.target && evt.target.closest ? evt.target.closest(''.section-back'') : null;'
\qecho '      if (!back) {'
\qecho '        return;'
\qecho '      }'
\qecho '      evt.preventDefault();'
\qecho '      window.location.hash = ''#report_index'';'
\qecho '      applySectionFocusMode();'
\qecho '      var idx = document.getElementById(''report_index'');'
\qecho '      if (idx && idx.scrollIntoView) {'
\qecho '        idx.scrollIntoView({ behavior: ''smooth'', block: ''start'' });'
\qecho '      }'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function initCopyBlocks() {'
\qecho '    var blocks = document.querySelectorAll(''.finding-fix, .code-block'');'
\qecho '    blocks.forEach(function (el) {'
\qecho '      el.classList.add(''copyable'');'
\qecho '      el.setAttribute(''title'', ''Click to copy'');'
\qecho '      el.addEventListener(''click'', function () {'
\qecho '        var text = this.innerText || '''';'
\qecho '        if (!navigator.clipboard || !navigator.clipboard.writeText) {'
\qecho '          return;'
\qecho '        }'
\qecho '        navigator.clipboard.writeText(text).then(function () {'
\qecho '          el.classList.add(''copy-flash'');'
\qecho '          window.setTimeout(function () {'
\qecho '            el.classList.remove(''copy-flash'');'
\qecho '          }, 450);'
\qecho '        }).catch(function () {'
\qecho '          /* no-op */'
\qecho '        });'
\qecho '      });'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function parseDurationToSeconds(text) {'
\qecho '    var dayMatch = text.match(/^\\s*(\\d+)\\s*d\\s*(\\d{1,2}):(\\d{2}):(\\d{2})\\s*$/i);'
\qecho '    if (dayMatch) {'
\qecho '      return (parseInt(dayMatch[1], 10) * 86400) +'
\qecho '        (parseInt(dayMatch[2], 10) * 3600) +'
\qecho '        (parseInt(dayMatch[3], 10) * 60) +'
\qecho '        parseInt(dayMatch[4], 10);'
\qecho '    }'
\qecho ''
\qecho '    var hmsMatch = text.match(/^\\s*(\\d{1,3}):(\\d{2}):(\\d{2})\\s*$/);'
\qecho '    if (hmsMatch) {'
\qecho '      return (parseInt(hmsMatch[1], 10) * 3600) +'
\qecho '        (parseInt(hmsMatch[2], 10) * 60) +'
\qecho '        parseInt(hmsMatch[3], 10);'
\qecho '    }'
\qecho '    return null;'
\qecho '  }'
\qecho ''
\qecho '  function parseSizeToBytes(raw) {'
\qecho '    var match = raw.match(/^\\s*(-?\\d+(?:\\.\\d+)?)\\s*(bytes|b|kb|mb|gb|tb)\\s*$/i);'
\qecho '    if (!match) {'
\qecho '      return null;'
\qecho '    }'
\qecho '    var value = parseFloat(match[1]);'
\qecho '    var unit = match[2].toLowerCase();'
\qecho '    var factor = 1;'
\qecho '    if (unit === ''kb'') {'
\qecho '      factor = 1024;'
\qecho '    } else if (unit === ''mb'') {'
\qecho '      factor = 1024 * 1024;'
\qecho '    } else if (unit === ''gb'') {'
\qecho '      factor = 1024 * 1024 * 1024;'
\qecho '    } else if (unit === ''tb'') {'
\qecho '      factor = 1024 * 1024 * 1024 * 1024;'
\qecho '    }'
\qecho '    return value * factor;'
\qecho '  }'
\qecho ''
\qecho '  function parseSortableNumber(text) {'
\qecho '    if (!text) {'
\qecho '      return null;'
\qecho '    }'
\qecho '    var raw = text.replace(/\\u00a0/g, '' '').trim();'
\qecho '    if (!raw || raw === ''-'' || raw === ''N/A'') {'
\qecho '      return null;'
\qecho '    }'
\qecho ''
\qecho '    var duration = parseDurationToSeconds(raw);'
\qecho '    if (duration !== null) {'
\qecho '      return duration;'
\qecho '    }'
\qecho ''
\qecho '    var sized = parseSizeToBytes(raw);'
\qecho '    if (sized !== null) {'
\qecho '      return sized;'
\qecho '    }'
\qecho ''
\qecho '    var normalized = raw'
\qecho '      .replace(/,/g, '''')'
\qecho '      .replace(/x$/i, '''')'
\qecho '      .replace(/%$/, '''')'
\qecho '      .trim();'
\qecho ''
\qecho '    if (/^-?\\d+(\\.\\d+)?$/.test(normalized)) {'
\qecho '      return parseFloat(normalized);'
\qecho '    }'
\qecho '    return null;'
\qecho '  }'
\qecho ''
\qecho '  function detectNumericColumns(table) {'
\qecho '    var headers = table.querySelectorAll(''thead th'');'
\qecho '    if (!headers.length) {'
\qecho '      return;'
\qecho '    }'
\qecho '    var rows = Array.prototype.slice.call(table.querySelectorAll(''tbody tr''))'
\qecho '      .filter(function (row) {'
\qecho '        return !row.querySelector(''.table-empty'');'
\qecho '      });'
\qecho '    if (!rows.length) {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    headers.forEach(function (th, colIndex) {'
\qecho '      var numericHits = 0;'
\qecho '      var seen = 0;'
\qecho '      rows.forEach(function (row) {'
\qecho '        var cell = row.cells[colIndex];'
\qecho '        if (!cell) {'
\qecho '          return;'
\qecho '        }'
\qecho '        var txt = (cell.textContent || '''').trim();'
\qecho '        if (!txt) {'
\qecho '          return;'
\qecho '        }'
\qecho '        seen += 1;'
\qecho '        if (parseSortableNumber(txt) !== null) {'
\qecho '          numericHits += 1;'
\qecho '        }'
\qecho '      });'
\qecho ''
\qecho '      var isNumeric = seen > 0 && (numericHits / seen) >= 0.6;'
\qecho '      if (isNumeric) {'
\qecho '        th.classList.add(''numeric'');'
\qecho '        rows.forEach(function (row) {'
\qecho '          if (row.cells[colIndex]) {'
\qecho '            row.cells[colIndex].classList.add(''numeric'');'
\qecho '          }'
\qecho '        });'
\qecho '      }'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function sortTable(table, colIndex, forceDirection) {'
\qecho '    var tbody = table.tBodies && table.tBodies[0];'
\qecho '    if (!tbody) {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    var headers = table.querySelectorAll(''thead th'');'
\qecho '    var currentCol = parseInt(table.getAttribute(''data-sort-col'') || ''-1'', 10);'
\qecho '    var currentDir = table.getAttribute(''data-sort-dir'') || ''asc'';'
\qecho '    var nextDir = forceDirection || (currentCol === colIndex && currentDir === ''asc'' ? ''desc'' : ''asc'');'
\qecho '    table.setAttribute(''data-sort-col'', String(colIndex));'
\qecho '    table.setAttribute(''data-sort-dir'', nextDir);'
\qecho ''
\qecho '    headers.forEach(function (th, idx) {'
\qecho '      th.classList.remove(''sort-asc'', ''sort-desc'');'
\qecho '      th.setAttribute(''aria-sort'', idx === colIndex ? (nextDir === ''asc'' ? ''ascending'' : ''descending'') : ''none'');'
\qecho '      if (idx === colIndex) {'
\qecho '        th.classList.add(nextDir === ''asc'' ? ''sort-asc'' : ''sort-desc'');'
\qecho '      }'
\qecho '    });'
\qecho ''
\qecho '    var rows = Array.prototype.slice.call(tbody.querySelectorAll(''tr''));'
\qecho '    var dataRows = rows.filter(function (row) {'
\qecho '      return !row.querySelector(''.table-empty'');'
\qecho '    });'
\qecho '    var emptyRows = rows.filter(function (row) {'
\qecho '      return !!row.querySelector(''.table-empty'');'
\qecho '    });'
\qecho ''
\qecho '    var numericMode = true;'
\qecho '    var numericSeen = 0;'
\qecho '    dataRows.forEach(function (row) {'
\qecho '      var cell = row.cells[colIndex];'
\qecho '      var parsed = parseSortableNumber(cell ? cell.textContent : '''');'
\qecho '      if (parsed !== null) {'
\qecho '        numericSeen += 1;'
\qecho '      } else {'
\qecho '        numericMode = false;'
\qecho '      }'
\qecho '    });'
\qecho '    if (numericSeen === 0) {'
\qecho '      numericMode = false;'
\qecho '    }'
\qecho ''
\qecho '    dataRows.sort(function (a, b) {'
\qecho '      var aCell = a.cells[colIndex];'
\qecho '      var bCell = b.cells[colIndex];'
\qecho '      var aText = (aCell ? aCell.textContent : '''').trim();'
\qecho '      var bText = (bCell ? bCell.textContent : '''').trim();'
\qecho ''
\qecho '      var cmp = 0;'
\qecho '      if (numericMode) {'
\qecho '        var aNum = parseSortableNumber(aText);'
\qecho '        var bNum = parseSortableNumber(bText);'
\qecho '        if (aNum === null && bNum === null) {'
\qecho '          cmp = 0;'
\qecho '        } else if (aNum === null) {'
\qecho '          cmp = -1;'
\qecho '        } else if (bNum === null) {'
\qecho '          cmp = 1;'
\qecho '        } else {'
\qecho '          cmp = aNum - bNum;'
\qecho '        }'
\qecho '      } else {'
\qecho '        cmp = aText.localeCompare(bText, undefined, { sensitivity: ''base'', numeric: true });'
\qecho '      }'
\qecho '      return nextDir === ''asc'' ? cmp : -cmp;'
\qecho '    });'
\qecho ''
\qecho '    dataRows.concat(emptyRows).forEach(function (row) {'
\qecho '      tbody.appendChild(row);'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function initSortableTables() {'
\qecho '    var tables = document.querySelectorAll(''table.pg360'');'
\qecho '    tables.forEach(function (table) {'
\qecho '      detectNumericColumns(table);'
\qecho '      var headers = table.querySelectorAll(''thead th'');'
\qecho '      headers.forEach(function (th, idx) {'
\qecho '        th.classList.add(''sortable'');'
\qecho '        th.setAttribute(''tabindex'', ''0'');'
\qecho '        th.setAttribute(''role'', ''button'');'
\qecho '        th.setAttribute(''aria-sort'', ''none'');'
\qecho '        th.addEventListener(''click'', function () {'
\qecho '          sortTable(table, idx);'
\qecho '        });'
\qecho '        th.addEventListener(''keydown'', function (evt) {'
\qecho '          if (evt.key === ''Enter'' || evt.key === '' '') {'
\qecho '            evt.preventDefault();'
\qecho '            sortTable(table, idx);'
\qecho '          }'
\qecho '        });'
\qecho '      });'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function filterTableRows(input) {'
\qecho '    var targetId = input.getAttribute(''data-table-target'');'
\qecho '    if (!targetId) {'
\qecho '      var siblingTable = input.parentElement ? input.parentElement.querySelector(''table.pg360'') : null;'
\qecho '      if (siblingTable && siblingTable.id) {'
\qecho '        targetId = siblingTable.id;'
\qecho '      }'
\qecho '    }'
\qecho '    if (!targetId) {'
\qecho '      return;'
\qecho '    }'
\qecho '    var table = document.getElementById(targetId);'
\qecho '    if (!table) {'
\qecho '      return;'
\qecho '    }'
\qecho '    var filter = (input.value || '''').toLowerCase();'
\qecho '    var rows = table.querySelectorAll(''tbody tr'');'
\qecho '    rows.forEach(function (row) {'
\qecho '      row.style.display = row.textContent.toLowerCase().indexOf(filter) !== -1 ? '''' : ''none'';'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function initTableFilters() {'
\qecho '    var inputs = document.querySelectorAll(''.table-search'');'
\qecho '    inputs.forEach(function (input) {'
\qecho '      input.addEventListener(''input'', function () {'
\qecho '        filterTableRows(input);'
\qecho '      });'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function initCollapsibleSubsections() {'
\qecho '    var subsections = document.querySelectorAll(''.subsection'');'
\qecho '    subsections.forEach(function (subsection) {'
\qecho '      var directDetails = Array.prototype.find.call(subsection.children, function (child) {'
\qecho '        return child.tagName === ''DETAILS'' && child.classList.contains(''subsection-details'');'
\qecho '      });'
\qecho '      if (directDetails) {'
\qecho '        return;'
\qecho '      }'
\qecho '      var title = Array.prototype.find.call(subsection.children, function (child) {'
\qecho '        return child.classList && child.classList.contains(''subsection-title'');'
\qecho '      });'
\qecho '      if (!title) {'
\qecho '        return;'
\qecho '      }'
\qecho ''
\qecho '      var details = document.createElement(''details'');'
\qecho '      details.className = ''subsection-details'';'
\qecho '      details.open = true;'
\qecho ''
\qecho '      var summary = document.createElement(''summary'');'
\qecho '      summary.className = ''subsection-summary'';'
\qecho ''
\qecho '      var text = document.createElement(''span'');'
\qecho '      text.className = ''subsection-summary-text'';'
\qecho '      text.textContent = title.textContent.trim();'
\qecho '      summary.appendChild(text);'
\qecho ''
\qecho '      var marker = document.createElement(''span'');'
\qecho '      marker.className = ''subsection-summary-marker'';'
\qecho '      marker.setAttribute(''aria-hidden'', ''true'');'
\qecho '      marker.textContent = '''';'
\qecho '      summary.appendChild(marker);'
\qecho ''
\qecho '      details.appendChild(summary);'
\qecho '      var content = document.createElement(''div'');'
\qecho '      content.className = ''subsection-content'';'
\qecho ''
\qecho '      subsection.removeChild(title);'
\qecho '      while (subsection.firstChild) {'
\qecho '        content.appendChild(subsection.firstChild);'
\qecho '      }'
\qecho '      details.appendChild(content);'
\qecho '      subsection.appendChild(details);'
\qecho '      subsection.classList.add(''is-collapsible'');'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function getDirectSubsections(section) {'
\qecho '    return Array.prototype.filter.call(section.children || [], function (child) {'
\qecho '      return child.classList && child.classList.contains(''subsection'');'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  function getDirectSubsectionTitle(subsection) {'
\qecho '    return Array.prototype.find.call(subsection.children || [], function (child) {'
\qecho '      return child.classList && child.classList.contains(''subsection-title'');'
\qecho '    }) || subsection.querySelector(''.subsection-title'');'
\qecho '  }'
\qecho ''
\qecho '  function detectArtifactType(subsection) {'
\qecho '    if (subsection.querySelector(''tbody tr'')) {'
\qecho '      return ''table'';'
\qecho '    }'
\qecho '    if (subsection.querySelector(''.card'')) {'
\qecho '      return ''cards'';'
\qecho '    }'
\qecho '    if (subsection.querySelector(''.finding'')) {'
\qecho '      return ''findings'';'
\qecho '    }'
\qecho '    if (subsection.querySelector(''.code-block'')) {'
\qecho '      return ''code'';'
\qecho '    }'
\qecho '    return ''detail'';'
\qecho '  }'
\qecho ''
\qecho '  function getEvidenceCount(subsection) {'
\qecho '    var rows = Array.prototype.filter.call(subsection.querySelectorAll(''tbody tr''), function (row) {'
\qecho '      return !row.querySelector(''.table-empty'');'
\qecho '    });'
\qecho '    if (rows.length) {'
\qecho '      return rows.length;'
\qecho '    }'
\qecho '    var cards = subsection.querySelectorAll(''.card'');'
\qecho '    if (cards.length) {'
\qecho '      return cards.length;'
\qecho '    }'
\qecho '    var findings = subsection.querySelectorAll(''.finding'');'
\qecho '    if (findings.length) {'
\qecho '      return findings.length;'
\qecho '    }'
\qecho '    var codeBlocks = subsection.querySelectorAll(''.code-block'');'
\qecho '    if (codeBlocks.length) {'
\qecho '      return codeBlocks.length;'
\qecho '    }'
\qecho '    if (subsection.querySelector(''table.pg360'')) {'
\qecho '      return 0;'
\qecho '    }'
\qecho '    return 1;'
\qecho '  }'
\qecho ''
\qecho '  function ensureCheckChip(titleEl, checkCode) {'
\qecho '    if (!titleEl || titleEl.querySelector(''.check-chip'')) {'
\qecho '      return;'
\qecho '    }'
\qecho '    var chip = document.createElement(''span'');'
\qecho '    chip.className = ''check-chip'';'
\qecho '    chip.textContent = checkCode;'
\qecho '    titleEl.insertBefore(chip, titleEl.firstChild);'
\qecho '  }'
\qecho ''
\qecho '  function ensureSubsectionAnchor(subsection, fallbackId) {'
\qecho '    if (!subsection.id) {'
\qecho '      subsection.id = fallbackId;'
\qecho '    }'
\qecho '    return subsection.id;'
\qecho '  }'
\qecho ''
\qecho '  function createCatalogStat(label, value) {'
\qecho '    var item = document.createElement(''div'');'
\qecho '    item.className = ''catalog-stat'';'
\qecho '    item.innerHTML = ''<div class="catalog-stat-label">'' + label + ''</div><div class="catalog-stat-value">'' + value + ''</div>'';'
\qecho '    return item;'
\qecho '  }'
\qecho ''
\qecho '  var CATALOG_LAYOUT = ['
\qecho '    [{label: ''1a. Environment & Instance'', sectionId: ''s00''},'
\qecho '     {label: ''1b. Database Overview'', sectionId: ''s01''},'
\qecho '     {label: ''1c. Config Audit'', sectionId: ''s23''},'
\qecho '     {label: ''1d. Extension Inventory'', sectionId: ''s29''},'
\qecho '     {label: ''1e. Security Audit'', sectionId: ''s12''},'
\qecho '     {label: ''1f. Access Review'', sectionId: ''s25''}],'
\qecho '    [{label: ''2a. Top SQL Analysis'', sectionId: ''s02''},'
\qecho '     {label: ''2b. Workload & Tuning'', sectionId: ''s11''},'
\qecho '     {label: ''2c. Join Risk Detection'', sectionId: ''s30''},'
\qecho '     {label: ''2d. Parallel Efficiency'', sectionId: ''s31''},'
\qecho '     {label: ''2e. JIT Analysis'', sectionId: ''s32''},'
\qecho '     {label: ''2f. JSONB Detection'', sectionId: ''s33''}],'
\qecho '    [{label: ''3a. Table Health & Bloat'', sectionId: ''s05''},'
\qecho '     {label: ''3b. Index Health'', sectionId: ''s06''},'
\qecho '     {label: ''3c. Index Bloat'', sectionId: ''s24''},'
\qecho '     {label: ''3d. HOT & Fillfactor'', sectionId: ''s19''},'
\qecho '     {label: ''3e. Planner Statistics'', sectionId: ''s20''},'
\qecho '     {label: ''3f. Partitioning'', sectionId: ''s13''},'
\qecho '     {label: ''3g. Data Quality Checks'', sectionId: ''s15''}],'
\qecho '    [{label: ''4a. Wait Events & Sessions'', sectionId: ''s03''},'
\qecho '     {label: ''4b. Lock Analysis'', sectionId: ''s04''},'
\qecho '     {label: ''4c. Buffer Cache & I/O'', sectionId: ''s07''},'
\qecho '     {label: ''4d. WAL & Replication'', sectionId: ''s08''},'
\qecho '     {label: ''4e. Connections & Pooling'', sectionId: ''s09''},'
\qecho '     {label: ''4f. Pool Advisor'', sectionId: ''s22''}],'
\qecho '    [{label: ''5a. Vacuum & Maintenance'', sectionId: ''s10''},'
\qecho '     {label: ''5b. Autovacuum Advisor'', sectionId: ''s21''},'
\qecho '     {label: ''5c. Capacity & Growth'', sectionId: ''s16''},'
\qecho '     {label: ''5d. Capacity Detail'', sectionId: ''s26''},'
\qecho '     {label: ''5e. HA & DR Readiness'', sectionId: ''s17''},'
\qecho '     {label: ''5f. Action Plan Detail'', sectionId: ''s28''}]'
\qecho '  ];'
\qecho ''
\qecho '  var CATALOG_TITLE_OVERRIDES = {'
\qecho '    ''Role-Scoped Timeout Starting Points by Workload'': ''Timeout Starting Points'','
\qecho '    ''Timeout Guardrails and Session Safety'': ''Timeout Guardrails & Session Safety'','
\qecho '    ''Advanced Statistics and Progress Coverage'': ''Statistics & Progress Coverage'','
\qecho '    ''Version Currency and Security Posture'': ''Version Currency & Security Posture'','
\qecho '    ''Managed Service Application Paths'': ''Managed Service Apply Paths'','
\qecho '    ''Instance Fingerprint (Identity &amp; Build)'': ''Instance Fingerprint'','
\qecho '    ''Platform Detection (Managed Service / Container / Role)'': ''Platform Detection'','
\qecho '    ''Configuration Posture Snapshot (Summary only)'': ''Config Posture Snapshot'','
\qecho '    ''Diagnostic Completeness (Visibility Prerequisites)'': ''Diagnostic Completeness'','
\qecho '    ''Baseline Health Flags (Green / Yellow / Red)'': ''Baseline Health Flags'','
\qecho '    ''Primary Workload Database Detection'': ''Primary Workload DB Detection'','
\qecho '    ''Top SQL by Total Execution Time (with Resource Class)'': ''Top SQL by Total Execution Time'','
\qecho '    ''Top SQL by Calls (Load Share and p95 Approximation)'': ''Top SQL by Calls'','
\qecho '    ''Query Resource Attribution by Fingerprint'': ''Query Resource Attribution'','
\qecho '    ''Rows Efficiency and Wasted-Work Signals'': ''Rows Efficiency & Wasted-Work'','
\qecho '    ''Over-calling and Chatty Access Patterns'': ''Over-calling & Chatty Access'','
\qecho '    ''Workload Attribution by Application Name'': ''Workload Attribution by App Name'','
\qecho '    ''Regressions Since Previous Snapshot'': ''Regressions Since Previous Snapshot'','
\qecho '    ''Unified SQL Leaderboard (Triage View)'': ''Unified SQL Leaderboard'','
\qecho '    ''Session Sampling Window (T+0s, T+15s, T+30s)'': ''Session Sampling Window'','
\qecho '    ''Wait and Timeout Pressure Snapshot'': ''Wait & Timeout Pressure Snapshot'','
\qecho '    ''Root Cause Summary and Operational Actions'': ''Root Cause Summary'','
\qecho '    ''AccessExclusiveLock and DDL Lock Exposure'': ''DDL Lock Exposure'','
\qecho '    ''Ranked Mitigation Actions (Fix / Verify / Rollback)'': ''Mitigation Actions'','
\qecho '    ''Authentication and Audit Logging Posture'': ''Authentication & Audit Logging'','
\qecho '    ''Top Tables by Dead Tuple Ratio (bloat risk)'': ''Top Tables by Dead Tuples'','
\qecho '    ''XID Wraparound Risk (CRITICAL if age &gt; 1.5 billion)'': ''XID Wraparound Risk'','
\qecho '    ''Sequence Synchronization Check (post-migration risk: next insert = PK violation)'': ''Sequence Synchronization Check'','
\qecho '    ''Trigger Inventory (High trigger count = potential Oracle migration artifact)'': ''Trigger Inventory'','
\qecho '    ''Tables Without Primary Keys (replication risk, data quality issue)'': ''Tables Without Primary Keys'','
\qecho '    ''Freeze Age and Wraparound Countdown'': ''Freeze Age & Wraparound Countdown'','
\qecho '    ''Table Churn Rate Profile (Insert/Update/Delete)'': ''Table Churn Rate Profile'','
\qecho '    ''Prioritized Table-Health Actions (Fix / Verify / Rollback)'': ''Table-Health Actions'','
\qecho '    ''Unused Non-Unique Indexes Since Last Stats Reset (PK / UNIQUE / FK-supporting indexes excluded)'': ''Unused Non-Unique Indexes'','
\qecho '    ''Tables With High Sequential Scans (missing index candidates)'': ''Tables With High Sequential Scans'','
\qecho '    ''Invalid Indexes (failed CREATE INDEX CONCURRENTLY  must be rebuilt)'': ''Invalid Indexes'','
\qecho '    ''Index Write-Cost Posture on DML-Heavy Tables'': ''Index Write-Cost Posture'','
\qecho '    ''Index-Only Scan Potential (Heap Fetch Pressure)'': ''Index-Only Scan Potential'','
\qecho '    ''Foreign Key Supporting Index Gaps (Benefit and Risk)'': ''Foreign Key Index Gaps'','
\qecho '    ''SQL Performance Telemetry and Version-Aware Insights'': ''SQL Telemetry & Version Insights'','
\qecho '    ''Index Readiness Status (invalid or not ready)'': ''Index Readiness Status'','
\qecho '    ''Index Remediation Queue (Fix / Verify / Rollback)'': ''Index Remediation Queue'','
\qecho '    ''BGWriter &amp; Checkpoint Statistics'': ''BGWriter & Checkpoints'','
\qecho '    ''Table-Level Cache Hit Ratios (Bottom 20)'': ''Table Cache Hit Ratios'','
\qecho '    ''Background Write Pressure Classification'': ''BG Write Pressure Classification'','
\qecho '    ''Replication Slots (inactive slots = WAL accumulation = disk full risk)'': ''Replication Slots'','
\qecho '    ''Backup Verification and Incremental Backup Readiness'': ''Backup & Incremental Readiness'','
\qecho '    ''Logical Replication Failover Readiness'': ''Logical Repl Failover Readiness'','
\qecho '    ''Connection Churn and Session Age Distribution'': ''Connection Churn & Session Age'','
\qecho '    ''Long-Lived and Idle-In-Transaction Sessions'': ''Long-Lived & Idle-In-Tx Sessions'','
\qecho '    ''Connection Distribution by User, Application, and State'': ''Connection Distribution'','
\qecho '    ''Connection Saturation and Queue Risk'': ''Connection Saturation & Queue Risk'','
\qecho '    ''Parameter Mutability and Managed-Service Constraints'': ''Parameter Mutability & Constraints'','
\qecho '    ''Tables Most in Need of Vacuum (by dead tuple ratio)'': ''Tables Most in Need of Vacuum'','
\qecho '    ''Vacuum Debt Score (dead tuples vs autovacuum threshold)'': ''Vacuum Debt Score'','
\qecho '    ''Maintenance and Index-Build Progress Reporting'': ''Maintenance Progress Reporting'','
\qecho '    ''Parameter Action Matrix (Safe Now vs Load-Test First)'': ''Parameter Action Matrix'','
\qecho '    ''Tables with autovacuum_enabled = off'': ''Tables with autovacuum off'','
\qecho '    ''Autovacuum Workers Currently Running'': ''Autovacuum Workers Running'','
\qecho '    ''Superuser Accounts (minimize number of superusers)'': ''Superuser Accounts'','
\qecho '    ''Login Roles (password hashes intentionally not shown)'': ''Login Roles'','
\qecho '    ''SECURITY DEFINER Functions Without Fixed search_path (privilege escalation risk)'': ''SECURITY DEFINER search_path Risk'','
\qecho '    ''Public Schema Privileges (PG15+ changed default  verify)'': ''Public Schema Privileges'','
\qecho '    ''Role Membership and ADMIN OPTION Exposure'': ''Role Membership Exposure'','
\qecho '    ''Tables With Low NOT NULL Coverage (columns accepting NULL that may not should)'': ''Tables With Low NOT NULL Coverage'','
\qecho '    ''NOT VALID Constraints Backlog (Integrity Checks Pending Validation)'': ''NOT VALID Constraints Backlog'','
\qecho '    ''FK Orphan-Risk Indicators (Metadata-Only)'': ''FK Orphan-Risk Indicators'','
\qecho '    ''Candidate Key Coverage Gaps (ID-like Columns Without UNIQUE/PK)'': ''Candidate Key Coverage Gaps'','
\qecho '    ''Sensitive Table RLS Coverage Handoff (to S25)'': ''Sensitive Table RLS Coverage'','
\qecho '    ''Top Tables by Insert Rate (growth leaders)'': ''Top Tables by Insert Rate'','
\qecho '    ''Index Overhead Ratio (indexes &gt; 2x table size = over-indexed)'': ''Index Overhead Ratio'','
\qecho '    ''Projection Confidence (Stats Window and Activity Volume)'': ''Projection Confidence'','
\qecho '    ''Projection Confidence and Assumptions'': ''Projection Confidence & Assumptions'','
\qecho '    ''Growth Decomposition (Heap, Index, and Write Pressure)'': ''Growth Decomposition'','
\qecho '    ''Backup and PITR Configuration Health Matrix'': ''Backup & PITR Health Matrix'','
\qecho '    ''Failover Readiness and RPO Posture'': ''Failover Readiness & RPO Posture'','
\qecho '    ''Replication Evidence for RPO Discussion'': ''Repl Evidence for RPO Discussion'','
\qecho '    ''Operational DR Checklist (Runbook Completeness)'': ''Operational DR Checklist'','
\qecho '    ''Category Score Breakdown (Transparent Weighting)'': ''Category Score Breakdown'','
\qecho '    ''Diagnostic Confidence (Data Completeness)'': ''Diagnostic Confidence'','
\qecho '    ''Timeout, Wait, and Telemetry Priorities'': ''Timeout / Wait / Telemetry'','
\qecho '    ''HOT Update Ratio by Table (low ratio = fillfactor tuning opportunity)'': ''HOT Update Ratio by Table'','
\qecho '    ''Fillfactor Candidate Matrix (Apply Only on HOT-Poor, High-Update Tables)'': ''Fillfactor Candidate Matrix'','
\qecho '    ''Write Amplification Posture (HOT Misses, Dead Tuples, Vacuum Debt)'': ''Write Amplification Posture'','
\qecho '    ''Tables With Stale Statistics (modified since last analyze)'': ''Tables With Stale Statistics'','
\qecho '    ''Column Correlation Gaps (tables that may benefit from extended statistics)'': ''Column Correlation Gaps'','
\qecho '    ''Extended Statistics Remediation Queue (Fix, Verify, Rollback)'': ''Extended Statistics Queue'','
\qecho '    ''Analyze Policy Tuning Queue (High Modification Tables)'': ''Analyze Policy Tuning Queue'','
\qecho '    ''Current Autovacuum Global Configuration vs Recommended'': ''Autovacuum Global Configuration'','
\qecho '    ''Per-Table Vacuum Urgency Matrix with Custom Settings Script'': ''Per-Table Vacuum Urgency Matrix'','
\qecho '    ''XID Wraparound Countdown (Database Level)'': ''XID Wraparound Countdown'','
\qecho '    ''Per-Table XID Age (tables needing VACUUM FREEZE first)'': ''Per-Table XID Age'','
\qecho '    ''Autovacuum Tuning Guardrails (Apply Safely in Production)'': ''Autovacuum Tuning Guardrails'','
\qecho '    ''Pool Sizing Calculator and Recommended Pooling Mode'': ''Pool Sizing Calculator'','
\qecho '    ''Workload Attribution Completeness (application_name Coverage)'': ''Workload Attribution Completeness'','
\qecho '    ''Critical Parameter Recommendations vs Current Values'': ''Critical Parameter Recommendations'','
\qecho '    ''BTree Index Bloat Estimation (heuristic  use REINDEX CONCURRENTLY to reclaim)'': ''BTree Index Bloat Estimation'','
\qecho '    ''Reindex Priority Queue (Waste and Access Combined)'': ''Reindex Priority Queue'','
\qecho '    ''Extension Security Audit (dangerous extensions in user schemas)'': ''Extension Security Audit'','
\qecho '    ''Objects Accessible to PUBLIC Role (privilege exposure)'': ''Objects Accessible to PUBLIC'','
\qecho '    ''Role Membership and Inherited Access'': ''Role Membership & Inherited Access'','
\qecho '    ''Grant Exposure Audit (PUBLIC and grant-option review)'': ''Grant Exposure Audit'','
\qecho '    ''Table Size &amp; Growth Velocity Analysis'': ''Table Size & Growth Velocity'','
\qecho '    ''Top Growth Drivers (30-Day Directional Projection)'': ''Top Growth Drivers'','
\qecho '    ''Runbook Matrix (Risk, Downtime, Verification, Rollback)'': ''Runbook Matrix'','
\qecho '    ''Extension &amp; Monitoring Readiness Snapshot'': ''Extension & Monitoring Snapshot'','
\qecho '    ''Optimal pg_stat_statements Configuration'': ''Optimal pg_stat_statements Config'','
\qecho '    ''Observability Tiers (Minimal, Recommended, Advanced)'': ''Observability Tiers'','
\qecho '    ''auto_explain Safe Baseline (Production-Friendly)'': ''auto_explain Safe Baseline'','
\qecho '    ''JSON / JSONB / Array Column Inventory'': ''JSON / JSONB Column Inventory'','
\qecho '    ''Extensions: Installed / Missing / Risk'': ''Extensions: Installed/Missing/Risk'','
\qecho '    ''Foreign Keys Without Supporting Indexes (Oracle auto-indexes these  PG does NOT)'': ''FKs Without Supporting Indexes'','
\qecho '    ''Join-Column Index Gaps Under Scan Pressure'': ''Join-Column Index Gaps'''
\qecho '  };'
\qecho ''
\qecho '  function compactCatalogTitle(rawText) {'
\qecho '    var text = (rawText || '''').replace(/^\\s*[A-Z]\\d{2}\\.\\d{2}\\s*/, '''').trim();'
\qecho '    var normalized = text.replace(/\\s+/g, '' '' ).trim();'
\qecho '    if (CATALOG_TITLE_OVERRIDES[text]) {'
\qecho '      return CATALOG_TITLE_OVERRIDES[text];'
\qecho '    }'
\qecho '    if (CATALOG_TITLE_OVERRIDES[normalized]) {'
\qecho '      return CATALOG_TITLE_OVERRIDES[normalized];'
\qecho '    }'
\qecho '    for (var key in CATALOG_TITLE_OVERRIDES) {'
\qecho '      if (Object.prototype.hasOwnProperty.call(CATALOG_TITLE_OVERRIDES, key) && key.replace(/\\s+/g, '' '' ).trim() === normalized) {'
\qecho '        return CATALOG_TITLE_OVERRIDES[key];'
\qecho '      }'
\qecho '    }'
\qecho '    text = normalized.replace(/\\s*\\([^)]*\\)\\s*/g, '''').replace(/\\s{2,}/g, '' '').trim();'
\qecho '    text = text.replace(/ and /g, '' & '');'
\qecho '    text = text.replace(/Configuration/g, ''Config'');'
\qecho '    text = text.replace(/Statistics/g, ''Stats'');'
\qecho '    text = text.replace(/Historical/g, ''History'');'
\qecho '    text = text.replace(/Transactions/g, ''Tx'');'
\qecho '    text = text.replace(/Transaction/g, ''Tx'');'
\qecho '    text = text.replace(/Application/g, ''App'');'
\qecho '    text = text.replace(/Distribution/g, ''Dist'');'
\qecho '    text = text.replace(/Background/g, ''BG'');'
\qecho '    text = text.replace(/Replication/g, ''Repl'');'
\qecho '    text = text.replace(/Maintenance/g, ''Maint'');'
\qecho '    text = text.replace(/Incremental/g, ''Incr'');'
\qecho '    text = text.replace(/Operational/g, ''Ops'');'
\qecho '    return text;'
\qecho '  }'
\qecho ''
\qecho '  function buildEvidenceCatalog() {'
\qecho '    var shell = document.getElementById(''evidence_catalog_shell'');'
\qecho '    if (!shell) {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    var sectionMap = {};'
\qecho '    Array.prototype.forEach.call(document.querySelectorAll(''.section[id]''), function (section) {'
\qecho '      sectionMap[section.id] = section;'
\qecho '    });'
\qecho '    var nextOrdinal = 1;'
\qecho ''
\qecho '    CATALOG_LAYOUT.forEach(function (columnGroups, colIdx) {'
\qecho '      var cell = document.getElementById(''catalog_col_'' + String(colIdx + 1));'
\qecho '      if (!cell) {'
\qecho '        return;'
\qecho '      }'
\qecho '      cell.innerHTML = '''';'
\qecho ''
\qecho '      columnGroups.forEach(function (groupDef) {'
\qecho '        var section = sectionMap[groupDef.sectionId];'
\qecho '        if (!section) {'
\qecho '          return;'
\qecho '        }'
\qecho '        var subsections = getDirectSubsections(section);'
\qecho '        if (!subsections.length) {'
\qecho '          return;'
\qecho '        }'
\qecho ''
\qecho '        var h2 = document.createElement(''h2'');'
\qecho '        h2.textContent = groupDef.label;'
\qecho '        cell.appendChild(h2);'
\qecho ''
\qecho '        var list = document.createElement(''ol'');'
\qecho '        list.start = nextOrdinal;'
\qecho ''
\qecho '        subsections.forEach(function (subsection, idx) {'
\qecho '          var subsectionTitle = getDirectSubsectionTitle(subsection);'
\qecho '          if (!subsectionTitle) {'
\qecho '            return;'
\qecho '          }'
\qecho '          var sectionCode = (section.id || '''').toUpperCase();'
\qecho '          var checkCode = sectionCode + ''.'' + String(idx + 1).padStart(2, ''0'');'
\qecho '          var anchorId = ensureSubsectionAnchor(subsection, (section.id || ''section'') + ''_check_'' + String(idx + 1).padStart(2, ''0''));'
\qecho '          ensureCheckChip(subsectionTitle, checkCode);'
\qecho ''
\qecho '          var item = document.createElement(''li'');'
\qecho '          item.className = ''catalog-item'';'
\qecho '          item.title = sectionCode;'
\qecho ''
\qecho '          var titleText = document.createElement(''span'');'
\qecho '          titleText.className = ''catalog-item-title'';'
\qecho '          titleText.textContent = compactCatalogTitle(subsectionTitle.textContent) + '' '';'
\qecho '          item.appendChild(titleText);'
\qecho ''
\qecho '          var format = document.createElement(''a'');'
\qecho '          format.className = ''catalog-format'';'
\qecho '          format.href = ''#'' + anchorId;'
\qecho '          format.textContent = detectArtifactType(subsection);'
\qecho '          item.appendChild(format);'
\qecho ''
\qecho '          var small = document.createElement(''small'');'
\qecho '          small.className = ''catalog-count'';'
\qecho '          var em = document.createElement(''em'');'
\qecho '          em.textContent = ''('' + String(getEvidenceCount(subsection)) + '')'';'
\qecho '          small.appendChild(document.createTextNode('' ''));'
\qecho '          small.appendChild(em);'
\qecho '          item.appendChild(small);'
\qecho ''
\qecho '          list.appendChild(item);'
\qecho '          nextOrdinal += 1;'
\qecho '        });'
\qecho ''
\qecho '        cell.appendChild(list);'
\qecho '      });'
\qecho '    });'
\qecho '  }'
\qecho ''
\qecho '  var AREA_LABELS = {'
\qecho '    ''00'': ''Environment & Instance'','
\qecho '    ''01'': ''Database Overview'','
\qecho '    ''02'': ''Top SQL Analysis'','
\qecho '    ''03'': ''Wait Events & Sessions'','
\qecho '    ''04'': ''Lock Analysis'','
\qecho '    ''05'': ''Table Health & Bloat'','
\qecho '    ''06'': ''Index Health'','
\qecho '    ''07'': ''Buffer Cache & I/O'','
\qecho '    ''08'': ''WAL & Replication'','
\qecho '    ''09'': ''Connections & Pooling'','
\qecho '    ''10'': ''Vacuum & Maintenance'','
\qecho '    ''11'': ''Workload Profile & Tuning'','
\qecho '    ''12'': ''Security Audit'','
\qecho '    ''13'': ''Partitioning Health'','
\qecho '    ''15'': ''Data Quality Checks'','
\qecho '    ''16'': ''Capacity & Growth'','
\qecho '    ''17'': ''HA & DR Readiness'','
\qecho '    ''18'': ''Executive Health Score'','
\qecho '    ''19'': ''HOT Updates & Fillfactor'','
\qecho '    ''20'': ''Planner Statistics Quality'','
\qecho '    ''21'': ''Autovacuum Full Advisor'','
\qecho '    ''22'': ''Connection Pool Advisor'','
\qecho '    ''23'': ''Configuration Audit'','
\qecho '    ''24'': ''Index Bloat Estimation'','
\qecho '    ''25'': ''Security & Access Review'','
\qecho '    ''26'': ''Capacity Enhanced View'','
\qecho '    ''28'': ''Remediation Action Plan'','
\qecho '    ''29'': ''Extension Inventory'','
\qecho '    ''30'': ''Join Risk Detection'','
\qecho '    ''31'': ''Parallel Query Efficiency'','
\qecho '    ''32'': ''JIT Usage Analysis'','
\qecho '    ''33'': ''JSONB Workload Detection'''
\qecho '    ,''m01'': ''Executive Summary'''
\qecho '    ,''m02'': ''Platform and Diagnostic Context'''
\qecho '    ,''m03'': ''Instance and Database Profile'''
\qecho '    ,''m04'': ''Monitoring and Observability Readiness'''
\qecho '    ,''m05'': ''Workload Characterization'''
\qecho '    ,''m18'': ''Prioritized Remediation Plan'''
\qecho '  };'
\qecho ''
\qecho '  function normalizeAreaReferences() {'
\qecho '    if (!document.body || typeof document.createTreeWalker !== ''function'') {'
\qecho '      return;'
\qecho '    }'
\qecho ''
\qecho '    var walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);'
\qecho '    var node;'
\qecho '    while ((node = walker.nextNode())) {'
\qecho '      var text = node.nodeValue || '''';'
\qecho '      if (!/[SM]\\d{2}/.test(text)) {'
\qecho '        continue;'
\qecho '      }'
\qecho '      var parent = node.parentElement;'
\qecho '      if (!parent) {'
\qecho '        continue;'
\qecho '      }'
\qecho '      var tag = parent.tagName;'
\qecho '      if (tag === ''SCRIPT'' || tag === ''STYLE'') {'
\qecho '        continue;'
\qecho '      }'
\qecho '      if (parent.closest && parent.closest(''a, .idx-title, .idx-desc, .section-title, .section-id, .subsection-title, .nav-item, .report-index-title'')) {'
\qecho '        continue;'
\qecho '      }'
\qecho ''
\qecho '      node.nodeValue = text.replace(/\\b([SM])(\\d{2})(?:\\.[A-Za-z0-9_]+)?\\b/g, function (_, prefix, code) {'
\qecho '        var key = (prefix + code).toLowerCase();'
\qecho '        return AREA_LABELS[key] || AREA_LABELS[code] || (prefix + code);'
\qecho '      });'
\qecho '    }'
\qecho '  }'
\qecho ''
\qecho '  document.addEventListener(''DOMContentLoaded'', function () {'
\qecho '    normalizeAreaReferences();'
\qecho '    buildEvidenceCatalog();'
\qecho '    initDensityToggle();'
\qecho '    initSectionFocusMode();'
\qecho '    document.body.classList.remove(''pg360-index-loading'');'
\qecho '    initSectionObserver();'
\qecho '    initBackLinks();'
\qecho '    // Keep section content always expanded for deterministic diagnostics visibility.'
\qecho '    initSortableTables();'
\qecho '    initTableFilters();'
\qecho '    initCopyBlocks();'
\qecho '  });'
\qecho '})();'
\qecho '</script>'

\qecho '</body>'
\qecho '</html>'

-- =============================================================================
-- END OF PG360 REPORT
-- Safely close the read-only transaction
-- =============================================================================
COMMIT;
\o

\qecho PG360 output directory: :pg360_output_dir
\qecho PG360 report page: :pg360_full_report_path

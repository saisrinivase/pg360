-- =============================================================================
-- PG360 SQL Deep Dive
-- PURPOSE: Read-only deep dive for one PostgreSQL query shape using pg_stat_statements
-- USAGE  : psql -X -A -t -d mydb -v ON_ERROR_STOP=1 \
--            -v pg360_ack_nonprod_first=on \
--            -v pg360_queryid='<queryid>' \
--            -f pg360_sql_deep_dive.sql
-- NOTES  : If pg360_queryid / pg360_sql_fingerprint is not supplied, the top SQL by
--          total execution time is selected automatically.
-- =============================================================================

\set QUIET 1
\set ON_ERROR_STOP 1
\pset footer off
\pset tuples_only on
\pset format unaligned
\pset pager off
\if :{?pg360_ack_nonprod_first}
\else
\qecho 'ERROR: missing required flag -v pg360_ack_nonprod_first=on'
SELECT 1/0;
\endif
\if :{?pg360_output_dir}
\else
\set pg360_output_dir ./reports/latest
\endif
\if :{?pg360_sql_report_file}
\else
\set pg360_sql_report_file sql_deep_dive.html
\endif
\if :{?pg360_queryid}
\else
\set pg360_queryid ''
\endif
\if :{?pg360_sql_fingerprint}
\else
\set pg360_sql_fingerprint ''
\endif
\if :{?pg360_history_days}
\else
\set pg360_history_days 14
\endif

BEGIN;
SET TRANSACTION READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '2s';
SET LOCAL idle_in_transaction_session_timeout = '60s';
SET LOCAL search_path = pg_catalog, public;

SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'on' ELSE 'off' END AS pg360_has_pgss \gset
\if :pg360_has_pgss
\else
\qecho 'ERROR: pg_stat_statements is required for pg360_sql_deep_dive.sql'
SELECT 1/0;
\endif

SELECT
  CASE
    WHEN to_regclass('pg360_history.sql_snapshot') IS NOT NULL
     AND to_regclass('pg360_history.run_snapshot') IS NOT NULL
     AND has_table_privilege(current_user, 'pg360_history.sql_snapshot', 'SELECT')
     AND has_table_privilege(current_user, 'pg360_history.run_snapshot', 'SELECT')
    THEN 'on' ELSE 'off'
  END AS pg360_has_history_sql
\gset

WITH base AS (
  SELECT
    md5(query || '|' || userid::text || '|' || dbid::text) AS fingerprint,
    COALESCE(queryid::text, md5(query)) AS queryid_text,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    userid,
    calls,
    total_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    temp_blks_written,
    CASE WHEN to_regclass('pg_stat_statements') IS NOT NULL THEN NULL::numeric ELSE NULL::numeric END AS wal_bytes
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND query NOT ILIKE '%pg_stat_statements%'
    AND query NOT ILIKE 'BEGIN%'
    AND query NOT ILIKE 'COMMIT%'
    AND query NOT ILIKE 'SET %'
), ranked AS (
  SELECT *, row_number() OVER (ORDER BY total_exec_time DESC, calls DESC) AS rn
  FROM base
  WHERE (:'pg360_queryid' = '' OR queryid_text = :'pg360_queryid')
    AND (:'pg360_sql_fingerprint' = '' OR fingerprint = :'pg360_sql_fingerprint')
)
SELECT
  CASE WHEN COUNT(*) > 0 THEN 'on' ELSE 'off' END AS pg360_has_target
FROM ranked
\gset

\if :pg360_has_target
\else
\qecho 'ERROR: no matching query found in pg_stat_statements for the supplied selector.'
SELECT 1/0;
\endif

WITH base AS (
  SELECT
    md5(query || '|' || userid::text || '|' || dbid::text) AS fingerprint,
    COALESCE(queryid::text, md5(query)) AS queryid_text,
    regexp_replace(query, E'\\s+', ' ', 'g') AS query_text,
    userid,
    calls,
    total_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    temp_blks_written
  FROM pg_stat_statements
  WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND query NOT ILIKE '%pg360%'
    AND query NOT ILIKE '%pg_stat_statements%'
    AND query NOT ILIKE 'BEGIN%'
    AND query NOT ILIKE 'COMMIT%'
    AND query NOT ILIKE 'SET %'
), ranked AS (
  SELECT *, row_number() OVER (ORDER BY total_exec_time DESC, calls DESC) AS rn
  FROM base
  WHERE (:'pg360_queryid' = '' OR queryid_text = :'pg360_queryid')
    AND (:'pg360_sql_fingerprint' = '' OR fingerprint = :'pg360_sql_fingerprint')
)
SELECT
  fingerprint AS pg360_target_fingerprint,
  queryid_text AS pg360_target_queryid,
  replace(replace(replace(replace(replace(query_text,'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;') AS pg360_target_query_text,
  userid::text AS pg360_target_userid,
  calls::text AS pg360_target_calls,
  round(total_exec_time::numeric, 2)::text AS pg360_target_total_exec_ms,
  round(mean_exec_time::numeric, 2)::text AS pg360_target_mean_exec_ms,
  round(stddev_exec_time::numeric, 2)::text AS pg360_target_stddev_exec_ms,
  rows::text AS pg360_target_rows,
  shared_blks_hit::text AS pg360_target_shared_hit,
  shared_blks_read::text AS pg360_target_shared_read,
  temp_blks_written::text AS pg360_target_temp_written
FROM ranked
ORDER BY rn
LIMIT 1
\gset

SELECT current_database() AS pg360_current_db \gset

SELECT :'pg360_output_dir' || '/' || :'pg360_sql_report_file' AS pg360_sql_report_path \gset
\o :pg360_sql_report_path
\qecho '<!DOCTYPE html>'
\qecho '<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">'
\qecho '<meta http-equiv="Content-Security-Policy" content="default-src ''self''; style-src ''self'' ''unsafe-inline''; img-src ''self'' data:">'
\qecho '<title>PG360 SQL Deep Dive - ' :pg360_target_queryid '</title>'
\qecho '<style>'
\qecho 'body{margin:0;background:#f4f6f7;color:#102028;font-family:Arial,Helvetica,sans-serif;font-size:12px;line-height:1.4}'
\qecho '.wrap{max-width:1080px;margin:0 auto;padding:16px}'
\qecho '.hero,.panel,.code,.table-wrap,.card{background:#fff;border:1px solid #c2ccd1}'
\qecho '.hero{margin-bottom:12px}.hero h1{margin:0;padding:10px 14px;background:#22353e;color:#f7fafb;font-size:18px}.hero p{margin:0;padding:10px 14px;color:#50616b}'
\qecho '.section{margin-bottom:12px}.section h2{margin:0 0 8px;color:#1c3038;font-size:16px}'
\qecho '.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));border:1px solid #c2ccd1;margin-bottom:12px}.card{border:0;border-right:1px solid #d6dee2;border-bottom:1px solid #d6dee2;padding:10px 12px}.label{color:#4f616b;font-weight:700}.value{margin-top:4px;font-size:15px;font-weight:700;color:#102028}.sub{margin-top:4px;color:#61717a;font-size:11px}'
\qecho '.code{padding:12px;white-space:pre-wrap;word-break:break-word;margin-bottom:12px}.table-wrap{overflow:auto}.pg360{width:100%;border-collapse:collapse}.pg360 th{background:#dde5e8;color:#1c3038;text-align:left;padding:8px 10px;border-bottom:1px solid #c2ccd1}.pg360 td{padding:8px 10px;border-top:1px solid #d6dee2;vertical-align:top}.pg360 tr:nth-child(even){background:#f7f9fa}.good{color:#1f7a52}.warn{color:#9a5c06}.crit{color:#b42318}'
\qecho '</style></head><body><div class="wrap">'
\qecho '<div class="hero"><h1>PG360 SQL Deep Dive</h1><p>QueryID: ' :pg360_target_queryid ' | Fingerprint: ' :pg360_target_fingerprint ' | Database: ' :pg360_current_db '</p></div>'
\qecho '<div class="section"><h2>Current Profile</h2>'
SELECT
  '<div class="cards">' ||
  '<div class="card"><div class="label">Calls</div><div class="value">' || :'pg360_target_calls' || '</div></div>' ||
  '<div class="card"><div class="label">Total Exec Time</div><div class="value">' || :'pg360_target_total_exec_ms' || ' ms</div></div>' ||
  '<div class="card"><div class="label">Mean Exec Time</div><div class="value">' || :'pg360_target_mean_exec_ms' || ' ms</div></div>' ||
  '<div class="card"><div class="label">Stddev</div><div class="value">' || :'pg360_target_stddev_exec_ms' || ' ms</div></div>' ||
  '<div class="card"><div class="label">Rows</div><div class="value">' || :'pg360_target_rows' || '</div></div>' ||
  '<div class="card"><div class="label">Shared Hit / Read</div><div class="value">' || :'pg360_target_shared_hit' || ' / ' || :'pg360_target_shared_read' || '</div></div>' ||
  '<div class="card"><div class="label">Temp Blocks Written</div><div class="value">' || :'pg360_target_temp_written' || '</div></div>' ||
  '<div class="card"><div class="label">Resource Class</div><div class="value">' ||
    CASE
      WHEN :'pg360_target_temp_written'::bigint > 0 THEN 'Spill-prone'
      WHEN :'pg360_target_shared_read'::bigint > :'pg360_target_shared_hit'::bigint THEN 'I/O heavy'
      WHEN :'pg360_target_calls'::bigint > 5000 THEN 'Chatty / high-frequency'
      ELSE 'General'
    END || '</div></div>' ||
  '</div>';
\qecho '</div>'
\qecho '<div class="section"><h2>Normalized Query Text</h2><div class="code">' :pg360_target_query_text '</div></div>'
\qecho '<div class="section"><h2>Interpretation</h2><div class="table-wrap"><table class="pg360"><thead><tr><th>Signal</th><th>Observed</th><th>Meaning</th></tr></thead><tbody>'
SELECT
  '<tr><td>Latency variability</td><td class="' || CASE WHEN :'pg360_target_stddev_exec_ms'::numeric > :'pg360_target_mean_exec_ms'::numeric * 2 THEN 'warn' ELSE 'good' END || '">' || :'pg360_target_stddev_exec_ms' || ' ms</td><td>' ||
  CASE WHEN :'pg360_target_stddev_exec_ms'::numeric > :'pg360_target_mean_exec_ms'::numeric * 2 THEN 'Execution time is highly variable; check data skew, spills, or plan instability.' ELSE 'Latency variability is within a typical range.' END || '</td></tr>' ||
  '<tr><td>Temp spill signal</td><td class="' || CASE WHEN :'pg360_target_temp_written'::bigint > 0 THEN 'warn' ELSE 'good' END || '">' || :'pg360_target_temp_written' || '</td><td>' ||
  CASE WHEN :'pg360_target_temp_written'::bigint > 0 THEN 'Query writes temp blocks; inspect work_mem, sort/hash patterns, and row volume.' ELSE 'No temp spill was recorded for this query shape.' END || '</td></tr>' ||
  '<tr><td>Read pressure</td><td class="' || CASE WHEN :'pg360_target_shared_read'::bigint > :'pg360_target_shared_hit'::bigint THEN 'warn' ELSE 'good' END || '">' || :'pg360_target_shared_hit' || ' / ' || :'pg360_target_shared_read' || '</td><td>' ||
  CASE WHEN :'pg360_target_shared_read'::bigint > :'pg360_target_shared_hit'::bigint THEN 'Physical read pressure dominates; inspect access path and cache locality.' ELSE 'Buffer hits dominate read behavior.' END || '</td></tr>' ||
  '<tr><td>Call pattern</td><td class="' || CASE WHEN :'pg360_target_calls'::bigint > 5000 THEN 'warn' ELSE 'good' END || '">' || :'pg360_target_calls' || '</td><td>' ||
  CASE WHEN :'pg360_target_calls'::bigint > 5000 THEN 'High call count suggests chatty execution or pagination loops.' ELSE 'Call volume is not extreme for this sample window.' END || '</td></tr>';
\qecho '</tbody></table></div></div>'
\qecho '<div class="section"><h2>Repository Baseline</h2>'
\if :pg360_has_history_sql
SELECT COALESCE((
  WITH hist AS (
    SELECT
      count(*)::int AS sample_count,
      min(r.captured_at) AS first_capture,
      max(r.captured_at) AS last_capture,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(s.total_exec_time, 0)) AS med_total_exec,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY COALESCE(s.calls, 0)::numeric) AS med_calls,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(s.total_exec_time, 0)) AS p90_total_exec
    FROM pg360_history.sql_snapshot s
    JOIN pg360_history.run_snapshot r ON r.run_id = s.run_id
    WHERE s.dbname = current_database()
      AND s.fingerprint = :'pg360_target_fingerprint'
      AND r.captured_at >= now() - (:'pg360_history_days' || ' days')::interval
  )
  SELECT
    '<div class="cards">' ||
    '<div class="card"><div class="label">History Samples</div><div class="value">' || sample_count || '</div><div class="sub">Last ' || :'pg360_history_days' || ' days</div></div>' ||
    '<div class="card"><div class="label">First / Last</div><div class="value">' || COALESCE(to_char(first_capture, 'MM-DD HH24:MI'), 'N/A') || ' -> ' || COALESCE(to_char(last_capture, 'MM-DD HH24:MI'), 'N/A') || '</div></div>' ||
    '<div class="card"><div class="label">Median Total Exec</div><div class="value">' || COALESCE(to_char(round(med_total_exec::numeric, 2), 'FM999,999,990.00'), 'N/A') || ' ms</div></div>' ||
    '<div class="card"><div class="label">P90 Total Exec</div><div class="value">' || COALESCE(to_char(round(p90_total_exec::numeric, 2), 'FM999,999,990.00'), 'N/A') || ' ms</div></div>' ||
    '<div class="card"><div class="label">Current vs Median</div><div class="value">' ||
      CASE WHEN COALESCE(med_total_exec, 0) = 0 THEN 'N/A' ELSE to_char(round((:'pg360_target_total_exec_ms'::numeric / NULLIF(med_total_exec, 0))::numeric, 2), 'FM999,990.00') || 'x' END ||
      '</div></div>' ||
    '<div class="card"><div class="label">Current Calls vs Median</div><div class="value">' ||
      CASE WHEN COALESCE(med_calls, 0) = 0 THEN 'N/A' ELSE to_char(round((:'pg360_target_calls'::numeric / NULLIF(med_calls, 0))::numeric, 2), 'FM999,990.00') || 'x' END ||
      '</div></div>' ||
    '</div>'
  FROM hist
), '<div class="code">No repository history exists yet for this query fingerprint.</div>');
\else
SELECT '<div class="code">Repository mode is not enabled. Run pg360_repo_setup.sql once, then pg360_repo_capture.sql on a schedule to unlock query-level history and baseline comparisons.</div>';
\endif
\qecho '</div>'
\qecho '</div></body></html>'
\o
COMMIT;
\qecho PG360 SQL deep-dive report: :pg360_sql_report_path

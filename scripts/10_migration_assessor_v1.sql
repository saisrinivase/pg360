/*
Purpose: Oracle -> PostgreSQL migration assessor (single SQL file) with numbered sub-reports.
Area: Migration Validation
Usage:
  psql "host=<host> port=<port> dbname=<db> user=<user>" \
    -v target_schema_regex='.*' \
    -v critical_schema_regex='.*' \
    -v report_file='samples/pg360_report.html' \
    -v report_01_file='samples/pg360_01_type_consistency.html' \
    -v report_02_file='samples/pg360_02_casting_issues.html' \
    -v report_03_file='samples/pg360_03_indexes_needed.html' \
    -v report_04_file='samples/pg360_04_unused_duplicate_indexes.html' \
    -v report_05_file='samples/pg360_05_bloat_report.html' \
    -v report_06_file='samples/pg360_06_partition_health.html' \
    -v report_07_file='samples/pg360_07_config_readiness.html' \
    -v report_08_file='samples/pg360_08_compatibility_matrix.html' \
    -v gate_html_file='samples/pg360_09_gate_summary.html' \
    -v json_file='samples/migration_assessment_v1.json' \
    -v gate_output_file='samples/migration_gate_v1.txt' \
    -v enforce_exit=false \
    -f scripts/10_migration_assessor_v1.sql

Notes:
  - One file only (SQL + PL/pgSQL).
  - Produces:
      1) Main HTML report with numbered sections (1,2,3,...)
      2) JSON output for automation
      3) Gate summary text (GO/CONDITIONAL_GO/NO_GO)
*/

\set ON_ERROR_STOP on
\pset pager off
\pset border 1
\pset footer off

\if :{?target_schema_regex}
\else
\set target_schema_regex '.*'
\endif

\if :{?critical_schema_regex}
\else
\set critical_schema_regex '.*'
\endif

\if :{?report_file}
\else
\set report_file 'samples/pg360_report.html'
\endif

\if :{?report_01_file}
\else
\set report_01_file 'samples/pg360_01_type_consistency.html'
\endif

\if :{?report_02_file}
\else
\set report_02_file 'samples/pg360_02_casting_issues.html'
\endif

\if :{?report_03_file}
\else
\set report_03_file 'samples/pg360_03_indexes_needed.html'
\endif

\if :{?report_04_file}
\else
\set report_04_file 'samples/pg360_04_unused_duplicate_indexes.html'
\endif

\if :{?report_05_file}
\else
\set report_05_file 'samples/pg360_05_bloat_report.html'
\endif

\if :{?report_06_file}
\else
\set report_06_file 'samples/pg360_06_partition_health.html'
\endif

\if :{?report_07_file}
\else
\set report_07_file 'samples/pg360_07_config_readiness.html'
\endif

\if :{?report_08_file}
\else
\set report_08_file 'samples/pg360_08_compatibility_matrix.html'
\endif

\if :{?gate_html_file}
\else
\set gate_html_file 'samples/pg360_09_gate_summary.html'
\endif

\if :{?json_file}
\else
\set json_file 'samples/migration_assessment_v1.json'
\endif

\if :{?gate_output_file}
\else
\set gate_output_file 'samples/migration_gate_v1.txt'
\endif

\if :{?enforce_exit}
\else
\set enforce_exit false
\endif

\! mkdir -p samples

\set QUIET 1

CREATE OR REPLACE FUNCTION pg_temp.safe_count(sql_text text)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
    c bigint;
BEGIN
    EXECUTE format('SELECT count(*) FROM (%s) q', sql_text) INTO c;
    RETURN COALESCE(c, 0);
EXCEPTION
    WHEN undefined_table OR undefined_column OR undefined_function THEN
        RETURN 0;
END;
$$;

CREATE OR REPLACE FUNCTION pg_temp.safe_text(sql_text text, default_text text)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v text;
BEGIN
    EXECUTE format('SELECT COALESCE((%s)::text, %L)', sql_text, default_text) INTO v;
    RETURN COALESCE(v, default_text);
EXCEPTION
    WHEN undefined_table OR undefined_column OR undefined_function THEN
        RETURN default_text;
END;
$$;

CREATE TEMP TABLE tmp_schema_type_consistency AS
WITH cols AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS current_type
    FROM pg_attribute a
    JOIN pg_class c
      ON c.oid = a.attrelid
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
), agg AS (
    SELECT
        column_name,
        count(DISTINCT current_type) AS type_count,
        count(*) AS total_occurrences,
        string_agg(DISTINCT current_type, ', ' ORDER BY current_type) AS all_types_used
    FROM cols
    GROUP BY column_name
)
SELECT
    c.schema_name,
    c.table_name,
    c.column_name,
    c.current_type,
    a.type_count,
    a.total_occurrences,
    a.all_types_used,
    CASE
        WHEN a.type_count >= 4 THEN 'PROBLEM'
        WHEN a.type_count >= 2 THEN 'REVIEW'
        ELSE 'OK'
    END AS status,
    CASE
        WHEN a.type_count >= 4 THEN 'Directly standardize target type and run cast validation dry-run'
        WHEN a.all_types_used ~ 'character varying' AND a.all_types_used ~ '(bigint|integer|numeric|smallint)' THEN 'Clean data, then migrate to numeric/bigint standard type'
        ELSE 'Review and standardize'
    END AS recommendation
FROM cols c
JOIN agg a
  ON a.column_name = c.column_name
WHERE a.type_count > 1
ORDER BY a.type_count DESC, a.total_occurrences DESC, c.column_name, c.schema_name, c.table_name;

CREATE TEMP TABLE tmp_casting_issues AS
WITH defs AS (
    SELECT
        n.nspname AS schema_name,
        'FUNCTION'::text AS object_type,
        p.proname AS object_name,
        pg_get_functiondef(p.oid) AS def_text
    FROM pg_proc p
    JOIN pg_namespace n
      ON n.oid = p.pronamespace
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
      AND p.prokind IN ('f', 'p')

    UNION ALL

    SELECT
        n.nspname AS schema_name,
        CASE WHEN c.relkind = 'm' THEN 'MVIEW' ELSE 'VIEW' END AS object_type,
        c.relname AS object_name,
        pg_get_viewdef(c.oid, true) AS def_text
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
      AND c.relkind IN ('v', 'm')
), dc AS (
    SELECT
        d.schema_name,
        d.object_type,
        d.object_name,
        'DOUBLE_COLON'::text AS syntax,
        trim(m[1]) AS expression,
        trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(
        lower(d.def_text),
        '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)',
        'g'
    ) AS m
), cf AS (
    SELECT
        d.schema_name,
        d.object_type,
        d.object_name,
        'CAST_FUNCTION'::text AS syntax,
        trim(m[1]) AS expression,
        trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(
        lower(d.def_text),
        'cast\s*[(]\s*([^,)]+?)\s+as\s+([a-z0-9_ ]+)\s*[)]',
        'g'
    ) AS m
), all_casts AS (
    SELECT * FROM dc
    UNION ALL
    SELECT * FROM cf
)
SELECT
    schema_name,
    object_type,
    object_name,
    syntax,
    expression,
    cast_to,
    CASE
        WHEN cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)' THEN 'HIGH'
        WHEN cast_to ~ '(char|character varying|varchar|text)' THEN 'MED'
        ELSE 'LOW'
    END AS risk_level,
    CASE
        WHEN position('.' IN expression) > 0 THEN split_part(expression, '.', 2)
        ELSE expression
    END AS column_guess
FROM all_casts;

CREATE TEMP TABLE tmp_fk_index_coverage AS
WITH fk AS (
    SELECT
        con.oid,
        n.nspname AS schema_name,
        c.relname AS table_name,
        con.conname AS fk_name,
        con.conrelid,
        con.conkey,
        array_to_string(
            ARRAY(
                SELECT a.attname
                FROM unnest(con.conkey) WITH ORDINALITY k(attnum, ord)
                JOIN pg_attribute a
                  ON a.attrelid = con.conrelid
                 AND a.attnum = k.attnum
                ORDER BY k.ord
            ),
            ', '
        ) AS fk_columns
    FROM pg_constraint con
    JOIN pg_class c
      ON c.oid = con.conrelid
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE con.contype = 'f'
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
)
SELECT
    fk.schema_name,
    fk.table_name,
    fk.fk_name,
    fk.fk_columns,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        ) THEN 'YES'
        ELSE 'NO'
    END AS index_exists,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        ) THEN 'No action required'
        ELSE 'Create btree index on FK columns as leading keys'
    END AS recommendation
FROM fk
ORDER BY fk.schema_name, fk.table_name, fk.fk_name;

CREATE TEMP TABLE tmp_unused_indexes AS
SELECT
    s.schemaname AS schema_name,
    s.relname AS table_name,
    s.indexrelname AS index_name,
    pg_relation_size(s.indexrelid) AS index_bytes,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    CASE
        WHEN pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE'
        ELSE 'MONITOR'
    END AS status,
    CASE
        WHEN pg_relation_size(s.indexrelid) >= 1073741824 THEN 'Validate with workload replay, then drop if truly unused'
        ELSE 'Monitor; may be seasonal or batch-driven index'
    END AS recommendation
FROM pg_stat_user_indexes s
JOIN pg_index i
  ON i.indexrelid = s.indexrelid
WHERE s.schemaname !~ '^pg_'
  AND s.schemaname <> 'information_schema'
  AND s.schemaname ~ :'target_schema_regex'
  AND i.indisprimary = false
  AND i.indisunique = false
  AND s.idx_scan = 0
ORDER BY pg_relation_size(s.indexrelid) DESC;

CREATE TEMP TABLE tmp_duplicate_indexes AS
WITH idx AS (
    SELECT
        n.nspname AS schema_name,
        t.relname AS table_name,
        i.relname AS index_name,
        x.indrelid,
        x.indkey::text AS indkey,
        COALESCE(pg_get_expr(x.indexprs, x.indrelid), '') AS index_expr,
        COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS index_pred,
        x.indisprimary
    FROM pg_index x
    JOIN pg_class i
      ON i.oid = x.indexrelid
    JOIN pg_class t
      ON t.oid = x.indrelid
    JOIN pg_namespace n
      ON n.oid = t.relnamespace
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
), grp AS (
    SELECT
        schema_name,
        table_name,
        indrelid,
        indkey,
        index_expr,
        index_pred,
        count(*) AS duplicate_count,
        string_agg(index_name, ', ' ORDER BY index_name) AS duplicate_indexes
    FROM idx
    WHERE indisprimary = false
    GROUP BY schema_name, table_name, indrelid, indkey, index_expr, index_pred
    HAVING count(*) > 1
)
SELECT
    schema_name,
    table_name,
    duplicate_count,
    duplicate_indexes,
    'Keep one index, drop redundant duplicates after plan validation' AS recommendation
FROM grp
ORDER BY duplicate_count DESC, schema_name, table_name;

CREATE TEMP TABLE tmp_bloat_report AS
WITH tbl AS (
    SELECT
        s.schemaname AS schema_name,
        s.relname AS object_name,
        'TABLE'::text AS object_type,
        pg_total_relation_size(s.relid) AS total_bytes,
        pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
        s.n_dead_tup::bigint AS dead_tuples,
        round((s.n_dead_tup::numeric * 100.0) / NULLIF((s.n_live_tup + s.n_dead_tup)::numeric, 0), 2) AS dead_pct,
        CASE
            WHEN s.n_dead_tup >= 500000 AND s.n_dead_tup > (s.n_live_tup * 0.20) THEN 'VACUUM_ANALYZE_NOW'
            WHEN s.n_dead_tup >= 100000 AND s.n_dead_tup > (s.n_live_tup * 0.10) THEN 'MONITOR_AND_TUNE_AUTOVACUUM'
            ELSE 'MONITOR'
        END AS recommended_action
    FROM pg_stat_user_tables s
    WHERE s.schemaname !~ '^pg_'
      AND s.schemaname <> 'information_schema'
      AND s.schemaname ~ :'target_schema_regex'
), idx AS (
    SELECT
        s.schemaname AS schema_name,
        s.indexrelname AS object_name,
        'INDEX'::text AS object_type,
        pg_relation_size(s.indexrelid) AS total_bytes,
        pg_size_pretty(pg_relation_size(s.indexrelid)) AS total_size,
        0::bigint AS dead_tuples,
        0::numeric AS dead_pct,
        CASE
            WHEN i.indisvalid = false THEN 'REINDEX_CONCURRENTLY'
            WHEN s.idx_scan = 0 AND pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE'
            WHEN pg_relation_size(s.indexrelid) >= 21474836480 THEN 'REINDEX_CONCURRENTLY'
            ELSE 'MONITOR'
        END AS recommended_action
    FROM pg_stat_user_indexes s
    JOIN pg_index i
      ON i.indexrelid = s.indexrelid
    WHERE s.schemaname !~ '^pg_'
      AND s.schemaname <> 'information_schema'
      AND s.schemaname ~ :'target_schema_regex'
)
SELECT * FROM tbl
UNION ALL
SELECT * FROM idx;

CREATE TEMP TABLE tmp_partition_health AS
WITH pt AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        p.partstrat,
        c.oid AS relid,
        (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
    FROM pg_partitioned_table p
    JOIN pg_class c
      ON c.oid = p.partrelid
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
)
SELECT
    pt.schema_name,
    pt.table_name,
    CASE pt.partstrat
        WHEN 'r' THEN 'RANGE'
        WHEN 'l' THEN 'LIST'
        WHEN 'h' THEN 'HASH'
        ELSE 'UNKNOWN'
    END AS partition_strategy,
    pt.partition_count,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.relid
              AND i.indisvalid
              AND i.indisready
        ) THEN 'YES'
        ELSE 'NO'
    END AS has_parent_index,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.relid
              AND i.indisvalid
              AND i.indisready
        ) THEN 'OK'
        ELSE 'REVIEW'
    END AS status,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.relid
              AND i.indisvalid
              AND i.indisready
        ) THEN 'No action required'
        ELSE 'Create parent-level partition-aware indexes for major join/filter paths'
    END AS recommendation
FROM pt
ORDER BY pt.schema_name, pt.table_name;

CREATE TEMP TABLE tmp_config_readiness AS
SELECT
    'autovacuum'::text AS parameter_name,
    setting AS current_value,
    CASE WHEN lower(setting) IN ('on', 'true', '1') THEN 'PASS' ELSE 'FAIL' END AS status,
    'Must be enabled to control dead tuples and xid risk'::text AS recommendation
FROM pg_settings
WHERE name = 'autovacuum'

UNION ALL

SELECT
    'track_io_timing',
    setting,
    CASE WHEN lower(setting) IN ('on', 'true', '1') THEN 'PASS' ELSE 'WARN' END,
    'Enable for better I/O diagnostics and migration tuning'
FROM pg_settings
WHERE name = 'track_io_timing'

UNION ALL

SELECT
    'max_wal_size',
    setting || unit,
    CASE WHEN pg_size_bytes(setting || unit) >= 4294967296 THEN 'PASS' ELSE 'WARN' END,
    'Recommended baseline >= 4GB for larger migration workloads'
FROM pg_settings
WHERE name = 'max_wal_size'

UNION ALL

SELECT
    'checkpoint_timeout',
    setting || 's',
    CASE WHEN setting::int >= 600 THEN 'PASS' ELSE 'WARN' END,
    'Recommended baseline >= 600 seconds to reduce checkpoint pressure'
FROM pg_settings
WHERE name = 'checkpoint_timeout'

UNION ALL

SELECT
    'checkpoint_completion_target',
    setting,
    CASE WHEN setting::numeric >= 0.90 THEN 'PASS' ELSE 'WARN' END,
    'Recommended baseline >= 0.90 for smoother checkpoints'
FROM pg_settings
WHERE name = 'checkpoint_completion_target'

UNION ALL

SELECT
    'archive_mode',
    setting,
    CASE WHEN setting IN ('on', 'always') THEN 'PASS' ELSE 'FAIL' END,
    'Enable for PITR readiness and rollback safety'
FROM pg_settings
WHERE name = 'archive_mode'

UNION ALL

SELECT
    'wal_level',
    setting,
    CASE WHEN setting IN ('replica', 'logical') THEN 'PASS' ELSE 'FAIL' END,
    'Should be replica or logical for HA/replication-ready posture'
FROM pg_settings
WHERE name = 'wal_level'

UNION ALL

SELECT
    'pg_stat_statements',
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'installed' ELSE 'missing' END,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'PASS' ELSE 'WARN' END,
    'Install for workload regression analysis and SQL tuning evidence';

CREATE TEMP TABLE tmp_main_findings (
    finding_id text NOT NULL,
    finding_title text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL,
    issue_count bigint NOT NULL,
    evidence text NOT NULL,
    recommended_action text NOT NULL
);

INSERT INTO tmp_main_findings
SELECT
    'MIG-TYPE-001',
    'Schema type consistency: PROBLEM rows',
    'P1',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
    x.cnt,
    'type_problem_rows=' || x.cnt,
    'Standardize inconsistent key/business column types before cutover'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_schema_type_consistency WHERE status = 'PROBLEM') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-TYPE-002',
    'Schema type consistency: REVIEW rows',
    'P2',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'type_review_rows=' || x.cnt,
    'Review and standardize remaining mixed-type columns'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_schema_type_consistency WHERE status = 'REVIEW') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-CAST-001',
    'Casting issues: HIGH risk',
    'P2',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'high_cast_issues=' || x.cnt,
    'Prioritize removing HIGH-risk casts in JOIN/WHERE/business logic'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_casting_issues WHERE risk_level = 'HIGH') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-INDEX-001',
    'Indexes needed: FK coverage gaps',
    'P1',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
    x.cnt,
    'missing_fk_indexes=' || x.cnt,
    'Create supporting indexes for FK columns before Day-1'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_fk_index_coverage WHERE index_exists = 'NO') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-INDEX-002',
    'Unused index candidates',
    'P3',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'unused_indexes=' || x.cnt,
    'Review and drop large unused indexes only after workload replay validation'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_unused_indexes) x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-INDEX-003',
    'Duplicate index candidates',
    'P3',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'duplicate_index_groups=' || x.cnt,
    'Retain one index per equivalent key/expression/predicate set'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_duplicate_indexes) x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-BLOAT-001',
    'Table bloat pressure',
    'P2',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'high_bloat_tables=' || x.cnt,
    'Run VACUUM (ANALYZE) and tune autovacuum thresholds for heavy tables'
FROM (
    SELECT count(*)::bigint AS cnt
    FROM tmp_bloat_report
    WHERE object_type = 'TABLE'
      AND dead_tuples >= 500000
      AND dead_pct >= 20
) x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-PART-001',
    'Partition index coverage review',
    'P2',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'partition_index_review=' || x.cnt,
    'Ensure partitioned parent tables have appropriate indexing strategy'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_partition_health WHERE status = 'REVIEW') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-CONFIG-001',
    'Critical configuration failures',
    'P1',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
    x.cnt,
    'critical_config_fails=' || x.cnt,
    'Fix FAIL-level configuration parameters before migration takeover'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_config_readiness WHERE status = 'FAIL') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-CONFIG-002',
    'Configuration warnings',
    'P2',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'WARN' END,
    x.cnt,
    'config_warns=' || x.cnt,
    'Tune WARN-level settings and validate with performance test runs'
FROM (SELECT count(*)::bigint AS cnt FROM tmp_config_readiness WHERE status = 'WARN') x;

INSERT INTO tmp_main_findings
SELECT
    'MIG-APP-001',
    'Idle-in-transaction session risk',
    'P1',
    CASE WHEN x.cnt = 0 THEN 'PASS' ELSE 'FAIL' END,
    x.cnt,
    'idle_in_tx_over_10m=' || x.cnt,
    'Fix app transaction boundaries and enforce transaction timeout policies'
FROM (
    SELECT count(*)::bigint AS cnt
    FROM pg_stat_activity
    WHERE state = 'idle in transaction'
      AND xact_start IS NOT NULL
      AND now() - xact_start > interval '10 minutes'
) x;

CREATE TEMP TABLE tmp_mig_v1_summary AS
WITH agg AS (
    SELECT
        count(*)::bigint AS total_checks,
        count(*) FILTER (WHERE status = 'PASS')::bigint AS pass_checks,
        count(*) FILTER (WHERE status = 'WARN')::bigint AS warn_checks,
        count(*) FILTER (WHERE status = 'FAIL')::bigint AS fail_checks,
        count(*) FILTER (WHERE severity = 'P1' AND status IN ('FAIL', 'WARN'))::bigint AS p1_open,
        count(*) FILTER (WHERE severity = 'P2' AND status IN ('FAIL', 'WARN'))::bigint AS p2_open,
        count(*) FILTER (WHERE severity = 'P3' AND status IN ('FAIL', 'WARN'))::bigint AS p3_open,
        count(*) FILTER (WHERE status IN ('FAIL', 'WARN'))::bigint AS open_findings
    FROM tmp_main_findings
)
SELECT
    total_checks,
    pass_checks,
    warn_checks,
    fail_checks,
    p1_open,
    p2_open,
    p3_open,
    open_findings,
    ((p1_open * 40) + (p2_open * 15) + (p3_open * 5))::bigint AS risk_score,
    CASE
        WHEN p1_open > 0 THEN 'NO_GO'
        WHEN p2_open > 0 OR p3_open > 0 THEN 'CONDITIONAL_GO'
        ELSE 'GO'
    END AS final_decision,
    now() AS generated_at
FROM agg;

\set QUIET 0

\pset format html

\o :report_file
\qecho <!DOCTYPE html>
\qecho <html>
\qecho <head>
\qecho <meta charset="utf-8">
\qecho <title>Migration Report</title>
\qecho <style>
\qecho body { font-family: "Segoe UI", Arial, sans-serif; margin: 0; padding: 24px; color: #1d2b3a; background: linear-gradient(180deg, #f8fbff 0%, #eef5fb 100%); }
\qecho h1 { color: #1f4e79; margin: 0 0 8px 0; letter-spacing: 0.2px; }
\qecho h2 { color: #245580; margin-top: 22px; padding: 8px 10px; background: #f1f6fc; border: 1px solid #d8e4f2; border-radius: 8px; }
\qecho .meta { color: #36506b; margin: 12px 0; background: #ffffff; border: 1px solid #d8e4f2; border-radius: 10px; padding: 10px 12px; box-shadow: 0 1px 2px rgba(14, 39, 72, 0.06); }
\qecho table { border-collapse: collapse; width: 100%; background: #ffffff; margin: 14px 0 18px 0; border: 1px solid #d8e4f2; }
\qecho caption { caption-side: top; text-align: left; font-weight: 700; color: #244b74; background: #f3f8fe; border: 1px solid #d8e4f2; border-bottom: none; padding: 9px 10px; }
\qecho th { background: #eaf2fb; color: #1f3f63; padding: 8px; border: 1px solid #d8e4f2; text-align: left; }
\qecho td { padding: 8px; border: 1px solid #deebf7; vertical-align: top; }
\qecho tr:nth-child(even) td { background: #f9fcff; }
\qecho a { color: #1d5ea8; text-decoration: none; font-weight: 600; }
\qecho a:hover { text-decoration: underline; }
\qecho li { margin: 8px 0; }
\qecho .foot { margin-top: 18px; color: #4a5f79; font-size: 12px; }
\qecho </style>
\qecho </head>
\qecho <body>
\qecho <h1>PG360 Migration Dashboard</h1>
\qecho <div class="meta">Summary checks with links to detailed sub-reports.</div>

\pset title 'Metadata'
SELECT
    current_database() AS database_name,
    current_user AS executed_by,
    version() AS postgres_version,
    :'target_schema_regex' AS target_schema_regex,
    :'critical_schema_regex' AS critical_schema_regex,
    now() AS report_generated_at;

\pset title 'Gate Decision and Risk Score'
SELECT
    final_decision,
    risk_score,
    total_checks,
    pass_checks,
    warn_checks,
    fail_checks,
    p1_open,
    p2_open,
    p3_open,
    open_findings
FROM tmp_mig_v1_summary;

\pset title 'Main Findings (Action Queue)'
SELECT
    finding_id,
    finding_title,
    severity,
    status,
    issue_count,
    evidence,
    recommended_action
FROM tmp_main_findings
ORDER BY
    CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
    finding_id;

\qecho <h2>Sub-Reports</h2>
\qecho <ol>
\qecho <li><a href="pg360_01_type_consistency.html">1. Schema Type Consistency Report</a></li>
\qecho <li><a href="pg360_02_casting_issues.html">2. Casting Issues Report</a></li>
\qecho <li><a href="pg360_03_indexes_needed.html">3. Indexes Needed Report (FK Coverage)</a></li>
\qecho <li><a href="pg360_04_unused_duplicate_indexes.html">4. Unused and Duplicate Indexes Report</a></li>
\qecho <li><a href="pg360_05_bloat_report.html">5. Table and Index Bloat Report</a></li>
\qecho <li><a href="pg360_06_partition_health.html">6. Partition Health Report</a></li>
\qecho <li><a href="pg360_07_config_readiness.html">7. Configuration Readiness Report</a></li>
\qecho <li><a href="pg360_08_compatibility_matrix.html">8. Compatibility Matrix Report</a></li>
\qecho <li><a href="pg360_09_gate_summary.html">9. Gate Summary Report</a></li>
\qecho </ol>
\qecho <div class="foot">Generated by scripts/10_migration_assessor_v1.sql</div>
\qecho </body>
\qecho </html>

\o :report_01_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>1. Schema Type Consistency Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>1. Schema Type Consistency Report (Dry Run)</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '1.1 Type Inconsistency Details'
SELECT
    schema_name,
    table_name,
    column_name,
    current_type,
    type_count,
    total_occurrences,
    all_types_used,
    status,
    recommendation
FROM tmp_schema_type_consistency
ORDER BY type_count DESC, total_occurrences DESC, schema_name, table_name, column_name
LIMIT 3000;
\qecho </body></html>

\o :report_02_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>2. Casting Issues Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>2. Casting Issues Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '2.1 Casting Summary by Risk'
SELECT
    risk_level,
    count(*) AS issue_count
FROM tmp_casting_issues
GROUP BY risk_level
ORDER BY CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END;

\pset title '2.2 Casting Issue Details'
SELECT
    schema_name,
    object_type,
    object_name,
    syntax,
    expression,
    cast_to,
    risk_level,
    column_guess
FROM tmp_casting_issues
ORDER BY CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END, schema_name, object_type, object_name
LIMIT 5000;
\qecho </body></html>

\o :report_03_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>3. Indexes Needed Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>3. Indexes Needed Report (FK Coverage)</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '3.1 FK Index Coverage'
SELECT
    schema_name,
    table_name,
    fk_name,
    fk_columns,
    index_exists,
    recommendation
FROM tmp_fk_index_coverage
ORDER BY schema_name, table_name, fk_name;

\pset title '3.2 Missing FK Indexes'
SELECT
    schema_name,
    table_name,
    fk_name,
    fk_columns,
    recommendation
FROM tmp_fk_index_coverage
WHERE index_exists = 'NO'
ORDER BY schema_name, table_name, fk_name;
\qecho </body></html>

\o :report_04_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>4. Unused and Duplicate Indexes Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>4. Unused and Duplicate Indexes Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '4.1 Unused Index Candidates'
SELECT
    schema_name,
    table_name,
    index_name,
    index_size,
    idx_scan,
    status,
    recommendation
FROM tmp_unused_indexes
ORDER BY index_bytes DESC
LIMIT 3000;

\pset title '4.2 Duplicate Index Groups'
SELECT
    schema_name,
    table_name,
    duplicate_count,
    duplicate_indexes,
    recommendation
FROM tmp_duplicate_indexes
ORDER BY duplicate_count DESC, schema_name, table_name;
\qecho </body></html>

\o :report_05_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>5. Table and Index Bloat Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>5. Table and Index Bloat Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '5.1 Bloat Details'
SELECT
    schema_name,
    object_name,
    object_type,
    total_size,
    dead_tuples,
    dead_pct,
    recommended_action
FROM tmp_bloat_report
ORDER BY total_bytes DESC
LIMIT 5000;
\qecho </body></html>

\o :report_06_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>6. Partition Health Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>6. Partition Health Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '6.1 Partitioning Checks'
SELECT
    schema_name,
    table_name,
    partition_strategy,
    partition_count,
    has_parent_index,
    status,
    recommendation
FROM tmp_partition_health
ORDER BY schema_name, table_name;
\qecho </body></html>

\o :report_07_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>7. Configuration Readiness Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>7. Configuration Readiness Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '7.1 Configuration Parameters'
SELECT
    parameter_name,
    current_value,
    status,
    recommendation
FROM tmp_config_readiness
ORDER BY
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 WHEN 'PASS' THEN 3 ELSE 4 END,
    parameter_name;
\qecho </body></html>

\o :report_08_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>8. Compatibility Matrix Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>8. Compatibility Matrix Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '8.1 Compatibility Mapping (By Main Findings)'
SELECT
    finding_id,
    finding_title,
    severity,
    status,
    issue_count,
    evidence,
    recommended_action
FROM tmp_main_findings
ORDER BY
    CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
    finding_id;
\qecho </body></html>

\o :gate_html_file
\qecho <!DOCTYPE html>
\qecho <html><head><meta charset="utf-8"><title>9. Gate Summary Report</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body>
\qecho <h1>9. Gate Summary Report</h1>
\qecho <p><a href="pg360_report.html">Back to Main Migration Report</a></p>
\pset title '9.1 Gate Decision'
SELECT
    final_decision,
    p1_open,
    p2_open,
    p3_open,
    open_findings,
    risk_score,
    generated_at
FROM tmp_mig_v1_summary;

\pset title '9.2 P1 Open Findings (Blockers)'
SELECT
    finding_id,
    finding_title,
    status,
    issue_count,
    evidence,
    recommended_action
FROM tmp_main_findings
WHERE severity = 'P1'
  AND status IN ('FAIL', 'WARN')
ORDER BY finding_id;
\qecho </body></html>

\o :json_file

\o :json_file
\pset format unaligned
\pset tuples_only on

SELECT jsonb_pretty(
    jsonb_build_object(
        'metadata', jsonb_build_object(
            'database_name', current_database(),
            'executed_by', current_user,
            'target_schema_regex', :'target_schema_regex',
            'critical_schema_regex', :'critical_schema_regex',
            'generated_at', now()
        ),
        'report_files', jsonb_build_object(
            'main_report', :'report_file',
            'schema_type_consistency', :'report_01_file',
            'casting_issues', :'report_02_file',
            'indexes_needed', :'report_03_file',
            'unused_duplicate_indexes', :'report_04_file',
            'bloat_report', :'report_05_file',
            'partition_health', :'report_06_file',
            'config_readiness', :'report_07_file',
            'compatibility_matrix', :'report_08_file',
            'gate_html', :'gate_html_file',
            'gate_text', :'gate_output_file'
        ),
        'summary', (SELECT to_jsonb(s) FROM tmp_mig_v1_summary s),
        'main_findings', (
            SELECT COALESCE(jsonb_agg(to_jsonb(mf) ORDER BY
                CASE mf.severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
                CASE mf.status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
                mf.finding_id), '[]'::jsonb)
            FROM tmp_main_findings mf
        ),
        'reports', jsonb_build_object(
            'schema_type_consistency', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.type_count DESC, t.total_occurrences DESC, t.schema_name, t.table_name, t.column_name), '[]'::jsonb)
                FROM (
                    SELECT *
                    FROM tmp_schema_type_consistency
                    ORDER BY type_count DESC, total_occurrences DESC, schema_name, table_name, column_name
                    LIMIT 2000
                ) t
            ),
            'casting_issues', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY
                    CASE t.risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
                    t.schema_name, t.object_type, t.object_name), '[]'::jsonb)
                FROM (
                    SELECT *
                    FROM tmp_casting_issues
                    ORDER BY
                        CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END,
                        schema_name, object_type, object_name
                    LIMIT 2500
                ) t
            ),
            'fk_index_coverage', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.schema_name, t.table_name, t.fk_name), '[]'::jsonb)
                FROM tmp_fk_index_coverage t
            ),
            'unused_indexes', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.index_bytes DESC), '[]'::jsonb)
                FROM tmp_unused_indexes t
            ),
            'duplicate_indexes', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.duplicate_count DESC, t.schema_name, t.table_name), '[]'::jsonb)
                FROM tmp_duplicate_indexes t
            ),
            'bloat_report', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.total_bytes DESC), '[]'::jsonb)
                FROM (
                    SELECT *
                    FROM tmp_bloat_report
                    ORDER BY total_bytes DESC
                    LIMIT 2500
                ) t
            ),
            'partition_health', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY t.schema_name, t.table_name), '[]'::jsonb)
                FROM tmp_partition_health t
            ),
            'config_readiness', (
                SELECT COALESCE(jsonb_agg(to_jsonb(t) ORDER BY
                    CASE t.status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 WHEN 'PASS' THEN 3 ELSE 4 END,
                    t.parameter_name), '[]'::jsonb)
                FROM tmp_config_readiness t
            )
        )
    )
);

\pset tuples_only off
\pset format aligned

\o :gate_output_file
\pset title 'Migration Assessor Gate (V1)'
SELECT
    final_decision,
    p1_open,
    p2_open,
    p3_open,
    open_findings,
    risk_score,
    generated_at
FROM tmp_mig_v1_summary;

\pset title 'P1 Open Findings (Blockers)'
SELECT
    finding_id,
    finding_title,
    status,
    issue_count,
    evidence,
    recommended_action
FROM tmp_main_findings
WHERE severity = 'P1'
  AND status IN ('FAIL', 'WARN')
ORDER BY finding_id;

\o
\pset title
\pset pager on

SELECT
    final_decision = 'NO_GO' AS is_no_go,
    final_decision
FROM tmp_mig_v1_summary
\gset

\if :enforce_exit
\if :is_no_go
DO $$
BEGIN
    RAISE EXCEPTION 'NO_GO decision reached. enforce_exit=true';
END;
$$;
\endif
\endif

-- SAMPLE_OUTPUT_BEGIN
-- final_decision | p1_open | p2_open | p3_open | open_findings | risk_score
-- --------------+---------+---------+---------+---------------+-----------
-- NO_GO         | 2       | 5       | 1       | 8             | 160
-- SAMPLE_OUTPUT_END

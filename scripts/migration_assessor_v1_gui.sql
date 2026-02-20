/*
Purpose: Oracle -> PostgreSQL migration assessor for pgAdmin/DBeaver (pure SELECT).
Area: Migration Validation

How to use:
1. Copy and run as script in pgAdmin or DBeaver.
2. Edit schema regex values in each cfg CTE if needed.
3. Review results section by section.
4. Open section `10. HTML Report Payload` and save `html_report` cell as `.html`.

Constraints:
- Pure SELECT queries with CTEs only.
- No CREATE/DROP/ALTER/INSERT/UPDATE/DELETE.
*/

-- 1. Executive Summary
SELECT '1. Executive Summary' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), findings AS (
    SELECT 'MIG-TYPE-001'::text AS finding_id, 'Schema type consistency: PROBLEM rows'::text AS finding_title, 'P1'::text AS severity,
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END AS status, cnt AS issue_count,
           'type_problem_rows=' || cnt AS evidence,
           'Standardize inconsistent key/business column types before cutover'::text AS recommended_action
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt
        FROM agg
        WHERE type_count >= 4
    ) s

    UNION ALL

    SELECT 'MIG-TYPE-002', 'Schema type consistency: REVIEW rows', 'P2',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'type_review_rows=' || cnt,
           'Review and standardize remaining mixed-type columns'
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt
        FROM agg
        WHERE type_count BETWEEN 2 AND 3
    ) s

    UNION ALL

    SELECT 'MIG-CAST-001', 'Casting issues: HIGH risk', 'P2',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_cast_issues=' || cnt,
           'Prioritize removing HIGH-risk casts in JOIN/WHERE/business logic'
    FROM (
        WITH defs AS (
            SELECT n.nspname AS schema_name, pg_get_functiondef(p.oid) AS def_text
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND p.prokind IN ('f', 'p')

            UNION ALL

            SELECT n.nspname AS schema_name, pg_get_viewdef(c.oid, true) AS def_text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND c.relkind IN ('v', 'm')
        ), casts AS (
            SELECT trim(m[2]) AS cast_to
            FROM defs d
            CROSS JOIN LATERAL regexp_matches(lower(d.def_text),
                '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
        )
        SELECT count(*)::bigint AS cnt
        FROM casts
        WHERE cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)'
    ) s

    UNION ALL

    SELECT 'MIG-INDEX-001', 'Indexes needed: FK coverage gaps', 'P1',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'missing_fk_indexes=' || cnt,
           'Create supporting indexes for FK columns before Day-1'
    FROM (
        WITH fk AS (
            SELECT con.conrelid, con.conkey
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM fk
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        )
    ) s

    UNION ALL

    SELECT 'MIG-INDEX-002', 'Unused index candidates', 'P3',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'unused_indexes=' || cnt,
           'Review and drop large unused indexes only after workload replay validation'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON i.indexrelid = s.indexrelid
        JOIN cfg ON true
        WHERE s.schemaname !~ '^pg_'
          AND s.schemaname <> 'information_schema'
          AND s.schemaname ~ cfg.target_schema_regex
          AND i.indisprimary = false
          AND i.indisunique = false
          AND s.idx_scan = 0
    ) s

    UNION ALL

    SELECT 'MIG-INDEX-003', 'Duplicate index candidates', 'P3',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'duplicate_index_groups=' || cnt,
           'Retain one index per equivalent key/expression/predicate set'
    FROM (
        WITH idx AS (
            SELECT
                x.indrelid,
                x.indkey::text AS indkey,
                COALESCE(pg_get_expr(x.indexprs, x.indrelid), '') AS index_expr,
                COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS index_pred,
                x.indisprimary
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM (
            SELECT indrelid, indkey, index_expr, index_pred
            FROM idx
            WHERE indisprimary = false
            GROUP BY indrelid, indkey, index_expr, index_pred
            HAVING count(*) > 1
        ) d
    ) s

    UNION ALL

    SELECT 'MIG-BLOAT-001', 'Table bloat pressure', 'P2',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_bloat_tables=' || cnt,
           'Run VACUUM (ANALYZE) and tune autovacuum thresholds for heavy tables'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_tables s
        JOIN cfg ON true
        WHERE s.schemaname ~ cfg.target_schema_regex
          AND s.n_dead_tup >= 500000
          AND s.n_live_tup > 0
          AND (s.n_dead_tup::numeric / s.n_live_tup::numeric) >= 0.20
    ) s

    UNION ALL

    SELECT 'MIG-PART-001', 'Partition index coverage review', 'P2',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'partition_index_review=' || cnt,
           'Ensure partitioned parent tables have appropriate indexing strategy'
    FROM (
        WITH pt AS (
            SELECT c.oid
            FROM pg_partitioned_table p
            JOIN pg_class c ON c.oid = p.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM pt
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.oid
              AND i.indisvalid
              AND i.indisready
        )
    ) s

    UNION ALL

    SELECT 'MIG-CONFIG-001', 'Critical configuration failures', 'P1',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'critical_config_fails=' || cnt,
           'Fix FAIL-level configuration parameters before migration takeover'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on', 'true', '1') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on', 'always') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica', 'logical') THEN 1 ELSE 0 END
        )::bigint AS cnt
    ) s

    UNION ALL

    SELECT 'MIG-CONFIG-002', 'Configuration warnings', 'P2',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'config_warns=' || cnt,
           'Tune WARN-level settings and validate with performance test runs'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on', 'true', '1') THEN 0 ELSE 1 END +
            CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) >= 4294967296 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') >= 600 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name = 'checkpoint_completion_target') >= 0.90 THEN 0 ELSE 1 END
        )::bigint AS cnt
    ) s

    UNION ALL

    SELECT 'MIG-APP-001', 'Idle-in-transaction session risk', 'P1',
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'idle_in_tx_over_10m=' || cnt,
           'Fix app transaction boundaries and enforce transaction timeout policies'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND xact_start IS NOT NULL
          AND now() - xact_start > interval '10 minutes'
    ) s
), summary AS (
    SELECT
        count(*)::bigint AS total_checks,
        count(*) FILTER (WHERE status = 'PASS')::bigint AS pass_checks,
        count(*) FILTER (WHERE status = 'WARN')::bigint AS warn_checks,
        count(*) FILTER (WHERE status = 'FAIL')::bigint AS fail_checks,
        count(*) FILTER (WHERE severity = 'P1' AND status IN ('FAIL', 'WARN'))::bigint AS p1_open,
        count(*) FILTER (WHERE severity = 'P2' AND status IN ('FAIL', 'WARN'))::bigint AS p2_open,
        count(*) FILTER (WHERE severity = 'P3' AND status IN ('FAIL', 'WARN'))::bigint AS p3_open,
        count(*) FILTER (WHERE status IN ('FAIL', 'WARN'))::bigint AS open_findings
    FROM findings
)
SELECT
    current_database() AS database_name,
    current_user AS executed_by,
    version() AS postgres_version,
    (SELECT target_schema_regex FROM cfg) AS target_schema_regex,
    (SELECT critical_schema_regex FROM cfg) AS critical_schema_regex,
    CASE
        WHEN summary.p1_open > 0 THEN 'NO_GO'
        WHEN summary.p2_open > 0 OR summary.p3_open > 0 THEN 'CONDITIONAL_GO'
        ELSE 'GO'
    END AS final_decision,
    ((summary.p1_open * 40) + (summary.p2_open * 15) + (summary.p3_open * 5))::bigint AS risk_score,
    summary.total_checks,
    summary.pass_checks,
    summary.warn_checks,
    summary.fail_checks,
    summary.p1_open,
    summary.p2_open,
    summary.p3_open,
    summary.open_findings,
    now() AS generated_at
FROM cfg, summary;

-- 1.3 Main Findings (Action Queue)
WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), findings AS (
    SELECT 'MIG-TYPE-001'::text AS finding_id, 'Schema type consistency: PROBLEM rows'::text AS finding_title, 'P1'::text AS severity,
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END AS status, cnt AS issue_count,
           'type_problem_rows=' || cnt AS evidence,
           'Standardize inconsistent key/business column types before cutover'::text AS recommended_action
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count >= 4
    ) s
    UNION ALL
    SELECT 'MIG-TYPE-002', 'Schema type consistency: REVIEW rows', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'type_review_rows=' || cnt, 'Review and standardize remaining mixed-type columns'
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count BETWEEN 2 AND 3
    ) s
    UNION ALL
    SELECT 'MIG-CAST-001', 'Casting issues: HIGH risk', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_cast_issues=' || cnt, 'Prioritize removing HIGH-risk casts in JOIN/WHERE/business logic'
    FROM (
        WITH defs AS (
            SELECT n.nspname AS schema_name, pg_get_functiondef(p.oid) AS def_text
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND p.prokind IN ('f', 'p')
            UNION ALL
            SELECT n.nspname AS schema_name, pg_get_viewdef(c.oid, true) AS def_text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND c.relkind IN ('v', 'm')
        ), casts AS (
            SELECT trim(m[2]) AS cast_to
            FROM defs d
            CROSS JOIN LATERAL regexp_matches(lower(d.def_text), '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
        )
        SELECT count(*)::bigint AS cnt
        FROM casts
        WHERE cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)'
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-001', 'Indexes needed: FK coverage gaps', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'missing_fk_indexes=' || cnt, 'Create supporting indexes for FK columns before Day-1'
    FROM (
        WITH fk AS (
            SELECT con.conrelid, con.conkey
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM fk
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        )
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-002', 'Unused index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'unused_indexes=' || cnt, 'Review and drop large unused indexes only after workload replay validation'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON i.indexrelid = s.indexrelid
        JOIN cfg ON true
        WHERE s.schemaname !~ '^pg_'
          AND s.schemaname <> 'information_schema'
          AND s.schemaname ~ cfg.target_schema_regex
          AND i.indisprimary = false
          AND i.indisunique = false
          AND s.idx_scan = 0
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-003', 'Duplicate index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'duplicate_index_groups=' || cnt, 'Retain one index per equivalent key/expression/predicate set'
    FROM (
        WITH idx AS (
            SELECT x.indrelid, x.indkey::text AS indkey,
                   COALESCE(pg_get_expr(x.indexprs, x.indrelid), '') AS index_expr,
                   COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS index_pred,
                   x.indisprimary
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM (
            SELECT indrelid, indkey, index_expr, index_pred
            FROM idx
            WHERE indisprimary = false
            GROUP BY indrelid, indkey, index_expr, index_pred
            HAVING count(*) > 1
        ) d
    ) s
    UNION ALL
    SELECT 'MIG-BLOAT-001', 'Table bloat pressure', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_bloat_tables=' || cnt, 'Run VACUUM (ANALYZE) and tune autovacuum thresholds for heavy tables'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_tables s
        JOIN cfg ON true
        WHERE s.schemaname ~ cfg.target_schema_regex
          AND s.n_dead_tup >= 500000
          AND s.n_live_tup > 0
          AND (s.n_dead_tup::numeric / s.n_live_tup::numeric) >= 0.20
    ) s
    UNION ALL
    SELECT 'MIG-PART-001', 'Partition index coverage review', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'partition_index_review=' || cnt, 'Ensure partitioned parent tables have appropriate indexing strategy'
    FROM (
        WITH pt AS (
            SELECT c.oid
            FROM pg_partitioned_table p
            JOIN pg_class c ON c.oid = p.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM pt
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.oid
              AND i.indisvalid
              AND i.indisready
        )
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-001', 'Critical configuration failures', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'critical_config_fails=' || cnt, 'Fix FAIL-level configuration parameters before migration takeover'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on', 'true', '1') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on', 'always') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica', 'logical') THEN 1 ELSE 0 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-002', 'Configuration warnings', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'config_warns=' || cnt, 'Tune WARN-level settings and validate with performance test runs'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on', 'true', '1') THEN 0 ELSE 1 END +
            CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) >= 4294967296 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') >= 600 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name = 'checkpoint_completion_target') >= 0.90 THEN 0 ELSE 1 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-APP-001', 'Idle-in-transaction session risk', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'idle_in_tx_over_10m=' || cnt, 'Fix app transaction boundaries and enforce transaction timeout policies'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND xact_start IS NOT NULL
          AND now() - xact_start > interval '10 minutes'
    ) s
)
SELECT
    finding_id,
    finding_title,
    severity,
    status,
    issue_count,
    evidence,
    recommended_action
FROM findings
ORDER BY
    CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END,
    CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
    finding_id;

-- 2. Schema Type Consistency Report
SELECT '2. Schema Type Consistency Report' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), cols AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS current_type
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE c.relkind IN ('r', 'p')
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
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
    CASE WHEN a.type_count >= 4 THEN 'PROBLEM' WHEN a.type_count >= 2 THEN 'REVIEW' ELSE 'OK' END AS status,
    CASE
        WHEN a.type_count >= 4 THEN 'Directly standardize target type and run cast validation dry-run'
        WHEN a.all_types_used ~ 'character varying' AND a.all_types_used ~ '(bigint|integer|numeric|smallint)' THEN 'Clean data, then migrate to numeric/bigint standard type'
        ELSE 'Review and standardize'
    END AS recommendation
FROM cols c
JOIN agg a ON a.column_name = c.column_name
WHERE a.type_count > 1
ORDER BY a.type_count DESC, a.total_occurrences DESC, c.column_name, c.schema_name, c.table_name;

-- 3. Casting Issues Report
SELECT '3. Casting Issues Report' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), defs AS (
    SELECT n.nspname AS schema_name, 'FUNCTION'::text AS object_type, p.proname AS object_name, pg_get_functiondef(p.oid) AS def_text
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
      AND p.prokind IN ('f', 'p')

    UNION ALL

    SELECT n.nspname AS schema_name, CASE WHEN c.relkind = 'm' THEN 'MVIEW' ELSE 'VIEW' END, c.relname, pg_get_viewdef(c.oid, true)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
      AND c.relkind IN ('v', 'm')
), dc AS (
    SELECT schema_name, object_type, object_name, 'DOUBLE_COLON'::text AS syntax, trim(m[1]) AS expression, trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(lower(d.def_text), '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
), cf AS (
    SELECT schema_name, object_type, object_name, 'CAST_FUNCTION'::text AS syntax, trim(m[1]) AS expression, trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(lower(d.def_text), 'cast\s*[(]\s*([^,)]+?)\s+as\s+([a-z0-9_ ]+)\s*[)]', 'g') AS m
), all_casts AS (
    SELECT * FROM dc
    UNION ALL
    SELECT * FROM cf
), final AS (
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
        CASE WHEN position('.' IN expression) > 0 THEN split_part(expression, '.', 2) ELSE expression END AS column_guess
    FROM all_casts
)
SELECT risk_level, count(*) AS issue_count
FROM final
GROUP BY risk_level
ORDER BY CASE risk_level WHEN 'HIGH' THEN 1 WHEN 'MED' THEN 2 ELSE 3 END;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), defs AS (
    SELECT n.nspname AS schema_name, 'FUNCTION'::text AS object_type, p.proname AS object_name, pg_get_functiondef(p.oid) AS def_text
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
      AND p.prokind IN ('f', 'p')

    UNION ALL

    SELECT n.nspname AS schema_name, CASE WHEN c.relkind = 'm' THEN 'MVIEW' ELSE 'VIEW' END, c.relname, pg_get_viewdef(c.oid, true)
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
      AND c.relkind IN ('v', 'm')
), dc AS (
    SELECT schema_name, object_type, object_name, 'DOUBLE_COLON'::text AS syntax, trim(m[1]) AS expression, trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(lower(d.def_text), '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
), cf AS (
    SELECT schema_name, object_type, object_name, 'CAST_FUNCTION'::text AS syntax, trim(m[1]) AS expression, trim(m[2]) AS cast_to
    FROM defs d
    CROSS JOIN LATERAL regexp_matches(lower(d.def_text), 'cast\s*[(]\s*([^,)]+?)\s+as\s+([a-z0-9_ ]+)\s*[)]', 'g') AS m
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
    CASE WHEN position('.' IN expression) > 0 THEN split_part(expression, '.', 2) ELSE expression END AS column_guess
FROM all_casts
ORDER BY CASE
    WHEN cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)' THEN 1
    WHEN cast_to ~ '(char|character varying|varchar|text)' THEN 2
    ELSE 3
END, schema_name, object_type, object_name;

-- 4. Index Coverage and Needed Indexes
SELECT '4. Index Coverage and Needed Indexes' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), fk AS (
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
                JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                ORDER BY k.ord
            ), ', '
        ) AS fk_columns
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE con.contype = 'f'
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
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
        ) THEN 'YES' ELSE 'NO'
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

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), fk AS (
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
                JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                ORDER BY k.ord
            ), ', '
        ) AS fk_columns
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE con.contype = 'f'
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
)
SELECT
    fk.schema_name,
    fk.table_name,
    fk.fk_name,
    fk.fk_columns,
    'Create btree index on FK columns as leading keys' AS recommendation
FROM fk
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_index i
    WHERE i.indrelid = fk.conrelid
      AND i.indisvalid
      AND i.indisready
      AND i.indnatts >= cardinality(fk.conkey)
      AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
)
ORDER BY fk.schema_name, fk.table_name, fk.fk_name;

-- 5. Unused and Duplicate Indexes
SELECT '5. Unused and Duplicate Indexes' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
)
SELECT
    s.schemaname AS schema_name,
    s.relname AS table_name,
    s.indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    CASE WHEN pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE' ELSE 'MONITOR' END AS status,
    CASE WHEN pg_relation_size(s.indexrelid) >= 1073741824
         THEN 'Validate with workload replay, then drop if truly unused'
         ELSE 'Monitor; may be seasonal or batch-driven index'
    END AS recommendation
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
JOIN cfg ON true
WHERE s.schemaname !~ '^pg_'
  AND s.schemaname <> 'information_schema'
  AND s.schemaname ~ cfg.target_schema_regex
  AND i.indisprimary = false
  AND i.indisunique = false
  AND s.idx_scan = 0
ORDER BY pg_relation_size(s.indexrelid) DESC;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), idx AS (
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
    JOIN pg_class i ON i.oid = x.indexrelid
    JOIN pg_class t ON t.oid = x.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
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

-- 6. Table and Index Bloat Report
SELECT '6. Table and Index Bloat Report' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), tbl AS (
    SELECT
        s.schemaname AS schema_name,
        s.relname AS object_name,
        'TABLE'::text AS object_type,
        pg_size_pretty(pg_total_relation_size(s.relid)) AS total_size,
        s.n_dead_tup::bigint AS dead_tuples,
        round((s.n_dead_tup::numeric * 100.0) / NULLIF((s.n_live_tup + s.n_dead_tup)::numeric, 0), 2) AS dead_pct,
        CASE
            WHEN s.n_dead_tup >= 500000 AND s.n_dead_tup > (s.n_live_tup * 0.20) THEN 'VACUUM_ANALYZE_NOW'
            WHEN s.n_dead_tup >= 100000 AND s.n_dead_tup > (s.n_live_tup * 0.10) THEN 'MONITOR_AND_TUNE_AUTOVACUUM'
            ELSE 'MONITOR'
        END AS recommended_action,
        pg_total_relation_size(s.relid) AS total_bytes
    FROM pg_stat_user_tables s
    JOIN cfg ON true
    WHERE s.schemaname !~ '^pg_'
      AND s.schemaname <> 'information_schema'
      AND s.schemaname ~ cfg.target_schema_regex
), idx AS (
    SELECT
        s.schemaname AS schema_name,
        s.indexrelname AS object_name,
        'INDEX'::text AS object_type,
        pg_size_pretty(pg_relation_size(s.indexrelid)) AS total_size,
        0::bigint AS dead_tuples,
        0::numeric AS dead_pct,
        CASE
            WHEN i.indisvalid = false THEN 'REINDEX_CONCURRENTLY'
            WHEN s.idx_scan = 0 AND pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE'
            WHEN pg_relation_size(s.indexrelid) >= 21474836480 THEN 'REINDEX_CONCURRENTLY'
            ELSE 'MONITOR'
        END AS recommended_action,
        pg_relation_size(s.indexrelid) AS total_bytes
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON i.indexrelid = s.indexrelid
    JOIN cfg ON true
    WHERE s.schemaname !~ '^pg_'
      AND s.schemaname <> 'information_schema'
      AND s.schemaname ~ cfg.target_schema_regex
), all_rows AS (
    SELECT * FROM tbl
    UNION ALL
    SELECT * FROM idx
)
SELECT
    schema_name,
    object_name,
    object_type,
    total_size,
    dead_tuples,
    dead_pct,
    recommended_action
FROM all_rows
ORDER BY total_bytes DESC;

-- 7. Partition Health Report
SELECT '7. Partition Health Report' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), pt AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        p.partstrat,
        c.oid AS relid,
        (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
    FROM pg_partitioned_table p
    JOIN pg_class c ON c.oid = p.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN cfg ON true
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ cfg.target_schema_regex
)
SELECT
    pt.schema_name,
    pt.table_name,
    CASE pt.partstrat WHEN 'r' THEN 'RANGE' WHEN 'l' THEN 'LIST' WHEN 'h' THEN 'HASH' ELSE 'UNKNOWN' END AS partition_strategy,
    pt.partition_count,
    CASE WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.relid
              AND i.indisvalid
              AND i.indisready
         ) THEN 'YES' ELSE 'NO' END AS has_parent_index,
    CASE WHEN EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.relid
              AND i.indisvalid
              AND i.indisready
         ) THEN 'OK' ELSE 'REVIEW' END AS status,
    CASE WHEN EXISTS (
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

-- 8. Configuration Readiness Report
SELECT '8. Configuration Readiness Report' AS section;

SELECT
    parameter_name,
    current_value,
    status,
    recommendation
FROM (
    SELECT
        'autovacuum'::text AS parameter_name,
        setting AS current_value,
        CASE WHEN lower(setting) IN ('on', 'true', '1') THEN 'PASS' ELSE 'FAIL' END AS status,
        'Must be enabled to control dead tuples and xid risk'::text AS recommendation
    FROM pg_settings WHERE name = 'autovacuum'

    UNION ALL

    SELECT
        'track_io_timing',
        setting,
        CASE WHEN lower(setting) IN ('on', 'true', '1') THEN 'PASS' ELSE 'WARN' END,
        'Enable for better I/O diagnostics and migration tuning'
    FROM pg_settings WHERE name = 'track_io_timing'

    UNION ALL

    SELECT
        'max_wal_size',
        setting || unit,
        CASE WHEN pg_size_bytes(setting || unit) >= 4294967296 THEN 'PASS' ELSE 'WARN' END,
        'Recommended baseline >= 4GB for larger migration workloads'
    FROM pg_settings WHERE name = 'max_wal_size'

    UNION ALL

    SELECT
        'checkpoint_timeout',
        setting || 's',
        CASE WHEN setting::int >= 600 THEN 'PASS' ELSE 'WARN' END,
        'Recommended baseline >= 600 seconds to reduce checkpoint pressure'
    FROM pg_settings WHERE name = 'checkpoint_timeout'

    UNION ALL

    SELECT
        'checkpoint_completion_target',
        setting,
        CASE WHEN setting::numeric >= 0.90 THEN 'PASS' ELSE 'WARN' END,
        'Recommended baseline >= 0.90 for smoother checkpoints'
    FROM pg_settings WHERE name = 'checkpoint_completion_target'

    UNION ALL

    SELECT
        'archive_mode',
        setting,
        CASE WHEN setting IN ('on', 'always') THEN 'PASS' ELSE 'FAIL' END,
        'Enable for PITR readiness and rollback safety'
    FROM pg_settings WHERE name = 'archive_mode'

    UNION ALL

    SELECT
        'wal_level',
        setting,
        CASE WHEN setting IN ('replica', 'logical') THEN 'PASS' ELSE 'FAIL' END,
        'Should be replica or logical for HA/replication-ready posture'
    FROM pg_settings WHERE name = 'wal_level'

    UNION ALL

    SELECT
        'pg_stat_statements',
        CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'installed' ELSE 'missing' END,
        CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'PASS' ELSE 'WARN' END,
        'Install for workload regression analysis and SQL tuning evidence'
) s
ORDER BY CASE status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 WHEN 'PASS' THEN 3 ELSE 4 END, parameter_name;

-- 9. Compatibility Matrix and Gate Summary
SELECT '9. Compatibility Matrix and Gate Summary' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), findings AS (
    SELECT 'MIG-TYPE-001'::text AS finding_id, 'Schema type consistency: PROBLEM rows'::text AS finding_title, 'P1'::text AS severity,
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END AS status, cnt AS issue_count,
           'type_problem_rows=' || cnt AS evidence,
           'Standardize inconsistent key/business column types before cutover'::text AS recommended_action
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count >= 4
    ) s
    UNION ALL
    SELECT 'MIG-TYPE-002', 'Schema type consistency: REVIEW rows', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'type_review_rows=' || cnt, 'Review and standardize remaining mixed-type columns'
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count BETWEEN 2 AND 3
    ) s
    UNION ALL
    SELECT 'MIG-CAST-001', 'Casting issues: HIGH risk', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_cast_issues=' || cnt, 'Prioritize removing HIGH-risk casts in JOIN/WHERE/business logic'
    FROM (
        WITH defs AS (
            SELECT n.nspname AS schema_name, pg_get_functiondef(p.oid) AS def_text
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND p.prokind IN ('f', 'p')
            UNION ALL
            SELECT n.nspname AS schema_name, pg_get_viewdef(c.oid, true) AS def_text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND c.relkind IN ('v', 'm')
        ), casts AS (
            SELECT trim(m[2]) AS cast_to
            FROM defs d
            CROSS JOIN LATERAL regexp_matches(lower(d.def_text), '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
        )
        SELECT count(*)::bigint AS cnt
        FROM casts
        WHERE cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)'
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-001', 'Indexes needed: FK coverage gaps', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'missing_fk_indexes=' || cnt, 'Create supporting indexes for FK columns before Day-1'
    FROM (
        WITH fk AS (
            SELECT con.conrelid, con.conkey
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM fk
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        )
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-002', 'Unused index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'unused_indexes=' || cnt, 'Review and drop large unused indexes only after workload replay validation'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON i.indexrelid = s.indexrelid
        JOIN cfg ON true
        WHERE s.schemaname !~ '^pg_'
          AND s.schemaname <> 'information_schema'
          AND s.schemaname ~ cfg.target_schema_regex
          AND i.indisprimary = false
          AND i.indisunique = false
          AND s.idx_scan = 0
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-003', 'Duplicate index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'duplicate_index_groups=' || cnt, 'Retain one index per equivalent key/expression/predicate set'
    FROM (
        WITH idx AS (
            SELECT x.indrelid, x.indkey::text AS indkey,
                   COALESCE(pg_get_expr(x.indexprs, x.indrelid), '') AS index_expr,
                   COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS index_pred,
                   x.indisprimary
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM (
            SELECT indrelid, indkey, index_expr, index_pred
            FROM idx
            WHERE indisprimary = false
            GROUP BY indrelid, indkey, index_expr, index_pred
            HAVING count(*) > 1
        ) d
    ) s
    UNION ALL
    SELECT 'MIG-BLOAT-001', 'Table bloat pressure', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_bloat_tables=' || cnt, 'Run VACUUM (ANALYZE) and tune autovacuum thresholds for heavy tables'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_tables s
        JOIN cfg ON true
        WHERE s.schemaname ~ cfg.target_schema_regex
          AND s.n_dead_tup >= 500000
          AND s.n_live_tup > 0
          AND (s.n_dead_tup::numeric / s.n_live_tup::numeric) >= 0.20
    ) s
    UNION ALL
    SELECT 'MIG-PART-001', 'Partition index coverage review', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'partition_index_review=' || cnt, 'Ensure partitioned parent tables have appropriate indexing strategy'
    FROM (
        WITH pt AS (
            SELECT c.oid
            FROM pg_partitioned_table p
            JOIN pg_class c ON c.oid = p.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM pt
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.oid
              AND i.indisvalid
              AND i.indisready
        )
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-001', 'Critical configuration failures', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'critical_config_fails=' || cnt, 'Fix FAIL-level configuration parameters before migration takeover'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on', 'true', '1') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on', 'always') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica', 'logical') THEN 1 ELSE 0 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-002', 'Configuration warnings', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'config_warns=' || cnt, 'Tune WARN-level settings and validate with performance test runs'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on', 'true', '1') THEN 0 ELSE 1 END +
            CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) >= 4294967296 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') >= 600 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name = 'checkpoint_completion_target') >= 0.90 THEN 0 ELSE 1 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-APP-001', 'Idle-in-transaction session risk', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'idle_in_tx_over_10m=' || cnt, 'Fix app transaction boundaries and enforce transaction timeout policies'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND xact_start IS NOT NULL
          AND now() - xact_start > interval '10 minutes'
    ) s
), summary AS (
    SELECT
        count(*)::bigint AS total_checks,
        count(*) FILTER (WHERE status = 'PASS')::bigint AS pass_checks,
        count(*) FILTER (WHERE status = 'WARN')::bigint AS warn_checks,
        count(*) FILTER (WHERE status = 'FAIL')::bigint AS fail_checks,
        count(*) FILTER (WHERE severity = 'P1' AND status IN ('FAIL', 'WARN'))::bigint AS p1_open,
        count(*) FILTER (WHERE severity = 'P2' AND status IN ('FAIL', 'WARN'))::bigint AS p2_open,
        count(*) FILTER (WHERE severity = 'P3' AND status IN ('FAIL', 'WARN'))::bigint AS p3_open,
        count(*) FILTER (WHERE status IN ('FAIL', 'WARN'))::bigint AS open_findings
    FROM findings
)
SELECT
    finding_id,
    finding_title,
    severity,
    status,
    issue_count,
    evidence,
    recommended_action
FROM findings
ORDER BY CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END, finding_id;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), findings AS (
    SELECT 'MIG-CONFIG-001'::text AS finding_id, 'Critical configuration failures'::text AS finding_title, 'P1'::text AS severity,
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END AS status, cnt AS issue_count,
           'critical_config_fails=' || cnt AS evidence,
           'Fix FAIL-level configuration parameters before migration takeover'::text AS recommended_action
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on', 'true', '1') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on', 'always') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica', 'logical') THEN 1 ELSE 0 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-001', 'Indexes needed: FK coverage gaps', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'missing_fk_indexes=' || cnt, 'Create supporting indexes for FK columns before Day-1'
    FROM (
        WITH fk AS (
            SELECT con.conrelid, con.conkey
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM fk
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        )
    ) s
)
SELECT
    finding_id,
    finding_title,
    status,
    issue_count,
    evidence,
    recommended_action
FROM findings
WHERE status IN ('FAIL', 'WARN')
ORDER BY finding_id;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), reports_json AS (
    SELECT jsonb_build_object(
        'metadata', jsonb_build_object(
            'database_name', current_database(),
            'executed_by', current_user,
            'target_schema_regex', cfg.target_schema_regex,
            'critical_schema_regex', cfg.critical_schema_regex,
            'generated_at', now()
        ),
        'notes', 'GUI pure-select run. Use result tabs as sectioned report output.'
    ) AS j
    FROM cfg
)
SELECT jsonb_pretty(j) AS pg360_json
FROM reports_json;

-- 10. HTML Report Payload (single-row output for pgAdmin/DBeaver export)
SELECT '10. HTML Report Payload' AS section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex,
           '.*'::text AS critical_schema_regex
), findings AS (
    SELECT 'MIG-TYPE-001'::text AS finding_id, 'Schema type consistency: PROBLEM rows'::text AS finding_title, 'P1'::text AS severity,
           CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END AS status, cnt AS issue_count,
           'type_problem_rows=' || cnt AS evidence,
           'Standardize inconsistent key/business column types before cutover'::text AS recommended_action
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count >= 4
    ) s
    UNION ALL
    SELECT 'MIG-TYPE-002', 'Schema type consistency: REVIEW rows', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'type_review_rows=' || cnt, 'Review and standardize remaining mixed-type columns'
    FROM (
        WITH cols AS (
            SELECT a.attname AS column_name, format_type(a.atttypid, a.atttypmod) AS current_type
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        ), agg AS (
            SELECT column_name, count(DISTINCT current_type) AS type_count
            FROM cols
            GROUP BY column_name
        )
        SELECT count(*)::bigint AS cnt FROM agg WHERE type_count BETWEEN 2 AND 3
    ) s
    UNION ALL
    SELECT 'MIG-CAST-001', 'Casting issues: HIGH risk', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_cast_issues=' || cnt, 'Prioritize removing HIGH-risk casts in JOIN/WHERE/business logic'
    FROM (
        WITH defs AS (
            SELECT n.nspname AS schema_name, pg_get_functiondef(p.oid) AS def_text
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND p.prokind IN ('f', 'p')
            UNION ALL
            SELECT n.nspname AS schema_name, pg_get_viewdef(c.oid, true) AS def_text
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND c.relkind IN ('v', 'm')
        ), casts AS (
            SELECT trim(m[2]) AS cast_to
            FROM defs d
            CROSS JOIN LATERAL regexp_matches(lower(d.def_text), '([a-z0-9_$."'']+)\s*::\s*([a-z0-9_ ]+)', 'g') AS m
        )
        SELECT count(*)::bigint AS cnt
        FROM casts
        WHERE cast_to ~ '(smallint|integer|bigint|numeric|decimal|real|double precision|date|timestamp)'
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-001', 'Indexes needed: FK coverage gaps', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'missing_fk_indexes=' || cnt, 'Create supporting indexes for FK columns before Day-1'
    FROM (
        WITH fk AS (
            SELECT con.conrelid, con.conkey
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM fk
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = fk.conrelid
              AND i.indisvalid
              AND i.indisready
              AND i.indnatts >= cardinality(fk.conkey)
              AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
        )
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-002', 'Unused index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'unused_indexes=' || cnt, 'Review and drop large unused indexes only after workload replay validation'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON i.indexrelid = s.indexrelid
        JOIN cfg ON true
        WHERE s.schemaname !~ '^pg_'
          AND s.schemaname <> 'information_schema'
          AND s.schemaname ~ cfg.target_schema_regex
          AND i.indisprimary = false
          AND i.indisunique = false
          AND s.idx_scan = 0
    ) s
    UNION ALL
    SELECT 'MIG-INDEX-003', 'Duplicate index candidates', 'P3', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'duplicate_index_groups=' || cnt, 'Retain one index per equivalent key/expression/predicate set'
    FROM (
        WITH idx AS (
            SELECT x.indrelid, x.indkey::text AS indkey,
                   COALESCE(pg_get_expr(x.indexprs, x.indrelid), '') AS index_expr,
                   COALESCE(pg_get_expr(x.indpred, x.indrelid), '') AS index_pred,
                   x.indisprimary
            FROM pg_index x
            JOIN pg_class t ON t.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM (
            SELECT indrelid, indkey, index_expr, index_pred
            FROM idx
            WHERE indisprimary = false
            GROUP BY indrelid, indkey, index_expr, index_pred
            HAVING count(*) > 1
        ) d
    ) s
    UNION ALL
    SELECT 'MIG-BLOAT-001', 'Table bloat pressure', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'high_bloat_tables=' || cnt, 'Run VACUUM (ANALYZE) and tune autovacuum thresholds for heavy tables'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_user_tables s
        JOIN cfg ON true
        WHERE s.schemaname ~ cfg.target_schema_regex
          AND s.n_dead_tup >= 500000
          AND s.n_live_tup > 0
          AND (s.n_dead_tup::numeric / s.n_live_tup::numeric) >= 0.20
    ) s
    UNION ALL
    SELECT 'MIG-PART-001', 'Partition index coverage review', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'partition_index_review=' || cnt, 'Ensure partitioned parent tables have appropriate indexing strategy'
    FROM (
        WITH pt AS (
            SELECT c.oid
            FROM pg_partitioned_table p
            JOIN pg_class c ON c.oid = p.partrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
        )
        SELECT count(*)::bigint AS cnt
        FROM pt
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_index i
            WHERE i.indrelid = pt.oid
              AND i.indisvalid
              AND i.indisready
        )
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-001', 'Critical configuration failures', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'critical_config_fails=' || cnt, 'Fix FAIL-level configuration parameters before migration takeover'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on', 'true', '1') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on', 'always') THEN 1 ELSE 0 END +
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica', 'logical') THEN 1 ELSE 0 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-CONFIG-002', 'Configuration warnings', 'P2', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'WARN' END, cnt,
           'config_warns=' || cnt, 'Tune WARN-level settings and validate with performance test runs'
    FROM (
        SELECT (
            CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on', 'true', '1') THEN 0 ELSE 1 END +
            CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) >= 4294967296 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') >= 600 THEN 0 ELSE 1 END +
            CASE WHEN (SELECT setting::numeric FROM pg_settings WHERE name = 'checkpoint_completion_target') >= 0.90 THEN 0 ELSE 1 END
        )::bigint AS cnt
    ) s
    UNION ALL
    SELECT 'MIG-APP-001', 'Idle-in-transaction session risk', 'P1', CASE WHEN cnt = 0 THEN 'PASS' ELSE 'FAIL' END, cnt,
           'idle_in_tx_over_10m=' || cnt, 'Fix app transaction boundaries and enforce transaction timeout policies'
    FROM (
        SELECT count(*)::bigint AS cnt
        FROM pg_stat_activity
        WHERE state = 'idle in transaction'
          AND xact_start IS NOT NULL
          AND now() - xact_start > interval '10 minutes'
    ) s
), summary AS (
    SELECT
        count(*)::bigint AS total_checks,
        count(*) FILTER (WHERE status = 'PASS')::bigint AS pass_checks,
        count(*) FILTER (WHERE status = 'WARN')::bigint AS warn_checks,
        count(*) FILTER (WHERE status = 'FAIL')::bigint AS fail_checks,
        count(*) FILTER (WHERE severity = 'P1' AND status IN ('FAIL', 'WARN'))::bigint AS p1_open,
        count(*) FILTER (WHERE severity = 'P2' AND status IN ('FAIL', 'WARN'))::bigint AS p2_open,
        count(*) FILTER (WHERE severity = 'P3' AND status IN ('FAIL', 'WARN'))::bigint AS p3_open,
        count(*) FILTER (WHERE status IN ('FAIL', 'WARN'))::bigint AS open_findings
    FROM findings
), scored AS (
    SELECT
        s.*,
        (s.p1_open * 30 + s.p2_open * 10 + s.p3_open * 3)::bigint AS risk_score,
        CASE
            WHEN s.p1_open > 0 THEN 'NO_GO'
            WHEN s.fail_checks > 0 THEN 'NO_GO'
            WHEN s.warn_checks > 0 THEN 'CONDITIONAL_GO'
            ELSE 'GO'
        END AS final_decision
    FROM summary s
), html_rows AS (
    SELECT string_agg(
        '<tr>' ||
        '<td>' || finding_id || '</td>' ||
        '<td>' || replace(replace(replace(finding_title, '&', '&amp;'), '<', '&lt;'), '>', '&gt;') || '</td>' ||
        '<td>' || severity || '</td>' ||
        '<td>' || status || '</td>' ||
        '<td style="text-align:right;">' || issue_count || '</td>' ||
        '<td>' || replace(replace(replace(evidence, '&', '&amp;'), '<', '&lt;'), '>', '&gt;') || '</td>' ||
        '<td>' || replace(replace(replace(recommended_action, '&', '&amp;'), '<', '&lt;'), '>', '&gt;') || '</td>' ||
        '</tr>',
        E'\n'
        ORDER BY CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, finding_id
    ) AS rows_html
    FROM findings
)
SELECT
    'pg360_gui_v1.html'::text AS suggested_file_name,
    (
        '<!doctype html><html><head><meta charset="utf-8"><title>Oracle to PostgreSQL Migration Report (GUI)</title>' ||
        '<style>body{font-family:Segoe UI,Arial,sans-serif;margin:16px;color:#111;}h1{margin:0 0 8px 0;}h2{margin-top:20px;}table{border-collapse:collapse;width:100%;font-size:12px;}th,td{border:1px solid #cfd8dc;padding:6px;vertical-align:top;}th{background:#eceff1;text-align:left;} .meta{background:#f8f9fb;border:1px solid #dde3e8;padding:10px;margin:12px 0;} .k{font-weight:600;}</style>' ||
        '</head><body>' ||
        '<h1>Oracle to PostgreSQL Migration Report (GUI)</h1>' ||
        '<div class="meta"><div><span class="k">Generated at:</span> ' || now() || '</div>' ||
        '<div><span class="k">Database:</span> ' || current_database() || '</div>' ||
        '<div><span class="k">Executed by:</span> ' || current_user || '</div>' ||
        '<div><span class="k">Target schema regex:</span> ' || (SELECT target_schema_regex FROM cfg) || '</div>' ||
        '<div><span class="k">Critical schema regex:</span> ' || (SELECT critical_schema_regex FROM cfg) || '</div></div>' ||
        '<h2>Gate Summary</h2>' ||
        '<table><tr><th>Final Decision</th><th>Risk Score</th><th>P1 Open</th><th>P2 Open</th><th>P3 Open</th><th>Open Findings</th><th>PASS</th><th>WARN</th><th>FAIL</th></tr>' ||
        '<tr><td>' || scored.final_decision || '</td><td style="text-align:right;">' || scored.risk_score ||
        '</td><td style="text-align:right;">' || scored.p1_open || '</td><td style="text-align:right;">' || scored.p2_open ||
        '</td><td style="text-align:right;">' || scored.p3_open || '</td><td style="text-align:right;">' || scored.open_findings ||
        '</td><td style="text-align:right;">' || scored.pass_checks || '</td><td style="text-align:right;">' || scored.warn_checks ||
        '</td><td style="text-align:right;">' || scored.fail_checks || '</td></tr></table>' ||
        '<h2>Executive Findings</h2>' ||
        '<table><tr><th>Finding ID</th><th>Title</th><th>Severity</th><th>Status</th><th>Issue Count</th><th>Evidence</th><th>Recommended Action</th></tr>' ||
        COALESCE(html_rows.rows_html, '<tr><td colspan="7">No findings generated</td></tr>') ||
        '</table>' ||
        '<p>Source: migration_assessor_v1_gui.sql (pure SELECT mode).</p>' ||
        '</body></html>'
    ) AS html_report
FROM scored, html_rows
LIMIT 1;

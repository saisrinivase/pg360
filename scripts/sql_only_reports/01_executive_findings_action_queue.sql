/* Topics: Executive Findings and Action Queue */

SELECT 'Executive Findings and Action Queue' AS report_section;

WITH cfg AS (
    SELECT '.*'::text AS target_schema_regex
), checks AS (
    SELECT 'Missing FK supporting indexes'::text AS check_name,
           (SELECT count(*)::bigint
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN cfg ON true
            WHERE con.contype = 'f'
              AND n.nspname !~ '^pg_'
              AND n.nspname <> 'information_schema'
              AND n.nspname ~ cfg.target_schema_regex
              AND NOT EXISTS (
                  SELECT 1 FROM pg_index i
                  WHERE i.indrelid = con.conrelid
                    AND i.indisvalid
                    AND i.indisready
                    AND i.indnatts >= cardinality(con.conkey)
                    AND (i.indkey::smallint[])[1:cardinality(con.conkey)] = con.conkey
              )) AS issue_count,
           'P1'::text AS severity,
           'Create btree index on FK columns as leading keys'::text AS recommendation

    UNION ALL

    SELECT 'Critical config failures',
           (SELECT (
                CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') NOT IN ('on','true','1') THEN 1 ELSE 0 END +
                CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'archive_mode') NOT IN ('on','always') THEN 1 ELSE 0 END +
                CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'wal_level') NOT IN ('replica','logical') THEN 1 ELSE 0 END
            )::bigint),
           'P1',
           'Fix autovacuum/archive_mode/wal_level before cutover'

    UNION ALL

    SELECT 'Idle in transaction > 10 minutes',
           (SELECT count(*)::bigint FROM pg_stat_activity WHERE state = 'idle in transaction' AND xact_start IS NOT NULL AND now() - xact_start > interval '10 minutes'),
           'P1',
           'Fix transaction boundaries and enforce timeout controls'

    UNION ALL

    SELECT 'Unused index candidates',
           (SELECT count(*)::bigint
            FROM pg_stat_user_indexes s
            JOIN pg_index i ON i.indexrelid = s.indexrelid
            JOIN cfg ON true
            WHERE s.schemaname !~ '^pg_'
              AND s.schemaname <> 'information_schema'
              AND s.schemaname ~ cfg.target_schema_regex
              AND i.indisprimary = false
              AND i.indisunique = false
              AND s.idx_scan = 0),
           'P3',
           'Validate with workload replay before dropping'
)
SELECT
    check_name,
    severity,
    CASE
        WHEN severity = 'P1' AND issue_count > 0 THEN 'FAIL'
        WHEN severity IN ('P2','P3') AND issue_count > 0 THEN 'WARN'
        ELSE 'PASS'
    END AS status,
    issue_count,
    recommendation
FROM checks
ORDER BY CASE severity WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END, check_name;

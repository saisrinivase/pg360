/* Topics: Connection Pooling Pressure, Authentication Connections */

SELECT 'Connection Pooling Pressure' AS report_section;

SELECT
    count(*)::bigint AS active_connections,
    (SELECT setting::bigint FROM pg_settings WHERE name = 'max_connections') AS max_connections,
    round((count(*)::numeric * 100.0) / nullif((SELECT setting::numeric FROM pg_settings WHERE name = 'max_connections'), 0), 2) AS usage_pct,
    CASE
        WHEN round((count(*)::numeric * 100.0) / nullif((SELECT setting::numeric FROM pg_settings WHERE name = 'max_connections'), 0), 2) >= 95 THEN 'FAIL'
        WHEN round((count(*)::numeric * 100.0) / nullif((SELECT setting::numeric FROM pg_settings WHERE name = 'max_connections'), 0), 2) >= 85 THEN 'WARN'
        ELSE 'PASS'
    END AS status
FROM pg_stat_activity;

SELECT 'Authentication Connections' AS report_section;

SELECT
    COALESCE(application_name, '(null)') AS application_name,
    usename,
    client_addr,
    state,
    count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY COALESCE(application_name, '(null)'), usename, client_addr, state
ORDER BY sessions DESC, application_name, usename;

/* Topics: Activity and Waits, Locks and Blocking, Long Running Transactions Detail,
   Lock Conflict Matrix, Wait Event Classification Live */

SELECT 'Activity and Waits' AS report_section;

SELECT
    state,
    wait_event_type,
    wait_event,
    count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY state, wait_event_type, wait_event
ORDER BY sessions DESC, state, wait_event_type, wait_event;

SELECT 'Locks and Blocking' AS report_section;

SELECT
    a.pid AS blocked_pid,
    a.usename AS blocked_user,
    a.application_name AS blocked_app,
    a.state AS blocked_state,
    pg_blocking_pids(a.pid) AS blocking_pids,
    now() - a.query_start AS blocked_duration,
    left(a.query, 200) AS blocked_query
FROM pg_stat_activity a
WHERE cardinality(pg_blocking_pids(a.pid)) > 0
ORDER BY blocked_duration DESC;

SELECT 'Long Running Transactions Detail' AS report_section;

SELECT
    pid,
    usename,
    application_name,
    client_addr,
    state,
    xact_start,
    now() - xact_start AS tx_age,
    left(query, 250) AS query_snippet
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY tx_age DESC
LIMIT 200;

SELECT 'Lock Conflict Matrix' AS report_section;

SELECT
    locktype,
    mode,
    granted,
    count(*)::bigint AS lock_count
FROM pg_locks
GROUP BY locktype, mode, granted
ORDER BY lock_count DESC, locktype, mode, granted;

SELECT 'Wait Event Classification Live' AS report_section;

SELECT
    COALESCE(wait_event_type, 'NONE') AS wait_event_type,
    COALESCE(wait_event, 'NONE') AS wait_event,
    count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY COALESCE(wait_event_type, 'NONE'), COALESCE(wait_event, 'NONE')
ORDER BY sessions DESC, wait_event_type, wait_event;

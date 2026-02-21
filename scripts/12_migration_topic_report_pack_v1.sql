/*
Purpose: Generate PG360 report pack:
  - Core migration assessor (main + 9 subreports)
  - Topic deep-dive pack (main + 40 subreports)
Tooling: psql (uses psql meta-commands for file output).
Usage:
  psql "host=<host> port=<port> dbname=<db> user=<user>" \
    -v target_schema_regex='.*' \
    -f scripts/12_migration_topic_report_pack_v1.sql
*/

\set ON_ERROR_STOP on
\pset pager off
\pset border 1
\pset footer off
\pset format html

\if :{?target_schema_regex}
\else
\set target_schema_regex '.*'
\endif

\! mkdir -p reports

\set main_file 'reports/pg360_topics_main.html'
\set r01 'reports/pg360_topic_01_connection_pooling_pressure.html'
\set r02 'reports/pg360_topic_02_logical_replication_slots_publications.html'
\set r03 'reports/pg360_topic_03_instance_configuration.html'
\set r04 'reports/pg360_topic_04_activity_and_waits.html'
\set r05 'reports/pg360_topic_05_locks_and_blocking.html'
\set r06 'reports/pg360_topic_06_database_and_io_health.html'
\set r07 'reports/pg360_topic_07_table_and_index_health.html'
\set r08 'reports/pg360_topic_08_bloat_and_xid_risk.html'
\set r09 'reports/pg360_topic_09_replication_health.html'
\set r10 'reports/pg360_topic_10_executive_findings_action_queue.html'
\set r11 'reports/pg360_topic_11_sql_hotspots_pg_stat_statements.html'
\set r12 'reports/pg360_topic_12_maintenance_progress.html'
\set r13 'reports/pg360_topic_13_diagnostic_readiness.html'
\set r14 'reports/pg360_topic_14_io_profile_pg_stat_io.html'
\set r15 'reports/pg360_topic_15_security_roles_privileges.html'
\set r16 'reports/pg360_topic_16_authentication_connections.html'
\set r17 'reports/pg360_topic_17_schema_object_inventory.html'
\set r18 'reports/pg360_topic_18_tablespace_storage_layout.html'
\set r19 'reports/pg360_topic_19_top_objects_by_size.html'
\set r20 'reports/pg360_topic_20_partitioning_inventory.html'
\set r21 'reports/pg360_topic_21_constraint_fk_health.html'
\set r22 'reports/pg360_topic_22_index_bloat_usage_patterns.html'
\set r23 'reports/pg360_topic_23_toast_large_values.html'
\set r24 'reports/pg360_topic_24_long_running_transactions_detail.html'
\set r25 'reports/pg360_topic_25_wal_checkpoint_pressure.html'
\set r26 'reports/pg360_topic_26_autovacuum_worker_thresholds.html'
\set r27 'reports/pg360_topic_27_statistics_quality_analyze_health.html'
\set r28 'reports/pg360_topic_28_table_growth_churn_hotspots.html'
\set r29 'reports/pg360_topic_29_sequence_health.html'
\set r30 'reports/pg360_topic_30_materialized_foreign_unlogged_inventory.html'
\set r31 'reports/pg360_topic_31_function_trigger_profile.html'
\set r32 'reports/pg360_topic_32_extension_fdw_runtime_audit.html'
\set r33 'reports/pg360_topic_33_lock_conflict_matrix.html'
\set r34 'reports/pg360_topic_34_partition_maintenance_risk.html'
\set r35 'reports/pg360_topic_35_settings_antipatterns.html'
\set r36 'reports/pg360_topic_36_plan_cache_prepared_advisory_locks.html'
\set r37 'reports/pg360_topic_37_wait_event_classification_live.html'
\set r38 'reports/pg360_topic_38_xid_multixact_wraparound_watch.html'
\set r39 'reports/pg360_topic_39_checkpoint_wal_rate_estimates.html'
\set r40 'reports/pg360_topic_40_database_growth_objects_90d.html'

-- Ensure core assessor pack (main + 9 subreports) is generated in the same run
\set report_file 'reports/pg360_report.html'
\set report_01_file 'reports/pg360_01_type_consistency.html'
\set report_02_file 'reports/pg360_02_casting_issues.html'
\set report_03_file 'reports/pg360_03_indexes_needed.html'
\set report_04_file 'reports/pg360_04_unused_duplicate_indexes.html'
\set report_05_file 'reports/pg360_05_bloat_report.html'
\set report_06_file 'reports/pg360_06_partition_health.html'
\set report_07_file 'reports/pg360_07_config_readiness.html'
\set report_08_file 'reports/pg360_08_compatibility_matrix.html'
\set gate_html_file 'reports/pg360_09_gate_summary.html'
\set json_file 'reports/migration_assessment_v1.json'
\set gate_output_file 'reports/migration_gate_v1.txt'
\set enforce_exit false
\ir 10_migration_assessor_v1.sql
\pset format html
\pset footer off

-- Main index
\o :main_file
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>PG360 Migration Readiness Command Center</title>'
\qecho '<style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:24px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 8px 0;}h2{color:#245580;margin:18px 0 8px 0;padding:8px 10px;background:#f1f6fc;border:1px solid #d8e4f2;border-radius:8px;}ol,ul{line-height:1.6;margin:8px 0 0 20px;}li{margin:6px 0;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}.meta{background:#fff;border:1px solid #d8e4f2;border-radius:10px;padding:10px 12px;box-shadow:0 1px 2px rgba(14,39,72,0.06);}table{border-collapse:collapse;width:100%;background:#fff;margin:8px 0 10px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;}tr:nth-child(even) td{background:#f9fcff;}</style></head><body>'
\qecho '<h1>PG360 Migration Readiness Command Center</h1>'
\qecho '<div class="meta">'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at, :'target_schema_regex'::text AS target_schema_regex;
\qecho '</div>'
\qecho '<h2>A. Core Migration Checks (9 Reports)</h2>'
\qecho '<ol>'
\qecho '<li><a href="pg360_01_type_consistency.html">Schema Type Consistency Report</a></li>'
\qecho '<li><a href="pg360_02_casting_issues.html">Casting Issues Report</a></li>'
\qecho '<li><a href="pg360_03_indexes_needed.html">Indexes Needed Report (FK Coverage)</a></li>'
\qecho '<li><a href="pg360_04_unused_duplicate_indexes.html">Unused and Duplicate Indexes Report</a></li>'
\qecho '<li><a href="pg360_05_bloat_report.html">Table and Index Bloat Report</a></li>'
\qecho '<li><a href="pg360_06_partition_health.html">Partition Health Report</a></li>'
\qecho '<li><a href="pg360_07_config_readiness.html">Configuration Readiness Report</a></li>'
\qecho '<li><a href="pg360_08_compatibility_matrix.html">Compatibility Matrix Report</a></li>'
\qecho '<li><a href="pg360_09_gate_summary.html">Gate Summary Report</a></li>'
\qecho '</ol>'
\qecho '<h2>B. Operational Deep-Dive Topics (40 Reports)</h2>'
\qecho '<ol>'
\qecho '<li><a href="pg360_topic_01_connection_pooling_pressure.html">Connection Pooling Pressure</a></li>'
\qecho '<li><a href="pg360_topic_02_logical_replication_slots_publications.html">Logical Replication Slots Publications</a></li>'
\qecho '<li><a href="pg360_topic_03_instance_configuration.html">Instance Configuration</a></li>'
\qecho '<li><a href="pg360_topic_04_activity_and_waits.html">Activity and Waits</a></li>'
\qecho '<li><a href="pg360_topic_05_locks_and_blocking.html">Locks and Blocking</a></li>'
\qecho '<li><a href="pg360_topic_06_database_and_io_health.html">Database and IO Health</a></li>'
\qecho '<li><a href="pg360_topic_07_table_and_index_health.html">Table and Index Health</a></li>'
\qecho '<li><a href="pg360_topic_08_bloat_and_xid_risk.html">Bloat and XID Risk</a></li>'
\qecho '<li><a href="pg360_topic_09_replication_health.html">Replication Health</a></li>'
\qecho '<li><a href="pg360_topic_10_executive_findings_action_queue.html">Executive Findings and Action Queue</a></li>'
\qecho '<li><a href="pg360_topic_11_sql_hotspots_pg_stat_statements.html">SQL Hotspots (pg_stat_statements)</a></li>'
\qecho '<li><a href="pg360_topic_12_maintenance_progress.html">Maintenance Progress</a></li>'
\qecho '<li><a href="pg360_topic_13_diagnostic_readiness.html">Diagnostic Readiness</a></li>'
\qecho '<li><a href="pg360_topic_14_io_profile_pg_stat_io.html">IO Profile (pg_stat_io)</a></li>'
\qecho '<li><a href="pg360_topic_15_security_roles_privileges.html">Security Roles Privileges</a></li>'
\qecho '<li><a href="pg360_topic_16_authentication_connections.html">Authentication Connections</a></li>'
\qecho '<li><a href="pg360_topic_17_schema_object_inventory.html">Schema Object Inventory</a></li>'
\qecho '<li><a href="pg360_topic_18_tablespace_storage_layout.html">Tablespace Storage Layout</a></li>'
\qecho '<li><a href="pg360_topic_19_top_objects_by_size.html">Top Objects By Size</a></li>'
\qecho '<li><a href="pg360_topic_20_partitioning_inventory.html">Partitioning Inventory</a></li>'
\qecho '<li><a href="pg360_topic_21_constraint_fk_health.html">Constraint Fk Health</a></li>'
\qecho '<li><a href="pg360_topic_22_index_bloat_usage_patterns.html">Index Bloat Usage Patterns</a></li>'
\qecho '<li><a href="pg360_topic_23_toast_large_values.html">Toast Large Values</a></li>'
\qecho '<li><a href="pg360_topic_24_long_running_transactions_detail.html">Long Running Transactions Detail</a></li>'
\qecho '<li><a href="pg360_topic_25_wal_checkpoint_pressure.html">Wal Checkpoint Pressure</a></li>'
\qecho '<li><a href="pg360_topic_26_autovacuum_worker_thresholds.html">Autovacuum Worker Thresholds</a></li>'
\qecho '<li><a href="pg360_topic_27_statistics_quality_analyze_health.html">Statistics Quality Analyze Health</a></li>'
\qecho '<li><a href="pg360_topic_28_table_growth_churn_hotspots.html">Table Growth Churn Hotspots</a></li>'
\qecho '<li><a href="pg360_topic_29_sequence_health.html">Sequence Health</a></li>'
\qecho '<li><a href="pg360_topic_30_materialized_foreign_unlogged_inventory.html">Materialized Foreign Unlogged Inventory</a></li>'
\qecho '<li><a href="pg360_topic_31_function_trigger_profile.html">Function Trigger Profile</a></li>'
\qecho '<li><a href="pg360_topic_32_extension_fdw_runtime_audit.html">Extension Fdw Runtime Audit</a></li>'
\qecho '<li><a href="pg360_topic_33_lock_conflict_matrix.html">Lock Conflict Matrix</a></li>'
\qecho '<li><a href="pg360_topic_34_partition_maintenance_risk.html">Partition Maintenance Risk</a></li>'
\qecho '<li><a href="pg360_topic_35_settings_antipatterns.html">Settings Antipatterns</a></li>'
\qecho '<li><a href="pg360_topic_36_plan_cache_prepared_advisory_locks.html">Plan Cache Prepared And Advisory Locks</a></li>'
\qecho '<li><a href="pg360_topic_37_wait_event_classification_live.html">Wait Event Classification Live</a></li>'
\qecho '<li><a href="pg360_topic_38_xid_multixact_wraparound_watch.html">Xid Multixact Wraparound Watch</a></li>'
\qecho '<li><a href="pg360_topic_39_checkpoint_wal_rate_estimates.html">Checkpoint Wal Rate Estimates</a></li>'
\qecho '<li><a href="pg360_topic_40_database_growth_objects_90d.html">Database Growth Objects 90D</a></li>'
\qecho '</ol>'
\qecho '</body></html>'
\o

-- 01 Connection Pooling Pressure
\o :r01
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Connection Pooling Pressure</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Connection Pooling Pressure</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
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
\qecho '</body></html>'
\o

-- 02 Logical Replication Slots Publications
\o :r02
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Logical Replication Slots Publications</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Logical Replication Slots Publications</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT slot_name, plugin, slot_type, database, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
ORDER BY slot_name;
SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete, pubtruncate
FROM pg_publication
ORDER BY pubname;
\qecho '</body></html>'
\o

-- 03 Instance Configuration
\o :r03
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Instance Configuration</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Instance Configuration</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT name, setting, unit, source, pending_restart
FROM pg_settings
WHERE name IN (
    'max_connections',
    'shared_buffers',
    'work_mem',
    'maintenance_work_mem',
    'effective_cache_size',
    'autovacuum',
    'max_wal_size',
    'checkpoint_timeout',
    'checkpoint_completion_target',
    'wal_level',
    'archive_mode',
    'track_io_timing'
)
ORDER BY name;
\qecho '</body></html>'
\o

-- 04 Activity and Waits
\o :r04
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Activity and Waits</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Activity and Waits</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT state, wait_event_type, wait_event, count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY state, wait_event_type, wait_event
ORDER BY sessions DESC, state, wait_event_type, wait_event;
\qecho '</body></html>'
\o

-- 05 Locks and Blocking
\o :r05
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Locks and Blocking</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Locks and Blocking</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
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
\qecho '</body></html>'
\o

-- 06 Database and IO Health
\o :r06
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Database and IO Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Database and IO Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT datname, numbackends, xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks
FROM pg_stat_database
WHERE datname = current_database();
\qecho '</body></html>'
\o

-- 07 Table and Index Health
\o :r07
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Table and Index Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Table and Index Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    s.schemaname,
    s.relname,
    s.seq_scan,
    s.idx_scan,
    s.n_live_tup,
    s.n_dead_tup,
    s.vacuum_count,
    s.autovacuum_count,
    s.analyze_count,
    s.autoanalyze_count
FROM pg_stat_user_tables s
ORDER BY s.n_live_tup DESC
LIMIT 300;
\qecho '</body></html>'
\o

-- 08 Bloat and XID Risk
\o :r08
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Bloat and XID Risk</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Bloat and XID Risk</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    s.schemaname,
    s.relname,
    s.n_live_tup,
    s.n_dead_tup,
    round((s.n_dead_tup::numeric * 100.0) / nullif((s.n_live_tup + s.n_dead_tup)::numeric, 0), 2) AS dead_pct,
    age(c.relfrozenxid) AS relfrozenxid_age
FROM pg_stat_user_tables s
JOIN pg_class c ON c.oid = s.relid
ORDER BY dead_pct DESC NULLS LAST, relfrozenxid_age DESC
LIMIT 300;
\qecho '</body></html>'
\o

-- 09 Replication Health
\o :r09
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Replication Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Replication Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    pid,
    application_name,
    client_addr,
    state,
    sync_state,
    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS replay_lag_bytes,
    sent_lsn,
    write_lsn,
    flush_lsn,
    replay_lsn
FROM pg_stat_replication
ORDER BY replay_lag_bytes DESC NULLS LAST;
\qecho '</body></html>'
\o

-- 10 Executive Findings and Action Queue
\o :r10
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Executive Findings and Action Queue</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Executive Findings and Action Queue</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at, :'target_schema_regex'::text AS target_schema_regex;
WITH cfg AS (
    SELECT :'target_schema_regex'::text AS target_schema_regex
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
\qecho '</body></html>'
\o

-- 11 SQL Hotspots (pg_stat_statements)
\o :r11
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>SQL Hotspots (pg_stat_statements)</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>SQL Hotspots (pg_stat_statements)</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    current_setting('server_version_num')::int AS server_version_num,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'INSTALLED' ELSE 'NOT_INSTALLED' END AS pg_stat_statements_status,
    CASE
        WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements')
            THEN 'Extension detected. Use extension-level hotspot query if enabled in environment.'
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
\qecho '</body></html>'
\o

-- 12 Maintenance Progress
\o :r12
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Maintenance Progress</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Maintenance Progress</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT pid, datname, relid::regclass AS relation, phase
FROM pg_stat_progress_vacuum
ORDER BY pid;
\qecho '</body></html>'
\o

-- 13 Diagnostic Readiness
\o :r13
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Diagnostic Readiness</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Diagnostic Readiness</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    'pg_stat_statements'::text AS check_name,
    CASE WHEN EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN 'PASS' ELSE 'WARN' END AS status,
    'Install extension for SQL hotspot and regression diagnostics'::text AS recommendation
UNION ALL
SELECT
    'track_io_timing',
    CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'track_io_timing') IN ('on','true','1') THEN 'PASS' ELSE 'WARN' END,
    'Enable track_io_timing for precise I/O attribution';
\qecho '</body></html>'
\o

-- 14 IO Profile (pg_stat_io)
\o :r14
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>IO Profile (pg_stat_io)</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>IO Profile (pg_stat_io)</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    schemaname,
    relname,
    (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read) AS blocks_read,
    (heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit) AS blocks_hit,
    CASE
        WHEN (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read +
              heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit) > 0
        THEN round(
            (heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit)::numeric * 100.0 /
            (heap_blks_read + idx_blks_read + toast_blks_read + tidx_blks_read +
             heap_blks_hit + idx_blks_hit + toast_blks_hit + tidx_blks_hit)::numeric, 2
        )
        ELSE NULL
    END AS cache_hit_pct
FROM pg_statio_user_tables
ORDER BY blocks_read DESC NULLS LAST
LIMIT 200;
SELECT
    current_setting('server_version_num')::int AS server_version_num,
    CASE WHEN to_regclass('pg_stat_io') IS NULL THEN 'NO' ELSE 'YES' END AS pg_stat_io_available,
    CASE
        WHEN to_regclass('pg_stat_io') IS NULL THEN 'Using pg_statio_user_tables fallback'
        ELSE 'pg_stat_io available for deeper object/context I/O split'
    END AS note;
\qecho '</body></html>'
\o

-- 15 Security Roles Privileges
\o :r15
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Security Roles Privileges</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Security Roles Privileges</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT rolname, rolsuper, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls
FROM pg_roles
ORDER BY rolsuper DESC, rolname;
SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE table_schema !~ '^pg_'
  AND table_schema <> 'information_schema'
ORDER BY grantee, table_schema, table_name, privilege_type
LIMIT 2000;
\qecho '</body></html>'
\o

-- 16 Authentication Connections
\o :r16
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Authentication Connections</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Authentication Connections</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    COALESCE(application_name, '(null)') AS application_name,
    usename,
    client_addr,
    state,
    count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY COALESCE(application_name, '(null)'), usename, client_addr, state
ORDER BY sessions DESC, application_name, usename;
\qecho '</body></html>'
\o

-- 17 Schema Object Inventory
\o :r17
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Schema Object Inventory</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Schema Object Inventory</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    c.relkind,
    count(*)::bigint AS object_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
GROUP BY n.nspname, c.relkind
ORDER BY n.nspname, c.relkind;
\qecho '</body></html>'
\o

-- 18 Tablespace Storage Layout
\o :r18
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Tablespace Storage Layout</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Tablespace Storage Layout</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    spcname AS tablespace_name,
    pg_size_pretty(pg_tablespace_size(oid)) AS size,
    pg_tablespace_location(oid) AS location
FROM pg_tablespace
ORDER BY pg_tablespace_size(oid) DESC;
\qecho '</body></html>'
\o

-- 19 Top Objects By Size
\o :r19
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Top Objects By Size</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Top Objects By Size</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    c.relname AS object_name,
    c.relkind,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
  AND c.relkind IN ('r','p','m','i')
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 300;
\qecho '</body></html>'
\o

-- 20 Partitioning Inventory
\o :r20
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Partitioning Inventory</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Partitioning Inventory</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE p.partstrat WHEN 'r' THEN 'RANGE' WHEN 'l' THEN 'LIST' WHEN 'h' THEN 'HASH' ELSE 'UNKNOWN' END AS partition_strategy,
    (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
FROM pg_partitioned_table p
JOIN pg_class c ON c.oid = p.partrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
ORDER BY schema_name, table_name;
\qecho '</body></html>'
\o

-- 21 Constraint Fk Health
\o :r21
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Constraint Fk Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Constraint Fk Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
WITH fk AS (
    SELECT
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
    WHERE con.contype = 'f'
      AND n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
)
SELECT
    schema_name,
    table_name,
    fk_name,
    fk_columns,
    CASE WHEN EXISTS (
        SELECT 1
        FROM pg_index i
        WHERE i.indrelid = fk.conrelid
          AND i.indisvalid
          AND i.indisready
          AND i.indnatts >= cardinality(fk.conkey)
          AND (i.indkey::smallint[])[1:cardinality(fk.conkey)] = fk.conkey
    ) THEN 'YES' ELSE 'NO' END AS index_exists
FROM fk
ORDER BY schema_name, table_name, fk_name;
\qecho '</body></html>'
\o

-- 22 Index Bloat Usage Patterns
\o :r22
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Index Bloat Usage Patterns</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Index Bloat Usage Patterns</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    s.schemaname,
    s.relname,
    s.indexrelname,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS index_size,
    s.idx_scan,
    CASE
        WHEN s.idx_scan = 0 AND pg_relation_size(s.indexrelid) >= 1073741824 THEN 'REVIEW_DROP_CANDIDATE'
        WHEN pg_relation_size(s.indexrelid) >= 21474836480 THEN 'REINDEX_REVIEW'
        ELSE 'MONITOR'
    END AS action_hint
FROM pg_stat_user_indexes s
ORDER BY pg_relation_size(s.indexrelid) DESC
LIMIT 500;
\qecho '</body></html>'
\o

-- 23 Toast Large Values
\o :r23
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Toast Large Values</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Toast Large Values</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    t.relname AS toast_table,
    pg_size_pretty(pg_total_relation_size(t.oid)) AS toast_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_class t ON t.oid = c.reltoastrelid
WHERE c.reltoastrelid <> 0
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY pg_total_relation_size(t.oid) DESC
LIMIT 300;
\qecho '</body></html>'
\o

-- 24 Long Running Transactions Detail
\o :r24
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Long Running Transactions Detail</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Long Running Transactions Detail</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
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
\qecho '</body></html>'
\o

-- 25 Wal Checkpoint Pressure
\o :r25
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Wal Checkpoint Pressure</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Wal Checkpoint Pressure</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    name,
    setting,
    unit,
    source
FROM pg_settings
WHERE name IN (
    'checkpoint_timeout',
    'checkpoint_completion_target',
    'max_wal_size',
    'min_wal_size',
    'wal_level',
    'wal_buffers',
    'wal_writer_delay'
)
ORDER BY name;
\qecho '</body></html>'
\o

-- 26 Autovacuum Worker Thresholds
\o :r26
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Autovacuum Worker Thresholds</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Autovacuum Worker Thresholds</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT name, setting, unit
FROM pg_settings
WHERE name IN (
    'autovacuum',
    'autovacuum_max_workers',
    'autovacuum_naptime',
    'autovacuum_vacuum_threshold',
    'autovacuum_vacuum_scale_factor',
    'autovacuum_analyze_threshold',
    'autovacuum_analyze_scale_factor'
)
ORDER BY name;
\qecho '</body></html>'
\o

-- 27 Statistics Quality Analyze Health
\o :r27
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Statistics Quality Analyze Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Statistics Quality Analyze Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
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
\qecho '</body></html>'
\o

-- 28 Table Growth Churn Hotspots
\o :r28
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Table Growth Churn Hotspots</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Table Growth Churn Hotspots</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    schemaname,
    relname,
    n_tup_ins,
    n_tup_upd,
    n_tup_del,
    n_mod_since_analyze,
    n_live_tup,
    n_dead_tup
FROM pg_stat_user_tables
ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC
LIMIT 300;
\qecho '</body></html>'
\o

-- 29 Sequence Health
\o :r29
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Sequence Health</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Sequence Health</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    schemaname,
    sequencename,
    data_type,
    start_value,
    min_value,
    max_value,
    increment_by,
    cycle
FROM pg_sequences
WHERE schemaname !~ '^pg_'
  AND schemaname <> 'information_schema'
ORDER BY schemaname, sequencename;
\qecho '</body></html>'
\o

-- 30 Materialized Foreign Unlogged Inventory
\o :r30
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Materialized Foreign Unlogged Inventory</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Materialized Foreign Unlogged Inventory</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    c.relname AS object_name,
    c.relkind,
    c.relpersistence,
    CASE c.relkind WHEN 'm' THEN 'MATERIALIZED_VIEW' WHEN 'f' THEN 'FOREIGN_TABLE' ELSE 'OTHER' END AS object_type
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
  AND (c.relkind IN ('m','f') OR c.relpersistence = 'u')
ORDER BY schema_name, object_type, object_name;
\qecho '</body></html>'
\o

-- 31 Function Trigger Profile
\o :r31
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Function Trigger Profile</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Function Trigger Profile</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    n.nspname AS schema_name,
    count(*) FILTER (WHERE p.prokind = 'f')::bigint AS function_count,
    count(*) FILTER (WHERE p.prokind = 'p')::bigint AS procedure_count
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
GROUP BY n.nspname
ORDER BY n.nspname;
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    t.tgname AS trigger_name,
    t.tgenabled,
    pg_get_triggerdef(t.oid, true) AS trigger_def
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE NOT t.tgisinternal
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY n.nspname, c.relname, t.tgname
LIMIT 1000;
\qecho '</body></html>'
\o

-- 32 Extension Fdw Runtime Audit
\o :r32
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Extension Fdw Runtime Audit</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Extension Fdw Runtime Audit</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT extname, extversion, n.nspname AS extension_schema
FROM pg_extension e
JOIN pg_namespace n ON n.oid = e.extnamespace
ORDER BY extname;
SELECT fdwname, fdwhandler::regproc AS handler, fdwvalidator::regproc AS validator, fdwacl
FROM pg_foreign_data_wrapper
ORDER BY fdwname;
SELECT s.srvname, f.fdwname, s.srvtype, s.srvversion, s.srvoptions
FROM pg_foreign_server s
JOIN pg_foreign_data_wrapper f ON f.oid = s.srvfdw
ORDER BY s.srvname;
\qecho '</body></html>'
\o

-- 33 Lock Conflict Matrix
\o :r33
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Lock Conflict Matrix</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Lock Conflict Matrix</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT locktype, mode, granted, count(*)::bigint AS lock_count
FROM pg_locks
GROUP BY locktype, mode, granted
ORDER BY lock_count DESC, locktype, mode, granted;
\qecho '</body></html>'
\o

-- 34 Partition Maintenance Risk
\o :r34
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Partition Maintenance Risk</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Partition Maintenance Risk</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
WITH pt AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        c.oid AS relid,
        (SELECT count(*) FROM pg_inherits i WHERE i.inhparent = c.oid) AS partition_count
    FROM pg_partitioned_table p
    JOIN pg_class c ON c.oid = p.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname !~ '^pg_'
      AND n.nspname <> 'information_schema'
      AND n.nspname ~ :'target_schema_regex'
)
SELECT
    schema_name,
    table_name,
    partition_count,
    CASE WHEN partition_count > 1000 THEN 'HIGH' WHEN partition_count > 200 THEN 'MEDIUM' ELSE 'LOW' END AS maintenance_risk,
    CASE
        WHEN NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = relid AND i.indisvalid AND i.indisready) THEN 'NO_PARENT_INDEX'
        ELSE 'OK'
    END AS index_maintenance_state
FROM pt
ORDER BY partition_count DESC, schema_name, table_name;
\qecho '</body></html>'
\o

-- 35 Settings Antipatterns
\o :r35
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Settings Antipatterns</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Settings Antipatterns</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    'autovacuum_off'::text AS antipattern,
    CASE WHEN (SELECT setting FROM pg_settings WHERE name = 'autovacuum') IN ('on','true','1') THEN 'NO' ELSE 'YES' END AS present,
    'Must be OFF only in exceptional controlled scenarios'::text AS impact
UNION ALL
SELECT
    'low_max_wal_size',
    CASE WHEN pg_size_bytes((SELECT setting || unit FROM pg_settings WHERE name = 'max_wal_size')) < 4294967296 THEN 'YES' ELSE 'NO' END,
    'May increase checkpoint pressure on write-heavy systems'
UNION ALL
SELECT
    'low_checkpoint_timeout',
    CASE WHEN (SELECT setting::int FROM pg_settings WHERE name = 'checkpoint_timeout') < 600 THEN 'YES' ELSE 'NO' END,
    'Frequent checkpoints can increase I/O spikes';
\qecho '</body></html>'
\o

-- 36 Plan Cache Prepared And Advisory Locks
\o :r36
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Plan Cache Prepared And Advisory Locks</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Plan Cache Prepared And Advisory Locks</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    (SELECT count(*)::bigint FROM pg_prepared_statements) AS prepared_statement_count,
    (SELECT count(*)::bigint FROM pg_locks WHERE locktype = 'advisory') AS advisory_lock_count,
    (SELECT count(*)::bigint FROM pg_locks WHERE locktype = 'advisory' AND granted = false) AS advisory_lock_waiters;
\qecho '</body></html>'
\o

-- 37 Wait Event Classification Live
\o :r37
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Wait Event Classification Live</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Wait Event Classification Live</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    COALESCE(wait_event_type, 'NONE') AS wait_event_type,
    COALESCE(wait_event, 'NONE') AS wait_event,
    count(*)::bigint AS sessions
FROM pg_stat_activity
GROUP BY COALESCE(wait_event_type, 'NONE'), COALESCE(wait_event, 'NONE')
ORDER BY sessions DESC, wait_event_type, wait_event;
\qecho '</body></html>'
\o

-- 38 Xid Multixact Wraparound Watch
\o :r38
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Xid Multixact Wraparound Watch</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Xid Multixact Wraparound Watch</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    datname,
    age(datfrozenxid) AS datfrozenxid_age,
    mxid_age(datminmxid) AS datminmxid_age
FROM pg_database
ORDER BY datfrozenxid_age DESC, datminmxid_age DESC;
\qecho '</body></html>'
\o

-- 39 Checkpoint Wal Rate Estimates
\o :r39
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Checkpoint Wal Rate Estimates</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Checkpoint Wal Rate Estimates</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    current_setting('server_version_num')::int AS server_version_num,
    pg_current_wal_lsn() AS current_wal_lsn,
    CASE WHEN to_regclass('pg_stat_wal') IS NULL THEN 'NO' ELSE 'YES' END AS pg_stat_wal_available,
    CASE
        WHEN to_regclass('pg_stat_checkpointer') IS NOT NULL THEN 'pg_stat_checkpointer'
        WHEN to_regclass('pg_stat_bgwriter') IS NOT NULL THEN 'pg_stat_bgwriter'
        ELSE 'unavailable'
    END AS checkpoint_stats_source,
    now() AS generated_at;
\qecho '</body></html>'
\o

-- 40 Database Growth Objects 90D
\o :r40
\qecho '<!doctype html><html><head><meta charset="utf-8"><title>Database Growth Objects 90D</title><style>body{font-family:Segoe UI,Arial,sans-serif;margin:0;padding:20px;color:#1d2b3a;background:linear-gradient(180deg,#f8fbff 0%,#eef5fb 100%);}h1{color:#1f4e79;margin:0 0 12px 0;font-size:24px;}p{color:#35506b;}a{color:#1d5ea8;text-decoration:none;font-weight:600;}a:hover{text-decoration:underline;}table{border-collapse:collapse;width:100%;background:#fff;margin:14px 0 18px;border:1px solid #d8e4f2;}th{background:#eaf2fb;color:#1f3f63;border:1px solid #d8e4f2;padding:8px;}td{border:1px solid #deebf7;padding:8px;vertical-align:top;}tr:nth-child(even) td{background:#f9fcff;}caption{caption-side:top;text-align:left;font-weight:700;color:#244b74;background:#f3f8fe;border:1px solid #d8e4f2;border-bottom:none;padding:9px 10px;}</style></head><body><h1>Database Growth Objects 90D</h1><p><a href="pg360_topics_main.html">Back to main</a></p>'
SELECT current_database() AS database_name, current_user AS executed_by, now() AS generated_at;
SELECT
    'Snapshot history not available in core catalogs by default. Use periodic size snapshots (daily) for true 90D growth trend.'::text AS note,
    current_database() AS database_name,
    now() AS generated_at;
\qecho '</body></html>'
\o

\echo 'Topic report pack generated: reports/pg360_topics_main.html'

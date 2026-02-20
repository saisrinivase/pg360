/* Topics: Logical Replication Slots Publications, Replication Health,
   Wal Checkpoint Pressure, Checkpoint Wal Rate Estimates */

SELECT 'Logical Replication Slots Publications' AS report_section;

SELECT
    slot_name,
    plugin,
    slot_type,
    database,
    active,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
ORDER BY slot_name;

SELECT
    pubname,
    puballtables,
    pubinsert,
    pubupdate,
    pubdelete,
    pubtruncate
FROM pg_publication
ORDER BY pubname;

SELECT 'Replication Health' AS report_section;

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

SELECT 'Wal Checkpoint Pressure' AS report_section;

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

SELECT 'Checkpoint Wal Rate Estimates' AS report_section;

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

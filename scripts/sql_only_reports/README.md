# SQL-Only Migration Report Pack

Purpose: pure SELECT report files for pgAdmin/DBeaver/psql execution.

How to use:
1. Open any `.sql` file.
2. Update schema regex constants in the `cfg` CTE if needed.
3. Execute script and review result tabs.

Prerequisites:
- PostgreSQL 13+ recommended (target compatibility: 13-18).
- Read access to catalog/stat views used by the scripts.
- Optional for hotspot analysis: `pg_stat_statements` extension enabled.

pgAdmin / DBeaver run:
1. Open a file (for example `01_executive_findings_action_queue.sql`).
2. Click `Execute Script` (not single statement).
3. Review each result grid in order (`report_section` header row followed by details).

Command-line batch run (all files):

```bash
for f in migration/sql_only_reports/*.sql; do
  base=$(basename "$f" .sql)
  psql -X -v ON_ERROR_STOP=1 -d pgbench_test -f "$f" \
    > "migration/samples/run_${base}_pgbench_test.txt"
done
```

Validation output:
- Example command-line logs are under `migration/samples/run_*_pgbench_test.txt`.
- These logs are useful for CI sanity checks and peer review attachment.

Compatibility behavior (13-18):
- Scripts avoid fragile version-specific columns wherever possible.
- Where telemetry differs by version (for example `pg_stat_io`, WAL/checkpointer split, extension views), scripts return availability/fallback notes instead of failing.
- Design target is forward-safe execution on newer PostgreSQL versions with equivalent catalog surfaces.

Design rules:
- No CREATE/DROP/ALTER/INSERT/UPDATE/DELETE.
- Catalog/statistics read-only queries only.
- Each file covers related migration-risk topics.

Report files:
1. `01_executive_findings_action_queue.sql`
2. `02_connection_pooling_authentication.sql`
3. `03_activity_waits_locks.sql`
4. `04_replication_slots_wal_checkpoint.sql`
5. `05_instance_config_diagnostic_readiness.sql`
6. `06_database_io_storage_growth.sql`
7. `07_table_index_constraint_health.sql`
8. `08_bloat_xid_partition_toast.sql`
9. `09_sql_hotspots_maintenance_plan_cache.sql`
10. `10_security_schema_function_extension_audit.sql`

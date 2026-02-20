# PG360 Client Tools Runbook

## Command Line (psql)

Run unified index + core + topics:

```bash
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "scripts/12_migration_topic_report_pack_v1.sql"
```

Open:
- `samples/pg360_topics_main.html`

## pgAdmin

Use:
- `scripts/migration_assessor_v1_gui.sql`

Steps:
1. Open Query Tool on target DB
2. Execute full script
3. Review section outputs
4. In section `10. HTML Report Payload`, export `html_report` as `.html`

## DBeaver

Use:
- `scripts/migration_assessor_v1_gui.sql`

Steps:
1. Open SQL Editor on target DB
2. Execute script (all statements)
3. Export `html_report` from section `10` to `.html`

## Notes

- `scripts/10_migration_assessor_v1.sql` and `scripts/12_migration_topic_report_pack_v1.sql` are psql-oriented because they use psql meta-commands for file generation.
- `scripts/migration_assessor_v1_gui.sql` is GUI-safe and pure SELECT.

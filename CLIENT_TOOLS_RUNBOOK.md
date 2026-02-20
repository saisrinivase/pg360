# PG360 Client Tools Runbook

## Command Line (psql)

Run one script to generate all outputs:

```bash
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "pg360.sql"
```

Primary bundle:
- `samples/pg360_bundle.zip`

This single run generates:
- main index + 40 topic reports
- core main + 9 core reports
- JSON and gate text outputs
- zipped bundle for sharing/review

## pgAdmin

For full main page + all subreports, use `PSQL Tool` in pgAdmin.

Steps (PSQL Tool):
1. Right-click your database in pgAdmin and open `PSQL Tool`.
2. Run:
```sql
\cd '/Users/saiendla/Documents/PostgreSQl SCripts /pg360'
\i pg360.sql
```
3. Open generated main page:
- `samples/pg360_topics_main.html`
4. Or share packaged zip:
- `samples/pg360_bundle.zip`

For single GUI payload only, use Query Tool script:
- `scripts/migration_assessor_v1_gui.sql`

## DBeaver

Use:
- `scripts/migration_assessor_v1_gui.sql`

Steps:
1. Open SQL Editor on target DB
2. Execute script (all statements)
3. Export `html_report` from section `10` to `.html`

## Notes

- `scripts/12_migration_topic_report_pack_v1.sql` is the primary entrypoint for full PG360.
- `pg360.sql` is the top-level one-command runner and zip packager.
- `scripts/10_migration_assessor_v1.sql` and `scripts/12_migration_topic_report_pack_v1.sql` are psql-oriented because they use psql meta-commands for file generation.
- `scripts/migration_assessor_v1_gui.sql` is GUI-safe and pure SELECT.
- Query Tool does not execute psql meta-commands like `\o`, `\i`, `\set`, `\qecho`.

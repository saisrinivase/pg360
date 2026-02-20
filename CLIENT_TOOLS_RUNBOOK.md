# PG360 Client Tools Runbook

## Goal

Run PG360 reliably and always get:
1. Main HTML entry page
2. 49 linked subreports (40 topic + 9 core)
3. shareable zip bundle

## Method A: Terminal (Recommended)

1. Open terminal.
2. Run:

```bash
cd "/Users/saiendla/Documents/PostgreSQl SCripts /pg360"
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "pg360.sql"
```

3. Validate:

```bash
ls -1 samples/pg360_topics_main.html
ls -1 samples/pg360_topic_*.html | wc -l
ls -1 samples/pg360_0*.html | wc -l
ls -1 samples/pg360_bundle.zip
```

4. Open:

```bash
open samples/pg360_topics_main.html
```

## Method B: pgAdmin PSQL Tool (Full Output Supported)

1. Open pgAdmin and connect DB.
2. Right-click DB and select `PSQL Tool`.
3. Execute:

```sql
\cd '/Users/saiendla/Documents/PostgreSQl SCripts /pg360'
\i pg360.sql
```

4. Open generated files from `samples/`.

## Method C: pgAdmin Query Tool / DBeaver SQL Editor (GUI-Only)

1. Execute:
`scripts/migration_assessor_v1_gui.sql`
2. Review section outputs.
3. Export `html_report` field manually to an `.html` file.

Note:
1. This mode does not generate 49 HTML files automatically.
2. Use Method A or B for full output.

## Why Some Users Fail

1. They run `pg360.sql` in Query Tool instead of `PSQL Tool`.
2. They run from wrong working directory.
3. Missing `zip` on local machine.
4. DB user lacks access to required system views.

## Quick Failure Mapping

1. `syntax error at or near "\"`: wrong tool; use `psql` or pgAdmin `PSQL Tool`.
2. `No such file or directory`: run from repo root.
3. No zip output: install `zip`, rerun.
4. Empty results: check user privileges and live workload stats.

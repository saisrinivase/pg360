# PG360

PG360 is an Oracle-to-PostgreSQL migration validation report pack.

It generates:
1. Core migration checks (9 reports)
2. Operational deep-dive checks (40 reports)
3. Main HTML entry page linking all reports
4. JSON and gate text outputs
5. One zip bundle for sharing

## Repository Layout

- `pg360.sql`: one-command runner for full generation + zip packaging
- `scripts/12_migration_topic_report_pack_v1.sql`: full HTML generation logic (core + topics)
- `scripts/10_migration_assessor_v1.sql`: core 9-report pack only
- `scripts/migration_assessor_v1_gui.sql`: pure SELECT script for Query Tool / DBeaver
- `samples/`: generated outputs and sample artifacts

## Compatibility

- PostgreSQL versions: 13 to 18
- Execution clients:
1. `psql` command line (recommended for full output)
2. pgAdmin `PSQL Tool` (works for full output)
3. pgAdmin Query Tool or DBeaver SQL Editor (GUI script only)

## Prerequisites

1. `psql` is installed and available in terminal.
2. `zip` is installed (required for `samples/pg360_bundle.zip`).
3. User has read access to system views (`pg_catalog`, `pg_stat_*`).
4. User can write files under repository `samples/`.
5. Current directory is repo root:
`/Users/saiendla/Documents/PostgreSQl SCripts /pg360`

## Standard Run (One Command)

Run from repo root:

```bash
cd "/Users/saiendla/Documents/PostgreSQl SCripts /pg360"
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "pg360.sql"
```

## Expected Output Files

After successful run:
1. `samples/pg360_topics_main.html` (main page)
2. `samples/pg360_topic_01...40...html` (40 topic reports)
3. `samples/pg360_report.html` (core main report)
4. `samples/pg360_01...09...html` (9 core reports)
5. `samples/migration_assessment_v1.json`
6. `samples/migration_gate_v1.txt`
7. `samples/pg360_bundle.zip`

Inside zip:
1. `pg360_bundle/pg360_topics_main.html` (start here)
2. all 40 topic reports
3. all 9 core reports
4. `pg360_report.html`, JSON, gate text, `START_HERE.txt`

## Validation Commands

Run these after execution:

```bash
cd "/Users/saiendla/Documents/PostgreSQl SCripts /pg360"
ls -1 samples/pg360_topics_main.html
ls -1 samples/pg360_topic_*.html | wc -l
ls -1 samples/pg360_0*.html | wc -l
ls -1 samples/pg360_bundle.zip
unzip -l samples/pg360_bundle.zip | grep "pg360_bundle/pg360_topics_main.html"
```

Expected counts:
1. topic reports = `40`
2. core reports = `9`

## Open Main Report

```bash
open "/Users/saiendla/Documents/PostgreSQl SCripts /pg360/samples/pg360_topics_main.html"
```

or open from zip:

```bash
open "/Users/saiendla/Documents/PostgreSQl SCripts /pg360/samples/pg360_bundle.zip"
```

## pgAdmin Usage

For full report set use `PSQL Tool` only.

Steps:
1. Connect to database in pgAdmin.
2. Right-click DB and open `PSQL Tool`.
3. Run:

```sql
\cd '/Users/saiendla/Documents/PostgreSQl SCripts /pg360'
\i pg360.sql
```

4. Open:
`samples/pg360_topics_main.html` or `samples/pg360_bundle.zip`.

## Query Tool / DBeaver Usage

Use only:
`scripts/migration_assessor_v1_gui.sql`

Result:
1. Sectioned tabular output
2. One row containing `html_report` payload

Limitation:
1. Query Tool / DBeaver SQL Editor cannot execute `psql` meta commands (`\set`, `\i`, `\o`, `\qecho`, `\!`).
2. So they cannot directly produce all 49 HTML files.

## Troubleshooting

1. Error: `syntax error at or near "\"`
Cause: `pg360.sql` or `scripts/12_...sql` executed in Query Tool.
Fix: run in terminal `psql` or pgAdmin `PSQL Tool`.

2. Error: `No such file or directory` for scripts
Cause: wrong working directory.
Fix: `cd "/Users/saiendla/Documents/PostgreSQl SCripts /pg360"` before run.

3. Error: zip file not generated
Cause: `zip` utility missing.
Fix: install zip and rerun `pg360.sql`.

4. Error: permission denied writing `samples/`
Cause: filesystem permissions.
Fix: grant write access to repo folder and rerun.

5. Reports generated but empty/limited findings
Cause: permissions or low activity stats.
Fix: run with higher-privileged read account and validate workload/stat collection.

## Cloud and On-Prem Notes

1. On-prem PostgreSQL: run as-is.
2. AWS RDS/Aurora: run as-is via endpoint connection string.
3. Some sections depend on runtime views/extensions (`pg_stat_statements`) and privileges.

## License

MIT. See `LICENSE`.

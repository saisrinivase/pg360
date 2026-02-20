# PG360

PG360 is a PostgreSQL migration readiness report pack focused on Oracle-to-PostgreSQL transitions.

It provides:
- Core migration checks (9 focused reports)
- Operational deep-dive checks (40 focused reports)
- A unified HTML command center index
- A GUI-safe SQL script for pgAdmin/DBeaver

## Repository Layout

- `scripts/10_migration_assessor_v1.sql`
  - psql script that generates `pg360_report.html` + 9 core subreports
- `scripts/12_migration_topic_report_pack_v1.sql`
  - psql script that generates a unified index and topic pack
  - includes links to core 9 + topic 40 reports
- `scripts/migration_assessor_v1_gui.sql`
  - pure SELECT script for pgAdmin/DBeaver
- `scripts/sql_only_reports/`
  - modular read-only SQL reports
- `samples/`
  - sample HTML outputs and run evidence from local execution

## Compatibility

- PostgreSQL target: 13 to 18
- Validated locally on PostgreSQL 18

## Quick Start (One Command For Everything)

Run this one script to generate all PG360 outputs (main page + all subreports):

```bash
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "scripts/12_migration_topic_report_pack_v1.sql"
```

Open:
- `samples/pg360_topics_main.html`

Generated output set from this single command:
- `samples/pg360_topics_main.html` (main index)
- `samples/pg360_topic_01...40...html` (40 operational topic reports)
- `samples/pg360_report.html` (core main report)
- `samples/pg360_01...09...html` (9 core migration reports)
- `samples/migration_assessment_v1.json`
- `samples/migration_gate_v1.txt`

Optional: run only the core 9 report pack with `scripts/10_migration_assessor_v1.sql`.

## pgAdmin / DBeaver Usage

For full PG360 main index + all subreports in pgAdmin, use `PSQL Tool`:

```sql
\cd '/Users/saiendla/Documents/PostgreSQl SCripts /pg360'
\i scripts/12_migration_topic_report_pack_v1.sql
```

Then open:
- `samples/pg360_topics_main.html`

For Query Tool / DBeaver SQL Editor (pure SQL only), use:
- `scripts/migration_assessor_v1_gui.sql`

This produces one HTML payload row (`html_report`) that you export manually.

## Deployment Contexts

### On-Prem PostgreSQL

- Works directly with local `psql` and GUI clients
- Run with DB user having read access to system catalog/stat views

### AWS RDS / Aurora PostgreSQL

- Works with standard RDS PostgreSQL connectivity
- Use endpoint-based `psql` connection string
- `pg_stat_statements` sections require extension to be enabled in parameter group and DB
- Some runtime metrics depend on permissions and engine settings

Example:

```bash
psql "host=<rds-endpoint> port=5432 dbname=<db> user=<user> sslmode=require" \
  -X -v ON_ERROR_STOP=1 \
  -v target_schema_regex='^(public|app|reporting)$' \
  -f "scripts/12_migration_topic_report_pack_v1.sql"
```

## Sample Outputs Included

Included under `samples/`:
- Unified index: `pg360_topics_main.html`
- Core report + 9 subreports: `pg360_report.html`, `pg360_01...09...html`
- Topic reports: `pg360_topic_01...40...html`
- Run evidence: `run_12_topic_report_pack_pg360_unified_pgbench_test.txt`

## License

MIT (see `LICENSE`).

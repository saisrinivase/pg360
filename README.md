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

## Quick Start (Command Line)

### 1) Unified PG360 Pack (recommended)

```bash
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v target_schema_regex='.*' \
  -f "scripts/12_migration_topic_report_pack_v1.sql"
```

Open:
- `samples/pg360_topics_main.html`

### 2) Core 9 Report Pack only

```bash
psql -X -v ON_ERROR_STOP=1 -d pgbench_test \
  -v report_file='samples/pg360_report.html' \
  -v report_01_file='samples/pg360_01_type_consistency.html' \
  -v report_02_file='samples/pg360_02_casting_issues.html' \
  -v report_03_file='samples/pg360_03_indexes_needed.html' \
  -v report_04_file='samples/pg360_04_unused_duplicate_indexes.html' \
  -v report_05_file='samples/pg360_05_bloat_report.html' \
  -v report_06_file='samples/pg360_06_partition_health.html' \
  -v report_07_file='samples/pg360_07_config_readiness.html' \
  -v report_08_file='samples/pg360_08_compatibility_matrix.html' \
  -v gate_html_file='samples/pg360_09_gate_summary.html' \
  -v json_file='samples/migration_assessment_v1.json' \
  -v gate_output_file='samples/migration_gate_v1.txt' \
  -f "scripts/10_migration_assessor_v1.sql"
```

## pgAdmin / DBeaver Usage

Use GUI-safe script:
- `scripts/migration_assessor_v1_gui.sql`

Steps:
1. Open Query Tool / SQL Editor on target database
2. Open and execute full script
3. Review sectioned outputs
4. Use section `10. HTML Report Payload` to export `html_report` into an `.html` file

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

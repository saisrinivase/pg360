# PG360

PG360 is a single-script, read-only PostgreSQL technical diagnostics report.

## What this repo contains
- `pg360.sql`: the canonical report generator
- `versions/version_0/`: locked baseline snapshot

## Prerequisites
- `psql` installed and available in `PATH`
- network access to the target PostgreSQL instance
- a login with enough visibility to query PostgreSQL monitoring views
- create the output directory before running:

```bash
mkdir -p reports/latest
```

Recommended visibility grants for the reporting role:
- `pg_monitor`
- `pg_read_all_stats`

## Usage
```bash
cd pg360

psql -h <host> -p <port> -U <user> -d <database> \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

Example:

```bash
cd pg360

mkdir -p reports/latest

psql -h localhost -p 5432 -U postgres -d pgbench_test \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

## What `pg360_ack_nonprod_first` does
`pg360_ack_nonprod_first=on` is a required safety acknowledgement.

PG360 refuses to run unless this flag is supplied. The purpose is to make the operator explicitly acknowledge the execution policy:
- test in sandbox or non-production first
- run production only during a low-load window
- avoid accidental execution without reading the safety guardrail

It does not change report content. It is only a deliberate pre-flight gate.

## Output
Each run writes one HTML file into the selected output directory:
- `./reports/latest/pg360_YYYYMMDD_HHMMSS.html`

If you want a custom file name:

```bash
psql -h <host> -p <port> -U <user> -d <database> \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -v pg360_full_report_file=pg360_mydb.html \
  -f ./pg360.sql
```

## Notes
- `pg360.sql` is the only runtime script required.
- The report is read-only.
- Generated output under `reports/latest/` is ephemeral and intentionally not tracked.

## Push to GitHub later
```bash
git remote add origin <your-github-repo-url>
git push -u origin main
```

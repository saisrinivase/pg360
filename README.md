# PG360 Diagnostic

PG360 is a PostgreSQL diagnostic report generator focused on technical evidence, triage, and remediation guidance.

## Canonical entry points
- `pg360.sql`: main report generator
- `pg360_repo_setup.sql`: history repository setup
- `pg360_repo_capture.sql`: history capture
- `pg360_sql_deep_dive.sql`: SQL deep-dive companion report

## Current baseline
- Permanent baseline snapshot: `versions/version_0/`
- Stable canonical script: `pg360.sql`
- Generated output under `reports/latest/` is ephemeral and intentionally not tracked

## Generate the main report
```bash
cd /Users/saiendla/Desktop/pg360

psql -X -A -t -d <database> \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -f /Users/saiendla/Desktop/pg360/pg360.sql
```

The default output file name is generated as:
- `pg360_YYYY-MM-DD HH:MI:SS.html`

## Push to GitHub later
```bash
git remote add origin <your-github-repo-url>
git push -u origin main
```

# PG360

PG360 is a single-script, read-only PostgreSQL technical diagnostics report.

## What this repo contains
- `pg360.sql`: the canonical report generator
- `versions/version_0/`: locked baseline snapshot

## Usage
```bash
cd /Users/saiendla/Desktop/pg360

psql -X -A -t -d <database> \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -f /Users/saiendla/Desktop/pg360/pg360.sql
```

## Output
Each run writes:
- `reports/latest/pg360_YYYY-MM-DD HH:MI:SS.html`
- `reports/latest/pg360_latest.html`

## Notes
- `pg360.sql` is the only runtime script required.
- The report is read-only.
- Generated output under `reports/latest/` is ephemeral and intentionally not tracked.

## Push to GitHub later
```bash
git remote add origin <your-github-repo-url>
git push -u origin main
```

# Changelog

## v1.0.0 - 2026-03-22

Initial release baseline.

Highlights
- Single-script, read-only PostgreSQL diagnostics report in `pg360.sql`.
- Supported PostgreSQL target range documented as 15-18.
- Validated HTML report generation with sample artifact in `versions/version_1.0.0/`.
- Graceful degradation for optional telemetry such as `pg_stat_statements`.
- Expanded diagnostics across Top SQL, waits, locks, vacuum/bloat, index health, WAL/replication, configuration, and security.
- Added sequence exhaustion runway check for migration-oriented risk detection.
- Added share-safe mode for safer report distribution.
- Typography/readability pass applied for laptop, monitor, and mobile-friendly viewing.
- Open-source packaging added with MIT license, disclaimer, author/contributor credits, and public-release documentation.

Release artifacts
- Script snapshot: `versions/version_1.0.0/pg360.sql`
- Sample report: `versions/version_1.0.0/pg360_20260322_140334.html`

Notes
- `demo/pg360_validation_prelude.sql` is for QA/demo seeding only and is not part of the production runtime path.
- `reports/latest/` remains ephemeral and is intentionally not tracked.

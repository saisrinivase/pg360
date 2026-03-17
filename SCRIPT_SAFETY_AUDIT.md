# PG360 Script Safety Audit

## Baseline
- Tag: `version_0.0.4`
- Post-audit working tree: share-safe redaction mode and Trigger Inventory empty-state fix
- Normal validation report: `reports/latest/pg360_20260317_175211.html`
- Share-safe validation report: `reports/latest/pg360_20260317_175242.html`

## Validated Controls
- `pg360.sql` opens a read-only transaction and commits at the end of the report.
- The script completed successfully when wrapped in an explicit outer `SET TRANSACTION READ ONLY` run.
- The script requires `-v pg360_ack_nonprod_first=on` and blocks on elevated-load preflight by default unless `pg360_force_high_load=on` is supplied.
- The script does not execute remediation SQL that it prints into the report.
- No shell execution, `\copy`, `\gexec`, `pg_read_file()`, `pg_ls_dir()`, or plaintext-credential access paths were found in the main script.

## Current Caveats
- The generated HTML report is still a sensitive operational artifact.
- Share-safe mode is now available, but it does not fully scrub query text or object names.
- The local HTML CSP still relies on inline CSS/JS (`unsafe-inline`) for report interactivity.
- The script writes a local HTML file via psql `\o`; this is a client-side side effect, not a database mutation.

## Operational Guidance
- Use `pg360.sql` directly for real environments.
- Use `-v pg360_share_safe=on` when the report will be shared beyond the immediate DBA/application team.
- Do not treat `demo/pg360_validation_prelude.sql` as production-safe; it is for QA/demo seeding only.

# PG360

PG360 is a single-script, read-only PostgreSQL operations workbook and diagnostics generator.

## Open-source release status
- PG360 is being prepared for public open-source release.
- Repository license: `MIT` for original PG360 material in this repository.
- Provenance gate before publishing: verify that no GPL-licensed code, copied report text, CSS, templates, or other protected material from `eDB360` / `SQLdb360` remain here unless PG360 is relicensed compatibly.
- See [ACKNOWLEDGMENTS.md](ACKNOWLEDGMENTS.md) and [OPEN_SOURCE_RELEASE_CHECKLIST.md](OPEN_SOURCE_RELEASE_CHECKLIST.md).

## Release
- Current release baseline: `v1.0.0`
- Canonical script: `pg360.sql`
- Release snapshot: `versions/version_1.0.0/`
- Sample report: `versions/version_1.0.0/pg360_20260322_140334.html`

## Repository
- Planned public repository: [saisrinivase/pg360](https://github.com/saisrinivase/pg360)

## What this repository contains
- `pg360.sql`: canonical runtime script
- `versions/version_1.0.0/`: locked release snapshot with sample report
- `CHANGELOG.md`: release history
- `RELEASE_GATE.md`: release criteria and validation lanes
- `VALIDATION_CHECKLIST.md`: validation checklist used during QA
- `SCRIPT_SAFETY_AUDIT.md`: execution safety posture and caveats
- `DISCLAIMER.md`: usage and liability disclaimer
- `ACKNOWLEDGMENTS.md`: attribution, inspiration, and provenance note
- `AUTHORS.md`: author and contributor credits
- `CONTRIBUTING.md`: contribution workflow and guardrails
- `CODE_OF_CONDUCT.md`: collaboration expectations
- `SECURITY.md`: security reporting guidance
- `OPEN_SOURCE_RELEASE_CHECKLIST.md`: publication checklist for the first public release
- `demo/pg360_validation_prelude.sql`: QA/demo seed script, not for production use

## Support statement
- Supported target range: PostgreSQL `15` through `18`
- Best coverage and deepest telemetry: PostgreSQL `16+`
- Supported environments: on-prem PostgreSQL and managed PostgreSQL with graceful degradation when optional telemetry is unavailable

## Safety model
- PG360 runs inside a read-only transaction.
- PG360 does not execute the remediation SQL it prints.
- PG360 requires an explicit acknowledgement flag before execution.
- Generated HTML should still be treated as a sensitive operational artifact.

## Open-source use
- You may use and modify PG360 for your own needs.
- PG360 is released under the [MIT License](LICENSE) for original PG360 material in this repository.
- See [DISCLAIMER.md](DISCLAIMER.md) for the no-warranty and no-liability statement.
- PG360 intentionally follows a DBA-friendly "360-degree report" experience similar in spirit to `eDB360` / `SQLdb360`; see [ACKNOWLEDGMENTS.md](ACKNOWLEDGMENTS.md) for the thank-you note and provenance guidance.

## Author and contributors
- Author: Sai Endla
- Contributors: Nagesh, Srikanth, Codex (OpenAI)
- Full credits: [AUTHORS.md](AUTHORS.md)

## Prerequisites
- `psql` installed and available in `PATH`
- network reachability to the target PostgreSQL instance
- a login with enough visibility to query PostgreSQL monitoring views
- output directory created before running

Recommended visibility grants for a reporting role:
- `pg_monitor`
- `pg_read_all_stats`

Create the default output directory:

```bash
mkdir -p reports/latest
```

## Quick start

```bash
psql -h <host> -p <port> -U <user> -d <database> \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

Example:

```bash
mkdir -p reports/latest

psql -h localhost -p 5432 -U postgres -d pgbenchc_test \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

## Core runtime variables
- `pg360_ack_nonprod_first=on`
  - required acknowledgement gate
- `pg360_output_dir=./reports/latest`
  - directory where the HTML report will be written
- `pg360_full_report_file=<name>.html`
  - optional custom file name
- `pg360_share_safe=on`
  - optional safer-distribution mode for report output
- `ON_ERROR_STOP=1`
  - recommended `psql` behavior so failures stop immediately

## Example modes

Standard run:

```bash
psql -h localhost -p 5432 -U postgres -d pgbenchc_test \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

Share-safe run:

```bash
psql -h localhost -p 5432 -U postgres -d pgbenchc_test \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_share_safe=on \
  -v pg360_output_dir=./reports/latest \
  -f ./pg360.sql
```

Custom output filename:

```bash
psql -h localhost -p 5432 -U postgres -d pgbenchc_test \
  -v ON_ERROR_STOP=1 \
  -v pg360_ack_nonprod_first=on \
  -v pg360_output_dir=./reports/latest \
  -v pg360_full_report_file=pg360_pgbenchc_test.html \
  -f ./pg360.sql
```

## Output
Each run writes one HTML report into the selected output directory:
- `./reports/latest/pg360_YYYYMMDD_HHMMSS.html`

Tracked release sample:
- `versions/version_1.0.0/pg360_20260322_140334.html`

## Execution policy
`pg360_ack_nonprod_first=on` is required.

The purpose of the acknowledgement gate is to make the operator explicitly confirm the execution policy:
- test in sandbox or non-production first
- use low-load windows for production execution
- avoid accidental runs without reading the guardrails

This flag does not change report content. It is only a deliberate pre-flight control.

## Notes
- `pg360.sql` is the only required runtime script.
- `demo/pg360_validation_prelude.sql` is for QA/demo seeding only.
- `reports/latest/` is intentionally ephemeral and not tracked.
- If optional telemetry is absent, PG360 should degrade gracefully rather than fail.

## Release and validation references
- `CHANGELOG.md`
- `RELEASE_GATE.md`
- `VALIDATION_CHECKLIST.md`
- `SCRIPT_SAFETY_AUDIT.md`
- `ACKNOWLEDGMENTS.md`
- `OPEN_SOURCE_RELEASE_CHECKLIST.md`
- `versions/version_1.0.0/NOTES.txt`

## Community
- Contribution guide: [CONTRIBUTING.md](CONTRIBUTING.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- Security reporting: [SECURITY.md](SECURITY.md)

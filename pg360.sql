/*
Purpose: One-command PG360 runner for command line.
What it does:
  1) Runs full PG360 generation (core 9 + topic 40 + main index files)
  2) Packages outputs into a zip for easy sharing and review

Usage:
  psql -X -v ON_ERROR_STOP=1 -d <database_name> \
    -v target_schema_regex='.*' \
    -f pg360.sql

Primary output:
  reports/pg360_bundle_YYYYMMDD_HHMMSS.zip
Entry point inside zip:
  pg360_bundle_YYYYMMDD_HHMMSS/pg360_topics_main.html
*/

\set ON_ERROR_STOP on
\pset pager off

\if :{?target_schema_regex}
\else
\set target_schema_regex '.*'
\endif

-- Generate full PG360 report set into reports/
\ir scripts/12_migration_topic_report_pack_v1.sql

-- Package all generated artifacts into a timestamped zip bundle.
\! /bin/sh -c 'set -e; ts="$(date +%Y%m%d_%H%M%S)"; bundle_name="pg360_bundle_${ts}"; bundle_zip="reports/${bundle_name}.zip"; tmp_root="$(mktemp -d /tmp/pg360_bundle_${ts}_XXXXXX)"; bundle_dir="${tmp_root}/${bundle_name}"; find reports -maxdepth 1 -type d -name "pg360_bundle_*" -exec rm -rf {} +; mkdir -p "$bundle_dir"; cp -f reports/pg360_topics_main.html reports/pg360_report.html reports/migration_assessment_v1.json reports/migration_gate_v1.txt reports/pg360_0*.html reports/pg360_topic_*.html "$bundle_dir"/; printf "Open pg360_topics_main.html to start.\n" > "$bundle_dir/START_HERE.txt"; (cd "$tmp_root" && zip -qr "$OLDPWD/$bundle_zip" "$bundle_name"); cp -f "$bundle_zip" reports/pg360_bundle_latest.zip; rm -rf "$tmp_root"; printf "%s" "$ts" > reports/.last_bundle_ts'

\set run_ts `cat reports/.last_bundle_ts`

\echo PG360 bundle generated: reports/pg360_bundle_:run_ts.zip
\echo Latest bundle alias: reports/pg360_bundle_latest.zip
\echo Open: pg360_bundle_:run_ts/pg360_topics_main.html

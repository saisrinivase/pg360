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
  samples/pg360_bundle.zip
Entry point inside zip:
  pg360_bundle/pg360_topics_main.html
*/

\set ON_ERROR_STOP on
\pset pager off

\if :{?target_schema_regex}
\else
\set target_schema_regex '.*'
\endif

-- Generate full PG360 report set into samples/
\ir scripts/12_migration_topic_report_pack_v1.sql

-- Package all generated artifacts into a single zip bundle.
\! /bin/sh -c "set -e; rm -rf samples/pg360_bundle samples/pg360_bundle.zip; mkdir -p samples/pg360_bundle; cp -f samples/pg360_topics_main.html samples/pg360_report.html samples/migration_assessment_v1.json samples/migration_gate_v1.txt samples/pg360_0*.html samples/pg360_topic_*.html samples/pg360_bundle/; printf 'Open pg360_topics_main.html to start.\n' > samples/pg360_bundle/START_HERE.txt; (cd samples && zip -qr pg360_bundle.zip pg360_bundle)"

\echo 'PG360 bundle generated: samples/pg360_bundle.zip'
\echo 'Open: pg360_bundle/pg360_topics_main.html'

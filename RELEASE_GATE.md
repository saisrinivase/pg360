# PG360 Release Gate

This document defines the minimum validation standard for releasing `pg360.sql` as a production-credible PostgreSQL diagnostic report.

## Product intent
- single script: `pg360.sql`
- read-only execution
- no UI/theme drift during validation cycles
- one report generated per run
- usable on both on-prem PostgreSQL and managed PostgreSQL platforms such as Amazon RDS / Aurora PostgreSQL

## Supported scope
Current support target:
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

Support statement:
- minimum supported branch for release claims: PostgreSQL 15
- best coverage and deepest telemetry: PostgreSQL 16+
- one single `pg360.sql` must run across supported branches
- if optional telemetry is unavailable, the report must degrade gracefully and continue

## Non-negotiable release principles
1. `pg360.sql` must remain read-only.
2. Missing extensions, views, or provider-restricted metadata must not crash the report.
3. Facts, derived signals, heuristics, and non-provable operational assertions must be clearly distinguished.
4. The report must be useful under real workload, not only on idle databases.
5. The report must complete successfully on both on-prem and RDS-style environments within the supported version range.

## Validation lanes
Release validation is performed across five lanes.

### 1. Version lane
Must be exercised on:
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

### 2. Environment lane
Must be exercised on:
- on-prem PostgreSQL
- managed PostgreSQL, at minimum Amazon RDS / Aurora PostgreSQL

### 3. Telemetry lane
Must be exercised with these combinations where possible:
- `pg_stat_statements` installed and preloaded
- `pg_stat_statements` not installed or not exposed
- `pg_stat_io` available
- `pg_stat_io` unavailable due to branch/version limits
- optional extensions absent:
  - `pg_buffercache`
  - `pgstattuple`
  - `pg_visibility`
  - `hypopg`

### 4. Load lane
Must be exercised under:
- low-load / idle validation
- `50%` endurance workload
- `S1` workload profile at endurance conditions
- `S2` workload profile at endurance conditions
- `100%` load simulation

### 5. Accuracy lane
Every subsection must be reviewed and classified as one of:
- `Fact`
- `Derived`
- `Heuristic`
- `Not provable from SQL`

## Must-pass runtime checks
These are mandatory before release.

1. Script execution
- `pg360.sql` exits with code `0`
- HTML output is fully generated
- no partial report is treated as release-valid

2. HTML validity baseline
- output contains `<!DOCTYPE html>`
- output contains `<html>`, `<body>`, `</body>`, `</html>`
- section rendering completes without truncated markup

3. Graceful degradation
- if `pg_stat_statements` is missing, Top SQL sections degrade to informative blocked-state output
- if `pg_stat_io` is unavailable, the report marks limited visibility and continues
- if optional extensions are unavailable, the report marks limited capability and continues
- provider-hidden objects on RDS must not cause runtime failure

4. Read-only safety
- no DDL
- no DML against user data
- no schema mutation
- no extension creation as part of report execution

5. Version compatibility
- no unit-cast or branch-specific failures such as direct numeric casts from settings that now return unit-qualified text
- branch-gated features must be protected by version and visibility checks

## Must-pass workload checks
These checks verify that PG360 remains useful under active load.

### A. Low-load sanity run
Purpose:
- verify the script runs cleanly on a mostly idle system
- confirm blocked or low-signal sections remain readable

Expected outcomes:
- script completes successfully
- no false crash due to absent telemetry
- no section generates broken formatting from empty datasets

### B. 50% endurance run
Purpose:
- confirm the report captures representative workload without becoming unstable
- confirm Top SQL, waits, WAL, autovacuum, and connections remain interpretable

Expected outcomes:
- `pg_stat_statements` captures real workload
- Top SQL subsections render with meaningful rows
- wait analysis reflects active workload instead of only idle backends
- report runtime remains acceptable for operational use

### C. S1 and S2 workload profiles
Purpose:
- confirm the report works across different workload shapes

Suggested interpretation:
- `S1`: read-heavy / OLTP-biased or latency-sensitive transactional profile
- `S2`: mixed or write-heavier profile with batch/reporting pressure

Expected outcomes:
- workload classification stays plausible and clearly inferred
- connection, lock, and vacuum sections remain useful across both profiles
- no section assumes one workload shape as universal truth

### D. 100% load simulation
Purpose:
- verify report resilience and usefulness during peak pressure

Expected outcomes:
- script still completes successfully
- no high-load crash caused by optional telemetry paths
- Top SQL, waits, lock analysis, WAL, and checkpoint sections remain diagnostically useful
- if a section has limited confidence under peak pressure, wording remains honest

## Must-pass accuracy checks
These are the review questions for every section and subsection.

1. Exact claim
- what is the report actually asserting?

2. Evidence source
- which PostgreSQL source proves it?
- examples:
  - `pg_stat_activity`
  - `pg_locks`
  - `pg_stat_statements`
  - `pg_stat_user_tables`
  - `pg_class`
  - `pg_settings`

3. Evidence type
- `Fact`
- `Derived`
- `Heuristic`
- `Not provable from SQL`

4. Confidence level
- `High`
- `Medium`
- `Low`
- `Not provable from SQL`

5. Wording discipline
- does the wording claim more certainty than the SQL supports?

6. Environment portability
- does this subsection still behave safely on both on-prem and RDS?

7. Version portability
- does this subsection still behave safely on PostgreSQL 15 through 18?

8. Missing telemetry handling
- does the report say `limited`, `unavailable`, or `blocked` instead of failing?

9. Duplication check
- is the same story already told elsewhere in the report?

10. Actionability check
- does this subsection help a DBA act, or is it only noise?

## Section confidence rules
These rules must be applied consistently.

### High confidence
Use for:
- direct catalog facts
- direct statistics view facts
- direct settings values

Examples:
- installed extension list
- current replication slot state
- current blocking tree
- current `max_connections`

### Medium confidence
Use for:
- ratios
- rankings
- thresholds
- summary scores derived from direct facts

Examples:
- HOT update ratio
- checkpoint pressure classification
- growth rankings from stats windows

### Low confidence
Use for:
- recommendations based on heuristics
- candidate signals
- inferred workload shape
- partitioning suitability
- likely missing index suggestions

### Not provable from SQL
Use for:
- backup success
- restore drill success
- failover drill success
- application failover correctness
- runbook quality
- business criticality labels unless externally defined

## Release blockers
Any of the following blocks release.

1. Runtime blocker
- any supported environment or supported PostgreSQL version causes a script failure

2. Misleading certainty
- a heuristic is presented as fact
- an operational assertion is presented as proven when SQL cannot prove it

3. Unsupported portability gap
- on-prem works but RDS fails, or vice versa, without graceful degradation

4. Telemetry dependency without fallback
- a section requires optional telemetry and crashes when it is absent

5. High-load failure
- report fails under 50% endurance or 100% load simulation

## Release signoff criteria
A release candidate is acceptable only when all of the following are true.

1. Runtime
- passes supported version runs
- passes on-prem and managed environment runs
- passes low-load, endurance, and peak-load runs

2. Accuracy
- all audited sections have explicit evidence and confidence classification
- no known major overstatement remains in high-signal sections

3. Single-script promise
- `pg360.sql` is the only required runtime artifact
- no repository/setup scripts are required for base functionality
- optional history or enrichments are clearly marked optional

4. Operational honesty
- blocked capabilities are reported honestly
- heuristic sections are labeled or worded conservatively
- report does not claim “0% error-free” or “full 360-degree truth”

5. Release statement
Safe public release statement:
- `PG360 is a single-script, read-only PostgreSQL diagnostic report for PostgreSQL 15-18, designed for on-prem and managed environments, with graceful degradation when optional telemetry is unavailable.`

## Recommended release workflow
1. Run the report on an idle or low-load database.
2. Run the report during a representative 50% endurance workload.
3. Run the report during S1 and S2 workload profiles.
4. Run the report during a 100% load simulation.
5. Review high-signal sections first:
- Top SQL
- Wait Events and Session Activity
- Lock Analysis
- WAL & Replication Runtime
- Vacuum & Maintenance
- Security sections
6. Record blockers, wording risks, and false alarms.
7. Fix SQL/runtime issues before adding new sections.
8. Tag the release only after all release blockers are closed.

## Current working assumptions
- UI is frozen during validation and accuracy work.
- New section expansion is lower priority than runtime safety and truthfulness.
- Real workload validation is mandatory for release credibility.

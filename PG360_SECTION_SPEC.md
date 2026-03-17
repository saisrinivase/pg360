# PG360 Section Specification

This document turns the current implementation direction into a concrete section-level spec for `pg360.sql`.

It is designed to preserve the product contract:
- one runtime artifact: `pg360.sql`
- read-only execution
- PostgreSQL 15 through 18
- on-prem and managed PostgreSQL support
- graceful degradation when optional telemetry is unavailable
- UI frozen unless explicitly approved

## How to read this spec
- `Existing coverage` means the report already has a related section or evidence path.
- `Missing / deepen` means the report needs new SQL, better rollups, or stronger interpretation.
- `Phase` maps the work to the current roadmap:
  - `A`: release baseline
  - `B`: post-release tuning depth
  - `C`: advanced assisted mode

## 1. Vacuum, autovacuum, and bloat

### 1.1 Vacuum & Bloat Health
Primary home:
- `S05 Table Health & Bloat`
- `S10 Vacuum & Maintenance`
- `S21 Autovacuum Full Advisor`
- `S24 Index Bloat Estimation`

Existing coverage:
- per-table dead tuples and dead-tuple ranking
- freeze age and wraparound countdown
- autovacuum effectiveness / backlog
- table storage and autovacuum overrides
- analyze freshness by table
- index bloat estimation exists separately

Missing / deepen:
- unify the operator story so vacuum, bloat, freeze, and overrides read as one health narrative
- add clearer “no autovacuum in stats window” surfacing
- add lightweight table bloat heuristic with explicit confidence labeling
- extend dangerous reloptions interpretation for hot tables
- keep exact bloat claims conservative unless validated by optional extensions

Inputs:
- `pg_stat_user_tables`
- `pg_class`
- `pg_namespace`
- `pg_database`
- `pg_settings`
- `pg_options_to_table` where available, or parsed `reloptions`

Phase:
- `A`

## 2. Index health (usage, duplication, missing)

### 2.1 Index Health
Primary home:
- `S06 Index Health & Missing Index Suggestions`
- `S24 Index Bloat Estimation`

Existing coverage:
- unused non-unique indexes since last reset
- duplicate and redundant indexes
- foreign keys without supporting indexes
- invalid indexes and readiness status
- missing-index style candidate logic
- index bloat estimation

Missing / deepen:
- improve low-use index logic relative to table activity, not just absolute `idx_scan`
- make overlap/superset detection more explicit and review-friendly
- add “candidate index investigation” linkage from top queries to large scanned tables
- keep missing-index logic explicitly heuristic
- separate inventory facts from tuning suggestions more clearly

Inputs:
- `pg_stat_user_indexes`
- `pg_index`
- `pg_class`
- `pg_namespace`
- `pg_attribute`
- `pg_stat_user_tables`
- `pg_stat_statements`

Phase:
- `A` for stronger health and candidate mapping
- `B` for why-index-not-used interpretation

## 3. Wait events, locks, and contention

### 3.1 Wait & Lock Profile
Primary home:
- `S03 Wait Events and Session Activity`
- `S04 Lock Analysis`

Existing coverage:
- current wait and timeout pressure snapshot
- wait diagnosis matrix
- lock blocker tree and blocked-session detail
- lock exposure and mitigation actions
- session sampling window

Missing / deepen:
- sampled wait-event distribution that reads more like a performance profile
- clearer separation between sampled backend counts and true server-time accounting
- stronger relation-level lock hotspot rollup
- optional historical lock evidence only when log ingestion exists

Inputs:
- `pg_stat_activity`
- `pg_locks`
- `pg_class`
- `pg_namespace`
- `pg_settings`
- optional log evidence for `log_lock_waits`

Phase:
- `A` for sampled wait/lock profile
- `C` for historical lock evidence from logs

## 4. Memory, work_mem, and temp usage

### 4.1 Memory & Temp Usage
Primary home:
- `S07 Buffer Cache & I/O`
- `S23 Full Configuration Parameter Audit`
- `S01/S11` summary cards where appropriate

Existing coverage:
- cache and memory behavior summary
- temp volume and temp file signals
- `work_mem`-related parameter discovery now exists in configuration
- buffer/cache posture exists

Missing / deepen:
- per-database cache hit framing by workload style
- stronger temp-spill-to-query linkage when `pg_stat_statements` temp metrics are available
- make `log_temp_files` gap more operationally visible
- keep RAM-relative interpretations conditional unless total memory is truly known

Inputs:
- `pg_stat_database`
- `pg_stat_statements`
- `pg_settings`
- optional parsed temp-file logs

Phase:
- `A`
- `C` for richer log-assisted temp diagnosis

## 5. I/O and WAL/storage behavior

### 5.1 I/O & WAL Behavior
Primary home:
- `S07 Buffer Cache & I/O`
- `S08 WAL & Replication Runtime`

Existing coverage:
- relation read hotspots
- checkpoint pressure indicators
- background write pressure classification
- WAL generation rate
- slot safety window
- archiver posture

Missing / deepen:
- more explicit per-table heap vs index I/O hotspots
- better use of `pg_stat_io` where available for read/write/open/fsync timing
- WAL latency and sync interpretation when the branch exposes timing columns
- keep branch-gated behavior explicit on PostgreSQL 15 where `pg_stat_io` is absent

Inputs:
- `pg_statio_user_tables`
- `pg_stat_bgwriter`
- `pg_stat_io` when available
- `pg_stat_wal` when available
- `pg_stat_archiver`

Phase:
- `A`
- `B` for deeper interpretation

## 6. Query plans and shapes

### 6.1 Plan Shape & Estimation
Primary home:
- `S02 Top SQL Analysis`
- `S20 Planner Statistics Quality & Estimation Errors`
- optional assisted-mode companion sections

Existing coverage:
- top SQL rankings
- planning vs execution overhead
- rows efficiency and wasted-work
- high-variability queries
- planner statistics quality signals
- long-query rewrite candidates exist heuristically

Missing / deepen:
- actual plan classification by scan/join/sort shape
- misestimation ratio from actual vs estimated rows
- anti-pattern spotlight grounded in real plans
- safe handling of EXPLAIN/ANALYZE so core report stays read-only and low-risk

Inputs:
- `pg_stat_statements`
- `EXPLAIN (FORMAT JSON)` for sampled hot queries only when explicitly enabled
- optional `EXPLAIN ANALYZE` in lab / assisted mode

Phase:
- `B` for heuristic shape interpretation without executing plans
- `C` for true plan parsing and actual-vs-estimated diagnosis

## 7. Configuration posture grid

### 7.1 Configuration Posture
Primary home:
- `S23 Full Configuration Parameter Audit`
- `S00.5 Configuration Posture Snapshot (Summary only)`
- limited summary in `S11 Workload Profile & Configuration Tuning`

Existing coverage:
- configuration posture summary exists
- parameter action matrix exists
- workload-driven parameter discovery matrix exists
- timeout/logging/timing posture already mapped to evidence

Missing / deepen:
- normalize the parameter story into three layers:
  1. baseline guardrails
  2. workload-driven discovery
  3. action summary
- group knobs into families: connections, memory, checkpoint/WAL, bgwriter, planner
- keep each knob tied to evidence, safety, verify step, and “why it matters”

Inputs:
- `pg_settings`
- evidence sections from S02/S03/S07/S08/S10/S20

Phase:
- `A`

## 8. Security & connectivity deep-dive

### 8.1 Security & Connectivity
Primary home:
- `S12 Security Baseline`
- `S25 Security & Access Review`
- early-environment posture in `S00`

Existing coverage:
- active connections without SSL
- role and privilege posture
- PUBLIC exposure checks
- some `pg_hba_file_rules` visibility and auth-method checks already exist

Missing / deepen:
- promote HBA classification into a first-class subsection when visible
- label exposure posture as lab-only vs production-ready carefully
- strengthen role hierarchy review around inherited elevated monitoring/access roles
- keep provider visibility limits explicit on managed services

Inputs:
- `pg_hba_file_rules` when visible
- `pg_settings`
- `pg_roles`
- `pg_auth_members`
- `pg_stat_ssl`

Phase:
- `A`

## 9. SLO and guardrail alignment

### 9.1 SLO Alignment
Primary home:
- separate optional section near workload/configuration, or summary subsection under `S11`

Existing coverage:
- timeout guardrails and workload starting points exist
- no true SLO-alignment layer yet

Missing / deepen:
- allow optional user-provided latency/runtime targets
- compare representative query groups against observed means and conservative derived latency indicators
- validate timeout posture against those targets
- do not overclaim percentiles if only mean/aggregate stats are available

Inputs:
- user-provided SLO targets
- `pg_stat_statements`
- `pg_settings`

Phase:
- `B` for initial SLO alignment
- `C` for stronger percentile-style evidence if history/log ingestion is added

## Implementation order

Recommended execution order inside the current UI and section structure:
1. Strengthen `S23` configuration posture into one consistent evidence story.
2. Strengthen `S03/S04` wait and lock profile with sampled wait-event distribution.
3. Deepen `S05/S10/S21/S24` vacuum, bloat, freeze, and override linkage.
4. Deepen `S06/S24` index health and candidate investigation linkage to top SQL.
5. Deepen `S07/S08` I/O, temp, and WAL interpretation with branch-aware timing.
6. Promote `S12/S25` HBA and role hierarchy review where visible.
7. Add optional SLO alignment hooks after the evidence baseline is stable.
8. Defer true plan parsing and historical lock/temp log synthesis to assisted mode.

## Non-goals for the core release
The following should not be presented as baseline core guarantees:
- exact bloat truth without validation extensions
- actual-vs-estimated row proof without assisted plan capture
- historical lock or temp-file forensics without log ingestion
- percentile latency claims when only aggregate SQL statistics are available

## Success criteria
This spec is satisfied when:
- each listed area has a clear home in the current report structure
- missing logic is implemented without UI drift
- each new subsection distinguishes fact vs derived vs heuristic evidence
- PostgreSQL 15 through 18 remain supported with graceful degradation
- on-prem and managed environments remain safe and readable

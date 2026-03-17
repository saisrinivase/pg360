# PG360 Baseline Diff

This document records the difference between the pre-phase baseline and the current phased working state.

## Baseline reference
Pre-phase locked baseline:
- tag: `version_0.0.2`
- intent: stable single-script baseline before phased roadmap execution

Current phased working state:
- branch head / working tree after Phase roadmap, release gate, validation checklist, PostgreSQL 18 compatibility fixes, workload-validation setup, and phase-logic implementation

## What existed before phases
At the pre-phase baseline, PG360 already had:
- single-script model: `pg360.sql`
- HTML report generation
- frozen visual theme direction
- broad operational diagnostics across PostgreSQL areas
- basic Top SQL, waits, locks, vacuum, WAL, security, capacity, and extension coverage
- initial release tagging structure

## What exists now that did not exist before phases
1. Release framework
- `RELEASE_GATE.md`
- `VALIDATION_CHECKLIST.md`
- explicit support statement for PostgreSQL 15-18

2. Compatibility hardening
- PostgreSQL 18 unit-safe handling for `shared_buffers`
- safer `pg_stat_statements` degradation when the extension is absent
- later optional sections degrade instead of failing when `pg_stat_statements` is unavailable

3. Real workload validation
- validated `pgbenchc_test` workload capture through `pg_stat_statements`
- validated S1 and S2 workload runs
- validated focused capture runs for plan logging

4. Plan-capture readiness
- `auto_explain` loaded successfully after restart
- log sink identified and verified
- actual `auto_explain` plan entries captured for workload queries

5. Product direction
- formal Phase A / B / C roadmap
- explicit UI freeze contract
- clear distinction between release baseline and assisted future mode

6. Phase logic now present in the report output
- Phase A logic:
  - diagnostic evidence model
  - SQL workload family classification
  - function / procedure hotspots
  - DML query hotspots
  - large-table partition candidates
  - data-model smell signals
  - JSONB / array operator fit guidance
- Phase B logic:
  - sequential scan interpretation heuristics
  - long-query rewrite candidates
  - stronger workload-archetype guidance through query family classification
- Phase C logic:
  - advanced assisted diagnostics readiness
  - prepared plan mix visibility
  - explicit separation of SQL-only evidence vs assisted-mode scope

## What has not changed by design
These areas are intentionally frozen:
- report visual theme
- index-page look and spacing
- section framing
- core table presentation
- section 1 through section 5 visual structure

## What is still not complete
The following roadmap items are still intentionally incomplete or only partially represented:
- Phase A:
  - remaining wording/accuracy cleanup across older sections
  - deeper ORM / app anti-pattern coverage beyond existing chatty-access logic
  - release-gate execution across the full version / environment / load matrix
- Phase B:
  - richer why-index-not-used logic tied to predicates and operator classes
  - deeper join-shape / join-order heuristics
  - broader long-query rewrite coverage beyond current candidate signals
- Phase C:
  - actual plan-node parsing and interpretation
  - actual-vs-estimated row diagnosis
  - partition pruning proof
  - external log/history ingestion beyond readiness and current-session visibility

## Current interpretation
The project is no longer in a purely exploratory state.
It now has:
- a support statement
- a release gate
- a validation checklist
- a phased roadmap
- validated workload instrumentation
- Phase A logic materially implemented in the report
- core Phase B heuristics implemented
- Phase C represented honestly as assisted readiness, not fake SQL-only proof

The current state is:
- Phase A: largely implemented in `pg360.sql`, pending broader validation and wording cleanup
- Phase B: partially implemented in `pg360.sql`
- Phase C: readiness/status logic implemented; full assisted analysis still deferred

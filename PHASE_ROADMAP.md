# PG360 Phase Roadmap

This document defines the implementation roadmap for PG360.

## Product contract
- one runtime artifact: `pg360.sql`
- read-only execution
- PostgreSQL 15 through 18
- on-prem and managed PostgreSQL support
- graceful degradation when optional telemetry is unavailable
- UI frozen during phase execution

## UI freeze contract
The visual presentation is frozen during Phase A, Phase B, and Phase C work unless explicitly approved.

Frozen areas:
- report theme and colors
- index page layout
- section framing and spacing
- section 1 through section 5 visual structure
- subsection heading style
- core table styling

Allowed changes without separate UI approval:
- SQL logic
- content accuracy
- section wording
- new evidence rows or subsections where needed
- compatibility fixes
- internal diagnostics logic

Not allowed without explicit approval:
- CSS/theme changes
- new UI widgets
- changes to the first five core sections' visual layout
- card/table redesigns
- navigation redesign

## Phase A
`Release baseline`

Objective:
- ship a trustworthy, production-credible SQL-only baseline

Scope:
1. runtime compatibility hardening
2. graceful degradation across PostgreSQL 15-18
3. on-prem and RDS-safe behavior
4. confidence framework
5. query family classification
6. DML tuning diagnostics
7. ORM / app anti-pattern detection
8. advanced operator / index fit guidance
9. partition candidate analysis
10. data-model smell detection
11. wording and accuracy cleanup
12. release-gate validation

Exit criteria:
- runtime lane passes
- telemetry lane passes
- load lane passes
- no major overstatements remain in high-signal sections
- `pg360.sql` remains the only required runtime script

## Phase B
`Post-release tuning depth`

Objective:
- deepen query tuning quality while preserving the single-script model

Scope:
1. why-index-not-used engine
2. long-query rewrite advisor
3. join-shape and join-order heuristics
4. function / procedure hotspot analysis
5. stronger workload archetype guidance
6. richer candidate logic for short vs long query tuning

Exit criteria:
- improved tuning specificity
- no unacceptable increase in false positives
- wording remains conservative where evidence is heuristic

## Phase C
`Advanced assisted mode`

Objective:
- add optimizer-aware analysis beyond what SQL-only core can prove directly

Scope:
1. actual plan-node analysis
2. actual-vs-estimated row diagnosis
3. partition pruning proof
4. generic vs custom plan risk
5. historical drift and regression engine
6. log-assisted slow-query and plan interpretation
7. optional auto_explain-assisted diagnostics

Exit criteria:
- advanced optimizer diagnostics are available as optional enrichment
- `pg360.sql` core remains usable without assisted mode

## Phase boundaries
- Phase A must complete before Phase B is considered release work.
- Phase B must not weaken the one-script contract.
- Phase C remains optional and must not become a dependency for the core release.

## Current implementation status
- Phase A: implemented in the report logic, with remaining work focused on validation coverage and wording cleanup.
- Phase B: core heuristics implemented for sequential-scan interpretation, rewrite candidates, and function hotspots.
- Phase C: assisted readiness/status is implemented; full plan parsing and historical ingestion remain future assisted-mode work.

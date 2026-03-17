# PG360 Validation Checklist

Use this checklist to track release-readiness for `pg360.sql`.

Related reference:
- `RELEASE_GATE.md`

Status values:
- `PASS`
- `FAIL`
- `BLOCKED`
- `NOT RUN`

## Build Under Test
- Script: `pg360.sql`
- Candidate version/tag: `____________________`
- Commit: `____________________`
- Reviewer: `____________________`
- Date: `____________________`

## 1. Runtime Baseline
| Check | Status | Notes |
|---|---|---|
| Script exits with code 0 | NOT RUN | |
| Full HTML file generated | NOT RUN | |
| Output contains complete HTML structure | NOT RUN | |
| No partial output treated as valid | NOT RUN | |
| Read-only execution confirmed | NOT RUN | |

## 2. Supported Version Lane
| PostgreSQL Version | On-Prem | Managed/RDS | Notes |
|---|---|---|---|
| 15 | NOT RUN | NOT RUN | |
| 16 | NOT RUN | NOT RUN | |
| 17 | NOT RUN | NOT RUN | |
| 18 | NOT RUN | NOT RUN | |

## 3. Telemetry Lane
| Scenario | Status | Notes |
|---|---|---|
| `pg_stat_statements` installed and preloaded | NOT RUN | |
| `pg_stat_statements` missing or unavailable | NOT RUN | |
| `pg_stat_io` available | NOT RUN | |
| `pg_stat_io` unavailable due to branch/version limits | NOT RUN | |
| `pg_buffercache` absent | NOT RUN | |
| `pgstattuple` absent | NOT RUN | |
| `pg_visibility` absent | NOT RUN | |
| `hypopg` absent | NOT RUN | |

## 4. Load Lane
| Scenario | Status | Notes |
|---|---|---|
| Low-load / idle run | NOT RUN | |
| 50% endurance run | NOT RUN | |
| S1 workload run | NOT RUN | |
| S2 workload run | NOT RUN | |
| 100% load simulation | NOT RUN | |

## 5. High-Signal Section Review
| Section | Status | Confidence | Notes |
|---|---|---|---|
| Top SQL Analysis | NOT RUN | __ | |
| Wait Events and Session Activity | NOT RUN | __ | |
| Lock Analysis | NOT RUN | __ | |
| Table Health & Bloat | NOT RUN | __ | |
| Index Health & Missing Index Suggestions | NOT RUN | __ | |
| Buffer Cache & I/O | NOT RUN | __ | |
| WAL & Replication Runtime | NOT RUN | __ | |
| Connections & Pooling | NOT RUN | __ | |
| Vacuum & Maintenance | NOT RUN | __ | |
| Security Baseline | NOT RUN | __ | |
| Security & Access Review | NOT RUN | __ | |
| Extension Inventory | NOT RUN | __ | |
| HA & Disaster Recovery Readiness | NOT RUN | __ | |
| Capacity & Growth | NOT RUN | __ | |

## 6. Accuracy Review Rules
For each reviewed subsection, record:
- exact claim
- evidence source
- evidence type: `Fact`, `Derived`, `Heuristic`, `Not provable from SQL`
- confidence: `High`, `Medium`, `Low`, `Not provable from SQL`
- wording risk
- portability risk
- duplication risk

## 7. Known Release Blockers
| Blocker | Status | Notes |
|---|---|---|
| Runtime failures on supported versions | OPEN | |
| Runtime failures on managed PostgreSQL | OPEN | |
| Optional telemetry paths crashing the script | OPEN | |
| Heuristics presented as facts | OPEN | |
| High-load failures | OPEN | |

## 8. Current Candidate Notes
- UI is frozen during validation.
- One script only: `pg360.sql`.
- Optional telemetry must degrade gracefully.
- Real workload validation is mandatory for release credibility.

## 9. Signoff
| Criterion | Status | Notes |
|---|---|---|
| Runtime lane complete | NOT RUN | |
| Version lane complete | NOT RUN | |
| Telemetry lane complete | NOT RUN | |
| Load lane complete | NOT RUN | |
| Accuracy review complete | NOT RUN | |
| No open release blockers | NOT RUN | |
| Release candidate approved | NOT RUN | |

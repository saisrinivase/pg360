# PG360 Column Audit Register

## Baseline
- Tag: `version_0.0.4`
- Commit: `2dbc4b9`
- Report under review: `reports/latest/pg360_20260317_143158.html`
- Scope status: `Audit started, not full-report complete`

## Status Legend
- `Validated`: Rendered HTML checked and columns populated as expected for the current dataset.
- `Validated (empty-state)`: Rendered HTML checked and the empty-state row/message is correct for the current dataset.
- `Validated (format fix)`: A broken column was traced to source SQL, fixed, and revalidated in rendered HTML.
- `Needs deeper semantic review`: Columns render, but the recommendation logic still needs deeper QA against workload meaning.
- `Pending`: Not yet audited in this register.

## Current Coverage Summary
- Full report column-by-column QA: `Not complete`
- First completed tranche: `S06 Index Health`
- Current audit focus: `Rendered HTML columns, empty states, and obvious formatting/data population defects`

## S06 Index Health
| Subsection | Audit Result | Notes |
|---|---|---|
| `Unused Secondary Indexes` | `Validated` | Note text verified. Columns `Schema`, `Table`, `Index`, `Size`, `Scans`, `Type`, `Review Script` all populated in HTML for current dataset. |
| `Duplicate & Redundant Indexes` | `Validated` | Columns `Table`, `Index 1`, `Index 2`, `Columns`, `Recommendation` populated. |
| `FKs Without Supporting Indexes` | `Validated (empty-state)` | Empty-state row `All foreign keys have supporting indexes` is present and structurally correct. |
| `Tables With High Sequential Scans` | `Validated (format fix)` | `Avg Rows/Scan` overflow defect fixed. HTML now shows `6,337,509` for `miglab.invoice_items` instead of `###,###`. |
| `Sequential Scan Interpretation Heuristics` | `Needs deeper semantic review` | Columns populate correctly, but recommendation meaning should still be cross-checked against workload evidence before signoff. |
| `Invalid Indexes` | `Validated (empty-state)` | Empty-state row `No invalid indexes found` is present and structurally correct. |
| `Index Write-Cost Posture` | `Validated` | Columns populate, but current dataset only shows a narrow demo row (`pg360_demo.lock_probe`). |
| `Index-Only Scan Potential` | `Validated` | Columns populate in HTML for current dataset. |
| `Foreign Key Index Gaps` | `Validated` | Columns populate and generated scripts render correctly. |
| `SQL Telemetry & Version Insights` | `Validated` | All columns populated; version-driven text renders correctly on PG18. |
| `Index Readiness Status` | `Validated (empty-state)` | Empty-state row `All indexes are valid and ready` is present and structurally correct. |
| `Index Remediation Queue` | `Validated` | Summary finding block renders with populated counts. |

## Defects Found So Far
1. `S06.04 Tables With High Sequential Scans`
   - Defect: `Avg Rows/Scan` rendered as `###,###` when value exceeded the `to_char(..., 'FM999,999')` picture mask.
   - Fix: widened numeric masks in `pg360.sql` for `Seq Scans`, `Avg Rows/Scan`, and `Index Scans`.
   - Validation: regenerated report `reports/latest/pg360_20260317_143158.html` and confirmed correct HTML output.

## Remaining Audit Backlog
- `S01` through `S05`
- `S07` onward
- Cross-check of recommendation semantics after column population passes
- Targeted sample-data validation for subsections that are structurally correct but rarely populated in the current workload window

## S01 Platform and Diagnostic Context
| Subsection | Audit Result | Notes |
|---|---|---|
| `Diagnostic Readiness Snapshot` | `Validated` | Card values populated for version, uptime, encoding, pg_stat_statements, track_io_timing, stats reset, and privilege scope. |
| `Visibility Warnings` | `Validated` | All columns populated; supporting-evidence links render correctly. |
| `Timeout Guardrails and Session Safety` | `Validated` | All columns populated; status and recommendation text render correctly. |
| `Timeout Starting Points by Workload` | `Validated` | All columns populated in HTML. Advisory-only content still needs later semantic review, but no population defect seen. |
| `Managed Service Fingerprint` | `Validated (empty-state)` | Provider-setting table empty-state is correct for self-managed environment; cards populated. |
| `Managed Service Apply Paths` | `Validated` | All columns populated. |
| `Version Currency & Security Posture` | `Validated` | All columns populated; mixed status styling renders correctly. |

## S04 Monitoring and Observability Readiness
| Subsection | Audit Result | Notes |
|---|---|---|
| `Observability Baseline` | `Validated` | Card values populated correctly for current telemetry state. |
| `Logging Configuration Sanity` | `Validated` | All columns populated. |
| `Performance Control Plane` | `Validated` | Cards and capability table populated. |
| `Advanced Statistics and Progress Coverage` | `Validated` | All columns populated. |
| `Incident Triage Telemetry Posture` | `Validated` | All columns populated, including partial-status rows. |
| `Plan Capture Readiness` | `Validated` | Table structure and rows render; current dataset shows populated plan-capture settings. |
| `Observability Gaps` | `Pending` | Section exists in report but not yet line-by-line reviewed in the register. |

## S05 Workload Characterization
| Subsection | Audit Result | Notes |
|---|---|---|
| `Workload Snapshot` | `Pending` | Present in report; not yet line-by-line checked in this tranche. |
| `Wait and Timeout Pressure Snapshot` | `Pending` | Present in report; not yet line-by-line checked in this tranche. |
| `Wait Diagnosis Matrix` | `Pending` | Present in report; not yet line-by-line checked in this tranche. |
| `Historical Baseline and Drift` | `Pending` | Present in report; optional-history semantics still need careful QA. |
| `Supporting Evidence` | `Pending` | Present in report; not yet reviewed column by column. |

## S04 Lock Analysis
| Subsection | Audit Result | Notes |
|---|---|---|
| `Blocking Tree with Blast Radius` | `Pending` | Present in report; needs row/empty-state verification in a dedicated lock pass. |
| `Blocked Session Detail` | `Pending` | Present in report; same lock-pass requirement. |
| `Relation-Level Lock Hotspots` | `Pending` | Present in report; timing-sensitive validation still needed. |
| `AccessExclusiveLock and DDL Lock Exposure` | `Pending` | Present in report; not yet verified at rendered-row level. |
| `Advisory Lock Summary` | `Pending` | Present in report; not yet verified at rendered-row level. |
| `Lock Timeout and Logging Posture` | `Pending` | Present in report; not yet verified at rendered-row level. |
| `Mitigation Actions` | `Pending` | Present in report; finding block not yet reviewed in the register. |

## S05 Table Health & Bloat
| Subsection | Audit Result | Notes |
|---|---|---|
| `Top Tables by Dead Tuples` | `Pending` | Present in report; not yet checked line by line in this tranche. |
| `Top Tables by Size and Storage Split` | `Validated` | Rendered columns populated for large tables. |
| `Top TOAST Tables by Size and Candidate Columns` | `Validated` | Rendered columns populated; candidate-wide-columns column renders multiline content correctly. |
| `Tables Where Index Size Exceeds Heap` | `Validated` | All columns populated. |
| `HOT Update Efficiency by Table` | `Validated` | Columns populated, though current dataset shows demo-driven narrow coverage. |
| `Analyze Freshness by Table` | `Validated (empty-state)` | Empty-state row `No analyze-freshness signals found` is structurally correct. |
| `Table Storage and Autovacuum Overrides` | `Validated (empty-state)` | Empty-state row `No table-level storage or autovacuum overrides found` is structurally correct. |
| `XID Wraparound Risk` | `Validated` | Columns populated. |
| `Sequence Synchronization Check` | `Validated` | Columns populated, including `VERIFY` cases and manual-check messaging. Later semantic review still needed. |
| `Trigger Inventory` | `Validated (empty-state)` | Empty-state row `No triggers found` now renders correctly when the trigger inventory is empty. |
| `Tables Without Primary Keys` | `Validated (empty-state)` | Empty-state row `All tables have primary keys` is structurally correct. |
| `Freeze Age and Wraparound Countdown` | `Validated` | Columns populated. |
| `Autovacuum Effectiveness and Backlog` | `Validated` | Columns populated for current dataset. |
| `Table Churn Rate Profile (Insert/Update/Delete)` | `Validated` | Columns populated for current dataset. |
| `Table-Health Actions` | `Validated` | Summary finding block renders with populated counts. |


## Script Safety & Security Review
| Check | Result | Notes |
|---|---|---|
| `Database mutability` | `Validated` | `pg360.sql` starts a read-only transaction and completed successfully under an explicit outer `SET TRANSACTION READ ONLY` wrapper. |
| `High-load guardrail` | `Validated` | Script requires explicit acknowledgment and blocks by default on elevated-load preflight unless overridden. |
| `Shell / server-file execution` | `Validated` | No `\!`, `\copy`, `\gexec`, `pg_read_file()`, `pg_ls_dir()`, or plaintext-credential access paths found. |
| `Share-safe redaction mode` | `Validated` | Optional `-v pg360_share_safe=on` now redacts identities, app names, client endpoints, and configured paths in rendered output. |
| `Report sensitivity notice` | `Validated` | Report header now states whether share-safe mode is enabled and reminds operators that remediation SQL is not executed by PG360. |
| `Residual privacy risk` | `Needs deeper semantic review` | Query text and object names still remain visible; share-safe mode is safer for sharing, but not a full public-distribution scrub. |
| `Content Security Policy` | `Known caveat` | Local HTML report still uses inline CSS/JS with `unsafe-inline`; acceptable for local artifacts, not ideal for hosted hardening. |

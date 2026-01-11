# Post-First-Hydration Improvement Plan

**Branch:** `post-first-hydration-1`  
**Target Merge:** `main` after validating the full lake fill and post-run enhancements  
**Repository Scope:** Bronze/raw-layer ingestion for synthetic ecommerce datasets (no Silver/Gold automation in this repo)

---

## 1. Background & Incident Log

| Timestamp (local)   | Stage                  | Outcome                 | Notes                                                                                                                                                             |
| ------------------- | ---------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Oct 19, ~18:15      | Backlog Bear launch    | Started at `2022-08-03` | `START_DATE` used legacy value; reran historical partitions.                                                                                                      |
| Oct 20, 00:30–09:00 | Generator/Export loops | Succeeded               | Multiple artifacts generated; exports produced manifests + `_SUCCESS`.                                                                                            |
| Oct 20, 09:05       | Upload                 | **Failure**             | `google.auth.exceptions.RefreshError`: ADC token expired mid-run. Manual `gcloud auth application-default login` required.                                        |
| Oct 20, 12:40       | Upload                 | **Failure**             | `requests.exceptions.ConnectionError` from GCS (`Connection reset by peer`) during multipart upload for `2024-01-04`. Partial partitions left without `_SUCCESS`. |
| Oct 20, 13:10       | Manual remediation     | Succeeded               | Identified last good ingest date by listing GCS directories; reissued targeted `ecomlake upload-raw` for missing tables; updated script `START_DATE`.             |

**Root Causes**
- Authentication tokens expire during long-running uploads; no automated re-auth or retry guard.
- Network interruptions stop the script without retrying individual tables.
- Backlog Bear does not persist progress, requiring manual `START_DATE` edits.
- Limited observability/log structure slows troubleshooting.

---

## 2. Objectives (What Success Looks Like)

1. **Resumable automation:** Backlog Bear can recover from mid-run failures without editing the script manually.
2. **Resilient uploads:** CLI upload command automatically retries transient failures and verifies completion.
3. **Actionable logging & help:** Operators can see command catalog, log levels, and targeted guidance from `--help`.
4. **Manifest & QA confidence:** Export stage surfaces row-count parity issues and makes hook usage clearer.
5. **Clean documentation footprint:** Legacy agent docs deprecated; plan and generator content organized for discoverability.
6. **Source-agnostic ingestion:** Any producer that honors a shared schema contract can feed the exporter/uploader without code changes.

---

## 3. Component Backlog

| Area                                           | Current State                                                                   | Improvement Tasks                                                                                                                                                                                         | Dependencies                                   |
| ---------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| **CLI (`src/ecom_datalake_extension/cli.py`)** | Monolithic, ~400 lines, limited logging                                         | Split subcommands into `cli/run.py`, `cli/export.py`, `cli/upload.py`; central logging init; add `--log-level`, `ecomlake commands`; wrap known exceptions with human-readable advice.                    | None (Click already in use)                    |
| **Uploader (`gcs_uploader.py`)**               | Single try upload, no verification                                              | Add `tenacity`-based retry decorator (3 attempts, exponential backoff); optional `validate=True` to check `_SUCCESS`; enrich `UploadResult` with bytes/files count.                                       | `tenacity` (new lightweight dep)               |
| **Parquet Writer & Manifest**                  | Writes lineage + manifests, silent on mismatches                                | Log chunk stats, enforce sum(rows) parity, surface min/max event dates; provide optional `validate-manifest` helper.                                                                                      | Existing pandas/pyarrow stack                  |
| **Hooks (`hooks.py`)**                         | Dynamic import + execution, minimal docs                                        | Document hook contract, ship example QA hook, allow typed base class (optional).                                                                                                                          | None                                           |
| **Backlog Bear Script**                        | Manual `START_DATE`, deletes artifacts even if uploads fail                     | Persist last successful ingest (`state/backlog_bear_state.json`), add upload retry loop per table/date, trap to skip cleanup on failure, pass through `--dry-run`, timestamped logging.                   | `jq` not required; use Python helper for state |
| **Docs**                                       | Generator docs in mixed locations                                               | ;relocate generator docs to dedicated folder; add troubleshooting + CLI reference to README.                                                                                                              | File moves only                                | Update docs, add CLI Reference Guide |
| **Source Adapter Contract**                    | Exporter implicitly assumes generator CSV layout; no contract for new producers | Define schema YAML + Pydantic models, extend configs with `source.type` + adapter-specific settings, register generator as default adapter, scaffold OLTP adapter entry point, document adapter workflow. | Builds on CLI refactor groundwork              |

---

## 4. Implementation Phases

### Phase 0 – Documentation & State Prep (CURRENT)
- Publish this plan and reorganize docs (generator folder, CLI Guide.)
- Add troubleshooting summary to README if needed.
- Outcome: shared blueprint for engineering work.

### Phase 1 – Backlog Bear Resiliency (2–3 days)
- Implement progress state file and restart logic.
- Add upload retries in the bash script, with optional targeted replays.
- Tests: simulate intentional failure (kill upload) and confirm resume picks up next ingest date.

### Phase 2 – CLI & Upload Hardening (3–4 days)
- Refactor CLI modules; integrate logging options and exception wrapping.
- Introduce `tenacity` retry for uploads and optional post-upload validation.
- Tests: pytest cases for retry counts, CLI help coverage, integration run using temp dirs.

### Phase 3 – Source Schema & Adapter Abstraction (3 days)
- Author schema contract files plus Pydantic validator; enforce schema presence during export.
- Add `source` config block + adapter registry (generator default, OLTP adapter stub) so producers emit standardized artifacts.
- Update CLI + README docs to explain adapter usage and source-agnostic workflows.
- Tests: cover schema loading, adapter dispatch, and backward compatibility for existing generator runs.

### Phase 4 – Data QA Enhancements (2 days)
- Manifest parity checks and logging.
- Example post-export hook; ensure existing hooks unaffected.
- Tests: expand Parquet writer unit tests; add manifest mismatch scenario.

### Phase 5 – Final Validation & Merge (1–2 days)
- Run short Backlog Bear dry-run (e.g., `2024-02-01..2024-02-03`); confirm `_SUCCESS` per table.
- Update changelog, README sections, ensure docs reference new behavior.
- Obtain sign-off before merging `post-first-hydration-1` into `main`.

---

## 5. Testing & Verification Matrix

| Layer         | Tooling                     | Scenarios                                                                                                                                      |
| ------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Unit          | `pytest`                    | Uploader retry/backoff, manifest parity, hook loading failures, state helper functions.                                                        |
| Integration   | `pytest` + temp directories | `ecomlake export-raw` end-to-end, verifying manifest + `_SUCCESS`; CLI `--log-level` and `--table` filters.                                    |
| Resilience    | Manual + scripted           | Simulate ADC expiry (force env var removal) to confirm error messaging; kill network mid-upload to confirm tenacity retries and script resume. |
| Acceptance    | Manual run                  | Backlog Bear chunk run with state file and automatic restart; inspect GCS for expected partitions.                                             |
| Documentation | Snapshot or manual review   | Ensure README hyperlinks updated, CLI/source-adapter docs published, help output matches references.                                           |

---

## 6. Documentation Actions

1. **Generator Docs Relocation:** Completed — generator docs now live under `docs/resource_hub/ecom_generator/`.
2. **Plan Publication:** `docs/post_first_hydration_improvement_plan.md` (this file).
3. **CLI Reference Doc:** Publish `docs/resource_hub/datalakes_extention/CLI_REFERENCE.md` as the authoritative command/options catalog generated from the refactored CLI.
4. **Source Adapter Guide:** Document schema contract + adapter configuration (README + resource hub) once Phase 3 ships.
5. **Future Update Hooks:** When CLI refactor lands, capture command list snippets and logging guidance in README + plan.

---

## 7. Dependencies & Tooling Notes

- **New dependency proposal:** `tenacity` (MIT license, minimal footprint) for upload retries.
- **Logging:** Stick with Python `logging` (no extra packages). Default formatter remains human-friendly text (emoji-safe); add `--log-format {text,json}` to toggle JSON output for production use.
- **State persistence:** Store as simple JSON (`{"last_successful_ingest": "YYYY-MM-DD"}`) under `state/`.
- **Retry tuning:** Implement three-attempt exponential backoff (5s, 15s, 35s) for uploads by default; allow overrides via env vars (`ECOMLAKE_UPLOAD_RETRIES`, `ECOMLAKE_UPLOAD_BACKOFF_BASE`) for power users.
- **No heavy frameworks** (e.g., Airflow, Prefect) introduced in this repo; keep scope focused on Bronze ingestion.

---

## 8. Risk & Mitigation

| Risk                                 | Mitigation                                                                   |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| Retry loops hide systemic failures   | Cap retry count; escalate error after final attempt with clear message.      |
| State file corruption                | Validate JSON before use; fall back to manual `START_DATE` if parsing fails. |
| CLI refactor causes breaking changes | Preserve command names/options; add regression tests for legacy arguments.   |
| Doc reorg breaks links               | Update README and relative links; run link check (manual or CI).             |

---

## 9. Acceptance Criteria (Before Merge to `main`)

- Backlog Bear resumes from state without manual edits; verified in dry-run.
- CLI upload retries transient network errors automatically; logs indicate attempts.
- README references new logging/resume guidance; deprecated docs point to replacements.
- `pytest` suite updated and passing; manual GCS inspection shows `_SUCCESS` for test range.
- Stakeholder review approves plan execution and documentation updates.

---

## 10. Open Questions & Follow-Up

1. Confirm final module structure for CLI refactor (`src/ecom_datalake_extension/cli/` vs. separate top-level modules).  
2. Decide on default log format (plain text vs. JSON) to align with upcoming `gcs-automation-project`.  
3. Determine whether manifest validation should become its own CLI command or remain a hook/example.  



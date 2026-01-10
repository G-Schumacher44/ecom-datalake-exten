<p align="center">
  <img src="../../img/datalakes_banner.png" width="1000"/>
  <br>
  <em>Generate · Partition · Publish — your synthetic lake delivery kit.</em>
</p>

<p align="center">
  <a href="https://github.com/G-Schumacher44/ecom-datalake-exten/actions/workflows/ci.yml">
    <img alt="CI" src="https://github.com/G-Schumacher44/ecom-datalake-exten/actions/workflows/ci.yml/badge.svg?branch=main&event=push">
  </a>
  <img alt="MIT License" src="https://img.shields.io/badge/license-MIT-blue">
  <img alt="Status" src="https://img.shields.io/badge/status-alpha-lightgrey">
  <a href="https://github.com/G-Schumacher44/ecom-datalake-exten/releases/latest">
    <img alt="Release" src="https://img.shields.io/github/v/release/G-Schumacher44/ecom-datalake-exten?display_name=tag">
  </a>
</p>

# CLI Reference Guide

Authoritative command catalog for the `ecomlake` CLI. Use this as the single source of truth when
running local hydrations, automation scripts, or building orchestrations. Options marked **(planned)**
will land during the post-first-hydration improvements documented in
`docs/post_first_hydration_improvement_plan.md`.

---

## 📚 Table of Contents

- [Global Usage](#global-usage)
  - [Global Options](#global-options)
  - [Environment Variables](#environment-variables)
- [Command Index](#command-index)
  - [Typical Flow](#typical-flow)
- [`ecomlake run-generator`](#ecomlake-run-generator)
- [`ecomlake export-raw`](#ecomlake-export-raw)
- [`ecomlake upload-raw`](#ecomlake-upload-raw)
- [Planned Enhancements Summary](#planned-enhancements-summary)

---

## Global Usage

```bash
ecomlake [OPTIONS] COMMAND [ARGS]...
```

### Global Options

| Option                                   | Description                                                                   | Notes                            |
| ---------------------------------------- | ----------------------------------------------------------------------------- | -------------------------------- |
| `--help`                                 | Show top-level help.                                                          | Includes registered subcommands. |
| `--log-level` **(planned)**              | Override default logging level (`INFO`, `DEBUG`, etc.).                       | Defaults to `INFO`.              |
| `--log-format {text,json}` **(planned)** | Switch between human-readable logs (default) and Cloud Logging–friendly JSON. | Planned for Phase 2.             |

### Environment Variables

| Variable                                     | Purpose                                           | Current Status             |
| -------------------------------------------- | ------------------------------------------------- | -------------------------- |
| `ECOMLAKE_UPLOAD_RETRIES` **(planned)**      | Override default retry attempts for uploads.      | Defaults to `3`.           |
| `ECOMLAKE_UPLOAD_BACKOFF_BASE` **(planned)** | Base seconds for exponential backoff (e.g., `5`). | Combined with retry count. |
| `GOOGLE_APPLICATION_CREDENTIALS`             | Path to service-account JSON for GCS uploads.     | Supported today.           |

---

## Command Index

| Command         | Summary                                                                           |
| --------------- | --------------------------------------------------------------------------------- |
| `run-generator` | Invoke the upstream `ecom_sales_data_generator` to produce CSV artifacts.         |
| `export-raw`    | Convert generator CSVs to Hive-partitioned Parquet with manifests and `_SUCCESS`. |
| `upload-raw`    | Publish local partitions to Google Cloud Storage.                                 |

### Typical Flow

1. **Generate** – `ecomlake run-generator --config <yaml>` (optionally point `--generator-src` at a local checkout) to create `raw_run_<timestamp>` artifacts.
2. **Transform** – `ecomlake export-raw --source <raw_run>` to materialize Parquet partitions, manifests, and `_SUCCESS` markers for the ingest dates you care about.
3. **Publish** – `ecomlake upload-raw --bucket <name> --ingest-date YYYY-MM-DD` to move validated partitions to cloud storage.

Cross-reference the Lake Config Guide for tuning inputs before step 1 and the workflow playbooks when you want to automate the loop.

---

## `ecomlake run-generator`

Runs the generator and writes CSVs plus QA output into an artifact directory.

```bash
ecomlake run-generator --config PATH [OPTIONS]
```

| Option                                                                | Required | Default     | Description                                                                           |
| --------------------------------------------------------------------- | -------- | ----------- | ------------------------------------------------------------------------------------- |
| `--config PATH`                                                       | ✅        | —           | YAML config consumed by `ecom_sales_data_generator`.                                  |
| `--artifact-root PATH`                                                | ❌        | `artifacts` | Parent directory for generator runs (each run gets `raw_run_<timestamp>`).            |
| `--messiness-level {baseline,none,light_mess,medium_mess,heavy_mess}` | ❌        | `baseline`  | Controls data imperfections injected during generation.                               |
| `--generator-src PATH`                                                | ❌        | `None`      | Path to generator source if not installed (e.g., `../ecom_sales_data_generator/src`). |

**Output:** new directory `ARTIFACT_ROOT/raw_run_<UTC timestamp>` with CSV files, QA logs, and SQL loader script.

---

## `ecomlake export-raw`

Transforms CSV artifacts into Hive-style Parquet partitions with manifests and optional post-export hooks.

```bash
ecomlake export-raw --source PATH [OPTIONS]
```

| Option                               | Required | Default                  | Description                                                                                 |
| ------------------------------------ | -------- | ------------------------ | ------------------------------------------------------------------------------------------- |
| `--source PATH`                      | ✅        | —                        | Directory produced by `run-generator`.                                                      |
| `--target PATH`                      | ❌        | `output/raw`             | Root directory for Parquet output.                                                          |
| `--ingest-date YYYY-MM-DD`           | ❌        | today                    | Export a single ingest date. Mutually exclusive with range options.                         |
| `--start-date YYYY-MM-DD`            | ❌        | —                        | Start date for a range of ingest dates. Requires `--end-date` or/and `--days`.              |
| `--end-date YYYY-MM-DD`              | ❌        | —                        | End date (inclusive) for range exports.                                                     |
| `--days INT`                         | ❌        | `None`                   | Number of consecutive days to export starting from `--start-date`.                          |
| `--dates LIST`                       | ❌        | —                        | Comma-separated list of specific ingest dates.                                              |
| `--batch-id TEXT`                    | ❌        | auto                     | Override auto-generated batch identifier.                                                   |
| `--target-size-mb INT`               | ❌        | `DEFAULT_TARGET_SIZE_MB` | Desired Parquet file size; used to calculate chunk rows.                                    |
| `--table TABLE`                      | ❌        | all tables               | Repeatable option to restrict export to specific tables.                                    |
| `--source-prefix TEXT`               | ❌        | `None`                   | URI prefix recorded in the `source_file` lineage column.                                    |
| `--lookups-from PATH`                | ❌        | —                        | Directory with static lookup CSVs (customers.csv, product_catalog.csv) for dimension export. |
| `--post-export-hook module:function` | ❌        | —                        | Repeatable hook invoked after each partition (QA, metrics, etc.).                           |

**Artifacts per table/date:**

Transactional tables (orders, shopping_carts, returns):
- `table/ingest_dt=YYYY-MM-DD/part-0000.parquet`
- `_MANIFEST.json` with file list, row counts, checksums
- `_SUCCESS` marker

Dimension tables (when using `--lookups-from`):

- `customers/signup_date=YYYY-MM-DD/part-0000.parquet` (partitioned by customer signup date)
- `product_catalog/category=CategoryName/part-0000.parquet` (partitioned by product category)
- `_MANIFEST.json` and `_SUCCESS` for each partition

**Example with dimension table export:**

```bash
# Export dimensions from static lookups on first chunk
ecomlake export-raw \
  --source artifacts/raw_run_20251019T173945Z \
  --target output/raw \
  --ingest-date 2024-02-15 \
  --lookups-from artifacts/static_lookups

# Results in:
# - customers/signup_date=2019-01-01/part-0000.parquet
# - customers/signup_date=2019-01-02/part-0000.parquet
# - ... (2,562 customer partitions for 2019-2026)
# - product_catalog/category=Electronics/part-0000.parquet
# - product_catalog/category=Books/part-0000.parquet
# - ... (5 category partitions)
# - orders/ingest_dt=2024-02-15/part-0000.parquet
# - shopping_carts/ingest_dt=2024-02-15/part-0000.parquet
# - returns/ingest_dt=2024-02-15/part-0000.parquet
```

---

## `ecomlake upload-raw`

Uploads local partitions to a GCS bucket, one ingest date at a time.

```bash
ecomlake upload-raw --bucket NAME --ingest-date YYYY-MM-DD [OPTIONS]
```

| Option                     | Required | Default        | Description                                                  |
| -------------------------- | -------- | -------------- | ------------------------------------------------------------ |
| `--source PATH`            | ❌        | `output/raw`   | Local Parquet root.                                          |
| `--bucket TEXT`            | ✅        | —              | Destination GCS bucket (without `gs://`).                    |
| `--prefix TEXT`            | ❌        | `ecom/raw`     | Prefix inside the bucket (`<prefix>/<table>/ingest_dt=...`). |
| `--ingest-date YYYY-MM-DD` | ✅        | —              | Hive partition date to upload.                               |
| `--table TABLE`            | ❌        | all tables     | Repeatable filter to upload only selected tables.            |
| `--dry-run / --no-dry-run` | ❌        | `--no-dry-run` | Print actions without uploading.                             |

**Runtime Behavior**
- Validates local partition directories before uploading.
- Uses Application Default Credentials or service-account JSON.
- Planned: automatic retries (3 attempts, exponential backoff) with optional verification that `_SUCCESS` exists on GCS.

---

## Planned Enhancements Summary

These items are defined in the improvement plan and will be added in upcoming sprints:

- Global `--log-level` and `--log-format {text,json}` flags.
- Tenacity-backed upload retries with env-var overrides.
- Optional post-upload validation (check for `_SUCCESS`).
- CLI-generated Markdown updates synced to this reference after each release.

Refer to `docs/post_first_hydration_improvement_plan.md` for implementation status and timelines.

<p align="center">
  <a href="../../../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="CONFIG_GUIDE.md">⚙️ <b>Lake Config</b></a>
  &nbsp;·&nbsp;
  <a href="CLI_REFERENCE.md">🧭 <b>CLI Reference</b></a>
  &nbsp;·&nbsp;
  <a href="TESTING_GUIDE.md">🧪 <b>Testing</b></a>
  &nbsp;·&nbsp;
  <a href="workflows/BACKLOG_BEAR.md">🧸 <b>Workflows</b></a>
  &nbsp;·&nbsp;
  <a href="../ecom_generator/CONFIG_GUIDE.md">🛠️ <b>Generator Config</b></a>
</p>

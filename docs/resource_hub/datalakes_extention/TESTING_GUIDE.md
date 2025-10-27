<p align="center">
  <img src="../../img/datalakes_banner.png" width="1000"/>
  <br>
  <em>Testing playbook for the ecom-datalake-extension</em>
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

---

# 🧪 Testing & Validation Guide

> Your quick reference for verifying the CLI, Parquet exporter, and GCS upload flow before promoting changes downstream.

---

## 📚 Table of Contents
- [🧪 Testing & Validation Guide](#-testing--validation-guide)
  - [🔧 Prerequisites](#-prerequisites)
  - [✅ Core Test Suite](#-core-test-suite)
  - [🧹 Lint & Format](#-lint--format)
  - [🚀 Smoke Test Workflow](#-smoke-test-workflow)
  - [🔁 Multi-date & Hooks Check](#-multi-date--hooks-check)
  - [☁️ Upload Dry Run](#️-upload-dry-run)
  - [📦 Release Checklist](#-release-checklist)

---

## 🔧 Prerequisites

```bash
conda activate ecom-datalake-exten
pip install -e '.[dev]'
pip install -e '../ecom_sales_data_generator'  # or install from git
```

If you use `pre-commit`, make sure it is installed locally:

```bash
pip install pre-commit
pre-commit install
```

---

## ✅ Core Test Suite

Run unit tests (no external services touched):

```bash
pytest
```

The suite covers:
- Parquet writer lineage columns and checksum metadata.
- CLI export command (single and multi-date) + manifest content.
- GCS uploader (mocked) to ensure correct object paths.
- Post-export hook execution with a temporary module.

---

## 🧹 Lint & Format

Keep the codebase CI-compliant:

```bash
ruff check src tests
black --check src tests
```

To autoformat locally, run `black src tests`. Ruff fixes: `ruff check src tests --fix`.

---

## 🚀 Smoke Test Workflow

Use the quick config to generate a small backlog and push through the CLI:

```bash
# 1. Generate CSVs
ecomlake run-generator \
  --generator-src ../ecom_sales_data_generator/src \
  --config gen_config/ecom_sales_gen_quick.yaml \
  --artifact-root artifacts \
  --messiness-level none

# 2. Export Parquet (single date)
ecomlake export-raw \
  --source artifacts/raw_run_<TIMESTAMP> \
  --target output/raw \
  --ingest-date 2024-02-15 \
  --target-size-mb 10
```

✅ Confirm `_SUCCESS` + `_MANIFEST.json` exist under `output/raw/<table>/ingest_dt=2024-02-15/` and that manifest files show checksums and row counts.

---

## 🔁 Multi-date & Hooks Check

```bash
ecomlake export-raw \
  --source artifacts/raw_run_<TIMESTAMP> \
  --target output/raw \
  --dates 2024-02-15,2024-02-16 \
  --post-export-hook analytics.metrics:record_partition
```

- Ensure both partitions are written and the hook output fires (e.g., logs or custom file).
- Use a temporary module like the test suite (`tests/test_cli_export.py`) demonstrates if you need a sample hook.

---

## ☁️ Upload Dry Run

```bash
gcloud auth application-default login

ecomlake upload-raw \
  --source output/raw \
  --bucket gcs-automation-project-raw \
  --prefix ecom/raw \
  --ingest-date 2024-02-15 \
  --dry-run
```

Check the console output to verify object paths before removing `--dry-run` for an actual upload.

---

## 📦 Release Checklist

- [ ] `pytest`
- [ ] `ruff check src tests`
- [ ] `black --check src tests`
- [ ] Optional: `python -m build`
- [ ] Optional: `pre-commit run --all-files`
- [ ] Backlog Bear workflow playbook updated if automation changed
- [ ] README + resource hub docs updated to reflect new behavior

Once green locally, CI (`.github/workflows/ci.yml`) mirrors these steps upon PR/push.

---

Need deeper diagnostics? See individual test modules under `tests/` for more targeted scenarios.

<p align="center">
  <sub>✨ Synthetic Data · Python · QA Framework ✨</sub>
</p>

<p align="center">
  <a href="../../../README.md">🏠 <b>Home</b></a>
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

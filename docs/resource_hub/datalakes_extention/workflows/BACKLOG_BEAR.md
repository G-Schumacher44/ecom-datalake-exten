<p align="center">
  <img src="../../../img/backlog_bear_banner.png" width="1000"/>
  <br>
  <em>6-year raw-zone backlog automation workflow with static lookup strategy.</em>
</p>

---

# 🧸 Backlog Bear Workflow Playbook

This playbook documents the Backlog Bear workflow for hydrating the bronze/raw zone with six years of
daily partitions. It captures the exact steps the automation script executes so you can run it
confidently and repeatably, with guaranteed data continuity across all partitions.

---

## 🎯 Objective

- Generate **static customer and product catalogs** for 300K customers and 3K products (2019-2026 signup dates)
- Generate medium mess synthetic CSVs with the ecom generator in 30-day chunks
- Convert each chunk into date-partitioned Parquet with manifests
- Upload those partitions to the raw GCS bucket (`ecom/raw`)
- Maintain sequential ID continuity across all chunks
- Clean up local staging artifacts between iterations

By default the playbook covers **2020-01-01 -> 2026-01-08** (6+ years) in **30-day** chunks, generating ~2,200 daily partitions.

---

## ✅ Prerequisites

```bash
conda activate ecom-datalake-exten
pip install -e '.[gcs]'
pip install -e '../ecom_sales_data_generator'  # or install from git
gcloud auth application-default login          # or export GOOGLE_APPLICATION_CREDENTIALS
```

Also ensure the destination bucket (`gs://gcs-automation-project-raw/ecom/raw`) exists and that you have
enough local disk space for temporary Parquet output (`artifacts/`, `output/raw/`).

---

## ⚙️ Script Location & Parameters

The automation lives at `scripts/backlog_bear.sh` and ships with sensible defaults:

| Variable                  | Description                                    | Default                                |
| ------------------------- | ---------------------------------------------- | -------------------------------------- |
| `CONFIG_PATH`             | Generator YAML driving behavior                | `gen_config/ecom_sales_gen_quick.yaml` |
| `ARTIFACT_ROOT`           | Location for generator CSV runs                | `artifacts`                            |
| `TARGET_ROOT`             | Parquet landing zone prior to upload           | `output/raw`                           |
| `BUCKET`                  | Raw bucket (no `gs://`)                        | `gcs-automation-project-raw`           |
| `PREFIX`                  | Path prefix inside the bucket                  | `ecom/raw`                             |
| `MESSINESS_LEVEL`         | Generator realism level                        | `medium_mess`                          |
| `START_DATE` / `END_DATE` | Six-year range                                 | `2020-01-01` → `2026-01-08`            |
| `CHUNK_SIZE`              | Days per iteration (30-day chunks)             | `30`                                   |
| `ID_STATE_FILE`           | Sequential ID checkpoint file                  | `artifacts/.id_state.json`             |
| `CHECKPOINT_FILE`         | Resume checkpoint for backfill progress        | `artifacts/.backlog_checkpoint`        |
| `POST_EXPORT_HOOK`        | Optional hook for extra manifest handling      | `""`                                   |

Tune them at the top of the script before launch.

### 🔑 Key Features

- **Static Lookups**: 
  - 300K customers generated across a 7-year period (2019-2026).
  - Product Catalog with 3K products generated once, reused across all chunks
- **Sequential IDs**: Cart, order, and return IDs maintain continuity across all 73 chunks via state file
- **Checkpoint Resume**: Automatically resumes from last completed date if interrupted
- **Date Filtering**: Each table partitioned by its event date (orders by order_date, carts by created_at, etc.)
- **Parent-Child Integrity**: Child tables (order_items, return_items) filtered via JOIN with parent tables

---

## 🤖 Running the Workflow

### Quick Start (Recommended)

```bash
# 1. Ensure config is set for 300K customers and 3K products
grep -E "num_customers|num_products" gen_config/ecom_sales_gen_quick.yaml

# 2. Generate static lookups (one-time, ~5-10 minutes)
rm -rf artifacts/static_lookups
./scripts/generate_static_lookups.sh

# 3. Run the full 6-year backfill
# Optional: use tmux/screen for long-running jobs
tmux new -s backlog_bear
caffeinate -dims &  # Keep Mac awake (macOS only)

./scripts/backlog_bear.sh

# Or run in background with logging
nohup ./scripts/backlog_bear.sh > backlog_bear.log 2>&1 &
tail -f backlog_bear.log
```

### What Happens During Execution

For each 30-day chunk (74 chunks total):

1. **Check Static Lookups** - Uses existing `artifacts/static_lookups/` or generates if missing
2. **Initialize ID State** - Creates `artifacts/.id_state.json` if needed (tracks last cart_id, order_id, return_id)
3. **Generate CSVs** - Runs `ecomlake run-generator` with `--load-lookups-from` and `--id-state-file`
4. **Export to Parquet** - Converts CSVs to date-partitioned Parquet with intelligent partitioning:
   - **Dimension tables** (first chunk only):
     - Customers: Partitioned by `signup_date` (2,562 partitions for 2019-2026)
     - Products: Partitioned by `category` (5 partitions: Books, Clothing, Electronics, Home, Toys)
     - Exported once using `--lookups-from artifacts/static_lookups`
   - **Transactional tables** (every chunk):
     - Orders, carts, returns: Partitioned by event date (`ingest_dt=YYYY-MM-DD`)
     - Child tables (order_items, cart_items, return_items): Filtered via parent table JOIN
5. **Upload to GCS** - Uploads partitions with manifests to `gs://gcs-automation-project-raw/ecom/raw`
6. **Clean Up** - Removes local CSV and Parquet files to save disk space
7. **Save Checkpoint** - Records completed date in `artifacts/.backlog_checkpoint`
8. **Update ID State** - Persists last used IDs for next chunk

**Estimated Runtime:** 5-8 hours for full 6-year run (depends on system specs)

### Resuming After Interruption

The script automatically resumes from the last checkpoint:

```bash
# Check where it stopped
cat artifacts/.backlog_checkpoint

# Just run again - it will resume automatically
./scripts/backlog_bear.sh
```

---

## 🧪 Smoke Test (Short Run)

Use the smoke test script to validate generator wiring, static lookups, and upload flow on a small date range.
It creates `scripts/backlog_bear_test.sh` with a reduced window and then runs it.

```bash
# Runs a short-range test and cleans up after completion
./scripts/smoke_test.sh
```

---

## 🧪 Verification Checklist

### Dimension Tables (Static, Business-Attribute Partitioned)

```bash
# Set your bucket and prefix once for all checks
export BUCKET="gcs-automation-project-raw"
export PREFIX="ecom/raw"

# Preflight: verify auth and path
gcloud auth list
gsutil ls "gs://${BUCKET}/${PREFIX}/"

# Check customer partitions (should be 2,562 signup_date partitions)
gsutil ls "gs://${BUCKET}/${PREFIX}/customers/" | wc -l

# Check product partitions (should be 5 category partitions)
gsutil ls "gs://${BUCKET}/${PREFIX}/product_catalog/"

# Sample a customer partition
gsutil cat "gs://${BUCKET}/${PREFIX}/customers/signup_date=2020-01-15/_MANIFEST.json" | jq
```

### Transactional Tables (Date Partitioned by ingest_dt)

```bash
# Check orders partitions (should be ~2,200 ingest_dt partitions)
gsutil ls -r "gs://${BUCKET}/${PREFIX}/orders" | head

# Verify manifests exist
gsutil ls "gs://${BUCKET}/${PREFIX}/orders/ingest_dt=2020-01-01/_MANIFEST.json"

# Spot-read Parquet locally
python -c "import pandas as pd; print(pd.read_parquet('output/raw/orders/ingest_dt=2020-01-01/part-0000.parquet').head())"
```

### Storage Footprint

```bash
gcloud storage du gs://gcs-automation-project-raw --recursive --summarize --human-readable
```

Expect ~17 GB of Parquet after the full six-year backlog (for example, 16.96 GB in the 2026-01-09 report),
costing well under $1/month in most regions.

---

## 🚨 Troubleshooting & Tuning

- **Slow progress?** Increase `CHUNK_SIZE` to process multiple weeks at once or cut generator volumes
  inside `gen_config/ecom_sales_gen_quick.yaml`.
- **Want CSV runs preserved?** Comment out the `rm -rf "$latest_run"` line and manage disk space manually.
- **Dry-run uploads:** add `--dry-run` to the `ecomlake upload-raw` call inside the loop.
- **Authentication issues:** rerun `gcloud auth application-default login` or point `GOOGLE_APPLICATION_CREDENTIALS` at a service-account key.

___

_Future workflows (silver transforms, QA sweeps, scheduled refreshes) belong in this `workflows/` hub
alongside Backlog Bear._

<p align="center">
  <a href="../../../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../CONFIG_GUIDE.md">⚙️ <b>Lake Config</b></a>
  &nbsp;·&nbsp;
  <a href="../CLI_REFERENCE.md">🧭 <b>CLI Reference</b></a>
  &nbsp;·&nbsp;
  <a href="../TESTING_GUIDE.md">🧪 <b>Testing</b></a>
  &nbsp;·&nbsp;
  <a href="workflows/BACKLOG_BEAR.md">🧸 <b>Workflows</b></a>
  &nbsp;·&nbsp;
  <a href="../ecom_generator/CONFIG_GUIDE.md">🛠️ <b>Generator Config</b></a>
</p>

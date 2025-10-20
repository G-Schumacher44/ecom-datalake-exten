#!/usr/bin/env bash
set -euo pipefail

# --- User parameters ---------------------------------------------------------
CONFIG_PATH="gen_config/ecom_sales_gen_quick.yaml"
ARTIFACT_ROOT="artifacts"
TARGET_ROOT="output/raw"
BUCKET="gcs-automation-project-raw"
PREFIX="ecom/raw"
MESSINESS_LEVEL="medium_mess"
START_DATE="2020-01-01"
END_DATE="2024-12-31"
CHUNK_SIZE=7         # number of consecutive days per export/upload cycle
POST_EXPORT_HOOK=""  # optional: e.g. "hooks.my_module:write_manifest_summary"
# -----------------------------------------------------------------------------

function date_to_epoch() {
  python - "$1" <<'PY'
import sys, datetime as dt
print(int(dt.date.fromisoformat(sys.argv[1]).toordinal()))
PY
}

function epoch_to_date() {
  python - "$1" <<'PY'
import sys, datetime as dt
print(dt.date.fromordinal(int(sys.argv[1])).isoformat())
PY
}

start_ord=$(date_to_epoch "$START_DATE")
end_ord=$(date_to_epoch "$END_DATE")
today_stamp=$(date +"%Y%m%dT%H%M%S")
batch_id="backlog-${today_stamp}"

echo "🚀 Backlog Bear starting (${START_DATE} → ${END_DATE}, batch=${batch_id})"

current_ord=$start_ord
while [ "$current_ord" -le "$end_ord" ]; do
  chunk_start=$(epoch_to_date "$current_ord")
  chunk_end_ord=$(( current_ord + CHUNK_SIZE - 1 ))
  if [ "$chunk_end_ord" -gt "$end_ord" ]; then
    chunk_end_ord=$end_ord
  fi
  chunk_end=$(epoch_to_date "$chunk_end_ord")

  echo "📦 Generating source CSVs for window ${chunk_start} → ${chunk_end}"
  ecomlake run-generator \
    --config "$CONFIG_PATH" \
    --artifact-root "$ARTIFACT_ROOT" \
    --messiness-level "$MESSINESS_LEVEL"

  latest_run=$(ls -dt "${ARTIFACT_ROOT}"/raw_run_* | head -n 1)

  echo "🧱 Exporting Parquet partitions (${chunk_start}..${chunk_end})"
  export_args=(
    --source "$latest_run"
    --target "$TARGET_ROOT"
    --start-date "$chunk_start"
    --end-date "$chunk_end"
    --batch-id "$batch_id"
    --target-size-mb 96
    --source-prefix "gs://${BUCKET}/${PREFIX}"
  )
  if [ -n "$POST_EXPORT_HOOK" ]; then
    export_args+=( --post-export-hook "$POST_EXPORT_HOOK" )
  fi
  ecomlake export-raw "${export_args[@]}"

  echo "☁️  Uploading partitions to gs://${BUCKET}/${PREFIX}"
  current_date="$chunk_start"
  while true; do
    ecomlake upload-raw \
      --source "$TARGET_ROOT" \
      --bucket "$BUCKET" \
      --prefix "$PREFIX" \
      --ingest-date "$current_date"
    if [ "$current_date" = "$chunk_end" ]; then
      break
    fi
    current_date=$( \
      python - "$current_date" <<'PY'
import sys, datetime as dt
print((dt.date.fromisoformat(sys.argv[1]) + dt.timedelta(days=1)).isoformat())
PY
    )
  done

  echo "🧹 Cleaning latest raw run directory $latest_run"
  rm -rf "$latest_run"

  current_ord=$(( chunk_end_ord + 1 ))
done

echo "🎉 Backlog Bear finished. Verify gs://${BUCKET}/${PREFIX} for ${START_DATE} → ${END_DATE}."

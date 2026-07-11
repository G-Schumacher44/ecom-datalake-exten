#!/usr/bin/env bash
set -euo pipefail

# --- User parameters ---------------------------------------------------------
CONFIG_PATH="gen_config/ecom_sales_gen_quick.yaml"
ARTIFACT_ROOT="artifacts"
TARGET_ROOT="output/raw"
BUCKET="${BUCKET:-acme-analytics-raw}"
PREFIX="ecom/raw"
MESSINESS_LEVEL="medium_mess"
START_DATE="2020-01-01"
END_DATE="2020-02-29"
CHUNK_SIZE=30        # number of consecutive days per export/upload cycle (increased from 7 for better return capture)
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

# Checkpoint file to track progress
CHECKPOINT_FILE="${ARTIFACT_ROOT}/.backlog_checkpoint"

# Track if we've exported dimension tables (only export once at start)
DIMENSIONS_EXPORTED_FILE="${ARTIFACT_ROOT}/.dimensions_exported"

# One-time setup: Generate static lookups if not exists
if [ ! -f "artifacts/static_lookups/customers.csv" ]; then
  echo "🏗️  Generating static lookups (300K customers, 3K products)..."
  echo "⚠️  NOTE: This is a one-time operation and may take 5-10 minutes"
  # TODO: Create scripts/generate_static_lookups.sh
  # For now, this will fail - the agent working on the generator repo needs to create this script
  if [ -f "scripts/generate_static_lookups.sh" ]; then
    ./scripts/generate_static_lookups.sh
    echo "✅ Static lookups generated"
  else
    echo "❌ ERROR: scripts/generate_static_lookups.sh not found"
    echo "   The generator repo agent needs to create this script first"
    exit 1
  fi
fi

# Initialize ID state file if not exists
ID_STATE_FILE="${ARTIFACT_ROOT}/.id_state.json"
if [ ! -f "$ID_STATE_FILE" ]; then
  echo "🆕 Initializing ID state file..."
  echo '{
  "last_cart_id": 0,
  "last_order_id": 0,
  "last_return_id": 0,
  "last_updated": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"
}' > "$ID_STATE_FILE"
  echo "✅ ID state file created at $ID_STATE_FILE"
fi

# Check if we have a previous checkpoint to resume from
if [ -f "$CHECKPOINT_FILE" ]; then
  last_completed=$(cat "$CHECKPOINT_FILE")
  last_completed_ord=$(date_to_epoch "$last_completed")
  resume_ord=$((last_completed_ord + 1))
  resume_date=$(epoch_to_date "$resume_ord")
  echo "📍 Found checkpoint: resuming from ${resume_date} (last completed: ${last_completed})"
  current_ord=$resume_ord
else
  echo "🚀 Backlog Bear starting (${START_DATE} → ${END_DATE}, batch=${batch_id})"
  current_ord=$start_ord
fi
while [ "$current_ord" -le "$end_ord" ]; do
  chunk_start=$(epoch_to_date "$current_ord")
  chunk_end_ord=$(( current_ord + CHUNK_SIZE - 1 ))
  if [ "$chunk_end_ord" -gt "$end_ord" ]; then
    chunk_end_ord=$end_ord
  fi
  chunk_end=$(epoch_to_date "$chunk_end_ord")

  # Retry loop for this chunk (max 3 attempts)
  MAX_RETRIES=3
  retry_count=0
  chunk_success=false

  while [ $retry_count -lt $MAX_RETRIES ] && [ "$chunk_success" = false ]; do
    if [ $retry_count -gt 0 ]; then
      echo "⚠️  Retry attempt $retry_count for chunk ${chunk_start} → ${chunk_end}"
      sleep 10  # Wait 10 seconds before retry
    fi

    (
      set -e  # Exit on any error within this subshell

      echo "📦 Generating source CSVs for window ${chunk_start} → ${chunk_end}"
      # Convert relative paths to absolute for generator
      STATIC_LOOKUPS_DIR="$(pwd)/artifacts/static_lookups"
      ecomlake run-generator \
        --config "$CONFIG_PATH" \
        --artifact-root "$ARTIFACT_ROOT" \
        --messiness-level "$MESSINESS_LEVEL" \
        --load-lookups-from "$STATIC_LOOKUPS_DIR" \
        --id-state-file "$ID_STATE_FILE" \
        --start-date "$chunk_start" \
        --end-date "$chunk_end"

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

      # Export dimension tables only on first chunk
      if [ ! -f "$DIMENSIONS_EXPORTED_FILE" ]; then
        echo "📊 First chunk: exporting dimension tables (customers by signup_date, products by category)"
        export_args+=( --lookups-from "$STATIC_LOOKUPS_DIR" )
      fi

      if [ -n "$POST_EXPORT_HOOK" ]; then
        export_args+=( --post-export-hook "$POST_EXPORT_HOOK" )
      fi
      ecomlake export-raw "${export_args[@]}"

      # Mark dimensions as exported after first successful chunk
      if [ ! -f "$DIMENSIONS_EXPORTED_FILE" ]; then
        echo "dimensions_exported" > "$DIMENSIONS_EXPORTED_FILE"
        echo "✅ Dimension tables exported (will not be re-exported in subsequent chunks)"
      fi

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

      echo "🧹 Cleaning uploaded local partitions (${chunk_start} → ${chunk_end})"
      current_date="$chunk_start"
      while true; do
        # Remove all table partitions for this date
        find "$TARGET_ROOT" -type d -name "ingest_dt=${current_date}" -exec rm -rf {} + 2>/dev/null || true
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
    )

    # Check if the chunk completed successfully
    if [ $? -eq 0 ]; then
      chunk_success=true
      # Save checkpoint after successful chunk
      echo "$chunk_end" > "$CHECKPOINT_FILE"
      echo "✅ Checkpoint saved: completed through ${chunk_end}"
    else
      retry_count=$((retry_count + 1))
      if [ $retry_count -lt $MAX_RETRIES ]; then
        echo "❌ Chunk failed, will retry..."
      fi
    fi
  done

  # If chunk still failed after all retries, exit
  if [ "$chunk_success" = false ]; then
    echo "❌ Chunk ${chunk_start} → ${chunk_end} failed after $MAX_RETRIES attempts"
    echo "💾 Progress saved to checkpoint. Run script again to resume from ${chunk_end}"
    exit 1
  fi

  current_ord=$(( chunk_end_ord + 1 ))
done

echo "🎉 Backlog Bear finished. Verify gs://${BUCKET}/${PREFIX} for ${START_DATE} → ${END_DATE}."

# Clean up checkpoint and dimensions marker files on successful completion
if [ -f "$CHECKPOINT_FILE" ]; then
  rm "$CHECKPOINT_FILE"
  echo "🧹 Checkpoint file cleaned up"
fi

if [ -f "$DIMENSIONS_EXPORTED_FILE" ]; then
  rm "$DIMENSIONS_EXPORTED_FILE"
  echo "🧹 Dimensions marker file cleaned up"
fi

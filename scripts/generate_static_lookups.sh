#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-gen_config/ecom_sales_gen_quick.yaml}"
OUTPUT_DIR="${2:-artifacts/static_lookups}"

# Spread customer signups across multiple years with weighted distribution
# Heavy signup activity in early years (2019-2020), declining over time
# This mimics real ecommerce: explosive early growth, then slower acquisition
# Customer signups from 2019 through early 2026, ending just before the data window ends
LOOKUP_START_DATE="${3:-2019-01-01}"
LOOKUP_END_DATE="${4:-2026-01-05}"

mkdir -p "$OUTPUT_DIR"

ecomgen \
  --config "$CONFIG_PATH" \
  --output-dir "$OUTPUT_DIR" \
  --messiness-level none \
  --generate-lookups-only \
  --start-date "$LOOKUP_START_DATE" \
  --end-date "$LOOKUP_END_DATE"

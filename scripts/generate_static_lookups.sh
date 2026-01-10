#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="${1:-gen_config/ecom_sales_gen_quick.yaml}"
OUTPUT_DIR="${2:-artifacts/static_lookups}"

# Use early dates so customers can shop throughout the 6-year window
# Customer signups spread across 2019, so they're active during 2020-2026
LOOKUP_START_DATE="${3:-2019-01-01}"
LOOKUP_END_DATE="${4:-2019-12-31}"

mkdir -p "$OUTPUT_DIR"

ecomgen \
  --config "$CONFIG_PATH" \
  --output-dir "$OUTPUT_DIR" \
  --messiness-level none \
  --generate-lookups-only \
  --start-date "$LOOKUP_START_DATE" \
  --end-date "$LOOKUP_END_DATE"

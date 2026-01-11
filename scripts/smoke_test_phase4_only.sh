#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Phase 4 Only Smoke Test - Export Date Filtering
# =============================================================================
# Tests ONLY the export date filtering fix (Phase 4) using existing CSV data
# This doesn't require generator repo updates (Phases 1, 2, 3, 5)
# =============================================================================

echo "🧪 Phase 4 Smoke Test - Export Date Filtering Only"
echo ""

# --- Configuration -----------------------------------------------------------
TEST_START="2020-01-01"
TEST_END="2020-01-03"  # Just 3 days for quick test

# --- Step 1: Check for existing CSV data -------------------------------------
echo "🔍 Step 1: Checking for existing CSV data to test with..."

latest_run=$(ls -dt artifacts/raw_run_* 2>/dev/null | head -n 1 || echo "")

if [ -z "$latest_run" ]; then
  echo "❌ No existing CSV data found in artifacts/"
  echo ""
  echo "You need to generate some CSV data first. Options:"
  echo ""
  echo "Option A: Install updated generator and run full smoke test:"
  echo "  cd ../ecom_sales_data_generator"
  echo "  pip install -e ."
  echo "  cd ../ecom-datalake-exten"
  echo "  ./scripts/smoke_test.sh"
  echo ""
  echo "Option B: Generate CSV with current generator (without new features):"
  echo "  ecomlake run-generator --start-date 2020-01-01 --end-date 2020-01-03"
  echo "  Then run this script again"
  echo ""
  exit 1
fi

echo "✅ Found existing CSV data: $latest_run"
echo ""

# --- Step 2: Clean previous parquet output -----------------------------------
echo "🧹 Step 2: Cleaning previous parquet output..."
rm -rf output/raw/*
echo "✅ Clean complete"
echo ""

# --- Step 3: Test export-raw with date filtering ----------------------------
echo "🚀 Step 3: Testing export-raw with date filtering..."
echo "   Exporting dates: ${TEST_START} to ${TEST_END}"
echo ""

ecomlake export-raw \
  --source "$latest_run" \
  --dest "output/raw" \
  --date "$TEST_START" \
  --date "$(date -j -v+1d -f "%Y-%m-%d" "$TEST_START" +%Y-%m-%d)" \
  --date "$TEST_END"

echo ""
echo "✅ Export complete"
echo ""

# --- Step 4: Validate results ------------------------------------------------
echo "🔬 Step 4: Validating results..."
echo ""

# Count parquet files
parquet_count=$(find output/raw -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
echo "📊 Generated ${parquet_count} parquet files"

# Count partitions
partition_count=$(find output/raw -type d -name "ingest_dt=*" 2>/dev/null | wc -l | tr -d ' ')
echo "📊 Created ${partition_count} date partitions"

# List partitions
echo ""
echo "Partitions created:"
find output/raw -type d -name "ingest_dt=*" | sort

echo ""

# --- Step 5: Run duplicate check ---------------------------------------------
if [ -f "check_duplicates.py" ]; then
  echo "🔍 Step 5: Running duplicate check..."
  echo ""
  python check_duplicates.py
  echo ""
else
  echo "⚠️  Step 5: Skipping duplicate check (check_duplicates.py not found)"
  echo ""
fi

# --- Summary -----------------------------------------------------------------
echo "============================================================================="
echo "📊 Phase 4 Test Summary"
echo "============================================================================="
echo ""
echo "Export Results:"
echo "  Source:      $latest_run"
echo "  Parquet:     ${parquet_count} files"
echo "  Partitions:  ${partition_count} dates"
echo ""
echo "Next Steps:"
echo ""
if [ "$parquet_count" -gt 0 ]; then
  echo "✅ Phase 4 export filtering is working!"
  echo ""
  echo "   Check the duplicate analysis above."
  echo "   If duplicates are eliminated, Phase 4 is successful."
  echo ""
  echo "   For full testing with Phases 1-5, install updated generator:"
  echo "   cd ../ecom_sales_data_generator && pip install -e ."
  echo ""
else
  echo "❌ Export failed - no parquet files generated"
  echo "   Check the output above for errors."
  echo ""
fi
echo "============================================================================="

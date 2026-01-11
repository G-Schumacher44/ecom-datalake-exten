#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Smoke Test for 6-Year Data Generation Fix
# =============================================================================
# Tests all fixes with a minimal 1-week dataset (2 chunks of 30 days each)
# Run this BEFORE attempting the full 6-year generation
# =============================================================================

echo "🧪 Starting smoke test for data generation fixes"
echo ""

# --- Configuration -----------------------------------------------------------
TEST_START="2020-01-01"
TEST_END="2020-02-29"  # 2 months to give customers time to shop
BACKUP_DIR=".backup_smoke_test_$(date +%Y%m%d_%H%M%S)"

# --- Step 1: Backup current state --------------------------------------------
echo "📦 Step 1: Backing up current state to ${BACKUP_DIR}"
mkdir -p "$BACKUP_DIR"

if [ -f "scripts/backlog_bear.sh" ]; then
  cp scripts/backlog_bear.sh "$BACKUP_DIR/"
fi

if [ -f "artifacts/.id_state.json" ]; then
  cp artifacts/.id_state.json "$BACKUP_DIR/"
fi

if [ -f "artifacts/.backlog_checkpoint" ]; then
  cp artifacts/.backlog_checkpoint "$BACKUP_DIR/"
fi

echo "✅ Backup complete"
echo ""

# --- Step 2: Clean test environment ------------------------------------------
echo "🧹 Step 2: Cleaning test environment"

rm -rf artifacts/static_lookups
rm -f artifacts/.id_state.json
rm -f artifacts/.backlog_checkpoint
rm -rf artifacts/raw_run_*
rm -rf output/raw/*

echo "✅ Clean complete"
echo ""

# --- Step 3: Modify backlog_bear.sh for test --------------------------------
echo "📝 Step 3: Configuring backlog_bear.sh for smoke test"

# Create temporary test version
cp scripts/backlog_bear.sh scripts/backlog_bear_test.sh

# Update dates for smoke test
if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS sed syntax
  sed -i '' "s/START_DATE=\"2020-01-01\"/START_DATE=\"${TEST_START}\"/" scripts/backlog_bear_test.sh
  sed -i '' "s/END_DATE=\"2026-01-08\"/END_DATE=\"${TEST_END}\"/" scripts/backlog_bear_test.sh
else
  # Linux sed syntax
  sed -i "s/START_DATE=\"2020-01-01\"/START_DATE=\"${TEST_START}\"/" scripts/backlog_bear_test.sh
  sed -i "s/END_DATE=\"2026-01-08\"/END_DATE=\"${TEST_END}\"/" scripts/backlog_bear_test.sh
fi

echo "✅ Test configuration created: scripts/backlog_bear_test.sh"
echo "   Date range: ${TEST_START} → ${TEST_END}"
echo ""

# --- Step 4: Check prerequisites ---------------------------------------------
echo "🔍 Step 4: Checking prerequisites"

# Check for static lookup script
if [ -f "scripts/generate_static_lookups.sh" ]; then
  echo "✅ Found scripts/generate_static_lookups.sh"
elif [ -f "../ecom_sales_data_generator/scripts/generate_static_lookups.sh" ]; then
  echo "⚠️  Found static lookup script in generator repo"
  echo "   Copying to extension repo..."
  cp ../ecom_sales_data_generator/scripts/generate_static_lookups.sh scripts/
  chmod +x scripts/generate_static_lookups.sh
  echo "✅ Copied scripts/generate_static_lookups.sh"
else
  echo "❌ ERROR: Cannot find scripts/generate_static_lookups.sh"
  echo "   Generator repo needs to create this script first"
  exit 1
fi

# Check for ecomlake command
if ! command -v ecomlake &> /dev/null; then
  echo "❌ ERROR: ecomlake command not found"
  echo "   Make sure the generator package is installed:"
  echo "   cd ../ecom_sales_data_generator && pip install -e ."
  exit 1
fi
echo "✅ ecomlake command available"

# Check for check_duplicates.py
if [ ! -f "check_duplicates.py" ]; then
  echo "⚠️  WARNING: check_duplicates.py not found"
  echo "   Duplicate validation will be skipped"
else
  echo "✅ check_duplicates.py available"
fi

echo ""

# --- Step 5: Run smoke test --------------------------------------------------
echo "🚀 Step 5: Running smoke test generation"
echo "   This will take 5-15 minutes depending on your system..."
echo ""

# Run the test version
chmod +x scripts/backlog_bear_test.sh
./scripts/backlog_bear_test.sh

echo ""
echo "✅ Smoke test generation complete!"
echo ""

# --- Step 6: Validate results ------------------------------------------------
echo "🔬 Step 6: Validating results"
echo ""

# Check if static lookups were created
if [ -f "artifacts/static_lookups/customers.csv" ]; then
  customer_count=$(wc -l < artifacts/static_lookups/customers.csv)
  echo "✅ Static lookups created: ${customer_count} customers"
else
  echo "❌ Static lookups NOT created"
fi

# Check if ID state file was created
if [ -f "artifacts/.id_state.json" ]; then
  echo "✅ ID state file created:"
  cat artifacts/.id_state.json
else
  echo "❌ ID state file NOT created"
fi

echo ""

# Check for parquet output
parquet_count=$(find output/raw -name "*.parquet" 2>/dev/null | wc -l | tr -d ' ')
if [ "$parquet_count" -gt 0 ]; then
  echo "✅ Generated ${parquet_count} parquet files"
else
  echo "❌ No parquet files generated"
fi

echo ""

# Run duplicate check if available
if [ -f "check_duplicates.py" ]; then
  echo "🔍 Running duplicate check..."
  echo ""
  python check_duplicates.py
  echo ""
else
  echo "⚠️  Skipping duplicate check (check_duplicates.py not found)"
fi

# --- Step 7: Summary ---------------------------------------------------------
echo "============================================================================="
echo "📊 Smoke Test Summary"
echo "============================================================================="
echo ""
echo "Test Configuration:"
echo "  Date Range: ${TEST_START} → ${TEST_END}"
echo "  Chunk Size: 30 days"
echo ""
echo "Generated Artifacts:"
echo "  Static Lookups: $([ -d artifacts/static_lookups ] && echo '✅' || echo '❌')"
echo "  ID State File:  $([ -f artifacts/.id_state.json ] && echo '✅' || echo '❌')"
echo "  Parquet Files:  ${parquet_count} files"
echo ""
echo "Next Steps:"
echo ""
if [ "$parquet_count" -gt 0 ] && [ -f "artifacts/.id_state.json" ]; then
  echo "✅ Smoke test PASSED!"
  echo ""
  echo "   Review the validation results above."
  echo "   If duplicate check shows 0% duplicates, you're ready for the full run:"
  echo ""
  echo "   ./scripts/backlog_bear.sh  # Full 6-year generation"
  echo ""
else
  echo "❌ Smoke test FAILED"
  echo ""
  echo "   Check the output above for errors."
  echo "   Review logs and fix issues before attempting full run."
  echo ""
fi

echo "Cleanup:"
echo "  Test artifacts: rm -rf artifacts/static_lookups artifacts/.id_state.json output/raw/*"
echo "  Restore backup: cp ${BACKUP_DIR}/* scripts/ artifacts/"
echo "  Remove test script: rm scripts/backlog_bear_test.sh"
echo ""
echo "============================================================================="

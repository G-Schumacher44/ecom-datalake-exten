conda activate ecom-datalake-exten

# Optional: tidy up previous builds
rm -rf dist build *.egg-info

# Lint + format checks
python -m ruff check src tests
python -m black --check src tests

# Run tests
pytest

# Rebuild wheel and sdist
python -m build


rm -rf output artifacts
ecomlake run-generator \
  --config gen_config/ecom_sales_gen_quick.yaml \
  --artifact-root artifacts \
  --messiness-level none \
  --generator-src ../ecom_sales_data_generator/src

ecomlake export-raw \
  --source artifacts/raw_run_20251019T220748Z> \
  --target output/raw \
  --dates 2024-02-15,2024-02-16

ecomlake upload-raw \
  --source output/raw \
  --bucket gcs-automation-project-raw \
  --prefix ecom/raw \
  --ingest-date 2024-02-15 \
  --dry-run
# Remove --dry-run for the real upload. 

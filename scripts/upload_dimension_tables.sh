#!/usr/bin/env bash
set -euo pipefail

BUCKET="${1:-${BUCKET:-acme-analytics-raw}}"
PREFIX="${2:-ecom/raw}"
SOURCE_ROOT="${3:-output/raw}"

customers_dir="${SOURCE_ROOT}/customers"
products_dir="${SOURCE_ROOT}/product_catalog"

if [ ! -d "${customers_dir}" ]; then
  echo "❌ Missing customers directory: ${customers_dir}"
  exit 1
fi

if [ ! -d "${products_dir}" ]; then
  echo "❌ Missing product_catalog directory: ${products_dir}"
  exit 1
fi

echo "📦 Uploading dimension tables to gs://${BUCKET}/${PREFIX}"
echo "  - customers: ${customers_dir} -> gs://${BUCKET}/${PREFIX}/customers"
echo "  - product_catalog: ${products_dir} -> gs://${BUCKET}/${PREFIX}/product_catalog"

gsutil -m rsync -r "${customers_dir}" "gs://${BUCKET}/${PREFIX}/customers"
gsutil -m rsync -r "${products_dir}" "gs://${BUCKET}/${PREFIX}/product_catalog"

echo "✅ Dimension table upload complete"

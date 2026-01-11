#!/usr/bin/env python3
"""Check for duplicate order_id's across partitions."""

from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

# Find all parquet files
samples_dir = Path("samples")
parquet_files = list(samples_dir.glob("**/part-*.parquet"))

print(f"Found {len(parquet_files)} parquet files")
print()

# Collect all order_ids by partition
order_ids_by_partition = {}
all_order_ids = defaultdict(list)

for pq_file in sorted(parquet_files):
    partition = pq_file.parent.name
    table = pq.read_table(pq_file)

    if "order_id" in table.column_names:
        order_ids = table["order_id"].to_pylist()
        unique_order_ids = set(order_ids)
        order_ids_by_partition[partition] = unique_order_ids

        # Track which partitions each order_id appears in
        for oid in unique_order_ids:
            all_order_ids[oid].append(partition)

        print(f"{partition}: {len(unique_order_ids)} unique order_ids, {len(order_ids)} total rows")

print()
print("=" * 80)
print("CHECKING FOR DUPLICATES ACROSS PARTITIONS")
print("=" * 80)

# Find order_ids that appear in multiple partitions
duplicates = {oid: partitions for oid, partitions in all_order_ids.items() if len(partitions) > 1}

if duplicates:
    print(f"\n🚨 FOUND {len(duplicates)} ORDER_IDs APPEARING IN MULTIPLE PARTITIONS!")
    print()

    # Show first 10 examples
    for i, (order_id, partitions) in enumerate(list(duplicates.items())[:10]):
        print(f"{i+1}. order_id={order_id}")
        print(f"   Appears in {len(partitions)} partitions: {sorted(partitions)}")
        print()

    if len(duplicates) > 10:
        print(f"... and {len(duplicates) - 10} more duplicates")

    print()
    print("SUMMARY:")
    print(f"  Total unique order_ids: {len(all_order_ids)}")
    print(f"  Order_ids in multiple partitions: {len(duplicates)}")
    print(f"  Percentage duplicated: {len(duplicates)/len(all_order_ids)*100:.1f}%")
else:
    print("\n✅ No duplicates found - each order_id appears in only one partition")

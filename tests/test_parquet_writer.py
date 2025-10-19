from datetime import date

import pandas as pd
from ecom_datalake_extension.config import require_table_config
from ecom_datalake_extension.parquet_writer import write_partitioned_parquet


def test_write_partitioned_parquet(tmp_path):
    output_root = tmp_path / "raw"
    table_config = require_table_config("orders")
    df = pd.DataFrame(
        [
            {
                "order_id": "ORDER-1",
                "order_date": "2024-01-10",
                "customer_id": "CUST-1",
                "gross_total": 120.0,
                "net_total": 115.0,
                "order_channel": "Web",
            },
            {
                "order_id": "ORDER-2",
                "order_date": "2024-01-11",
                "customer_id": "CUST-2",
                "gross_total": 60.0,
                "net_total": 55.0,
                "order_channel": "Phone",
            },
        ]
    )

    (
        manifest_files,
        min_event_dt,
        max_event_dt,
        total_rows,
        checksums,
    ) = write_partitioned_parquet(
        df,
        table_config=table_config,
        output_root=output_root,
        ingest_dt=date(2024, 1, 15),
        batch_id="batch_test",
        source_prefix="gs://foo/raw/orders/ingest_dt=2024-01-15",
        target_size_mb=1,
    )

    assert manifest_files
    assert min_event_dt == "2024-01-10"
    assert max_event_dt == "2024-01-11"
    assert total_rows == len(df)
    assert len(checksums) == len(manifest_files)

    parquet_path = output_root / manifest_files[0].path
    written_df = pd.read_parquet(parquet_path)
    assert {"event_id", "batch_id", "ingestion_ts"}.issubset(written_df.columns)
    assert written_df.iloc[0]["batch_id"] == "batch_test"

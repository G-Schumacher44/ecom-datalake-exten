from unittest.mock import MagicMock

from ecom_datalake_extension.gcs_uploader import (
    build_partition_prefix,
    upload_partition,
)


def test_build_partition_prefix():
    prefix = build_partition_prefix("orders", "ingest_dt=2024-02-15")
    assert prefix == "orders/ingest_dt=2024-02-15"


def test_upload_partition(tmp_path):
    partition_dir = tmp_path / "orders" / "ingest_dt=2024-02-15"
    partition_dir.mkdir(parents=True)
    (partition_dir / "_SUCCESS").write_text("")
    (partition_dir / "part-0000.parquet").write_text("data")

    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    result = upload_partition(
        bucket_name="bucket",
        prefix="ecom/raw/orders/ingest_dt=2024-02-15",
        local_partition_dir=partition_dir,
        client=mock_client,
    )

    assert result.files_uploaded == 2
    assert result.bucket == "bucket"
    mock_bucket.blob.assert_called()

import json
import sys
from unittest.mock import patch

import pandas as pd
from click.testing import CliRunner
from ecom_datalake_extension.cli import export_raw_cmd, upload_raw_cmd


def test_export_raw_cli(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()

    orders = pd.DataFrame(
        [
            {
                "order_id": "ORDER-123",
                "order_date": "2024-02-01",
                "customer_id": "CUST-001",
                "gross_total": 99.0,
                "net_total": 95.0,
                "order_channel": "Web",
            }
        ]
    )
    orders.to_csv(source_dir / "orders.csv", index=False)

    runner = CliRunner()
    result = runner.invoke(
        export_raw_cmd,
        [
            "--source",
            str(source_dir),
            "--target",
            str(target_dir),
            "--ingest-date",
            "2024-02-15",
        ],
    )

    assert result.exit_code == 0, result.output

    partition_dir = target_dir / "orders" / "ingest_dt=2024-02-15"
    assert (partition_dir / "_SUCCESS").exists()
    manifest_path = partition_dir / "_MANIFEST.json"
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text())
    assert manifest["table"] == "orders"
    assert manifest["partition"] == "ingest_dt=2024-02-15"
    assert manifest["files"], "expected manifest to list parquet files"
    assert manifest["total_rows"] == 1
    assert manifest["checksums"]

    parquet_rel_path = manifest["files"][0]["path"]
    parquet_path = target_dir / parquet_rel_path
    df = pd.read_parquet(parquet_path)
    assert "event_id" in df.columns
    assert df.iloc[0]["order_id"] == "ORDER-123"


def test_upload_raw_cli_dry_run(tmp_path):
    source_dir = tmp_path / "output" / "raw"
    partition_dir = source_dir / "orders" / "ingest_dt=2024-02-15"
    partition_dir.mkdir(parents=True)
    (partition_dir / "_SUCCESS").write_text("")

    runner = CliRunner()
    result = runner.invoke(
        upload_raw_cmd,
        [
            "--source",
            str(source_dir),
            "--bucket",
            "test-bucket",
            "--ingest-date",
            "2024-02-15",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "dry-run" in result.output


@patch("ecom_datalake_extension.cli.upload_partition")
def test_upload_raw_cli_invokes_uploader(mock_upload, tmp_path):
    source_dir = tmp_path / "output" / "raw"
    partition_dir = source_dir / "orders" / "ingest_dt=2024-02-15"
    partition_dir.mkdir(parents=True)
    (partition_dir / "_SUCCESS").write_text("")

    runner = CliRunner()
    result = runner.invoke(
        upload_raw_cmd,
        [
            "--source",
            str(source_dir),
            "--bucket",
            "test-bucket",
            "--ingest-date",
            "2024-02-15",
        ],
    )

    mock_upload.assert_called_once()
    assert result.exit_code == 0, result.output


def test_export_raw_cli_multiple_dates(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    source_dir.mkdir()

    data = pd.DataFrame(
        [
            {
                "order_id": "ORDER-001",
                "order_date": "2024-02-01",
                "customer_id": "CUST-1",
                "gross_total": 10.0,
                "net_total": 9.0,
                "order_channel": "Web",
            }
        ]
    )
    data.to_csv(source_dir / "orders.csv", index=False)

    runner = CliRunner()
    result = runner.invoke(
        export_raw_cmd,
        [
            "--source",
            str(source_dir),
            "--target",
            str(target_dir),
            "--dates",
            "2024-02-15,2024-02-16",
        ],
    )

    assert result.exit_code == 0, result.output

    part1 = target_dir / "orders" / "ingest_dt=2024-02-15"
    part2 = target_dir / "orders" / "ingest_dt=2024-02-16"
    assert (part1 / "_SUCCESS").exists()
    assert (part2 / "_SUCCESS").exists()


def test_export_raw_cli_with_hook(tmp_path):
    source_dir = tmp_path / "source"
    target_dir = tmp_path / "target"
    hook_dir = tmp_path / "hooks"
    source_dir.mkdir()
    hook_dir.mkdir()

    module_path = hook_dir / "custom_hook.py"
    module_path.write_text(
        """
from pathlib import Path

def write_manifest_summary(context):
    summary = context.partition_dir / "hook_summary.txt"
    summary.write_text(f"{context.table}|{context.manifest.total_rows}")
"""
    )

    sys.path.insert(0, str(hook_dir))
    data = pd.DataFrame(
        [
            {
                "order_id": "HOOK-001",
                "order_date": "2024-03-01",
                "customer_id": "HOOK",
                "gross_total": 20.0,
                "net_total": 18.0,
                "order_channel": "Web",
            }
        ]
    )
    data.to_csv(source_dir / "orders.csv", index=False)

    runner = CliRunner()
    result = runner.invoke(
        export_raw_cmd,
        [
            "--source",
            str(source_dir),
            "--target",
            str(target_dir),
            "--ingest-date",
            "2024-03-02",
            "--post-export-hook",
            "custom_hook:write_manifest_summary",
        ],
    )

    assert result.exit_code == 0, result.output
    summary_file = target_dir / "orders" / "ingest_dt=2024-03-02" / "hook_summary.txt"
    assert summary_file.exists()
    assert summary_file.read_text() == "orders|1"

    sys.path.pop(0)

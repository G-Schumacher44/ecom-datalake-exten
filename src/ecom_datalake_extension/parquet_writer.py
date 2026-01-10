"""
Core Parquet writing utilities for raw exports.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from .config import DEFAULT_TARGET_SIZE_MB, TableExportConfig
from .lineage import compute_event_id, utc_now_iso
from .manifest import ManifestFile
from .utils import chunk_dataframe, compute_checksum, estimate_row_size_bytes


def prepare_dataframe_with_lineage(
    df: pd.DataFrame,
    *,
    table_config: TableExportConfig,
    batch_id: str,
    ingestion_ts: str,
    source_prefix: str | None = None,
) -> pd.DataFrame:
    """
    Adds event_id, batch_id, ingestion_ts, and source_file columns.
    """
    enriched = df.copy()
    enriched["batch_id"] = batch_id
    enriched["ingestion_ts"] = ingestion_ts

    def event_id_builder(row: pd.Series) -> str:
        return compute_event_id(
            table_config.table_name,
            row,
            table_config.primary_keys,
        )

    enriched["event_id"] = enriched.apply(event_id_builder, axis=1)
    if source_prefix:
        enriched["source_file"] = source_prefix
    return enriched


def determine_rows_per_chunk(
    df: pd.DataFrame,
    *,
    target_size_mb: int,
) -> int:
    """
    Determines an appropriate number of rows per chunk based on the requested size.
    """
    target_bytes = max(1, target_size_mb) * 1024 * 1024
    avg_row_bytes = estimate_row_size_bytes(df)
    rows = max(int(target_bytes / avg_row_bytes), 1)
    return rows


def write_partitioned_parquet(
    df: pd.DataFrame,
    *,
    table_config: TableExportConfig,
    output_root: Path,
    ingest_dt: date | None,
    batch_id: str,
    source_prefix: str | None = None,
    target_size_mb: int = DEFAULT_TARGET_SIZE_MB,
    partition_path_override: str | None = None,
) -> tuple[list[ManifestFile], str | None, str | None, int, list[str]]:
    """
    Writes Parquet files for a single table partition and returns manifest metadata.

    Args:
        partition_path_override: If provided, use this path instead of table_name/ingest_dt=YYYY-MM-DD.
                                 Used for dimension tables with custom partitioning (e.g., customers/signup_date=YYYY-MM-DD)
    """
    if df.empty:
        return [], None, None, 0, []

    if partition_path_override:
        partition_dir = output_root / partition_path_override
    elif ingest_dt:
        partition_dir = output_root / table_config.table_name / f"ingest_dt={ingest_dt:%Y-%m-%d}"
    else:
        raise ValueError("Either ingest_dt or partition_path_override must be provided")

    partition_dir.mkdir(parents=True, exist_ok=True)
    ingestion_ts = utc_now_iso()

    rows_per_chunk = determine_rows_per_chunk(df, target_size_mb=target_size_mb)
    chunks = chunk_dataframe(df, rows_per_chunk)

    manifest_files: list[ManifestFile] = []
    min_event_dt: str | None = None
    max_event_dt: str | None = None

    if table_config.event_date_column and table_config.event_date_column in df.columns:
        dates = pd.to_datetime(
            df[table_config.event_date_column],
            format="mixed",
            errors="coerce",
        )
        valid_dates = dates.dropna()
        if not valid_dates.empty:
            min_event_dt = valid_dates.min().date().isoformat()
            max_event_dt = valid_dates.max().date().isoformat()

    total_rows_written = 0
    checksum_values: list[str] = []

    for index, chunk in enumerate(chunks):
        source_file = None
        if source_prefix:
            source_file = f"{source_prefix}/part-{index:04d}.parquet"
        enriched = prepare_dataframe_with_lineage(
            chunk,
            table_config=table_config,
            batch_id=batch_id,
            ingestion_ts=ingestion_ts,
            source_prefix=source_file,
        )
        filename = partition_dir / f"part-{index:04d}.parquet"
        enriched.to_parquet(filename, index=False)
        total_rows_written += len(enriched)
        checksum_values.append(compute_checksum(enriched))
        manifest_files.append(
            ManifestFile(
                path=str(filename.relative_to(output_root)),
                rows=len(enriched),
                checksum=checksum_values[-1],
            )
        )

    return manifest_files, min_event_dt, max_event_dt, total_rows_written, checksum_values

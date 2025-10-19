"""
Shared helper utilities.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator
from pathlib import Path

import pandas as pd


def estimate_row_size_bytes(df: pd.DataFrame) -> int:
    """
    Approximates the average row size by using DataFrame memory usage.
    """
    total_bytes = df.memory_usage(deep=True).sum()
    rows = max(len(df), 1)
    return max(1, math.ceil(total_bytes / rows))


def chunk_dataframe(df: pd.DataFrame, rows_per_chunk: int) -> list[pd.DataFrame]:
    """
    Splits a DataFrame into chunks of roughly equal size by row count.
    """
    if rows_per_chunk <= 0 or len(df) <= rows_per_chunk:
        return [df]
    chunks: list[pd.DataFrame] = []
    for start in range(0, len(df), rows_per_chunk):
        end = start + rows_per_chunk
        chunks.append(df.iloc[start:end].copy())
    return chunks


def iter_csv_tables(source_dir: str) -> Iterator[tuple[str, pd.DataFrame]]:
    """
    Yields pairs of table name and DataFrame for every CSV in a directory.
    """
    path = Path(source_dir)
    for csv_path in sorted(path.glob("*.csv")):
        yield csv_path.stem, pd.read_csv(csv_path)


def compute_checksum(df: pd.DataFrame) -> str:
    """Compute a stable SHA256 checksum over the DataFrame contents."""
    serialized = df.to_json(orient="records", date_format="iso", date_unit="s", default_handler=str)
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return digest

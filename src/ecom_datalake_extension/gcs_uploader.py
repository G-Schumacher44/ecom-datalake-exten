"""
Google Cloud Storage upload helper functions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    from google.cloud import storage  # type: ignore
except ImportError as exc:  # pragma: no cover - handled at runtime
    storage = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


class GCSDependencyError(RuntimeError):
    """
    Raised when google-cloud-storage is not installed.
    """


def _ensure_storage_client() -> storage.Client:
    if storage is None or _IMPORT_ERROR:
        raise GCSDependencyError(
            "google-cloud-storage is required for this command. "
            "Install with `pip install ecom-datalake-extension[gcs]`."
        )
    return storage.Client()


@dataclass(frozen=True)
class UploadResult:
    files_uploaded: int
    bucket: str
    prefix: str


def upload_partition(
    *,
    bucket_name: str,
    prefix: str,
    local_partition_dir: Path,
    client: storage.Client | None = None,
) -> UploadResult:
    """
    Uploads all files from a local Hive-style partition directory to GCS.
    """
    local_partition_dir = local_partition_dir.resolve()
    if not local_partition_dir.exists():
        raise FileNotFoundError(f"Partition directory not found: {local_partition_dir}")

    client = client or _ensure_storage_client()
    bucket = client.bucket(bucket_name)

    files_uploaded = 0
    for path in local_partition_dir.glob("*"):
        if path.is_file():
            relative_name = path.name
            blob_path = f"{prefix.strip('/')}/{relative_name}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(path)
            files_uploaded += 1

    return UploadResult(files_uploaded=files_uploaded, bucket=bucket_name, prefix=prefix)


def build_partition_prefix(table: str, partition: str) -> str:
    """
    Utility for composing a prefix like `ecom/raw/orders/ingest_dt=2024-02-15`.
    """
    table = table.strip("/")
    partition = partition.strip("/")
    return f"{table}/{partition}" if partition else table

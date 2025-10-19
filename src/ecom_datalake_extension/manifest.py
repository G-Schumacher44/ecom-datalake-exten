"""
Manifest writer for raw and bronze partitions.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import DEFAULT_MANIFEST_SCHEMA_VERSION


@dataclass(frozen=True)
class ManifestFile:
    path: str
    rows: int
    checksum: str | None = None


@dataclass(frozen=True)
class PartitionManifest:
    schema_version: str
    table: str
    batch_id: str
    partition: str
    files: list[ManifestFile]
    created_at: str
    min_event_dt: str | None
    max_event_dt: str | None
    generator_version: str | None
    total_rows: int | None
    checksums: list[str] | None


def build_manifest(
    *,
    table: str,
    batch_id: str,
    partition: str,
    files: Iterable[ManifestFile],
    created_at: str,
    min_event_dt: str | None = None,
    max_event_dt: str | None = None,
    generator_version: str | None = None,
    schema_version: str = DEFAULT_MANIFEST_SCHEMA_VERSION,
    total_rows: int | None = None,
    checksums: Iterable[str] | None = None,
) -> PartitionManifest:
    manifest_files = list(files)
    checksum_list = list(checksums) if checksums else None
    return PartitionManifest(
        schema_version=schema_version,
        table=table,
        batch_id=batch_id,
        partition=partition,
        files=manifest_files,
        created_at=created_at,
        min_event_dt=min_event_dt,
        max_event_dt=max_event_dt,
        generator_version=generator_version,
        total_rows=total_rows,
        checksums=checksum_list,
    )


def write_manifest(path: Path, manifest: PartitionManifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(asdict(manifest), fp, indent=2, sort_keys=True)


def write_success_marker(partition_dir: Path) -> None:
    marker = partition_dir / "_SUCCESS"
    marker.touch(exist_ok=True)

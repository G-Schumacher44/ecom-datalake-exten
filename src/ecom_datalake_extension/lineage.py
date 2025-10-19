"""
Lineage and identifier helpers.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

try:  # pragma: no cover - compatibility shim for older Python versions
    UTC = datetime.UTC
except AttributeError:  # pragma: no cover
    UTC = timezone.utc  # noqa: UP017


def generate_batch_id() -> str:
    """
    Returns a globally unique batch identifier for a single export run.
    """
    return f"batch_{uuid.uuid4().hex}"


def utc_now_iso() -> str:
    """
    Timestamp helper used for manifests and ingestion_ts columns.
    """
    return datetime.now(UTC).isoformat(timespec="seconds")


def compute_event_id(
    table_name: str,
    row: Mapping[str, object],
    primary_keys: Sequence[str],
) -> str:
    """
    Builds a deterministic SHA-256 hash from the table name and primary keys.

    This keeps `event_id` stable across retries. Missing keys raise KeyError
    to avoid silently generating bad identifiers.
    """
    payload = {key: row[key] for key in primary_keys}
    payload["_table"] = table_name
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(serialized).hexdigest()
    return f"evt_{digest}"

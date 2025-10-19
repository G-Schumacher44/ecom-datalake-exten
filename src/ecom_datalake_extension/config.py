"""
Configuration models and defaults for the data lake extension toolkit.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class BucketLayout:
    """
    Describes the three-bucket medallion layout used by the project.
    """

    raw_bucket: str
    bronze_bucket: str
    silver_bucket: str
    raw_prefix: str = "ecom/raw"
    bronze_prefix: str = "ecom/bronze"
    silver_prefix: str = "ecom/silver"

    def raw_path(self, table: str, ingest_dt: date) -> str:
        partition = ingest_dt.strftime("ingest_dt=%Y-%m-%d")
        return f"{self.raw_prefix}/{table}/{partition}"

    def bronze_path(self, table: str, event_dt: date) -> str:
        partition = event_dt.strftime("event_dt=%Y-%m-%d")
        return f"{self.bronze_prefix}/{table}/{partition}"

    def silver_path(self, table: str, event_dt: date) -> str:
        partition = event_dt.strftime("event_dt=%Y-%m-%d")
        return f"{self.silver_prefix}/{table}/{partition}"


@dataclass(frozen=True)
class TableExportConfig:
    """
    Metadata that guides partitioning, lineage, and manifest creation
    for individual tables.
    """

    table_name: str
    primary_keys: Sequence[str]
    event_date_column: str | None
    default_sort_columns: Sequence[str] = field(default_factory=tuple)


TABLE_EXPORT_CONFIGS: Mapping[str, TableExportConfig] = {
    "customers": TableExportConfig(
        table_name="customers",
        primary_keys=("customer_id",),
        event_date_column="signup_date",
        default_sort_columns=("customer_id",),
    ),
    "product_catalog": TableExportConfig(
        table_name="product_catalog",
        primary_keys=("product_id",),
        event_date_column=None,
        default_sort_columns=("product_id",),
    ),
    "shopping_carts": TableExportConfig(
        table_name="shopping_carts",
        primary_keys=("cart_id",),
        event_date_column="created_at",
        default_sort_columns=("created_at", "cart_id"),
    ),
    "cart_items": TableExportConfig(
        table_name="cart_items",
        primary_keys=("cart_item_id",),
        event_date_column="added_at",
        default_sort_columns=("added_at", "cart_item_id"),
    ),
    "orders": TableExportConfig(
        table_name="orders",
        primary_keys=("order_id",),
        event_date_column="order_date",
        default_sort_columns=("order_date", "order_id"),
    ),
    "order_items": TableExportConfig(
        table_name="order_items",
        primary_keys=("order_id", "product_id"),
        event_date_column=None,
        default_sort_columns=("order_id", "product_id"),
    ),
    "returns": TableExportConfig(
        table_name="returns",
        primary_keys=("return_id",),
        event_date_column="return_date",
        default_sort_columns=("return_date", "return_id"),
    ),
    "return_items": TableExportConfig(
        table_name="return_items",
        primary_keys=("return_item_id",),
        event_date_column=None,
        default_sort_columns=("return_id", "return_item_id"),
    ),
}


DEFAULT_TARGET_SIZE_MB = 16
DEFAULT_MANIFEST_SCHEMA_VERSION = "0.1.0"


def list_supported_tables() -> list[str]:
    return sorted(TABLE_EXPORT_CONFIGS)


def require_table_config(table: str) -> TableExportConfig:
    try:
        return TABLE_EXPORT_CONFIGS[table]
    except KeyError as exc:
        raise KeyError(
            f"No export configuration found for table '{table}'. "
            "Update TABLE_EXPORT_CONFIGS in config.py."
        ) from exc


def default_output_root() -> Path:
    return Path("output") / "raw"

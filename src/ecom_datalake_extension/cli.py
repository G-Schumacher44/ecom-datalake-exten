from __future__ import annotations

import sys
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

import click
import pandas as pd

from .config import (
    DEFAULT_TARGET_SIZE_MB,
    default_output_root,
    list_supported_tables,
    require_table_config,
)
from .gcs_uploader import (
    GCSDependencyError,
    build_partition_prefix,
    upload_partition,
)
from .generator_runner import run_generator_cli
from .hooks import ExportContext, execute_hooks, load_hook
from .lineage import generate_batch_id, utc_now_iso
from .manifest import build_manifest, write_manifest, write_success_marker
from .parquet_writer import write_partitioned_parquet
from .utils import iter_csv_tables


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise click.BadParameter("Expected YYYY-MM-DD format") from exc


def _parse_optional_date(value: str | None) -> date | None:
    if not value:
        return None
    return _parse_date(value)


@click.group()
def cli() -> None:
    """
    E-commerce data lake extension CLI.
    """


@cli.command("run-generator")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the YAML config used by ecom_sales_data_generator.",
)
@click.option(
    "--artifact-root",
    type=click.Path(dir_okay=True, file_okay=False, path_type=Path),
    default=Path("artifacts"),
    show_default=True,
    help="Directory for raw generator CSV outputs.",
)
@click.option(
    "--messiness-level",
    type=click.Choice(
        ["baseline", "none", "light_mess", "medium_mess", "heavy_mess"],
        case_sensitive=False,
    ),
    default="baseline",
    show_default=True,
)
@click.option(
    "--start-date",
    type=str,
    default=None,
    help="Start date for data generation (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    type=str,
    default=None,
    help="End date for data generation (YYYY-MM-DD).",
)
@click.option(
    "--generator-src",
    type=click.Path(dir_okay=True, file_okay=False, path_type=Path),
    default=None,
    help="Path to the ecom_sales_data_generator/src directory if not installed.",
)
@click.option(
    "--load-lookups-from",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    default=None,
    help="Directory containing pre-generated customers.csv and product_catalog.csv lookups.",
)
@click.option(
    "--id-state-file",
    type=click.Path(dir_okay=False, file_okay=True, path_type=Path),
    default=None,
    help="Path to JSON file for persisting sequential ID state across chunks.",
)
def run_generator_cmd(
    config_path: Path,
    artifact_root: Path,
    messiness_level: str,
    start_date: str | None,
    end_date: str | None,
    generator_src: Path | None,
    load_lookups_from: Path | None,
    id_state_file: Path | None,
) -> None:
    """
    Runs the ecom generator and stores CSV artifacts locally.
    """
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    output_dir = artifact_root / f"raw_run_{run_ts}"

    date_info = ""
    if start_date and end_date:
        date_info = f" for date range {start_date} to {end_date}"

    # Build extra args for new generator features
    extra_args = []
    if load_lookups_from:
        extra_args.extend(["--load-lookups-from", str(load_lookups_from)])
    if id_state_file:
        extra_args.extend(["--id-state-file", str(id_state_file)])

    click.echo(f"📦 Generating dataset into {output_dir}{date_info}")
    run_generator_cli(
        config_path=config_path,
        output_dir=output_dir,
        messiness_level=messiness_level,
        start_date=start_date,
        end_date=end_date,
        generator_src=generator_src,
        extra_args=extra_args if extra_args else None,
    )
    click.echo("✅ Generator run complete.")


@cli.command("export-raw")
@click.option(
    "--source",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    required=True,
    help="Directory containing the CSV output from run-generator.",
)
@click.option(
    "--target",
    type=click.Path(dir_okay=True, file_okay=False, path_type=Path),
    default=default_output_root,
    show_default=True,
    help="Root directory where raw partitions will be written.",
)
@click.option(
    "--ingest-date",
    callback=lambda _, __, value: _parse_optional_date(value),
    help="Specific ingest date (YYYY-MM-DD). Mutually exclusive with start/end/range options.",
)
@click.option(
    "--start-date",
    callback=lambda _, __, value: _parse_optional_date(value),
    help="Start date for a range of ingest dates (inclusive).",
)
@click.option(
    "--end-date",
    callback=lambda _, __, value: _parse_optional_date(value),
    help="End date for a range of ingest dates (inclusive).",
)
@click.option(
    "--days",
    type=int,
    default=None,
    help="Number of consecutive days to export starting from --start-date (defaults to 1).",
)
@click.option(
    "--dates",
    type=str,
    default=None,
    help="Comma-separated list of ingest dates (YYYY-MM-DD).",
)
@click.option(
    "--batch-id",
    type=str,
    default=None,
    help="Optional batch identifier. Auto-generated when omitted.",
)
@click.option(
    "--target-size-mb",
    type=int,
    default=DEFAULT_TARGET_SIZE_MB,
    show_default=True,
    help="Target Parquet file size in megabytes.",
)
@click.option(
    "--table",
    "tables",
    multiple=True,
    type=click.Choice(list_supported_tables()),
    help="Restrict export to specific tables.",
)
@click.option(
    "--source-prefix",
    type=str,
    default=None,
    help="Optional URI prefix recorded in the source_file column.",
)
@click.option(
    "--post-export-hook",
    "post_export_hooks",
    multiple=True,
    help="Dotted path 'module:function' to run after each partition is written (can repeat).",
)
@click.option(
    "--lookups-from",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    default=None,
    help="Directory containing static lookup CSVs (customers.csv, product_catalog.csv) to export as dimension tables.",
)
def export_raw_cmd(
    source: Path,
    target: Path,
    ingest_date: date | None,
    start_date: date | None,
    end_date: date | None,
    days: int | None,
    dates: str | None,
    batch_id: str | None,
    target_size_mb: int,
    tables: Iterable[str],
    source_prefix: str | None,
    post_export_hooks: Sequence[str],
    lookups_from: Path | None,
) -> None:
    """
    Converts generator CSVs into partitioned Parquet for the raw zone.
    """
    # Resolve ingestion dates
    resolved_dates: list[date] = []
    if dates:
        date_items = [item.strip() for item in dates.split(",")]
        try:
            resolved_dates = [_parse_date(item) for item in date_items if item]
        except click.BadParameter as exc:
            raise click.ClickException(str(exc)) from exc
    elif start_date:
        if days is not None and days < 1:
            raise click.ClickException("--days must be >= 1 when provided.")
        if end_date and end_date < start_date:
            raise click.ClickException("--end-date must be on or after --start-date.")
        actual_days = days or 1
        final_end = end_date or (start_date + timedelta(days=actual_days - 1))
        current = start_date
        while current <= final_end:
            resolved_dates.append(current)
            current += timedelta(days=1)
    elif ingest_date:
        resolved_dates = [ingest_date]
    else:
        resolved_dates = [date.today()]

    resolved_dates = sorted(set(resolved_dates))

    hook_functions = [load_hook(path) for path in post_export_hooks]

    batch = batch_id or generate_batch_id()
    click.echo(
        f"🚚 Exporting raw partitions for {', '.join(d.isoformat() for d in resolved_dates)} (batch={batch})"
    )
    processed_tables: list[str] = []

    # Cache parent tables for JOIN filtering of child tables
    parent_tables_cache: dict[str, pd.DataFrame] = {}

    # Export dimension tables from static lookups (customers partitioned by signup_date, products by category)
    if lookups_from:
        click.echo("📊 Exporting dimension tables from static lookups...")

        # Export customers partitioned by signup_date
        customers_path = lookups_from / "customers.csv"
        if customers_path.exists():
            if not tables or "customers" in tables:
                click.echo("  └─ customers (partitioned by signup_date)")
                customers_df = pd.read_csv(customers_path)

                # Group by signup_date and export each partition
                customers_df["signup_date_only"] = pd.to_datetime(
                    customers_df["signup_date"]
                ).dt.date
                for signup_dt, group_df in customers_df.groupby("signup_date_only"):
                    partition_df = group_df.drop(columns=["signup_date_only"])

                    # Create partition path: customers/signup_date=YYYY-MM-DD/
                    partition_dir = target / "customers" / f"signup_date={signup_dt}"
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    partition_prefix = None
                    if source_prefix:
                        partition_prefix = f"{source_prefix}/customers/signup_date={signup_dt}"

                    table_config = require_table_config("customers")
                    (
                        manifest_files,
                        min_event_dt,
                        max_event_dt,
                        total_rows,
                        checksums,
                    ) = write_partitioned_parquet(
                        partition_df,
                        table_config=table_config,
                        output_root=target,
                        ingest_dt=None,  # No ingest_dt for dimension tables
                        batch_id=batch,
                        source_prefix=partition_prefix,
                        target_size_mb=target_size_mb,
                        partition_path_override=f"customers/signup_date={signup_dt}",
                    )

                    manifest_path = partition_dir / "_MANIFEST.json"
                    manifest = build_manifest(
                        table="customers",
                        batch_id=batch,
                        partition=f"signup_date={signup_dt}",
                        files=manifest_files,
                        created_at=utc_now_iso(),
                        min_event_dt=min_event_dt,
                        max_event_dt=max_event_dt,
                        total_rows=total_rows,
                        checksums=checksums,
                    )
                    write_manifest(manifest_path, manifest)
                    write_success_marker(partition_dir)

                click.echo(
                    f"    ✅ Exported {len(customers_df.groupby('signup_date_only'))} signup_date partitions ({len(customers_df)} total customers)"
                )
                processed_tables.append("customers")

        # Export products partitioned by category
        products_path = lookups_from / "product_catalog.csv"
        if products_path.exists():
            if not tables or "product_catalog" in tables:
                click.echo("  └─ product_catalog (partitioned by category)")
                products_df = pd.read_csv(products_path)

                # Group by category and export each partition
                for category, group_df in products_df.groupby("category"):
                    # Create partition path: product_catalog/category=Electronics/
                    partition_dir = target / "product_catalog" / f"category={category}"
                    partition_dir.mkdir(parents=True, exist_ok=True)

                    partition_prefix = None
                    if source_prefix:
                        partition_prefix = f"{source_prefix}/product_catalog/category={category}"

                    table_config = require_table_config("product_catalog")
                    (
                        manifest_files,
                        min_event_dt,
                        max_event_dt,
                        total_rows,
                        checksums,
                    ) = write_partitioned_parquet(
                        group_df,
                        table_config=table_config,
                        output_root=target,
                        ingest_dt=None,  # No ingest_dt for dimension tables
                        batch_id=batch,
                        source_prefix=partition_prefix,
                        target_size_mb=target_size_mb,
                        partition_path_override=f"product_catalog/category={category}",
                    )

                    manifest_path = partition_dir / "_MANIFEST.json"
                    manifest = build_manifest(
                        table="product_catalog",
                        batch_id=batch,
                        partition=f"category={category}",
                        files=manifest_files,
                        created_at=utc_now_iso(),
                        min_event_dt=min_event_dt,
                        max_event_dt=max_event_dt,
                        total_rows=total_rows,
                        checksums=checksums,
                    )
                    write_manifest(manifest_path, manifest)
                    write_success_marker(partition_dir)

                click.echo(
                    f"    ✅ Exported {len(products_df.groupby('category'))} category partitions ({len(products_df)} total products)"
                )
                processed_tables.append("product_catalog")

    # Process tables in dependency order to ensure parents are cached before children
    # Parent tables must be processed before their children
    # Note: customers and product_catalog are now exported from static lookups if --lookups-from is provided
    table_processing_order = [
        "shopping_carts",  # Parent of cart_items
        "cart_items",  # Child of shopping_carts
        "orders",  # Parent of order_items
        "order_items",  # Child of orders
        "returns",  # Parent of return_items
        "return_items",  # Child of returns
    ]

    # Load all tables into memory first
    all_tables = {name: df for name, df in iter_csv_tables(str(source))}

    # Process in dependency order
    tables_to_process = []
    for table_name in table_processing_order:
        if table_name in all_tables:
            tables_to_process.append((table_name, all_tables[table_name]))

    # Add any tables not in the explicit order (for future-proofing)
    for table_name, df in all_tables.items():
        if table_name not in table_processing_order:
            tables_to_process.append((table_name, df))

    for table_name, df in tables_to_process:
        # Skip dimension tables if they were exported from static lookups
        if lookups_from and table_name in ("customers", "product_catalog"):
            click.echo(f"ℹ️  Skipping {table_name} (already exported from static lookups)")
            # Still cache for potential use by child tables
            parent_tables_cache[table_name] = df
            continue

        if tables and table_name not in tables:
            continue
        try:
            table_config = require_table_config(table_name)
        except KeyError:
            click.echo(f"⚠️  Skipping unconfigured table: {table_name}")
            continue

        # Cache this table for potential use by child tables
        parent_tables_cache[table_name] = df

        for current_date in resolved_dates:
            # Filter dataframe by date for this partition
            date_column = table_config.event_date_column

            if date_column and date_column in df.columns:
                # Type A: Table has its own date column
                current_date_str = current_date.isoformat()

                # Extract date portion from datetime strings (e.g., "2020-01-05T23:20:04" -> "2020-01-05")
                df_dates = df[date_column].astype(str).str[:10]
                filtered_df = df[df_dates == current_date_str].copy()

                if filtered_df.empty:
                    click.echo(
                        f"ℹ️  No rows for {table_name} on {current_date:%Y-%m-%d} (filtered by {date_column}), skipping"
                    )
                    continue
            elif table_name in ("order_items", "return_items"):
                # Type B: Child tables without date - JOIN with parent
                if table_name == "order_items":
                    parent_table = "orders"
                    join_key = "order_id"
                    parent_date_column = "order_date"
                elif table_name == "return_items":
                    parent_table = "returns"
                    join_key = "return_id"
                    parent_date_column = "return_date"

                # Check if parent table is available
                if parent_table not in parent_tables_cache:
                    click.echo(
                        f"⚠️  Cannot filter {table_name}: parent table {parent_table} not found"
                    )
                    filtered_df = df.copy()  # Fallback: replicate to all partitions
                else:
                    # JOIN with parent to get date
                    parent_df = parent_tables_cache[parent_table]
                    current_date_str = current_date.isoformat()

                    # Get IDs for this date from parent
                    # Extract date portion from datetime strings
                    parent_dates = parent_df[parent_date_column].astype(str).str[:10]
                    parent_for_date = parent_df[parent_dates == current_date_str]
                    valid_ids = set(parent_for_date[join_key])

                    # Filter child table to only matching IDs
                    filtered_df = df[df[join_key].isin(valid_ids)].copy()

                    if filtered_df.empty:
                        click.echo(
                            f"ℹ️  No rows for {table_name} on {current_date:%Y-%m-%d} (filtered via {parent_table}), skipping"
                        )
                        continue
            else:
                # Type C: Lookup tables - should not reach here if --lookups-from is used
                # These tables are now partitioned by their natural keys (signup_date, category)
                click.echo(
                    f"⚠️  Table {table_name} has no date column and is not a child table. "
                    f"Consider using --lookups-from to export as a dimension table."
                )
                filtered_df = df.copy()  # Fallback: replicate to all partitions

            partition_prefix = None
            if source_prefix:
                partition_prefix = f"{source_prefix}/{table_name}/ingest_dt={current_date:%Y-%m-%d}"

            (
                manifest_files,
                min_event_dt,
                max_event_dt,
                total_rows,
                checksums,
            ) = write_partitioned_parquet(
                filtered_df,
                table_config=table_config,
                output_root=target,
                ingest_dt=current_date,
                batch_id=batch,
                source_prefix=partition_prefix,
                target_size_mb=target_size_mb,
            )

            if not manifest_files:
                click.echo(
                    f"ℹ️  Table {table_name} produced no rows for {current_date:%Y-%m-%d}; skipping manifest."
                )
                continue

            partition_dir = target / table_name / f"ingest_dt={current_date:%Y-%m-%d}"
            manifest_path = partition_dir / "_MANIFEST.json"
            manifest = build_manifest(
                table=table_name,
                batch_id=batch,
                partition=f"ingest_dt={current_date:%Y-%m-%d}",
                files=manifest_files,
                created_at=utc_now_iso(),
                min_event_dt=min_event_dt,
                max_event_dt=max_event_dt,
                total_rows=total_rows,
                checksums=checksums,
            )
            write_manifest(manifest_path, manifest)
            write_success_marker(partition_dir)
            if hook_functions:
                context = ExportContext(
                    table=table_name,
                    partition_dir=partition_dir,
                    manifest_path=manifest_path,
                    manifest=manifest,
                )
                execute_hooks(hook_functions, context)
            processed_tables.append(f"{table_name}@{current_date:%Y-%m-%d}")
            click.echo(
                f"✅ Wrote {len(manifest_files)} file(s) for {table_name} [{current_date:%Y-%m-%d}]"
            )

    if not processed_tables:
        click.echo("⚠️  No tables were exported. Check the source directory and filters.")
        sys.exit(1)

    click.echo(f"🎉 Export complete for: {', '.join(processed_tables)}")


@cli.command("upload-raw")
@click.option(
    "--source",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, path_type=Path),
    default=default_output_root,
    show_default=True,
    help="Local raw output directory containing table partitions.",
)
@click.option(
    "--bucket",
    type=str,
    required=True,
    help="Destination GCS bucket (without gs:// prefix).",
)
@click.option(
    "--prefix",
    type=str,
    default="ecom/raw",
    show_default=True,
    help="Root prefix inside the bucket.",
)
@click.option(
    "--ingest-date",
    "ingest_date_str",
    required=True,
    help="Hive partition date to upload (YYYY-MM-DD).",
)
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="Restrict upload to specific tables.",
)
@click.option(
    "--dry-run/--no-dry-run",
    default=False,
    show_default=True,
    help="Only print what would be uploaded.",
)
def upload_raw_cmd(
    source: Path,
    bucket: str,
    prefix: str,
    ingest_date_str: str,
    tables: Iterable[str],
    dry_run: bool,
) -> None:
    """
    Uploads previously exported raw partitions to Google Cloud Storage.
    """
    try:
        ingest_date = _parse_date(ingest_date_str)
    except click.BadParameter as exc:
        raise click.BadParameter(str(exc)) from exc

    tables = tuple(tables)
    partition_name = f"ingest_dt={ingest_date:%Y-%m-%d}"
    source = source.resolve()
    if not source.exists():
        raise click.ClickException(f"Source directory does not exist: {source}")

    candidate_tables = (
        list(tables)
        if tables
        else [item.name for item in sorted(source.iterdir()) if item.is_dir()]
    )
    if not candidate_tables:
        click.echo("⚠️  No tables available in the source directory.")
        sys.exit(1)

    uploaded = []
    skipped = []
    for table in candidate_tables:
        partition_dir = source / table / partition_name
        if not partition_dir.exists():
            skipped.append((table, "missing partition"))
            continue

        table_prefix = "/".join(
            [
                prefix.strip("/"),
                build_partition_prefix(table, partition_name),
            ]
        ).strip("/")

        if dry_run:
            click.echo(f"📝 [dry-run] Would upload {partition_dir} → gs://{bucket}/{table_prefix}")
            uploaded.append((table, 0))
            continue

        try:
            result = upload_partition(
                bucket_name=bucket,
                prefix=table_prefix,
                local_partition_dir=partition_dir,
            )
        except GCSDependencyError as exc:
            raise click.ClickException(str(exc)) from exc
        uploaded.append((table, result.files_uploaded))
        click.echo(f"☁️  Uploaded {result.files_uploaded} file(s) → gs://{bucket}/{table_prefix}")

    if skipped:
        for table, reason in skipped:
            click.echo(f"⚠️  Skipped {table}: {reason}")

    if not uploaded:
        click.echo("⚠️  No tables were uploaded.")
    else:
        uploaded_tables = ", ".join(table for table, _ in uploaded)
        click.echo(f"✅ Upload complete for tables: {uploaded_tables}")

"""
Helpers for invoking `ecom_sales_data_generator`.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path


def ensure_generator_available(generator_src: Path | None = None) -> None:
    """
    Raises RuntimeError if the `ecomgen` package cannot be imported.
    """
    try:
        __import__("ecomgen")
    except ImportError as exc:
        if generator_src and generator_src.exists():
            # Allow call-site to set PYTHONPATH instead of failing immediately.
            return
        raise RuntimeError(
            "The `ecomgen` package was not found. "
            "Install the ecom_sales_data_generator project in editable mode "
            "or provide --generator-src pointing to the repo's src directory."
        ) from exc


def run_generator_cli(
    *,
    config_path: Path,
    output_dir: Path,
    messiness_level: str = "baseline",
    start_date: str | None = None,
    end_date: str | None = None,
    extra_args: Iterable[str] | None = None,
    generator_src: Path | None = None,
) -> None:
    """
    Invokes the generator CLI and streams stdout/stderr.
    """
    resolved_generator_src = generator_src
    if resolved_generator_src is None:
        candidate = Path.cwd().parent / "ecom_sales_data_generator" / "src"
        if candidate.exists():
            resolved_generator_src = candidate

    ensure_generator_available(resolved_generator_src)

    config_path = config_path.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "ecomgen.run_data_generation",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
        "--messiness-level",
        messiness_level,
    ]
    if start_date and end_date:
        cmd.extend(["--start-date", start_date, "--end-date", end_date])
    if extra_args:
        cmd.extend(extra_args)

    env = dict(os.environ)
    if resolved_generator_src and resolved_generator_src.exists():
        existing = env.get("PYTHONPATH", "")
        new_path = str(resolved_generator_src)
        env["PYTHONPATH"] = f"{new_path}:{existing}" if existing else new_path
        generator_cwd = resolved_generator_src.parent
    else:
        generator_cwd = None

    subprocess.run(cmd, check=True, env=env, cwd=str(generator_cwd) if generator_cwd else None)

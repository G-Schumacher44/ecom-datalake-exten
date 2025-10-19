"""
Plugin hooks for extending export behavior.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from .manifest import PartitionManifest


@dataclass(frozen=True)
class ExportContext:
    table: str
    partition_dir: Path
    manifest_path: Path
    manifest: PartitionManifest


HookCallable = Callable[[ExportContext], None]


def load_hook(path: str) -> HookCallable:
    """
    Load a hook callable from dotted path `module:function`.
    """
    if ":" not in path:
        raise ValueError(f"Invalid hook path '{path}'. Expected format module:function.")
    module_name, func_name = path.split(":", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, func_name, None)
    if not callable(func):
        raise ValueError(f"Hook '{path}' is not callable.")
    return func  # type: ignore[return-value]


def execute_hooks(hooks: Sequence[HookCallable], context: ExportContext) -> None:
    for hook in hooks:
        hook(context)

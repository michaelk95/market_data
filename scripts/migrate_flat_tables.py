"""
migrate_flat_tables.py
----------------------
One-shot migration (MD-09): convert the flat per-symbol ohlcv, indices and
options files to the bitemporal layout read by ``storage.read_table``.

Not implemented yet; the tests in tests/test_migrate_flat_tables.py define
the behaviour.
"""

from __future__ import annotations

from pathlib import Path


def run(
    table: str,
    data_dir: Path,
    *,
    symbols: list[str] | None = None,
    chunk_size: int = 250,
    dry_run: bool = False,
) -> int:
    """Migrate flat files for *table*; return net new rows (or rows read, if dry_run)."""
    return 0

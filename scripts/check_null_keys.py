"""
check_null_keys.py
------------------
Read-only audit for issue #74: null-key rows and flat-file symbols missing
from the bitemporal ``ohlcv``, ``indices`` and ``options`` tables.

For each table it reports:

- **Null-key rows in the table**, per symbol: rows with a null column of
  ``schema.DEDUP_KEYS``.  These can't be deduplicated or attributed and were
  stored by the writers before 0.12.1.
- **Null-key rows in the flat files**, per file (the old
  ``data/<table>/<SYMBOL>.parquet``).
- **Missing symbols**: a flat file has valid rows but its symbol has no rows
  in the table.  Those that also hold null-key rows are *recoverable*: the
  migration skipped them before 0.12.1, and re-running
  ``scripts/migrate_flat_tables.py --table <table>`` now migrates their good
  rows.
- **Skipped files**: files whose valid rows aren't all for the symbol in the
  file name (e.g. a stale ``merged.parquet``); the migration skips them too.

Nothing is written to the data directory.  Only the key columns are read.
Exits 1 if the table holds null-key rows, a symbol is missing or a file can't
be read; 0 otherwise.

Usage
-----
    python scripts/check_null_keys.py                     # options
    python scripts/check_null_keys.py --table all
    python scripts/check_null_keys.py --data-dir /tmp/data_copy
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import pyarrow as pa
from migrate_flat_tables import discover_flat_files

from market_data.config import cfg as _cfg
from market_data.flat_tables import FLAT_DATE_COLUMN, flat_key_columns
from market_data.logging_config import setup_logging
from market_data.schema import DEDUP_KEYS
from market_data.storage import read_table

logger = logging.getLogger("market_data.check_null_keys")

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
NO_SYMBOL = "<no symbol>"


@dataclass
class NullKeyReport:
    table: str
    table_null_rows: dict[str, int] = field(default_factory=dict)
    flat_null_rows: dict[str, int] = field(default_factory=dict)
    missing_symbols: list[str] = field(default_factory=list)
    recoverable: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    unreadable_files: list[str] = field(default_factory=list)

    @property
    def has_problems(self) -> bool:
        return bool(self.table_null_rows or self.missing_symbols or self.unreadable_files)


def _null_mask(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    return df[keys].isna().any(axis=1)


def run(table: str, data_dir: Path = DATA_DIR) -> NullKeyReport:
    """Audit *table* under *data_dir*; see the module docstring."""
    if table not in FLAT_DATE_COLUMN:
        raise ValueError(f"Unknown table '{table}'. Valid: {sorted(FLAT_DATE_COLUMN)}")
    data_dir = Path(data_dir)
    report = NullKeyReport(table)

    keys = DEDUP_KEYS[table]
    stored = read_table(table, data_dir, columns=keys)
    null_rows = _null_mask(stored, keys)
    report.table_null_rows = (
        stored.loc[null_rows, "symbol"].fillna(NO_SYMBOL).value_counts().sort_index().to_dict()
    )
    stored_symbols = set(stored.loc[~null_rows, "symbol"])

    flat_keys = flat_key_columns(table)
    for path in discover_flat_files(data_dir / table):
        try:
            flat = pd.read_parquet(path, columns=flat_keys)
        except (OSError, pa.ArrowException) as exc:
            logger.error("%s  failed to read: %s", path.name, exc)
            report.unreadable_files.append(path.name)
            continue
        null_rows = _null_mask(flat, flat_keys)
        if null_rows.any():
            report.flat_null_rows[path.stem] = int(null_rows.sum())
        good_symbols = set(flat.loc[~null_rows, "symbol"])
        if not good_symbols:
            continue
        if good_symbols != {path.stem}:
            report.skipped_files.append(path.name)
        elif path.stem not in stored_symbols:
            report.missing_symbols.append(path.stem)
            if null_rows.any():
                report.recoverable.append(path.stem)

    return report


def _log_report(report: NullKeyReport) -> None:
    t = report.table
    table_total = sum(report.table_null_rows.values())
    logger.info("%s: %d null-key row(s) in the table", t, table_total)
    for symbol, n in report.table_null_rows.items():
        logger.info("  %s=%d", symbol, n)
    logger.info(
        "%s: %d flat file(s) with null-key rows (%d row(s))",
        t, len(report.flat_null_rows), sum(report.flat_null_rows.values()),
    )
    logger.info(
        "%s: %d symbol(s) in flat files but missing from the table, %d recoverable "
        "by re-running migrate_flat_tables.py --table %s",
        t, len(report.missing_symbols), len(report.recoverable), t,
    )
    if report.missing_symbols:
        logger.info("  missing: %s", " ".join(report.missing_symbols))
    if report.recoverable:
        logger.info("  recoverable: %s", " ".join(report.recoverable))
    if report.skipped_files:
        logger.info("%s: skipped (not named for their symbol): %s", t, " ".join(report.skipped_files))
    if report.unreadable_files:
        logger.warning("%s: unreadable: %s", t, " ".join(report.unreadable_files))


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Read-only audit of null-key rows and symbols missing from the "
        "ohlcv/indices/options tables (issue #74)."
    )
    parser.add_argument(
        "--table",
        default="options",
        choices=[*sorted(FLAT_DATE_COLUMN), "all"],
        help="Table to check, or 'all' (default: options).",
    )
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR, help=f"Data root (default: {DATA_DIR}).")
    args = parser.parse_args()

    tables = sorted(FLAT_DATE_COLUMN) if args.table == "all" else [args.table]
    problems = False
    for table in tables:
        report = run(table, args.data_dir)
        _log_report(report)
        problems |= report.has_problems
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()

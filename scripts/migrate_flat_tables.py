"""
migrate_flat_tables.py
----------------------
One-shot migration (MD-09): convert the flat per-symbol files written by the
old collector to the bitemporal layout read by ``storage.read_table``.

    data/ohlcv/<SYMBOL>.parquet    → data/ohlcv/year=YYYY/data.parquet
    data/options/<SYMBOL>.parquet  → data/options/year=YYYY/data.parquet
    data/indices/<SYMBOL>.parquet  → data/indices/data.parquet

Column mapping
--------------
- ``date`` (ohlcv, indices) or ``snapshot_date`` (options) becomes
  ``period_start_date``, ``period_end_date`` and ``report_date``: a daily bar
  or a chain snapshot is known on the day it describes.
- ``report_time_marker`` is ``post-market`` for daily bars (known at the
  close) and ``pre-market`` for options, whose chains have always been fetched
  around 04:00 local time, before the open (see MD-14).
- ``collected_at`` is the flat file's modification time. The real collection
  time of each row was never recorded; the mtime keeps the newest
  ``collected_at`` equal to the last time the collector actually wrote, so
  freshness checks stay honest.
- Values are cast to ``schema.TABLE_SCHEMAS`` (e.g. ``volume`` int64 → float64).

The mapping lives in ``market_data.flat_tables`` and is shared with the live
writers, so migrated rows and rows written live deduplicate against each other.

Files whose ``symbol`` column is not exactly their file name are skipped with a
warning (the live ``data/ohlcv`` holds a stale multi-symbol ``merged.parquet``).

The flat files are left in place, and re-running is safe: rows already in the
table are deduplicated by ``write_table`` (the stored row wins), so a re-run
picks up only rows the old collector added since the last run, including when
the new writers have already stored some of the same bars (cutover re-sync).

Usage
-----
    python scripts/migrate_flat_tables.py --table ohlcv
    python scripts/migrate_flat_tables.py --table all --dry-run
    python scripts/migrate_flat_tables.py --table options --symbols AAPL MSFT
    python scripts/migrate_flat_tables.py --table ohlcv --data-dir /tmp/data_copy
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa

from market_data.config import cfg as _cfg
from market_data.flat_tables import (  # noqa: F401  (re-exported for callers)
    FLAT_DATE_COLUMN,
    REPORT_TIME_MARKER,
    conform_to_schema,
    drop_null_keys,
    to_bitemporal,
)
from market_data.storage import write_table

logger = logging.getLogger("market_data.migrate_flat_tables")

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
DEFAULT_CHUNK_SIZE = 250

# File name of the unpartitioned table (indices), which sits next to the flat files.
_UNPARTITIONED_FILE = "data.parquet"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def discover_flat_files(table_dir: Path, symbols: list[str] | None = None) -> list[Path]:
    """Return the per-symbol flat files in *table_dir*.

    Skips the migrated ``data.parquet``, leftover ``<SYMBOL>.tmp.parquet``
    files from interrupted atomic writes, hidden files and partition dirs.
    """
    if not table_dir.exists():
        return []
    files = sorted(
        p for p in table_dir.glob("*.parquet")
        if p.is_file()
        and p.name != _UNPARTITIONED_FILE
        and not p.name.startswith(".")
        and not p.stem.endswith(".tmp")
    )
    if symbols:
        wanted = {s.upper() for s in symbols}
        files = [p for p in files if p.stem.upper() in wanted]
    return files


def _file_mtime(path: Path) -> pd.Timestamp:
    return pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC").floor("us")


def _load_chunk(files: list[Path], table: str) -> pd.DataFrame:
    frames = []
    for path in files:
        try:
            flat = pd.read_parquet(path)
        except (OSError, pa.ArrowException) as exc:
            logger.error("%s  failed to read: %s", path.name, exc)
            continue
        # Legacy rows with a null key (issue #74) would otherwise put a NaN in
        # the symbol check below and skip the whole file.
        flat = drop_null_keys(flat, table)
        if flat.empty:
            continue
        # A per-symbol file holds exactly its own symbol. Anything else (e.g. a
        # stale merge.py output saved as ohlcv/merged.parquet) is not collector
        # data and could overwrite real rows in dedup.
        if set(flat["symbol"].unique()) != {path.stem}:
            logger.warning("%s  skipped: symbol column does not match the file name", path.name)
            continue
        frames.append(to_bitemporal(flat, table, _file_mtime(path)))
    if not frames:
        return pd.DataFrame()
    return conform_to_schema(pd.concat(frames, ignore_index=True), table)


# ---------------------------------------------------------------------------
# Main migration run
# ---------------------------------------------------------------------------


def run(
    table: str,
    data_dir: Path = DATA_DIR,
    *,
    symbols: list[str] | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    dry_run: bool = False,
) -> int:
    """Migrate the flat per-symbol files of *table* into the bitemporal store.

    Files are processed *chunk_size* at a time and written with one
    ``write_table`` call per chunk, so each year partition is rewritten once
    per chunk rather than once per symbol.

    Parameters
    ----------
    table:
        ``"ohlcv"``, ``"indices"`` or ``"options"``.
    data_dir:
        Root data directory (contains ``<table>/``).
    symbols:
        Restrict the migration to these symbols (case-insensitive).
    chunk_size:
        Number of flat files per ``write_table`` call.
    dry_run:
        Read and convert, but write nothing.

    Returns
    -------
    int
        Net new rows written, or rows read when *dry_run* is set.
    """
    if table not in FLAT_DATE_COLUMN:
        raise ValueError(
            f"Unknown table '{table}' for flat-file migration. "
            f"Valid: {sorted(FLAT_DATE_COLUMN)}"
        )
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")

    data_dir = Path(data_dir)
    files = discover_flat_files(data_dir / table, symbols)
    if not files:
        logger.info("%s: no flat files found in %s", table, data_dir / table)
        return 0

    mode = "DRY RUN — " if dry_run else ""
    logger.info("%s%s: migrating %d flat file(s)", mode, table, len(files))

    total = 0
    for start in range(0, len(files), chunk_size):
        chunk = files[start:start + chunk_size]
        df = _load_chunk(chunk, table)
        if df.empty:
            continue
        n = len(df) if dry_run else write_table(df, table, data_dir)
        total += n
        logger.info(
            "  %s files %d–%d of %d: rows_read=%d  %s=%d",
            table, start + 1, start + len(chunk), len(files), len(df),
            "rows" if dry_run else "new_rows", n,
        )

    logger.info("%s%s migration done: %d rows", mode, table, total)
    return total


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    from market_data.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Migrate flat per-symbol ohlcv/indices/options files to the bitemporal layout."
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=[*sorted(FLAT_DATE_COLUMN), "all"],
        help="Table to migrate, or 'all'.",
    )
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR, help=f"Data root (default: {DATA_DIR}).")
    parser.add_argument("--symbols", nargs="+", metavar="SYMBOL", help="Restrict to these symbols.")
    parser.add_argument(
        "--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
        help=f"Flat files per write (default: {DEFAULT_CHUNK_SIZE}).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Read and convert only; write nothing.")
    args = parser.parse_args()

    tables = sorted(FLAT_DATE_COLUMN) if args.table == "all" else [args.table]
    for table in tables:
        run(
            table,
            args.data_dir,
            symbols=args.symbols,
            chunk_size=args.chunk_size,
            dry_run=args.dry_run,
        )


if __name__ == "__main__":
    main()

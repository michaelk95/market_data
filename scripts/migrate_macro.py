"""
migrate_macro.py
----------------
One-shot migration: re-pull all FRED series with full vintage history and
write into the bitemporal storage layout (data/macro/data.parquet).

Old per-series files (data/macro/<SERIES_ID>.parquet) cannot be converted
in place — they have no vintage information. This script does a full re-pull
from FRED's realtime API (get_series_all_releases) starting from DEFAULT_START.

Usage
-----
    python scripts/migrate_macro.py               # migrate all default series
    python scripts/migrate_macro.py --backup      # rename old files to .bak
    python scripts/migrate_macro.py --dry-run     # report counts, write nothing
    python scripts/migrate_macro.py --series DFF T10Y2Y  # specific series only
    python scripts/migrate_macro.py --start 1990-01-01   # custom bootstrap start
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from market_data.config import cfg as _cfg
from market_data.fetch_macro import DEFAULT_START, _load_api_key, fetch_series_vintages
from market_data.storage import write_table

logger = logging.getLogger(__name__)

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

def _old_macro_dir(data_dir: Path) -> Path:
    return data_dir / "macro"


def _discover_old_files(macro_dir: Path) -> list[Path]:
    """Return per-series .parquet files in macro_dir (excludes data.parquet and subdirs)."""
    if not macro_dir.exists():
        return []
    return sorted(
        p for p in macro_dir.glob("*.parquet")
        if p.is_file()
        and p.name != "data.parquet"
        and not p.name.startswith(".")
    )


def _migrate_series(
    series_id: str,
    api_key: str,
    *,
    start: str,
    dry_run: bool,
    data_dir: Path,
) -> int:
    """
    Re-pull a single FRED series with full vintage history and write to the
    bitemporal store.

    Returns number of rows written (0 for dry runs).
    """
    df = fetch_series_vintages(series_id, realtime_start=start, api_key=api_key)

    if df.empty:
        logger.info("%s  no data returned from FRED", series_id)
        return 0

    if dry_run:
        return len(df)

    return write_table(df, "macro", data_dir)


# ---------------------------------------------------------------------------
# Main migration run
# ---------------------------------------------------------------------------

def run(
    series_ids: list[str] | None = None,
    *,
    start: str = DEFAULT_START,
    dry_run: bool = False,
    backup: bool = False,
    data_dir: Path = DATA_DIR,
) -> None:
    """
    Re-pull FRED series with full vintage history into the bitemporal store.

    Parameters
    ----------
    series_ids:
        Restrict migration to these series.  Defaults to all per-series files
        found in the old macro directory.
    start:
        Bootstrap start date passed to the FRED realtime API.
    dry_run:
        If True, fetch and report counts but do not write anything.
    backup:
        If True, rename migrated source files to ``<SERIES_ID>.parquet.bak``.
    data_dir:
        Root data directory (contains the ``macro/`` subdirectory).
    """
    macro_dir = _old_macro_dir(data_dir)
    old_files = _discover_old_files(macro_dir)

    if series_ids:
        series_set = {s.upper() for s in series_ids}
        targets = list(series_set)
        old_files = [p for p in old_files if p.stem.upper() in series_set]
    else:
        targets = [p.stem for p in old_files]

    if not targets:
        logger.info("No old-format macro files found in %s", macro_dir)
        return

    api_key = _load_api_key()
    mode = "DRY RUN — " if dry_run else ""
    logger.info("%sMigrating %d series from %s", mode, len(targets), macro_dir)

    total_written = 0
    file_map = {p.stem: p for p in old_files}

    for i, series_id in enumerate(targets, 1):
        logger.info("[%d/%d] %s", i, len(targets), series_id)
        try:
            written = _migrate_series(
                series_id,
                api_key,
                start=start,
                dry_run=dry_run,
                data_dir=data_dir,
            )
        except Exception as exc:
            logger.error("  %s  ERROR: %s", series_id, exc, exc_info=True)
            written = 0

        total_written += written
        logger.info("  %s  rows=%d", series_id, written)

        if not dry_run and backup and series_id in file_map:
            old_path = file_map[series_id]
            bak = old_path.with_suffix(".parquet.bak")
            old_path.rename(bak)
            logger.debug("  renamed %s → %s", old_path.name, bak.name)

    logger.info(
        "%smigration done: rows=%d  series=%d",
        mode, total_written, len(targets),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Migrate existing per-series macro parquets to the bitemporal "
            "storage layout, re-pulling full FRED vintage history."
        )
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="SERIES_ID",
        help="Restrict migration to these series (default: all).",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        metavar="YYYY-MM-DD",
        help=f"Bootstrap start date (default: {DEFAULT_START}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and log counts only — do not write any files.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Rename migrated source files to <SERIES_ID>.parquet.bak.",
    )
    args = parser.parse_args()

    run(
        series_ids=args.series,
        start=args.start,
        dry_run=args.dry_run,
        backup=args.backup,
    )


if __name__ == "__main__":
    main()

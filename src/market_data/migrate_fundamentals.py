"""
migrate_fundamentals.py
-----------------------
One-shot migration: convert existing per-ticker fundamentals Parquet files
(data/fundamentals/<SYMBOL>.parquet, old schema with `as_of`) to the new
bitemporal layout (data/fundamentals/year=YYYY/data.parquet).

For each historical row the migration:
  1. Looks up the most recent 10-K/10-Q EDGAR filing date on or before the
     row's `as_of` date.
  2. Uses that as `report_date` (and `period_start_date`/`period_end_date`).
  3. Sets `report_date_known=True`; falls back to `as_of` with
     `report_date_known=False` when no EDGAR date is available.

Usage
-----
    market-data-migrate-fundamentals               # migrate all, keep old files
    market-data-migrate-fundamentals --backup      # rename old files to .bak
    market-data-migrate-fundamentals --dry-run     # report counts, write nothing
    market-data-migrate-fundamentals --symbols AAPL MSFT  # specific tickers only
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from market_data import edgar
from market_data.config import cfg as _cfg
from market_data.fetch_fundamentals import INFO_FIELDS
from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import write_table

logger = logging.getLogger(__name__)

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
SLEEP_BETWEEN_EDGAR_CALLS: float = 0.15  # stay well under 10 req/s


# ---------------------------------------------------------------------------
# Migration helpers
# ---------------------------------------------------------------------------

def _old_fund_dir(data_dir: Path) -> Path:
    return data_dir / "fundamentals"


def _discover_old_files(fund_dir: Path) -> list[Path]:
    """Return all per-ticker .parquet files in fund_dir (not inside year=* subdirs)."""
    if not fund_dir.exists():
        return []
    return sorted(
        p for p in fund_dir.glob("*.parquet")
        if p.is_file() and not p.name.startswith(".")
    )


def _migrate_ticker(
    path: Path,
    *,
    dry_run: bool,
    data_dir: Path,
) -> tuple[int, int]:
    """
    Migrate a single per-ticker parquet to the bitemporal store.

    Returns (rows_written, edgar_hits).
    """
    symbol = path.stem
    try:
        old_df = pd.read_parquet(path)
    except Exception as exc:
        logger.error("%s  failed to read old file: %s", symbol, exc)
        return 0, 0

    if old_df.empty:
        logger.info("%s  empty file, skipping", symbol)
        return 0, 0

    records: list[dict] = []
    edgar_hits = 0

    for _, row in old_df.iterrows():
        as_of: date = row["as_of"] if isinstance(row["as_of"], date) else pd.to_datetime(row["as_of"]).date()

        # Try to get EDGAR filing date
        try:
            filing_date = edgar.get_latest_filing_date(symbol, before=as_of)
            time.sleep(SLEEP_BETWEEN_EDGAR_CALLS)
        except Exception as exc:
            logger.debug("%s  EDGAR error for %s: %s", symbol, as_of, exc)
            filing_date = None

        if filing_date is not None:
            report_date = filing_date
            report_date_known = True
            edgar_hits += 1
        else:
            report_date = as_of
            report_date_known = False

        record: dict = {"symbol": symbol}
        for col in INFO_FIELDS:
            val = row.get(col)
            if col == "analyst_recommendation":
                record[col] = str(val) if pd.notna(val) and val is not None else None
            elif col == "analyst_count":
                record[col] = int(val) if pd.notna(val) and val is not None else None
            else:
                record[col] = float(val) if pd.notna(val) and val is not None else None

        record["report_date_known"] = report_date_known
        record.update({
            "period_start_date":  report_date,
            "period_end_date":    report_date,
            "report_date":        report_date,
            "report_time_marker": ReportTimeMarker.POST_MARKET,
            "source":             DataSource.YFINANCE,
            "collected_at":       datetime.now(timezone.utc),
        })
        records.append(record)

    if dry_run or not records:
        return len(records), edgar_hits

    df = pd.DataFrame(records)
    written = write_table(df, "fundamentals", data_dir)
    return written, edgar_hits


# ---------------------------------------------------------------------------
# Main migration run
# ---------------------------------------------------------------------------

def run(
    symbols: list[str] | None = None,
    *,
    dry_run: bool = False,
    backup: bool = False,
    data_dir: Path = DATA_DIR,
) -> None:
    """
    Migrate existing per-ticker fundamentals parquets to the bitemporal store.

    Parameters
    ----------
    symbols:
        Restrict migration to these tickers.  Defaults to all per-ticker files.
    dry_run:
        If True, read and report counts but do not write anything.
    backup:
        If True, rename migrated source files to `<SYMBOL>.parquet.bak`.
    data_dir:
        Root data directory (contains the `fundamentals/` subdirectory).
    """
    fund_dir = _old_fund_dir(data_dir)
    old_files = _discover_old_files(fund_dir)

    if symbols:
        symbol_set = {s.upper() for s in symbols}
        old_files = [p for p in old_files if p.stem.upper() in symbol_set]

    if not old_files:
        logger.info("No old-format fundamentals files found in %s", fund_dir)
        return

    mode = "DRY RUN — " if dry_run else ""
    logger.info("%sMigrating %d ticker(s) from %s", mode, len(old_files), fund_dir)

    total_written = 0
    total_edgar_hits = 0
    total_edgar_misses = 0

    for i, path in enumerate(old_files, 1):
        symbol = path.stem
        logger.info("[%d/%d] %s", i, len(old_files), symbol)

        written, edgar_hits = _migrate_ticker(path, dry_run=dry_run, data_dir=data_dir)
        edgar_misses = written - edgar_hits if written >= edgar_hits else 0

        total_written += written
        total_edgar_hits += edgar_hits
        total_edgar_misses += edgar_misses

        logger.info(
            "  %s rows=%d  edgar_hits=%d  edgar_misses=%d",
            symbol, written, edgar_hits, edgar_misses,
        )

        if not dry_run and backup:
            bak = path.with_suffix(".parquet.bak")
            path.rename(bak)
            logger.debug("  renamed %s → %s", path.name, bak.name)

    logger.info(
        "%smigration done: rows=%d  edgar_hits=%d  edgar_misses=%d",
        mode, total_written, total_edgar_hits, total_edgar_misses,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Migrate existing per-ticker fundamentals parquets to the bitemporal "
            "storage layout, backfilling SEC EDGAR filing dates as report_date."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help="Restrict migration to these tickers (default: all).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and log counts only — do not write any files.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Rename migrated source files to <SYMBOL>.parquet.bak instead of leaving them.",
    )
    args = parser.parse_args()

    run(symbols=args.symbols, dry_run=args.dry_run, backup=args.backup)


if __name__ == "__main__":
    main()

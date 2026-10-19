"""
merge.py
--------
Combines the flat per-ticker OHLCV files (data/ohlcv/<SYMBOL>.parquet, written
by fetch.save_ticker_data) into a single data/merged.parquet for the
paper_trading backtest engine. The output keeps the flat schema (date, symbol,
open, high, low, close, volume), which paper_trading reads.

Only top-level *.parquet files are read. The year=YYYY/ partitions that
scripts/migrate_flat_tables.py creates alongside them are a one-off snapshot
the collector does not update, so they are deliberately ignored.

Usage
-----
    market-data-merge                          # reads data/ohlcv/, writes data/merged.parquet
    market-data-merge --data-dir ./data/ohlcv  # explicit paths
    market-data-merge --out ./merged.parquet
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data/ohlcv")
DEFAULT_OUT = Path("data") / "merged.parquet"


def run(data_dir: Path = DEFAULT_DATA_DIR, out: Path = DEFAULT_OUT) -> None:
    ticker_files = sorted(
        p for p in data_dir.glob("*.parquet")
        if p.name != out.name  # exclude any existing merged file
    )

    if not ticker_files:
        logger.warning("No per-ticker Parquet files found in %s/", data_dir)
        return

    logger.info("Merging %d ticker file(s)...", len(ticker_files))

    frames = []
    for path in ticker_files:
        try:
            df = pd.read_parquet(path)
            frames.append(df)
        except Exception as exc:
            logger.warning("skipping %s: %s", path.name, exc, exc_info=True)

    if not frames:
        logger.warning("Nothing to merge.")
        return

    merged = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["date", "symbol"])
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )

    out.parent.mkdir(parents=True, exist_ok=True)

    # Atomic write
    tmp = out.with_suffix(".tmp.parquet")
    merged.to_parquet(tmp, index=False)
    tmp.replace(out)

    symbols = merged["symbol"].nunique()
    dates = merged["date"].nunique()
    rows = len(merged)
    logger.info(
        "Wrote %s  —  %s rows  |  %s symbols  |  %s trading days",
        out,
        f"{rows:,}",
        f"{symbols:,}",
        f"{dates:,}",
    )


def main() -> None:
    from market_data.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Merge per-ticker OHLCV Parquet files into one file for the backtest engine."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Directory containing per-ticker Parquet files (default: {DEFAULT_DATA_DIR})",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output path for the merged file (default: {DEFAULT_OUT})",
    )
    args = parser.parse_args()
    run(data_dir=args.data_dir, out=args.out)


if __name__ == "__main__":
    main()

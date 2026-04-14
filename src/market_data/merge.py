"""
merge.py
--------
Combines all per-ticker Parquet files in the data/ directory into a single
merged.parquet file ready for the paper_trading backtest engine.

Usage
-----
    python merge.py                       # reads data/, writes data/merged.parquet
    python merge.py --data-dir ./data     # explicit paths
    python merge.py --out ./merged.parquet
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
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Merge per-ticker Parquet files into one file for the backtest engine."
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

"""
merge.py
--------
Builds data/merged.parquet for the paper_trading backtest engine from the
bitemporal ``ohlcv`` table (data/ohlcv/year=YYYY/data.parquet).

The output keeps the flat ``date, symbol, open, high, low, close, volume``
layout paper_trading's MarketData reads; ``date`` is ``period_start_date``.
Leftover flat ``data/ohlcv/<SYMBOL>.parquet`` files are not read.

Usage
-----
    python merge.py                       # reads data/ohlcv/, writes data/merged.parquet
    python merge.py --data-dir ./data     # explicit paths
    python merge.py --out ./merged.parquet
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from market_data.fetch import OHLCV_COLS
from market_data.storage import read_table

logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUT = Path("data") / "merged.parquet"


def run(data_dir: Path = DEFAULT_DATA_DIR, out: Path = DEFAULT_OUT) -> None:
    """Write *out* from the ``ohlcv`` table under the data root *data_dir*."""
    df = read_table(
        "ohlcv",
        Path(data_dir),
        columns=["period_start_date", "symbol", "open", "high", "low", "close", "volume"],
    )
    if df.empty:
        logger.warning("No rows in the ohlcv table under %s/ — nothing to merge.", data_dir)
        return

    merged = (
        df.rename(columns={"period_start_date": "date"})[OHLCV_COLS]
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
        description="Build merged.parquet for the backtest engine from the ohlcv table."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help=f"Data root containing the ohlcv/ table (default: {DEFAULT_DATA_DIR})",
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

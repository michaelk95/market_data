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
from pathlib import Path

import pandas as pd

DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUT = DEFAULT_DATA_DIR / "merged.parquet"


def run(data_dir: Path = DEFAULT_DATA_DIR, out: Path = DEFAULT_OUT) -> None:
    ticker_files = sorted(
        p for p in data_dir.glob("*.parquet")
        if p.name != out.name  # exclude any existing merged file
    )

    if not ticker_files:
        print(f"No per-ticker Parquet files found in {data_dir}/")
        return

    print(f"Merging {len(ticker_files)} ticker file(s)...")

    frames = []
    for path in ticker_files:
        try:
            df = pd.read_parquet(path)
            frames.append(df)
        except Exception as exc:
            print(f"  WARNING: skipping {path.name}: {exc}")

    if not frames:
        print("Nothing to merge.")
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
    print(f"Wrote {out}  —  {rows:,} rows  |  {symbols:,} symbols  |  {dates:,} trading days")


def main() -> None:
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

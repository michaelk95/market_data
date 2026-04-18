"""
fetch_constituent_history.py
----------------------------
Downloads and stores historical S&P 500 constituent membership.

Source: fja05680/sp500 on GitHub (MIT licence)
  sp500_ticker_start_end.csv — one row per ticker per membership period,
  with columns: ticker, start_date, end_date (empty = still active).
  Tickers that rejoined the index after being removed appear as multiple rows.

  URL: https://raw.githubusercontent.com/fja05680/sp500/refs/heads/master/
       sp500_ticker_start_end.csv

Russell 2000 historical constituent data has no free structured source
and is not collected here.  Add when a reliable source is identified.

Output: data/constituent_history.parquet
  Schema:
    ticker        str           yfinance-compatible symbol
    index         str           "SP500"
    date_added    datetime64    date ticker entered the index
    date_removed  datetime64    date ticker left the index; NaT = still active

  Note: a ticker may appear more than once when it left and later rejoined
  the index (e.g. AAL: 1996-01-02→1997-01-15, then 2015-03-23→2024-09-23).

CLI:  market-data-fetch-constituent-history
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from pathlib import Path

import pandas as pd
import requests

from market_data.config import cfg
from market_data.resilience import requests_retry

logger = logging.getLogger(__name__)

_TICKER_START_END_URL = (
    "https://raw.githubusercontent.com/fja05680/sp500/refs/heads/master/"
    "sp500_ticker_start_end.csv"
)


@requests_retry
def _fetch_raw() -> str:
    """Download sp500_ticker_start_end.csv and return the raw text."""
    resp = requests.get(_TICKER_START_END_URL, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_ticker_start_end(raw: str) -> pd.DataFrame:
    """Parse the sp500_ticker_start_end CSV into the constituent history schema.

    Parameters
    ----------
    raw:
        Raw CSV text from the fja05680 dataset.

    Returns
    -------
    DataFrame with columns: ticker, index, date_added, date_removed.
    Rows are sorted by ticker then date_added.  Tickers that rejoined the
    index appear as separate rows.
    """
    df = pd.read_csv(io.StringIO(raw), dtype=str)
    df.columns = [c.strip() for c in df.columns]

    required = {"ticker", "start_date", "end_date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Expected columns {required}; missing {missing}. "
            f"Got: {df.columns.tolist()}"
        )

    df["ticker"] = df["ticker"].str.strip()
    df = df[df["ticker"].notna() & (df["ticker"] != "")].copy()

    df["date_added"] = pd.to_datetime(
        df["start_date"].str.strip(), format="ISO8601", errors="coerce"
    )
    # Empty end_date means the ticker is still active → NaT
    df["date_removed"] = pd.to_datetime(
        df["end_date"].str.strip().replace("", pd.NA), format="ISO8601", errors="coerce"
    )

    result = df[["ticker", "date_added", "date_removed"]].copy()
    result.insert(1, "index", "SP500")
    result = (
        result.dropna(subset=["date_added"])
        .sort_values(["ticker", "date_added"])
        .reset_index(drop=True)
    )

    n_active = result["date_removed"].isna().sum()
    n_delisted = result["date_removed"].notna().sum()
    n_tickers = result["ticker"].nunique()
    logger.info(
        "Parsed SP500 constituent history: %d unique tickers, "
        "%d membership periods (%d active, %d with end date)",
        n_tickers, len(result), n_active, n_delisted,
    )
    return result


def run(out_path: Path) -> pd.DataFrame:
    """Fetch S&P 500 constituent history and write to *out_path* (parquet)."""
    logger.info("Fetching S&P 500 constituent history from fja05680/sp500...")
    raw = _fetch_raw()
    df = parse_ticker_start_end(raw)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.stem + ".tmp.parquet")
    df.to_parquet(tmp, index=False)
    tmp.replace(out_path)
    logger.info("Saved %d constituent records to %s", len(df), out_path)
    return df


def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Fetch S&P 500 historical constituent membership from "
            "github.com/fja05680/sp500 and save to parquet."
        )
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Output parquet path (default: data/constituent_history.parquet)",
    )
    args = parser.parse_args()

    out_path = (
        Path(args.out)
        if args.out
        else cfg.resolve_path(
            "paths.constituent_history_file", "data/constituent_history.parquet"
        )
    )

    try:
        run(out_path)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to fetch constituent history: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

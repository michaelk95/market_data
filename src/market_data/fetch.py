"""
fetch.py
--------
Core OHLCV fetch and storage logic.

Functions
---------
fetch_history(symbol, years)        Full historical pull (default 10 years).
fetch_incremental(symbol, since)    Pull data from a given date onward.
save_ticker_data(symbol, df, dir)   Append new rows to per-ticker Parquet,
                                    deduplicating on (date, symbol).
load_ticker_data(symbol, dir)       Load a per-ticker Parquet (or None).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

from market_data.config import cfg as _cfg
from market_data.resilience import yf_retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HISTORY_YEARS: int = _cfg.get("collection.history_years", 10)
OHLCV_COLS = ["date", "symbol", "open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Convert a raw yfinance DataFrame into our standard schema:
        date (date), symbol (str), open, high, low, close, volume (float)

    yfinance returns a DatetimeIndex that may be timezone-aware; we strip
    the time component and work purely with calendar dates.
    """
    if raw.empty:
        return pd.DataFrame(columns=OHLCV_COLS)

    df = raw.copy()

    # Flatten MultiIndex columns produced by yf.download() multi-ticker calls
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]

    # Strip timezone and convert index to plain date
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df.index = df.index.date
    df.index.name = "date"
    df = df.reset_index()

    df["symbol"] = symbol

    # Keep only the columns we care about; tolerate missing optional ones
    available = [c for c in OHLCV_COLS if c in df.columns]
    df = df[available].dropna(subset=["close"])

    # Ensure correct types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    return df.reset_index(drop=True)


def _ticker_path(symbol: str, data_dir: Path) -> Path:
    return data_dir / f"{symbol}.parquet"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@yf_retry
def fetch_history(symbol: str, years: int = DEFAULT_HISTORY_YEARS) -> pd.DataFrame:
    """
    Download `years` of daily OHLCV history for `symbol`.

    Returns an empty DataFrame (with correct columns) if no data is available
    (e.g. delisted ticker, bad symbol).
    """
    end = date.today()
    start = end.replace(year=end.year - years)

    raw = yf.Ticker(symbol).history(
        start=str(start),
        end=str(end),
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


@yf_retry
def fetch_incremental(symbol: str, since: date) -> pd.DataFrame:
    """
    Download daily OHLCV data for `symbol` from `since` onward.

    We start from `since` (inclusive) rather than since+1 so we never
    accidentally skip a trading day near the boundary. Duplicate rows are
    dropped by save_ticker_data().
    """
    raw = yf.Ticker(symbol).history(
        start=str(since),
        end=str(date.today() + timedelta(days=1)),  # end is exclusive in yf
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


@yf_retry
def fetch_date_range(
    symbol: str,
    start: date,
    end: date | None = None,
) -> pd.DataFrame:
    """Download daily OHLCV history for *symbol* from *start* to *end* inclusive.

    Used by the backfill pipeline to fetch bounded membership periods for
    delisted tickers.  Falls back to today when *end* is None.
    """
    if end is None:
        end = date.today()
    raw = yf.Ticker(symbol).history(
        start=str(start),
        end=str(end + timedelta(days=1)),  # yfinance end is exclusive
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


def load_ticker_data(symbol: str, data_dir: Path) -> pd.DataFrame | None:
    """
    Load the existing Parquet for `symbol`, or return None if it doesn't exist.
    """
    path = _ticker_path(symbol, data_dir)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def save_ticker_data(symbol: str, new_df: pd.DataFrame, data_dir: Path) -> int:
    """
    Merge `new_df` into the existing per-ticker Parquet file.

    Deduplicates on (date, symbol), sorts by date ascending, and overwrites
    the file atomically (write to temp then rename).

    Returns the number of net-new rows added.
    """
    if new_df.empty:
        return 0

    data_dir.mkdir(parents=True, exist_ok=True)
    path = _ticker_path(symbol, data_dir)

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    before = len(pd.read_parquet(path)) if path.exists() else 0

    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined = (
        combined
        .drop_duplicates(subset=["date", "symbol"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    # Atomic write: write to a temp file first, then rename
    tmp_path = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)

    after = len(combined)
    return after - before

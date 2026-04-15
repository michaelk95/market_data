"""
fetch_macro.py
--------------
Collect macroeconomic time series from the FRED API (Federal Reserve Bank
of St. Louis).

Series collected by default
----------------------------
Daily
  DFF       Effective Federal Funds Rate
  T10Y2Y    10-year minus 2-year Treasury spread

Monthly
  CPIAUCSL  CPI — All Urban Consumers (headline)
  CPILFESL  Core CPI (ex food & energy)
  PCEPI     PCE Price Index
  PCEPILFE  Core PCE
  UNRATE    Unemployment Rate
  PAYEMS    Nonfarm Payrolls (thousands of jobs)

Quarterly
  GDPC1     Real GDP (chained 2017 dollars)
  GDP       Nominal GDP

Data is stored under data/macro/<SERIES_ID>.parquet.
Schema: date (date), series_id (str), value (float).

Requires
--------
  FRED_API_KEY environment variable (or .env file at project root).

Usage
-----
    market-data-fetch-macro                        # update all default series
    market-data-fetch-macro --series DFF T10Y2Y    # update specific series
    market-data-fetch-macro --start 1990-01-01     # custom bootstrap start
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MACRO_DIR = Path(_cfg.get("paths.macro_dir", "data/macro"))

# Default bootstrap start date — FRED has data going back decades for most series
DEFAULT_START: str = _cfg.get("collection.macro_start", "1990-01-01")

# Series to collect by default — keys of the macro.series mapping in config.yaml
DEFAULT_SERIES: list[str] = list(
    _cfg.get(
        "macro.series",
        {
            # Daily
            "DFF":      "Effective Federal Funds Rate",
            "T10Y2Y":   "10yr minus 2yr Treasury yield spread",
            # Monthly
            "CPIAUCSL": "CPI All Urban Consumers (headline)",
            "CPILFESL": "Core CPI (ex food & energy)",
            "PCEPI":    "PCE Price Index",
            "PCEPILFE": "Core PCE",
            "UNRATE":   "Unemployment Rate",
            "PAYEMS":   "Nonfarm Payrolls",
            # Quarterly
            "GDPC1":    "Real GDP (chained 2017 dollars)",
            "GDP":      "Nominal GDP",
        },
    ).keys()
)


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------

def _load_api_key() -> str:
    """
    Load the FRED API key from the environment (or .env file).

    Raises RuntimeError with a clear message if the key is missing so the
    user knows exactly what to fix.
    """
    # Attempt to load .env from the current working directory
    try:
        from dotenv import load_dotenv  # type: ignore[import]
        load_dotenv()
    except ImportError:
        pass  # python-dotenv not installed; rely on the real environment

    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FRED_API_KEY is not set. Add it to your .env file:\n"
            "  FRED_API_KEY=your_key_here\n"
            "Get a free key at https://fred.stlouisfed.org/"
        )
    return key


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _series_path(series_id: str, macro_dir: Path) -> Path:
    return macro_dir / f"{series_id}.parquet"


def load_macro_series(series_id: str, macro_dir: Path) -> pd.DataFrame | None:
    """Load an existing macro Parquet, or return None if it doesn't exist."""
    path = _series_path(series_id, macro_dir)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def save_macro_series(series_id: str, new_df: pd.DataFrame, macro_dir: Path) -> int:
    """
    Merge new_df into the existing per-series Parquet file.

    Deduplicates on (date, series_id), sorts by date, and writes atomically.
    Returns the number of net-new rows added.
    """
    if new_df.empty:
        return 0

    macro_dir.mkdir(parents=True, exist_ok=True)
    path = _series_path(series_id, macro_dir)

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        before = len(existing)
    else:
        combined = new_df.copy()
        before = 0

    combined["date"] = pd.to_datetime(combined["date"]).dt.date
    combined = (
        combined
        .drop_duplicates(subset=["date", "series_id"])
        .sort_values("date")
        .reset_index(drop=True)
    )

    tmp_path = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)

    return len(combined) - before


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_series(
    series_id: str,
    start: str,
    api_key: str,
) -> pd.DataFrame:
    """
    Pull a FRED series from `start` to today.

    Returns a DataFrame with columns: date (date), series_id (str), value (float).
    Returns an empty DataFrame if no data is available.
    """
    try:
        import fredapi  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "fredapi is not installed. Run: pip install fredapi"
        ) from exc

    fred = fredapi.Fred(api_key=api_key)
    raw = fred.get_series(series_id, observation_start=start)

    if raw is None or raw.empty:
        return pd.DataFrame(columns=["date", "series_id", "value"])

    df = raw.reset_index()
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["series_id"] = series_id
    df = df[["date", "series_id", "value"]].dropna(subset=["value"])
    df["value"] = df["value"].astype(float)

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def update_series(series_id: str, api_key: str, start: str, macro_dir: Path) -> int:
    """
    Bootstrap or incrementally update a single FRED series.

    On first run: pulls from `start` to today.
    On subsequent runs: pulls from (last stored date - 7 days) to today.
    The 7-day lookback ensures we don't miss delayed revisions.

    Returns the number of new rows added.
    """
    existing = load_macro_series(series_id, macro_dir)

    if existing is None:
        fetch_start = start
        action = f"bootstrap from {start}"
    else:
        last_date = existing["date"].max()
        # Look back 7 days to catch revisions / late-released data points
        lookback = last_date - timedelta(days=7)
        fetch_start = str(lookback)
        action = f"incremental since {lookback}"

    df = fetch_series(series_id, start=fetch_start, api_key=api_key)

    if df.empty:
        logger.info("%s  no data  (%s)", series_id, action)
        return 0

    added = save_macro_series(series_id, df, macro_dir)
    logger.info("%s  +%d rows  (%s)", series_id, added, action)
    return added


def run(
    series_ids: list[str] | None = None,
    start: str = DEFAULT_START,
    macro_dir: Path = MACRO_DIR,
) -> None:
    """
    Update all macro series (or a custom subset).
    """
    targets = series_ids or DEFAULT_SERIES
    today = date.today()

    api_key = _load_api_key()

    logger.info("market_data macro  —  %s", today)
    logger.info("Series: %s", ", ".join(targets))

    total_added = 0
    for series_id in targets:
        try:
            added = update_series(series_id, api_key=api_key, start=start, macro_dir=macro_dir)
            total_added += added
        except Exception as exc:
            logger.error("%s  ERROR: %s", series_id, exc, exc_info=True)

    logger.info("macro done: %d new rows across %d series", total_added, len(targets))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Fetch/update macroeconomic series from FRED (CPI, GDP, Treasury, etc.)."
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="SERIES_ID",
        help="Override the default series list (e.g. --series DFF CPIAUCSL).",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        metavar="YYYY-MM-DD",
        help=f"Bootstrap start date (default: {DEFAULT_START}).",
    )
    args = parser.parse_args()

    run(series_ids=args.series, start=args.start)


if __name__ == "__main__":
    main()

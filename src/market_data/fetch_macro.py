"""
fetch_macro.py
--------------
Collect macroeconomic time series from the FRED API (Federal Reserve Bank
of St. Louis) with full vintage (realtime) history.

Each row carries the vintage date (``report_date = FRED realtime_start``) and
the supersession date (``valid_to_date = FRED realtime_end``; ``9999-12-31``
for the currently-active value), enabling point-in-time queries that are free
of look-ahead bias from data revisions (e.g., GDP advance vs. final estimates).

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

Data is stored under data/macro/data.parquet (single unpartitioned file).

Requires
--------
  FRED_API_KEY environment variable (or .env file at project root).

Usage
-----
    market-data-fetch-macro                        # update all default series
    market-data-fetch-macro --series DFF T10Y2Y    # update specific series
    market-data-fetch-macro --start 1990-01-01     # custom bootstrap start
    market-data-fetch-macro --data-dir /path/to/data  # custom data directory
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg
from market_data.resilience import fred_retry
from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import read_table, write_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))

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

_EMPTY_COLS: list[str] = [
    "series_id", "value", "valid_to_date",
    "period_start_date", "period_end_date",
    "report_date", "report_time_marker", "source", "collected_at",
]


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------

def _load_api_key() -> str:
    """
    Load the FRED API key from the environment (or .env file).

    Raises RuntimeError with a clear message if the key is missing so the
    user knows exactly what to fix.
    """
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
# Fetch
# ---------------------------------------------------------------------------

@fred_retry
def fetch_series_vintages(
    series_id: str,
    realtime_start: str,
    api_key: str,
) -> pd.DataFrame:
    """
    Pull all vintages of a FRED series from *realtime_start* to today.

    Uses ``get_series_all_releases`` so each row carries the vintage date
    (``report_date``) and supersession date (``valid_to_date``), enabling
    point-in-time queries that are free of look-ahead bias from revisions.

    Returns a DataFrame ready for ``storage.write_table("macro", ...)``.
    Returns an empty DataFrame (with correct columns) if no data is available.
    """
    try:
        import fredapi  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "fredapi is not installed. Run: pip install fredapi"
        ) from exc

    fred = fredapi.Fred(api_key=api_key)
    raw = fred.get_series_all_releases(series_id, realtime_start=realtime_start)

    if raw is None or raw.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    # After reset_index(), columns are: realtime_start, realtime_end, date, value
    df = raw.reset_index()
    df = df.dropna(subset=["value"])
    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    df["period_start_date"] = pd.to_datetime(df["date"]).dt.date
    df["period_end_date"] = df["period_start_date"]
    df["report_date"] = pd.to_datetime(df["realtime_start"]).dt.date
    df["valid_to_date"] = pd.to_datetime(df["realtime_end"]).dt.date
    df["series_id"] = series_id
    df["report_time_marker"] = ReportTimeMarker.POST_MARKET
    df["source"] = DataSource.FRED
    df["collected_at"] = datetime.now(timezone.utc)
    df["value"] = df["value"].astype(float)

    return df[_EMPTY_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def update_series(series_id: str, api_key: str, start: str, data_dir: Path) -> int:
    """
    Bootstrap or incrementally update a single FRED series with vintage data.

    On first run: pulls all vintages from *start* to today.
    On subsequent runs: pulls from (max report_date - 7 days) to today,
    catching late-released revisions.

    Returns the number of new rows added.
    """
    existing = read_table("macro", data_dir, series_ids=[series_id])

    if existing.empty:
        realtime_start = start
        action = f"bootstrap from {start}"
    else:
        latest_report = pd.to_datetime(existing["report_date"]).dt.date.max()
        lookback = latest_report - timedelta(days=7)
        realtime_start = str(lookback)
        action = f"incremental since {lookback}"

    df = fetch_series_vintages(series_id, realtime_start=realtime_start, api_key=api_key)

    if df.empty:
        logger.info("%s  no data  (%s)", series_id, action)
        return 0

    added = write_table(df, "macro", data_dir)
    logger.info("%s  +%d rows  (%s)", series_id, added, action)
    return added


def run(
    series_ids: list[str] | None = None,
    start: str = DEFAULT_START,
    data_dir: Path = DATA_DIR,
) -> None:
    """
    Update all macro series (or a custom subset) with full vintage history.
    """
    targets = series_ids or DEFAULT_SERIES
    today = date.today()

    api_key = _load_api_key()

    logger.info("market_data macro  —  %s", today)
    logger.info("Series: %s", ", ".join(targets))

    total_added = 0
    for series_id in targets:
        try:
            added = update_series(series_id, api_key=api_key, start=start, data_dir=data_dir)
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
        description="Fetch/update macroeconomic series from FRED with full vintage history."
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
    parser.add_argument(
        "--data-dir",
        default=str(DATA_DIR),
        metavar="DIR",
        help="Root data directory (default: data/).",
    )
    args = parser.parse_args()

    run(series_ids=args.series, start=args.start, data_dir=Path(args.data_dir))


if __name__ == "__main__":
    main()

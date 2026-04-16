"""
fetch_fundamentals.py
---------------------
Collect point-in-time fundamental snapshots for equity tickers via yfinance.

Each run appends one row per ticker to the unified fundamentals table
(data/fundamentals/year=YYYY/data.parquet) using the bitemporal schema
defined in schema.py.

The `report_date` for each snapshot is the most recent 10-K or 10-Q
filing date from SEC EDGAR — the official submission date when the data
became publicly available.  When EDGAR has no record for a ticker the
collection date is used as a fallback and `report_date_known` is set
to False.

Fields captured
---------------
Valuation
  market_cap            marketCap
  enterprise_value      enterpriseValue
  trailing_pe           trailingPE
  forward_pe            forwardPE
  price_to_book         priceToBook

Earnings & revenue
  trailing_eps          trailingEps
  forward_eps           forwardEps
  total_revenue         totalRevenue
  profit_margin         profitMargins

Analyst estimates
  analyst_target_mean   targetMeanPrice
  analyst_target_low    targetLowPrice
  analyst_target_high   targetHighPrice
  analyst_recommendation  recommendationMean  (stored as string, e.g. "1.8")
  analyst_count         numberOfAnalystOpinions

Usage
-----
    market-data-fetch-fundamentals                     # fetch all onboarded tickers
    market-data-fetch-fundamentals --symbols AAPL MSFT # specific symbols only
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

from market_data import edgar
from market_data.config import cfg as _cfg
from market_data.resilience import yf_retry
from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import write_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
SLEEP_BETWEEN_CALLS: int = _cfg.get("sources.sleep_between_calls.fundamentals", 2)

# Mapping: our column name → yfinance info key
INFO_FIELDS: dict[str, str] = {
    # Valuation
    "market_cap":               "marketCap",
    "enterprise_value":         "enterpriseValue",
    "trailing_pe":              "trailingPE",
    "forward_pe":               "forwardPE",
    "price_to_book":            "priceToBook",
    # Earnings & revenue
    "trailing_eps":             "trailingEps",
    "forward_eps":              "forwardEps",
    "total_revenue":            "totalRevenue",
    "profit_margin":            "profitMargins",
    # Analyst estimates
    "analyst_target_mean":      "targetMeanPrice",
    "analyst_target_low":       "targetLowPrice",
    "analyst_target_high":      "targetHighPrice",
    "analyst_recommendation":   "recommendationMean",
    "analyst_count":            "numberOfAnalystOpinions",
}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

@yf_retry
def _fetch_ticker_info(symbol: str) -> dict:
    """Fetch raw yfinance .info dict for *symbol*. Raises on network failure."""
    return yf.Ticker(symbol).info


def fetch_fundamentals(symbol: str, today: date | None = None) -> dict | None:
    """
    Pull a fundamentals snapshot for `symbol` via yfinance and attach
    bitemporal provenance fields.

    Parameters
    ----------
    symbol:
        Equity ticker symbol.
    today:
        Date to use as the collection date (defaults to ``date.today()``).
        Passed explicitly to make tests deterministic.

    Returns
    -------
    dict or None
        A record ready to pass to ``pd.DataFrame([record])`` and then
        ``storage.write_table()``, or None if the ticker returned no usable
        data (e.g. ETF, delisted, or no market cap reported).

    Raises on transient network errors after retries are exhausted, so the
    caller can distinguish a temporary outage from a permanently absent ticker.
    """
    if today is None:
        today = date.today()

    info = _fetch_ticker_info(symbol)

    # Require at least a market cap to consider the record valid
    if not info.get("marketCap"):
        return None

    record: dict = {"symbol": symbol}

    for col, yf_key in INFO_FIELDS.items():
        raw = info.get(yf_key)
        if col == "analyst_recommendation":
            record[col] = str(raw) if raw is not None else None
        elif col == "analyst_count":
            record[col] = int(raw) if raw is not None else None
        else:
            record[col] = float(raw) if raw is not None else None

    # --- EDGAR report_date ---
    filing_date = edgar.get_latest_filing_date(symbol, before=today)
    if filing_date is not None:
        report_date = filing_date
        report_date_known = True
    else:
        report_date = today
        report_date_known = False

    record["report_date_known"] = report_date_known

    # --- Bitemporal fields ---
    record.update({
        "period_start_date":  report_date,
        "period_end_date":    report_date,
        "report_date":        report_date,
        "report_time_marker": ReportTimeMarker.POST_MARKET,
        "source":             DataSource.YFINANCE,
        "collected_at":       datetime.now(timezone.utc),
    })

    return record


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def run(
    symbols: list[str],
    data_dir: Path = DATA_DIR,
) -> int:
    """
    Fetch and store a fundamentals snapshot for each symbol in `symbols`.

    Returns the total number of net new rows written to the store.
    """
    today = date.today()
    total = len(symbols)

    logger.info("market_data fundamentals  —  %s", today)
    logger.info("Tickers: %d", total)

    records: list[dict] = []
    skipped = 0
    failed = 0

    for i, symbol in enumerate(symbols, 1):
        prefix = f"[{i:>4}/{total}] {symbol:<8}"
        try:
            record = fetch_fundamentals(symbol, today=today)
            if record is None:
                logger.info("%s  no data", prefix)
                skipped += 1
            else:
                mktcap = record.get("market_cap")
                known = record.get("report_date_known")
                cap_str = f"  mktcap={mktcap:,.0f}" if mktcap else ""
                pit_str = f"  report_date={record['report_date']}" + ("" if known else " (estimated)")
                logger.info("%s  fetched%s%s", prefix, cap_str, pit_str)
                records.append(record)
        except Exception as exc:
            logger.error("%s  ERROR: %s", prefix, exc, exc_info=True)
            failed += 1

        if i < total:
            time.sleep(SLEEP_BETWEEN_CALLS)

    saved = 0
    if records:
        df = pd.DataFrame(records)
        saved = write_table(df, "fundamentals", data_dir)

    logger.info(
        "fundamentals done: saved=%d  skipped=%d  failed=%d", saved, skipped, failed
    )
    return saved


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Fetch fundamental snapshots (market cap, analyst estimates, etc.) "
            "for equity tickers via yfinance."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help=(
            "Symbols to fetch. Defaults to all tickers in state.json (onboarded list). "
            "Example: --symbols AAPL MSFT TSLA"
        ),
    )
    args = parser.parse_args()

    if args.symbols:
        symbols = args.symbols
    else:
        # Load from state.json
        import json  # noqa: PLC0415
        state_file = Path("state.json")
        if not state_file.exists():
            logger.warning("No state.json found and no --symbols provided. Nothing to do.")
            return
        state = json.loads(state_file.read_text())
        symbols = sorted(state.get("onboarded", []))
        if not symbols:
            logger.warning("No onboarded tickers in state.json. Run market-data-run first.")
            return

    run(symbols=symbols)


if __name__ == "__main__":
    main()

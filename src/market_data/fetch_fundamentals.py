"""
fetch_fundamentals.py
---------------------
Collect point-in-time fundamental snapshots for equity tickers via yfinance.

Each run appends one row per ticker to the unified fundamentals table
(data/fundamentals/year=YYYY/data.parquet) using the bitemporal schema
defined in schema.py.

The `report_date` for each snapshot is the collection date: the values are
a yfinance `.info` snapshot (market cap, P/E and other price-dependent
fields) and were not knowable before the day we took it.  The most recent
10-K or 10-Q filing date from SEC EDGAR is stored separately as
`last_filing_date` (informational; None when EDGAR has no record).

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

Analyst estimates are collected separately (daily cadence) by
fetch_analyst_estimates.py into the analyst_estimates table.

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
        record[col] = float(raw) if raw is not None else None

    # --- Point-in-time dates ---
    # The values are a snapshot of yfinance .info taken today, so the row must
    # not be visible before today: report_date is the collection date, which is
    # exact, hence report_date_known=True. Do NOT use the EDGAR filing date as
    # report_date: market_cap, trailing_pe etc. move with the price after the
    # filing, so that would leak today's values back to the filing date. The
    # filing date is kept as last_filing_date to show which quarter's reported
    # figures the snapshot reflects.
    report_date = today
    record["report_date_known"] = True
    record["last_filing_date"] = edgar.get_latest_filing_date(symbol, before=today)

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
                cap_str = f"  mktcap={mktcap:,.0f}" if mktcap else ""
                filing_str = f"  last_filing={record['last_filing_date']}"
                logger.info("%s  fetched%s%s", prefix, cap_str, filing_str)
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

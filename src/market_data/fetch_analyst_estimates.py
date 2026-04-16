"""
fetch_analyst_estimates.py
--------------------------
Collect daily point-in-time analyst estimate snapshots for equity tickers
via yfinance.

Each run appends one row per ticker to the analyst_estimates table
(data/analyst_estimates/year=YYYY/data.parquet) using the bitemporal schema
defined in schema.py.

Unlike fundamentals, analyst estimates have no SEC EDGAR filing date — the
consensus is updated continuously by individual brokers.  The collection date
is therefore used as both `period_start_date` and `report_date`, and
`report_date_known` is always False.

Running this daily accumulates a revision history of analyst consensus, enabling
point-in-time queries: filter ``report_date <= as_of_date`` to see what the
consensus was on any past date.

Fields captured
---------------
  analyst_target_mean       targetMeanPrice
  analyst_target_low        targetLowPrice
  analyst_target_high       targetHighPrice
  analyst_recommendation    recommendationMean  (stored as string, e.g. "1.8")
  analyst_count             numberOfAnalystOpinions

Usage
-----
    market-data-fetch-analyst-estimates                       # fetch all onboarded tickers
    market-data-fetch-analyst-estimates --symbols AAPL MSFT  # specific symbols only
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yfinance as yf

from market_data.config import cfg as _cfg
from market_data.resilience import yf_retry
from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import write_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
SLEEP_BETWEEN_CALLS: int = _cfg.get("sources.sleep_between_calls.analyst_estimates", 2)

# Mapping: our column name → yfinance info key
ANALYST_FIELDS: dict[str, str] = {
    "analyst_target_mean":    "targetMeanPrice",
    "analyst_target_low":     "targetLowPrice",
    "analyst_target_high":    "targetHighPrice",
    "analyst_recommendation": "recommendationMean",
    "analyst_count":          "numberOfAnalystOpinions",
}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

@yf_retry
def _fetch_ticker_info(symbol: str) -> dict:
    """Fetch raw yfinance .info dict for *symbol*. Raises on network failure."""
    return yf.Ticker(symbol).info


def fetch_analyst_estimates(symbol: str, today: date | None = None) -> dict | None:
    """
    Pull an analyst estimates snapshot for `symbol` via yfinance and attach
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
        ``storage.write_table()``, or None if the ticker has no analyst
        coverage (e.g. ETF, micro-cap, or no targetMeanPrice reported).

    Raises on transient network errors after retries are exhausted, so the
    caller can distinguish a temporary outage from a permanently absent ticker.
    """
    if today is None:
        today = date.today()

    info = _fetch_ticker_info(symbol)

    # Require at least a mean price target to consider the record valid
    if not info.get("targetMeanPrice"):
        return None

    record: dict = {"symbol": symbol}

    for col, yf_key in ANALYST_FIELDS.items():
        raw = info.get(yf_key)
        if col == "analyst_recommendation":
            record[col] = str(raw) if raw is not None else None
        elif col == "analyst_count":
            record[col] = int(raw) if raw is not None else None
        else:
            record[col] = float(raw) if raw is not None else None

    # Analyst estimates have no authoritative publication date — use collection
    # date as the report_date proxy and flag it as estimated.
    record["report_date_known"] = False

    # --- Bitemporal fields ---
    record.update({
        "period_start_date":  today,
        "period_end_date":    today,
        "report_date":        today,
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
    Fetch and store an analyst estimates snapshot for each symbol in `symbols`.

    Returns the total number of net new rows written to the store.
    """
    today = date.today()
    total = len(symbols)

    logger.info("market_data analyst-estimates  —  %s", today)
    logger.info("Tickers: %d", total)

    records: list[dict] = []
    skipped = 0
    failed = 0

    for i, symbol in enumerate(symbols, 1):
        prefix = f"[{i:>4}/{total}] {symbol:<8}"
        try:
            record = fetch_analyst_estimates(symbol, today=today)
            if record is None:
                logger.info("%s  no coverage", prefix)
                skipped += 1
            else:
                mean = record.get("analyst_target_mean")
                count = record.get("analyst_count")
                logger.info(
                    "%s  target_mean=%s  analyst_count=%s",
                    prefix,
                    f"{mean:.2f}" if mean is not None else "n/a",
                    count if count is not None else "n/a",
                )
                records.append(record)
        except Exception as exc:
            logger.error("%s  ERROR: %s", prefix, exc, exc_info=True)
            failed += 1

        if i < total:
            time.sleep(SLEEP_BETWEEN_CALLS)

    saved = 0
    if records:
        df = pd.DataFrame(records)
        saved = write_table(df, "analyst_estimates", data_dir)

    logger.info(
        "analyst-estimates done: saved=%d  skipped=%d  failed=%d", saved, skipped, failed
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
            "Fetch daily analyst estimate snapshots (price targets, recommendation) "
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
        from market_data.etf_config import ALL_ETFS  # noqa: PLC0415
        state_file = Path("state.json")
        if not state_file.exists():
            logger.warning("No state.json found and no --symbols provided. Nothing to do.")
            return
        state = json.loads(state_file.read_text())
        all_onboarded = sorted(state.get("onboarded", []))
        symbols = [s for s in all_onboarded if s not in ALL_ETFS]
        if not symbols:
            logger.warning("No onboarded equity tickers in state.json. Run market-data-run first.")
            return

    run(symbols=symbols)


if __name__ == "__main__":
    main()

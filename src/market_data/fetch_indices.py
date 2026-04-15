"""
fetch_indices.py
----------------
Collect daily OHLCV data for market index and rate symbols.

Covers:
  ^VIX   — CBOE Volatility Index
  ^TNX   — 10-year Treasury yield
  ^TYX   — 30-year Treasury yield
  ^FVX   — 5-year Treasury yield
  ^IRX   — 13-week T-bill yield
  ZQ=F   — 30-day Fed Funds Futures (front month)
  ^GSPC  — S&P 500 index level

Data is stored under data/indices/<SYMBOL>.parquet using the same schema
and atomic-write pattern as the equity OHLCV pipeline.

Usage
-----
    market-data-fetch-indices               # update all (or bootstrap if new)
    market-data-fetch-indices --history 20  # pull 20 years of history
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date
from pathlib import Path

from market_data.config import cfg as _cfg
from market_data.fetch import (
    DEFAULT_HISTORY_YEARS,
    fetch_history,
    fetch_incremental,
    load_ticker_data,
    save_ticker_data,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INDEX_SYMBOLS: list[str] = _cfg.get(
    "indices.symbols",
    [
        "^VIX",   # CBOE Volatility Index
        "^TNX",   # 10-year Treasury yield
        "^TYX",   # 30-year Treasury yield
        "^FVX",   # 5-year Treasury yield
        "^IRX",   # 13-week T-bill yield
        "ZQ=F",   # 30-day Fed Funds Futures (front month)
        "^GSPC",  # S&P 500 index level
    ],
)

INDICES_DIR = Path(_cfg.get("paths.indices_dir", "data/indices"))
SLEEP_BETWEEN_CALLS: int = _cfg.get("sources.sleep_between_calls.indices", 3)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def update_symbol(symbol: str, history_years: int) -> int:
    """
    Bootstrap or incrementally update a single index/rate symbol.

    If no local file exists yet, pulls `history_years` of history.
    Otherwise pulls data since the last stored date.

    Returns the number of new rows added.
    """
    existing = load_ticker_data(symbol, INDICES_DIR)

    if existing is None:
        df = fetch_history(symbol, years=history_years)
        action = f"bootstrap ({history_years}y)"
    else:
        last_date = existing["date"].max()
        df = fetch_incremental(symbol, since=last_date)
        action = f"incremental since {last_date}"

    if df.empty:
        logger.info("%s  no data  (%s)", symbol, action)
        return 0

    added = save_ticker_data(symbol, df, INDICES_DIR)
    logger.info("%s  +%d rows  (%s)", symbol, added, action)
    return added


def run(symbols: list[str] | None = None, history_years: int = DEFAULT_HISTORY_YEARS) -> None:
    """
    Update all index/rate symbols (or a custom subset).
    """
    targets = symbols or INDEX_SYMBOLS
    today = date.today()

    logger.info("market_data indices  —  %s", today)
    logger.info("Symbols: %s", ", ".join(targets))

    total_added = 0
    for i, symbol in enumerate(targets):
        try:
            added = update_symbol(symbol, history_years)
            total_added += added
        except Exception as exc:
            logger.error("%s  ERROR: %s", symbol, exc, exc_info=True)

        if i < len(targets) - 1:
            time.sleep(SLEEP_BETWEEN_CALLS)

    logger.info("indices done: %d new rows across %d symbols", total_added, len(targets))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Fetch/update market index and rate data (VIX, Treasury yields, etc.)."
    )
    parser.add_argument(
        "--history",
        type=int,
        default=DEFAULT_HISTORY_YEARS,
        metavar="YEARS",
        help=f"Years of history to pull on first bootstrap (default: {DEFAULT_HISTORY_YEARS}).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help="Override the default symbol list (e.g. --symbols ^VIX ^TNX).",
    )
    args = parser.parse_args()

    run(symbols=args.symbols, history_years=args.history)


if __name__ == "__main__":
    main()

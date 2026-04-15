"""
fetch_options.py
----------------
Collect daily option chain snapshots for S&P 500 tickers and sector/broad-market
ETFs via yfinance.

For each ticker the nearest `max_expiries` expiration dates are fetched and
all strikes (calls + puts) are stored as a single row-per-contract snapshot.

Data source note
----------------
yfinance option chains are an unofficial Yahoo Finance scraper.  They are
suitable for research and macro forecasting but should not be used for
production order routing.  Greek values (delta, gamma, theta, vega) are NOT
provided by yfinance; implied volatility is available but carries the same
reliability caveat.

Fields per contract
-------------------
  snapshot_date   date the snapshot was taken
  symbol          underlying ticker
  expiry          option expiration date
  strike          strike price
  option_type     "call" or "put"
  last_price      last traded price
  bid             bid price
  ask             ask price
  volume          contracts traded today
  open_interest   total open contracts
  implied_vol     implied volatility (annualised, as a decimal)
  in_the_money    True if the contract is currently ITM

Batching
--------
ETFs (19) are always processed first in each cycle; the remaining ~500 SP500
tickers follow in market-cap order.  Each run processes the next `batch_size`
tickers not yet covered in the current cycle.  When all tickers have been
covered the cycle resets automatically.

Cycle state is tracked in state.json under the key "options_cycle".

Usage
-----
    market-data-fetch-options                          # next batch (default 50)
    market-data-fetch-options --batch-size 25          # smaller batch
    market-data-fetch-options --symbols AAPL MSFT      # specific symbols (no state)
    market-data-fetch-options --max-expiries 2         # fewer expiry dates
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd
import yfinance as yf

from market_data.config import cfg as _cfg

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OPTIONS_DIR = Path(_cfg.get("paths.options_dir", "data/options"))
STATE_FILE = Path(_cfg.get("paths.state_file", "state.json"))
TICKERS_FILE = Path(_cfg.get("paths.tickers_file", "tickers.csv"))

DEFAULT_BATCH_SIZE: int = _cfg.get("collection.options_batch_size", 50)
DEFAULT_MAX_EXPIRIES: int = _cfg.get("collection.options_max_expiries", 4)
SLEEP_BETWEEN_CALLS: int = _cfg.get("sources.sleep_between_calls.options", 5)

OPTIONS_COLS = [
    "snapshot_date",
    "symbol",
    "expiry",
    "strike",
    "option_type",
    "last_price",
    "bid",
    "ask",
    "volume",
    "open_interest",
    "implied_vol",
    "in_the_money",
]


# ---------------------------------------------------------------------------
# Ticker helpers
# ---------------------------------------------------------------------------

def get_sp500_symbols(onboarded: set[str]) -> list[str]:
    """
    Return SP500 symbols from tickers.csv that are also in the onboarded set,
    sorted by market value descending (largest cap first).

    Falls back to all onboarded tickers if tickers.csv is missing or has no
    index column.
    """
    if not TICKERS_FILE.exists():
        return sorted(onboarded)

    df = pd.read_csv(TICKERS_FILE)

    if "index" in df.columns:
        sp500 = df[df["index"].str.contains("SP500", na=False)]
    else:
        sp500 = df  # fallback: treat everything as eligible

    symbols = sp500["symbol"].dropna().astype(str).tolist()
    # Restrict to onboarded (we need OHLCV data before options is useful)
    return [s for s in symbols if s in onboarded]


def get_etf_symbols(onboarded: set[str]) -> list[str]:
    """
    Return sector and broad-market ETF symbols that are in the onboarded set,
    in the order defined in etf_config (sector ETFs first, then broad ETFs).
    """
    from market_data.etf_config import ALL_ETFS  # noqa: PLC0415
    return [s for s in ALL_ETFS if s in onboarded]


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_option_chain(symbol: str, max_expiries: int) -> pd.DataFrame:
    """
    Fetch the nearest `max_expiries` option chains for `symbol`.

    Returns a DataFrame with OPTIONS_COLS schema, or an empty DataFrame if
    no data is available.
    """
    ticker = yf.Ticker(symbol)

    try:
        expiry_dates = ticker.options  # tuple of expiry date strings
    except Exception:
        return pd.DataFrame(columns=OPTIONS_COLS)

    if not expiry_dates:
        return pd.DataFrame(columns=OPTIONS_COLS)

    today = date.today()
    selected = expiry_dates[:max_expiries]
    frames: list[pd.DataFrame] = []

    for expiry_str in selected:
        try:
            chain = ticker.option_chain(expiry_str)
        except Exception:
            continue

        for side, df_raw in (("call", chain.calls), ("put", chain.puts)):
            if df_raw.empty:
                continue

            df = pd.DataFrame({
                "snapshot_date": today,
                "symbol": symbol,
                "expiry": pd.to_datetime(expiry_str).date(),
                "strike": pd.to_numeric(df_raw.get("strike"), errors="coerce"),
                "option_type": side,
                "last_price": pd.to_numeric(df_raw.get("lastPrice"), errors="coerce"),
                "bid": pd.to_numeric(df_raw.get("bid"), errors="coerce"),
                "ask": pd.to_numeric(df_raw.get("ask"), errors="coerce"),
                "volume": pd.to_numeric(df_raw.get("volume"), errors="coerce"),
                "open_interest": pd.to_numeric(df_raw.get("openInterest"), errors="coerce"),
                "implied_vol": pd.to_numeric(df_raw.get("impliedVolatility"), errors="coerce"),
                "in_the_money": df_raw.get("inTheMoney", pd.Series(dtype=bool)),
            })

            frames.append(df[OPTIONS_COLS])

    if not frames:
        return pd.DataFrame(columns=OPTIONS_COLS)

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def _options_path(symbol: str, options_dir: Path) -> Path:
    return options_dir / f"{symbol}.parquet"


def save_options_snapshot(
    symbol: str,
    new_df: pd.DataFrame,
    options_dir: Path,
) -> int:
    """
    Append new_df to the per-ticker options Parquet.

    Deduplicates on (snapshot_date, symbol, expiry, strike, option_type) so
    re-running on the same day is safe.  Writes atomically.

    Returns the number of net-new rows added.
    """
    if new_df.empty:
        return 0

    options_dir.mkdir(parents=True, exist_ok=True)
    path = _options_path(symbol, options_dir)

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        before = len(existing)
    else:
        combined = new_df.copy()
        before = 0

    dedup_keys = ["snapshot_date", "symbol", "expiry", "strike", "option_type"]
    combined["snapshot_date"] = pd.to_datetime(combined["snapshot_date"]).dt.date
    combined["expiry"] = pd.to_datetime(combined["expiry"]).dt.date
    combined = (
        combined
        .drop_duplicates(subset=dedup_keys)
        .sort_values(["snapshot_date", "expiry", "strike", "option_type"])
        .reset_index(drop=True)
    )

    tmp_path = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)

    return len(combined) - before


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def run(
    symbols: list[str],
    options_dir: Path = OPTIONS_DIR,
    max_expiries: int = DEFAULT_MAX_EXPIRIES,
) -> None:
    """
    Fetch and store option chain snapshots for each symbol in `symbols`.
    """
    today = date.today()
    total = len(symbols)

    logger.info("market_data options  —  %s", today)
    logger.info("Tickers: %d  max_expiries: %d", total, max_expiries)

    saved_rows = 0
    skipped = 0
    failed = 0

    for i, symbol in enumerate(symbols, 1):
        prefix = f"[{i:>3}/{total}] {symbol:<8}"
        try:
            df = fetch_option_chain(symbol, max_expiries=max_expiries)
            if df.empty:
                logger.info("%s  no data", prefix)
                skipped += 1
            else:
                added = save_options_snapshot(symbol, df, options_dir)
                expiry_count = df["expiry"].nunique()
                logger.info("%s  +%d rows  (%d expiries)", prefix, added, expiry_count)
                saved_rows += added
        except Exception as exc:
            logger.error("%s  ERROR: %s", prefix, exc, exc_info=True)
            failed += 1

        if i < total:
            time.sleep(SLEEP_BETWEEN_CALLS)

    logger.info(
        "options done: rows_saved=%d  skipped=%d  failed=%d", saved_rows, skipped, failed
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Fetch daily option chain snapshots (IV, bid/ask, OI) for SP500 tickers "
            "via yfinance.  Processes tickers in batches across days; cycle state is "
            "tracked in state.json."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Tickers to process per run (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--max-expiries",
        type=int,
        default=DEFAULT_MAX_EXPIRIES,
        metavar="N",
        help=f"Expiration dates to fetch per ticker (default: {DEFAULT_MAX_EXPIRIES}).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help="Override batch logic and fetch specific symbols only.",
    )
    args = parser.parse_args()

    if args.symbols:
        # Direct symbol override — bypass state/batching entirely
        run(symbols=args.symbols, max_expiries=args.max_expiries)
        return

    # --- Load state ---
    if not STATE_FILE.exists():
        logger.warning("No state.json found. Run market-data-run first to onboard tickers.")
        return

    state = json.loads(STATE_FILE.read_text())
    onboarded: set[str] = set(state.get("onboarded", []))

    if not onboarded:
        logger.warning("No onboarded tickers in state.json. Run market-data-run first.")
        return

    sp500 = get_sp500_symbols(onboarded)
    etfs = get_etf_symbols(onboarded)
    # ETFs first so they're always covered at the start of each cycle.
    etf_set = set(etfs)
    all_symbols = etfs + [s for s in sp500 if s not in etf_set]

    if not all_symbols:
        logger.warning("No eligible tickers found in onboarded set.")
        return

    # --- Determine batch from cycle state ---
    cycle_done: list[str] = state.get("options_cycle", [])
    cycle_done_set = set(cycle_done)

    pending = [s for s in all_symbols if s not in cycle_done_set]

    if not pending:
        # Full cycle complete — reset and start over
        logger.info(
            "Options cycle complete (%d tickers covered). Resetting cycle.", len(all_symbols)
        )
        cycle_done = []
        cycle_done_set = set()
        pending = list(all_symbols)

    batch = pending[:args.batch_size]
    remaining_after = len(pending) - len(batch)

    logger.info(
        "Options cycle progress: %d/%d done  |  %d pending  |  processing %d this run",
        len(cycle_done_set),
        len(all_symbols),
        len(pending),
        len(batch),
    )

    run(symbols=batch, max_expiries=args.max_expiries)

    # --- Persist updated cycle state ---
    state["options_cycle"] = sorted(cycle_done_set | set(batch))
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))

    if remaining_after == 0:
        logger.info(
            "Options cycle complete — all %d tickers covered.", len(all_symbols)
        )
    else:
        days_remaining = (remaining_after + args.batch_size - 1) // args.batch_size
        logger.info("~%d more run(s) to complete this cycle.", days_remaining)


if __name__ == "__main__":
    main()

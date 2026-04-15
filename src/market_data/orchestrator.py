"""
orchestrator.py
---------------
Daily runner for the market_data pipeline.

Each run executes in order:

  1. ONBOARD       — pick the next `--batch-size` tickers from tickers.csv that
                     haven't been fetched yet, and pull their full 10-year history.

  2. UPDATE        — for every already-onboarded ticker, pull any new trading days
                     since the last run and append them to their Parquet file.

  3. FUNDAMENTALS  — (optional, monthly) snapshot market cap, analyst estimates,
                     and valuation ratios for all onboarded tickers via yfinance.
                     Auto-skipped if last run was <30 days ago.

  4. OPTIONS       — (optional, daily) fetch option chain snapshots (IV, bid/ask,
                     open interest) for the next batch of SP500 + sector/broad ETF
                     tickers. Processes 50 tickers/run across a rolling cycle; cycle
                     state is tracked in state.json under "options_cycle".

  5. INDICES       — (optional, daily) update VIX, Treasury yields, Fed Funds
                     futures, and S&P 500 index level.

  6. MACRO         — (optional, daily) update FRED macro series (CPI, GDP,
                     Fed Funds rate, Treasury spread, unemployment, etc.).

  7. MERGE         — (optional) rebuild data/merged.parquet from all per-ticker
                     OHLCV files.

Progress is persisted in state.json so runs are safe to interrupt and resume.

Usage
-----
    market-data-run                                          # OHLCV only
    market-data-run --batch-size 25                         # onboard fewer per day
    market-data-run --batch-size 0                          # updates only, no new tickers
    market-data-run --no-update                             # onboard only, skip updates
    market-data-run --indices --macro --fundamentals --options --merge # full daily run (recommended)
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd

from market_data.fetch import DEFAULT_HISTORY_YEARS, fetch_history, fetch_incremental, save_ticker_data
from market_data import metrics

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths & defaults
# ---------------------------------------------------------------------------

STATE_FILE = Path("state.json")
TICKERS_FILE = Path("tickers.csv")
DATA_DIR = Path("data/ohlcv")

DEFAULT_BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 5  # seconds; be polite to the yfinance endpoint
TICKER_REFRESH_DAYS = 90
FUNDAMENTALS_REFRESH_DAYS = 30
DEFAULT_OPTIONS_BATCH_SIZE = 50
DEFAULT_MAX_EXPIRIES = 4


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text())
        return {
            "onboarded": raw.get("onboarded", []),
            "last_run": raw.get("last_run"),
            "last_ticker_refresh": raw.get("last_ticker_refresh"),
            "last_fundamentals_run": raw.get("last_fundamentals_run"),
            "options_cycle": raw.get("options_cycle", []),
        }
    return {
        "onboarded": [],
        "last_run": None,
        "last_ticker_refresh": None,
        "last_fundamentals_run": None,
        "options_cycle": [],
    }


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Ticker list refresh
# ---------------------------------------------------------------------------

def maybe_refresh_tickers(
    state: dict,
    tickers_path: Path,
    today: date,
) -> bool:
    """
    Refresh tickers.csv if last_ticker_refresh is absent or >90 days ago.
    Returns True if a refresh was performed.

    Failures are non-fatal: if the iShares site is down, the pipeline continues
    with the existing tickers.csv and logs a warning.
    """
    last_raw = state.get("last_ticker_refresh")
    if last_raw:
        days_since = (today - date.fromisoformat(last_raw)).days
        if days_since < TICKER_REFRESH_DAYS:
            logger.info(
                "Ticker list is fresh (%dd old, threshold %dd)",
                days_since,
                TICKER_REFRESH_DAYS,
            )
            return False

    logger.info("Refreshing ticker list (last refresh: %s)...", last_raw or "never")
    try:
        from market_data import fetch_tickers  # noqa: PLC0415
        fetch_tickers.run(tickers_path, today=today.isoformat())
        logger.info("Ticker list refreshed successfully.")
        return True
    except Exception as exc:
        logger.warning(
            "Ticker refresh failed: %s. Continuing with existing list.", exc, exc_info=True
        )
        return False


# ---------------------------------------------------------------------------
# Fundamentals refresh
# ---------------------------------------------------------------------------

def maybe_run_fundamentals(
    state: dict,
    onboarded: set[str],
    today: date,
) -> bool:
    """
    Run a fundamentals snapshot if last_fundamentals_run is absent or >30 days ago.
    Returns True if fundamentals were fetched this run.

    Failures are non-fatal and logged as warnings.
    """
    last_raw = state.get("last_fundamentals_run")
    if last_raw:
        days_since = (today - date.fromisoformat(last_raw)).days
        if days_since < FUNDAMENTALS_REFRESH_DAYS:
            logger.info(
                "Fundamentals are fresh (%dd old, threshold %dd)",
                days_since,
                FUNDAMENTALS_REFRESH_DAYS,
            )
            return False

    # ETFs are fund wrappers — their yfinance .info fields don't map to the
    # equity fundamentals schema (no P/E, margins, analyst targets, etc.).
    from market_data.etf_config import ALL_ETFS  # noqa: PLC0415
    symbols = sorted(s for s in onboarded if s not in ALL_ETFS)
    logger.info(
        "Fetching fundamentals for %d tickers (last run: %s)...",
        len(symbols),
        last_raw or "never",
    )
    try:
        from market_data import fetch_fundamentals  # noqa: PLC0415
        metrics.start_run("fundamentals")
        fetch_fundamentals.run(symbols=symbols)
        for s in symbols:
            metrics.record_symbol_result(s, success=True)
        metrics.finish_run()
        return True
    except Exception as exc:
        logger.warning("Fundamentals fetch failed: %s.", exc, exc_info=True)
        metrics.finish_run()
        return False


# ---------------------------------------------------------------------------
# Options batch step
# ---------------------------------------------------------------------------

def step_options(
    state: dict,
    onboarded: set[str],
    batch_size: int,
    max_expiries: int,
) -> set[str]:
    """
    Process the next `batch_size` SP500 + ETF tickers in the options cycle.

    Returns the updated set of tickers covered in the current cycle so the
    caller can persist it to state.json.
    """
    from market_data import fetch_options  # noqa: PLC0415

    sp500 = fetch_options.get_sp500_symbols(onboarded)
    etfs = fetch_options.get_etf_symbols(onboarded)
    # ETFs first so they're always covered at the start of each cycle, then
    # SP500 constituents (preserving largest-cap-first ordering from tickers.csv).
    etf_set = set(etfs)
    all_symbols = etfs + [s for s in sp500 if s not in etf_set]

    if not all_symbols:
        logger.info("OPTIONS  no eligible tickers in onboarded set — skipping.")
        return set(state.get("options_cycle", []))

    cycle_done: set[str] = set(state.get("options_cycle", []))
    pending = [s for s in all_symbols if s not in cycle_done]

    if not pending:
        logger.info(
            "OPTIONS  cycle complete (%d tickers). Resetting cycle.", len(all_symbols)
        )
        cycle_done = set()
        pending = list(all_symbols)

    batch = pending[:batch_size]
    remaining_after = len(pending) - len(batch)

    logger.info(
        "OPTIONS  %d tickers  (cycle: %d/%d done, %d remaining after this batch)",
        len(batch),
        len(cycle_done),
        len(all_symbols),
        remaining_after,
    )

    metrics.start_run("options")
    fetch_options.run(symbols=batch, max_expiries=max_expiries)

    # Silent failure detection: warn if all symbols produced no options data
    if batch:
        from market_data.fetch_options import OPTIONS_DIR  # noqa: PLC0415
        for s in batch:
            wrote = (OPTIONS_DIR / f"{s}.parquet").exists()
            metrics.record_symbol_result(s, success=wrote, reason="no options file written" if not wrote else None)
        options_written = sum(1 for s in batch if (OPTIONS_DIR / f"{s}.parquet").exists())
        if options_written == 0:
            logger.warning(
                "OPTIONS silent failure: processed %d symbol(s) but no options files exist",
                len(batch),
            )

    metrics.finish_run()

    updated_cycle = cycle_done | set(batch)
    if remaining_after == 0:
        logger.info(
            "Options cycle complete — all %d tickers covered.", len(all_symbols)
        )
        return set()  # reset for next cycle
    return updated_cycle


# ---------------------------------------------------------------------------
# Ticker list
# ---------------------------------------------------------------------------

def load_ordered_tickers() -> list[str]:
    """
    Load tickers.csv (produced by fetch_tickers.py) and return symbols in
    market-cap order (largest first).
    """
    if not TICKERS_FILE.exists():
        raise FileNotFoundError(
            f"{TICKERS_FILE} not found. Run fetch_tickers.py first."
        )
    df = pd.read_csv(TICKERS_FILE)
    if "symbol" not in df.columns:
        raise ValueError("tickers.csv must have a 'symbol' column.")
    return df["symbol"].dropna().astype(str).tolist()


# ---------------------------------------------------------------------------
# Core steps
# ---------------------------------------------------------------------------

def step_onboard(
    pending: list[str],
    batch_size: int,
    onboarded: set[str],
) -> tuple[set[str], set[str]]:
    """
    Fetch full history for the next `batch_size` pending tickers.

    Returns
    -------
    newly_onboarded : set[str]   tickers successfully added this run
    failed          : set[str]   tickers that errored (not added to state)
    """
    to_onboard = pending[:batch_size]
    newly_onboarded: set[str] = set()
    failed: set[str] = set()

    if not to_onboard:
        return newly_onboarded, failed

    logger.info(
        "ONBOARD  %d new tickers  (%d still pending after this run)",
        len(to_onboard),
        len(pending) - len(to_onboard),
    )

    metrics.start_run("onboard")
    for i, symbol in enumerate(to_onboard, 1):
        prefix = f"[{i:>3}/{len(to_onboard)}] {symbol:<8}"
        try:
            df = fetch_history(symbol, years=DEFAULT_HISTORY_YEARS)
            if df.empty:
                logger.info("%s  no data (skipping)", prefix)
                metrics.record_symbol_result(symbol, success=False, reason="no data")
            else:
                added = save_ticker_data(symbol, df, DATA_DIR)
                newly_onboarded.add(symbol)
                logger.info("%s  %d rows saved", prefix, added)
                metrics.record_symbol_result(symbol, success=True, rows_written=added)
        except Exception as exc:
            logger.error("%s  ERROR: %s", prefix, exc, exc_info=True)
            failed.add(symbol)
            metrics.record_symbol_result(symbol, success=False, reason=str(exc))

        time.sleep(SLEEP_BETWEEN_CALLS)

    metrics.finish_run()

    # Silent failure detection
    if to_onboard and not newly_onboarded:
        logger.warning(
            "ONBOARD silent failure: attempted %d symbol(s) but none succeeded",
            len(to_onboard),
        )

    return newly_onboarded, failed


def step_update(
    to_update: list[str],
    since: date,
) -> dict[str, int]:
    """
    Fetch incremental data for all `to_update` tickers since `since`.

    Returns a dict of {symbol: new_rows_added}.
    """
    results: dict[str, int] = {}

    if not to_update:
        return results

    logger.info("UPDATE   %d tickers  (since %s)", len(to_update), since)

    metrics.start_run("update")
    total_rows = 0
    for i, symbol in enumerate(to_update, 1):
        prefix = f"[{i:>4}/{len(to_update)}] {symbol:<8}"
        try:
            df = fetch_incremental(symbol, since=since)
            if df.empty:
                logger.info("%s  up to date", prefix)
                results[symbol] = 0
                metrics.record_symbol_result(symbol, success=True, rows_written=0)
            else:
                added = save_ticker_data(symbol, df, DATA_DIR)
                logger.info("%s  +%d rows", prefix, added)
                results[symbol] = added
                total_rows += added
                metrics.record_symbol_result(symbol, success=True, rows_written=added)
        except Exception as exc:
            logger.error("%s  ERROR: %s", prefix, exc, exc_info=True)
            results[symbol] = 0
            metrics.record_symbol_result(symbol, success=False, reason=str(exc))

        time.sleep(SLEEP_BETWEEN_CALLS)

    metrics.finish_run()

    # Silent failure detection: if last_run was recent but all symbols returned 0
    # rows AND the batch is large enough that some data was expected, warn.
    days_since_last = (date.today() - since).days
    if to_update and total_rows == 0 and days_since_last >= 2:
        logger.warning(
            "UPDATE silent failure: %d symbol(s) updated since %s (%dd ago) "
            "but 0 total rows written — market may have been closed or "
            "there may be a data source issue",
            len(to_update),
            since,
            days_since_last,
        )

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(batch_size: int, skip_update: bool, run_merge: bool, run_indices: bool = False, run_macro: bool = False, run_fundamentals: bool = False, run_options: bool = False, options_batch_size: int = 50) -> None:
    today = date.today()

    state = load_state()
    onboarded: set[str] = set(state["onboarded"])
    last_run: date | None = (
        date.fromisoformat(state["last_run"]) if state["last_run"] else None
    )

    # --- 0. Auto-refresh ticker list if stale ---
    refreshed = maybe_refresh_tickers(state, TICKERS_FILE, today)

    all_tickers = load_ordered_tickers()
    pending = [t for t in all_tickers if t not in onboarded]

    logger.info(
        "market_data orchestrator  —  %s  |  tickers=%d  onboarded=%d  pending=%d  last_run=%s",
        today,
        len(all_tickers),
        len(onboarded),
        len(pending),
        last_run or "never",
    )
    logger.info("Last ticker refresh: %s", state.get("last_ticker_refresh") or "never")

    # --- 1a. Priority-onboard ETFs (outside the normal batch limit) ---
    # ETFs sit at the bottom of tickers.csv (NaN market_value sorts last) so
    # without this pre-pass they would wait until all ~1,500 stock tickers are
    # onboarded.  There are at most 19 ETFs, so this adds <2 minutes once.
    from market_data.etf_config import ALL_ETFS  # noqa: PLC0415
    etf_pending = [t for t in ALL_ETFS if t not in onboarded and t in set(all_tickers)]
    etf_newly_onboarded: set[str] = set()
    if etf_pending:
        logger.info("ETFs pending onboarding: %s", etf_pending)
        etf_newly_onboarded, _ = step_onboard(
            etf_pending, batch_size=len(etf_pending), onboarded=onboarded
        )
        onboarded |= etf_newly_onboarded

    # --- 1b. Onboard new stock tickers (batch-limited, ETFs excluded) ---
    non_etf_pending = [t for t in pending if t not in set(ALL_ETFS)]
    newly_onboarded, _failed = step_onboard(non_etf_pending, batch_size, onboarded)
    onboarded |= newly_onboarded

    all_newly_onboarded = etf_newly_onboarded | newly_onboarded

    # --- 2. Incremental updates ---
    if not skip_update and last_run:
        # Update tickers that were already onboarded before this run
        # (newly onboarded ones just got full history; no need to update them)
        to_update = [t for t in all_tickers if t in onboarded and t not in all_newly_onboarded]
        step_update(to_update, since=last_run)
    elif not skip_update and not last_run:
        logger.info("UPDATE   skipped — no previous run date in state.json")

    # --- 3. Optional fundamentals snapshot (auto-throttled to monthly) ---
    fundamentals_ran = False
    if run_fundamentals:
        fundamentals_ran = maybe_run_fundamentals(state, onboarded, today)

    # --- 4. Optional options chain batch ---
    updated_options_cycle: set[str] | None = None
    if run_options:
        updated_options_cycle = step_options(
            state, onboarded, batch_size=options_batch_size, max_expiries=DEFAULT_MAX_EXPIRIES
        )

    # --- 5. Optional indices update ---
    if run_indices:
        from market_data import fetch_indices  # noqa: PLC0415
        metrics.start_run("indices")
        fetch_indices.run()
        metrics.finish_run()

    # --- 6. Optional macro update ---
    if run_macro:
        from market_data import fetch_macro  # noqa: PLC0415
        metrics.start_run("macro")
        fetch_macro.run()
        metrics.finish_run()

    # --- 7. Persist state ---
    state["onboarded"] = sorted(onboarded)
    state["last_run"] = str(today)
    if refreshed:
        state["last_ticker_refresh"] = str(today)
    if fundamentals_ran:
        state["last_fundamentals_run"] = str(today)
    if updated_options_cycle is not None:
        state["options_cycle"] = sorted(updated_options_cycle)
    save_state(state)

    # --- 8. Summary ---
    remaining = len([t for t in all_tickers if t not in onboarded])
    logger.info(
        "Done.  Onboarded: %d/%d  |  Remaining: %d",
        len(onboarded),
        len(all_tickers),
        remaining,
    )
    if remaining > 0 and batch_size > 0:
        eta = (remaining + batch_size - 1) // batch_size
        logger.info(
            "At %d tickers/day  →  ~%d more day(s) to full coverage", batch_size, eta
        )

    # --- 9. Optional merge ---
    if run_merge:
        from market_data import merge  # noqa: PLC0415
        merge.run(DATA_DIR)


def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Daily OHLCV data pipeline for the paper_trading engine."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"New tickers to onboard per run (default: {DEFAULT_BATCH_SIZE}). "
             "Set to 0 to skip onboarding and only run updates.",
    )
    parser.add_argument(
        "--no-update",
        action="store_true",
        help="Skip incremental updates; only onboard new tickers.",
    )
    parser.add_argument(
        "--merge",
        action="store_true",
        help="Run merge.py after the pipeline completes.",
    )
    parser.add_argument(
        "--indices",
        action="store_true",
        help="Also update index/rate symbols (VIX, Treasury yields, Fed Funds futures).",
    )
    parser.add_argument(
        "--macro",
        action="store_true",
        help="Also update FRED macro series (CPI, GDP, Fed Funds rate, etc.).",
    )
    parser.add_argument(
        "--fundamentals",
        action="store_true",
        help=(
            "Also snapshot per-ticker fundamentals (market cap, analyst estimates, etc.). "
            f"Auto-skipped if last run was <{FUNDAMENTALS_REFRESH_DAYS} days ago."
        ),
    )
    parser.add_argument(
        "--options",
        action="store_true",
        help=(
            "Also run the options chain batch (IV, bid/ask, OI) for SP500 + ETF tickers. "
            f"Processes the next --options-batch-size tickers in the cycle (default: {DEFAULT_OPTIONS_BATCH_SIZE})."
        ),
    )
    parser.add_argument(
        "--options-batch-size",
        type=int,
        default=DEFAULT_OPTIONS_BATCH_SIZE,
        metavar="N",
        help=f"Tickers to process per options run (default: {DEFAULT_OPTIONS_BATCH_SIZE}).",
    )
    args = parser.parse_args()

    run(
        batch_size=args.batch_size,
        skip_update=args.no_update,
        run_merge=args.merge,
        run_indices=args.indices,
        run_macro=args.macro,
        run_fundamentals=args.fundamentals,
        run_options=args.options,
        options_batch_size=args.options_batch_size,
    )


if __name__ == "__main__":
    main()

"""
orchestrator.py
---------------
Daily runner for the market_data pipeline.

Each run does two things, in order:

  1. ONBOARD  — pick the next `--batch-size` tickers from tickers.csv that
                haven't been fetched yet, and pull their full 10-year history.

  2. UPDATE   — for every already-onboarded ticker, pull any new trading days
                since the last run and append them to their Parquet file.

Progress is persisted in state.json so runs are safe to interrupt and resume.

Usage
-----
    python orchestrator.py                        # default batch size (50)
    python orchestrator.py --batch-size 25        # onboard fewer per day
    python orchestrator.py --batch-size 0         # updates only, no new tickers
    python orchestrator.py --no-update            # onboard only, skip updates
    python orchestrator.py --merge                # run merge.py when done
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

import pandas as pd

from market_data.fetch import DEFAULT_HISTORY_YEARS, fetch_history, fetch_incremental, save_ticker_data

# ---------------------------------------------------------------------------
# Paths & defaults
# ---------------------------------------------------------------------------

STATE_FILE = Path("state.json")
TICKERS_FILE = Path("tickers.csv")
DATA_DIR = Path("data")

DEFAULT_BATCH_SIZE = 50
SLEEP_BETWEEN_CALLS = 5  # seconds; be polite to the yfinance endpoint


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text())
        return {
            "onboarded": raw.get("onboarded", []),
            "last_run": raw.get("last_run"),
        }
    return {"onboarded": [], "last_run": None}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


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

    print(f"\n{'='*60}")
    print(f"ONBOARD  {len(to_onboard)} new tickers  "
          f"({len(pending) - len(to_onboard)} still pending after this run)")
    print(f"{'='*60}")

    for i, symbol in enumerate(to_onboard, 1):
        prefix = f"  [{i:>3}/{len(to_onboard)}] {symbol:<8}"
        try:
            df = fetch_history(symbol, years=DEFAULT_HISTORY_YEARS)
            if df.empty:
                print(f"{prefix}  no data (skipping)")
            else:
                added = save_ticker_data(symbol, df, DATA_DIR)
                newly_onboarded.add(symbol)
                print(f"{prefix}  {added:>5} rows saved")
        except Exception as exc:
            print(f"{prefix}  ERROR: {exc}")
            failed.add(symbol)

        time.sleep(SLEEP_BETWEEN_CALLS)

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

    print(f"\n{'='*60}")
    print(f"UPDATE   {len(to_update)} tickers  (since {since})")
    print(f"{'='*60}")

    for i, symbol in enumerate(to_update, 1):
        prefix = f"  [{i:>4}/{len(to_update)}] {symbol:<8}"
        try:
            df = fetch_incremental(symbol, since=since)
            if df.empty:
                print(f"{prefix}  up to date")
                results[symbol] = 0
            else:
                added = save_ticker_data(symbol, df, DATA_DIR)
                print(f"{prefix}  +{added} rows")
                results[symbol] = added
        except Exception as exc:
            print(f"{prefix}  ERROR: {exc}")
            results[symbol] = 0

        time.sleep(SLEEP_BETWEEN_CALLS)

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(batch_size: int, skip_update: bool, run_merge: bool) -> None:
    today = date.today()

    state = load_state()
    onboarded: set[str] = set(state["onboarded"])
    last_run: date | None = (
        date.fromisoformat(state["last_run"]) if state["last_run"] else None
    )

    all_tickers = load_ordered_tickers()
    pending = [t for t in all_tickers if t not in onboarded]

    print(f"\nmarket_data orchestrator  —  {today}")
    print(f"  Tickers in list : {len(all_tickers)}")
    print(f"  Already onboarded: {len(onboarded)}")
    print(f"  Pending          : {len(pending)}")
    print(f"  Last run         : {last_run or 'never'}")

    # --- 1. Onboard new tickers ---
    newly_onboarded, _failed = step_onboard(pending, batch_size, onboarded)
    onboarded |= newly_onboarded

    # --- 2. Incremental updates ---
    if not skip_update and last_run:
        # Update tickers that were already onboarded before this run
        # (newly onboarded ones just got full history; no need to update them)
        to_update = [t for t in all_tickers if t in onboarded and t not in newly_onboarded]
        step_update(to_update, since=last_run)
    elif not skip_update and not last_run:
        print("\nUPDATE   skipped — no previous run date in state.json")

    # --- 3. Persist state ---
    state["onboarded"] = sorted(onboarded)
    state["last_run"] = str(today)
    save_state(state)

    # --- 4. Summary ---
    remaining = len([t for t in all_tickers if t not in onboarded])
    print(f"\n{'='*60}")
    print(f"Done.  Onboarded: {len(onboarded)}/{len(all_tickers)}  |  "
          f"Remaining: {remaining}")
    if remaining > 0 and batch_size > 0:
        eta = (remaining + batch_size - 1) // batch_size
        print(f"At {batch_size} tickers/day  →  ~{eta} more day(s) to full coverage")
    print(f"{'='*60}\n")

    # --- 5. Optional merge ---
    if run_merge:
        from market_data import merge  # noqa: PLC0415
        merge.run(DATA_DIR)


def main() -> None:
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
    args = parser.parse_args()

    run(
        batch_size=args.batch_size,
        skip_update=args.no_update,
        run_merge=args.merge,
    )


if __name__ == "__main__":
    main()

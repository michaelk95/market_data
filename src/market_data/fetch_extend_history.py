"""
fetch_extend_history.py
-----------------------
Extends OHLCV history for all onboarded active tickers back to the maximum
available depth (targeting 1990) using yfinance period="max".

For each onboarded ticker whose existing data starts after EARLIEST_TARGET,
fetches the full available history and merges it into the existing parquet.
Deduplication is handled by save_ticker_data, so re-runs are safe.

Tickers whose earliest date is already at or before EARLIEST_TARGET are
marked complete immediately — no network call needed.

Progress state
--------------
Two keys are written to state.json after each run:

  extend_history_completed  list[str]       Tickers fully processed.
  extend_history_failures   dict[str, str]  Tickers that failed; value is reason.

Runs are safe to interrupt and resume: processed tickers are skipped on
subsequent calls.

CLI: market-data-extend-history
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg
from market_data.fetch import fetch_max_history, save_ticker_data

logger = logging.getLogger(__name__)

STATE_FILE = Path(_cfg.get("paths.state_file", "state.json"))
OHLCV_DIR = Path(_cfg.get("paths.ohlcv_dir", "data/ohlcv"))

EARLIEST_TARGET: date = date(1990, 1, 1)
DEFAULT_BATCH_SIZE: int = _cfg.get("collection.extend_history_batch_size", 50)
SLEEP_BETWEEN_CALLS: float = _cfg.get("sources.sleep_between_calls.ohlcv", 5)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text())
    else:
        raw = {}
    return {
        "extend_history_completed": raw.get("extend_history_completed", []),
        "extend_history_failures": raw.get("extend_history_failures", {}),
        **{k: v for k, v in raw.items()
           if k not in {"extend_history_completed", "extend_history_failures"}},
    }


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Pending-ticker logic
# ---------------------------------------------------------------------------

def _earliest_date(ticker: str, ohlcv_dir: Path) -> date | None:
    """Return the earliest date in an existing OHLCV parquet, or None if missing."""
    path = ohlcv_dir / f"{ticker}.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date"])
    if df.empty:
        return None
    return pd.to_datetime(df["date"]).min().date()


def pending_tickers(
    onboarded: set[str],
    ohlcv_dir: Path,
    completed: set[str],
    failures: set[str],
    earliest_target: date = EARLIEST_TARGET,
) -> tuple[list[str], list[str]]:
    """Classify onboarded tickers into those needing extension and those already deep.

    Returns
    -------
    (needs_fetch, already_deep)
        needs_fetch   — tickers whose earliest date is after earliest_target
        already_deep  — tickers already at or before earliest_target (can be
                        marked complete without a network call)
    """
    needs_fetch: list[str] = []
    already_deep: list[str] = []

    for ticker in sorted(onboarded):
        if ticker in completed or ticker in failures:
            continue
        earliest = _earliest_date(ticker, ohlcv_dir)
        if earliest is None:
            continue  # No file yet; not our job (orchestrator handles onboarding)
        if earliest <= earliest_target:
            already_deep.append(ticker)
        else:
            needs_fetch.append(ticker)

    return needs_fetch, already_deep


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run(
    ohlcv_dir: Path = OHLCV_DIR,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
    earliest_target: date = EARLIEST_TARGET,
) -> dict:
    """Extend OHLCV history for all onboarded tickers back to earliest_target.

    Parameters
    ----------
    ohlcv_dir:
        Directory where per-ticker OHLCV parquets are stored.
    batch_size:
        Maximum number of tickers to fetch in a single run (0 = no limit).
    dry_run:
        If True, log what would be fetched without making network calls.
    earliest_target:
        Skip tickers whose history already starts at or before this date.

    Returns
    -------
    dict with keys ``fetched``, ``skipped``, ``failed``, ``remaining``.
    """
    state = _load_state()
    onboarded: set[str] = set(state.get("onboarded", []))
    completed: set[str] = set(state["extend_history_completed"])
    failures: set[str] = set(state["extend_history_failures"].keys())

    if not onboarded:
        logger.info("No onboarded tickers found in state.json — nothing to do.")
        return {"fetched": 0, "skipped": 0, "failed": 0, "remaining": 0}

    needs_fetch, already_deep = pending_tickers(
        onboarded, ohlcv_dir, completed, failures, earliest_target
    )

    # Mark tickers that already have deep history as complete (no fetch needed)
    for ticker in already_deep:
        if ticker not in state["extend_history_completed"]:
            state["extend_history_completed"].append(ticker)
    if already_deep:
        _save_state(state)
        logger.info(
            "%d tickers already have history at or before %s — marked complete.",
            len(already_deep), earliest_target,
        )

    logger.info(
        "Extend-history summary: %d onboarded | %d already complete | "
        "%d failed previously | %d pending fetch",
        len(onboarded),
        len(set(state["extend_history_completed"])),
        len(failures),
        len(needs_fetch),
    )

    if not needs_fetch:
        logger.info("Nothing to extend.")
        return {
            "fetched": 0,
            "skipped": len(already_deep),
            "failed": 0,
            "remaining": 0,
        }

    if batch_size > 0:
        batch = needs_fetch[:batch_size]
        remaining = len(needs_fetch) - len(batch)
    else:
        batch = needs_fetch
        remaining = 0

    logger.info(
        "Processing %d tickers this run%s%s.",
        len(batch),
        f" (batch_size={batch_size})" if batch_size > 0 else "",
        " [DRY RUN]" if dry_run else "",
    )

    fetched = failed = 0

    for i, ticker in enumerate(batch, 1):
        if dry_run:
            logger.info("  [dry-run] %d/%d  %s", i, len(batch), ticker)
            continue

        logger.info("  %d/%d  %s", i, len(batch), ticker)

        try:
            df = fetch_max_history(ticker)
        except Exception as exc:  # noqa: BLE001
            reason = str(exc)[:200]
            logger.warning("    FAIL %s: %s", ticker, reason)
            state["extend_history_failures"][ticker] = reason
            failed += 1
            _save_state(state)
            continue

        if df.empty:
            reason = "no data returned by yfinance"
            logger.warning("    SKIP %s: %s", ticker, reason)
            state["extend_history_failures"][ticker] = reason
            failed += 1
            _save_state(state)
            continue

        new_rows = save_ticker_data(ticker, df, ohlcv_dir)
        earliest = df["date"].min()
        logger.info(
            "    OK   %s: %d rows (%d new, earliest=%s)",
            ticker, len(df), new_rows, earliest,
        )
        state["extend_history_completed"].append(ticker)
        fetched += 1
        _save_state(state)

        if i < len(batch):
            time.sleep(SLEEP_BETWEEN_CALLS)

    if not dry_run:
        logger.info(
            "Extend-history run complete: %d fetched, %d failed, %d remaining.",
            fetched, failed, remaining,
        )

    return {
        "fetched": fetched,
        "skipped": len(already_deep),
        "failed": failed,
        "remaining": remaining,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Extend OHLCV history for all onboarded tickers back to maximum "
            "available depth (targeting 1990) using yfinance period='max'."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            f"Max tickers to fetch per run (default: {DEFAULT_BATCH_SIZE}; "
            "0 = no limit)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be fetched without making network calls.",
    )
    args = parser.parse_args()

    try:
        run(batch_size=args.batch_size, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        logger.error("Extend-history failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

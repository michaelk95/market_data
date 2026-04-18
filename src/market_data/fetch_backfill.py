"""
fetch_backfill.py
-----------------
Backfills historical OHLCV data for S&P 500 tickers that were delisted or
removed from the index before the regular pipeline began tracking them.

Source:  data/constituent_history.parquet   (built by fetch_constituent_history)
Output:  data/ohlcv/<SYMBOL>.parquet        (same schema as the regular pipeline)

For each delisted ticker (date_removed is not NaT) that has no existing OHLCV
file, this module fetches the full span of its S&P 500 membership using
yfinance and stores it via the same atomic save used by the main pipeline.

Tickers with multiple membership periods (e.g. AAL: 1996→1997, 2015→2024) are
fetched as a single request from the earliest start to the latest end; gaps
where the stock was not traded produce no rows.

Active tickers (date_removed is NaT) are intentionally skipped — they are
managed by the normal daily pipeline.

Progress state
--------------
Two keys are written to state.json after each run:

  backfill_completed  list[str]        Tickers that produced ≥1 row of data.
  backfill_failures   dict[str, str]   Tickers that returned no data; value is
                                       a short reason string.

Runs are safe to interrupt and resume: completed and failed tickers are skipped
on subsequent calls.

CLI: market-data-backfill-constituents
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg
from market_data.fetch import fetch_date_range, save_ticker_data

logger = logging.getLogger(__name__)

STATE_FILE = Path(_cfg.get("paths.state_file", "state.json"))
OHLCV_DIR = Path(_cfg.get("paths.ohlcv_dir", "data/ohlcv"))
CONSTITUENT_HISTORY_FILE = _cfg.resolve_path(
    "paths.constituent_history_file", "data/constituent_history.parquet"
)

DEFAULT_BATCH_SIZE: int = _cfg.get("collection.backfill_batch_size", 50)
SLEEP_BETWEEN_CALLS: float = _cfg.get("sources.sleep_between_calls.ohlcv", 5)


# ---------------------------------------------------------------------------
# State helpers (mirrors orchestrator pattern)
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    if STATE_FILE.exists():
        raw = json.loads(STATE_FILE.read_text())
    else:
        raw = {}
    return {
        "backfill_completed": raw.get("backfill_completed", []),
        "backfill_failures": raw.get("backfill_failures", {}),
        **{k: v for k, v in raw.items()
           if k not in {"backfill_completed", "backfill_failures"}},
    }


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ---------------------------------------------------------------------------
# Pending-ticker logic
# ---------------------------------------------------------------------------

def pending_tickers(
    constituent_df: pd.DataFrame,
    ohlcv_dir: Path,
    completed: set[str],
    failures: set[str],
) -> list[dict]:
    """Return backfill jobs for delisted tickers that have no OHLCV file yet.

    Each entry is a dict with keys: ticker, start (date), end (date).

    Skips:
    - Active tickers (date_removed is NaT) — handled by the normal pipeline.
    - Tickers already in *completed* or *failures*.
    - Tickers that already have a file under *ohlcv_dir* (assumed covered).

    For tickers with multiple membership periods, start/end span the full
    range (earliest date_added → latest date_removed).
    """
    delisted = constituent_df[constituent_df["date_removed"].notna()].copy()

    # Collapse multiple periods per ticker into a single date range
    grouped = (
        delisted.groupby("ticker")
        .agg(start=("date_added", "min"), end=("date_removed", "max"))
        .reset_index()
    )

    jobs = []
    for row in grouped.itertuples(index=False):
        ticker = row.ticker
        if ticker in completed or ticker in failures:
            continue
        if (ohlcv_dir / f"{ticker}.parquet").exists():
            continue
        jobs.append({
            "ticker": ticker,
            "start": row.start.date() if hasattr(row.start, "date") else row.start,
            "end": row.end.date() if hasattr(row.end, "date") else row.end,
        })

    return jobs


# ---------------------------------------------------------------------------
# Main backfill runner
# ---------------------------------------------------------------------------

def run(
    constituent_path: Path = CONSTITUENT_HISTORY_FILE,
    ohlcv_dir: Path = OHLCV_DIR,
    batch_size: int = DEFAULT_BATCH_SIZE,
    dry_run: bool = False,
) -> dict:
    """Backfill OHLCV for delisted S&P 500 tickers.

    Parameters
    ----------
    constituent_path:
        Path to constituent_history.parquet.
    ohlcv_dir:
        Directory where per-ticker OHLCV parquets are stored.
    batch_size:
        Maximum number of tickers to fetch in a single run (0 = no limit).
    dry_run:
        If True, log what would be fetched without making any network calls
        or writing any files.

    Returns
    -------
    dict with keys ``fetched``, ``skipped``, ``failed``, ``remaining``.
    """
    if not constituent_path.exists():
        raise FileNotFoundError(
            f"Constituent history not found at {constituent_path}. "
            "Run market-data-fetch-constituent-history first."
        )

    constituent_df = pd.read_parquet(constituent_path)
    state = _load_state()
    completed: set[str] = set(state["backfill_completed"])
    failures: set[str] = set(state["backfill_failures"].keys())

    jobs = pending_tickers(constituent_df, ohlcv_dir, completed, failures)

    total_delisted = constituent_df["date_removed"].notna().sum()
    skipped_existing = total_delisted - len(jobs) - len(failures)

    logger.info(
        "Backfill summary: %d delisted tickers total | "
        "%d already complete or have OHLCV file | %d failed previously | "
        "%d pending",
        total_delisted, int(skipped_existing) + len(completed), len(failures), len(jobs),
    )

    if not jobs:
        logger.info("Nothing to backfill.")
        return {"fetched": 0, "skipped": int(skipped_existing), "failed": 0, "remaining": 0}

    if batch_size > 0:
        batch = jobs[:batch_size]
        remaining = len(jobs) - len(batch)
    else:
        batch = jobs
        remaining = 0

    logger.info(
        "Processing %d tickers this run%s%s.",
        len(batch),
        f" (batch_size={batch_size})" if batch_size > 0 else "",
        " [DRY RUN]" if dry_run else "",
    )

    fetched = failed = 0

    for i, job in enumerate(batch, 1):
        ticker = job["ticker"]
        start = job["start"]
        end = job["end"]

        if dry_run:
            logger.info(
                "  [dry-run] %d/%d  %s  %s → %s",
                i, len(batch), ticker, start, end,
            )
            continue

        logger.info(
            "  %d/%d  %s  %s → %s",
            i, len(batch), ticker, start, end,
        )

        try:
            df = fetch_date_range(ticker, start, end)
        except Exception as exc:  # noqa: BLE001
            reason = str(exc)[:200]
            logger.warning("    FAIL %s: %s", ticker, reason)
            state["backfill_failures"][ticker] = reason
            failed += 1
            _save_state(state)
            continue

        if df.empty:
            reason = "no data returned by yfinance"
            logger.warning("    SKIP %s: %s", ticker, reason)
            state["backfill_failures"][ticker] = reason
            failed += 1
            _save_state(state)
            continue

        new_rows = save_ticker_data(ticker, df, ohlcv_dir)
        logger.info("    OK   %s: %d rows (%d new)", ticker, len(df), new_rows)
        state["backfill_completed"].append(ticker)
        fetched += 1
        _save_state(state)

        if i < len(batch):
            time.sleep(SLEEP_BETWEEN_CALLS)

    if not dry_run:
        logger.info(
            "Backfill run complete: %d fetched, %d failed, %d remaining.",
            fetched, failed, remaining,
        )

    return {
        "fetched": fetched,
        "skipped": int(skipped_existing),
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
            "Backfill historical OHLCV for delisted S&P 500 constituents. "
            "Reads data/constituent_history.parquet and writes to data/ohlcv/."
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
        "--history-file",
        default=None,
        help="Path to constituent_history.parquet (default: data/constituent_history.parquet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be fetched without making network calls.",
    )
    args = parser.parse_args()

    constituent_path = (
        Path(args.history_file) if args.history_file else CONSTITUENT_HISTORY_FILE
    )

    try:
        run(
            constituent_path=constituent_path,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        logger.error("Backfill failed: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

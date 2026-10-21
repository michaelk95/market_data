"""
repair_split_adjustment.py
--------------------------
Re-download OHLCV history for symbols whose stored bars span an unadjusted
stock split, and replace those rows (ADR 0002).

Bars are stored split-adjusted as of their download and the nightly run only
appends, so before the nightly split re-download existed every split left the
older bars on the old price basis (BKNG fell 96% on 2026-04-06 in the store).
The nightly run also misses a split whose yfinance row falls outside its
update window. This script finds both.

For each candidate it:
  1. Re-downloads the symbol's stored date range with `fetch_date_range`
     (cached under `--cache-dir`, so a dry run followed by `--apply` downloads
     once; the configured ohlcv sleep runs after every download).
  2. Runs the same gate as the nightly run (`splits.check_refetch`): enough
     coverage of the stored dates, and few enough breaks in the stored/new
     close ratio. Rejected symbols are reported and left alone.
  3. Replaces the stored rows when the re-download has at least one break.
     Symbols with none are already consistent.

Candidates are the symbols whose stored close jumps by a split-like factor
(`splits.candidate_factors`, within `splits.ratio_tolerance`), on or after
`--since`; `--symbols` overrides the list. A real price move can match too;
step 3 tells them apart.

With `--apply` it copies the touched year partitions to
`<data_dir>/ohlcv_split_backup_<timestamp>/` first, then writes all
replacements in one `storage.replace_rows` call. Safe to re-run: a repaired
symbol has no breaks. Run `--apply` soon after the dry run (the cache is keyed
on the stored date range), and not while the daily collector is writing.

Usage
-----
    python scripts/repair_split_adjustment.py --since 2026-04-01           # dry run (default)
    python scripts/repair_split_adjustment.py --since 2026-04-01 --apply   # back up, then replace
    python scripts/repair_split_adjustment.py --symbols BKNG CRWD --apply
"""

from __future__ import annotations

import argparse
import logging
import shutil
import time
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from market_data import splits
from market_data.config import cfg as _cfg
from market_data.fetch import fetch_date_range
from market_data.storage import read_table

# Under "market_data" so setup_logging() handlers see it (__name__ is "__main__" when run as a script).
logger = logging.getLogger("market_data.repair_split_adjustment")

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
CACHE_DIR = _cfg.resolve_path("splits.repair_cache_dir", "logs/split_repair_cache")
SLEEP_BETWEEN_CALLS: float = _cfg.get("sources.sleep_between_calls.ohlcv", 5)
TABLE = "ohlcv"


def find_candidates(data_dir: Path, since: date | None) -> list[str]:
    """Return the symbols whose stored closes jump by a split-like factor."""
    # Read a few days before `since` so the first bar after it has a previous close.
    start = since - timedelta(days=10) if since else None
    bars = read_table(
        TABLE, data_dir, start_date=start, columns=["symbol", "period_start_date", "close"]
    ).rename(columns={"period_start_date": "date"})
    if bars.empty:
        return []
    return splits.split_candidates(bars, since=since)


def _cached_fetch(cache_dir: Path, fetch: Callable[..., pd.DataFrame], sleep: float):
    """Wrap *fetch* so each download is cached as parquet and followed by *sleep*."""
    cache_dir.mkdir(parents=True, exist_ok=True)

    def cached(symbol: str, start: date, end: date | None = None) -> pd.DataFrame:
        path = cache_dir / f"{symbol}_{start}_{end}.parquet"
        if path.exists():
            return pd.read_parquet(path)
        df = fetch(symbol, start, end)
        time.sleep(sleep)
        df.to_parquet(path, index=False)
        return df

    return cached


def _backup(data_dir: Path, frames: dict[str, pd.DataFrame]) -> Path:
    years = sorted({d.year for df in frames.values() for d in df["date"]})
    backup = data_dir / f"{TABLE}_split_backup_{datetime.now():%Y%m%d_%H%M%S_%f}"
    for year in years:
        part = data_dir / TABLE / f"year={year}"
        if part.exists():
            shutil.copytree(part, backup / part.name)
    logger.info("backed up %d partition(s) to %s", len(years), backup)
    return backup


def run(
    data_dir: Path,
    *,
    apply: bool,
    symbols: list[str] | None = None,
    since: date | None = None,
    cache_dir: Path = CACHE_DIR,
    fetch: Callable[..., pd.DataFrame] = fetch_date_range,
    sleep: float = SLEEP_BETWEEN_CALLS,
) -> dict[str, list[str]]:
    """Check the candidates and, with *apply*, replace their history. Returns a summary."""
    mode = "" if apply else "[dry run] "
    candidates = sorted(symbols) if symbols else find_candidates(data_dir, since)
    summary: dict[str, list[str]] = {
        "candidates": candidates, "replace": [], "consistent": [], "rejected": [], "failed": [],
    }
    logger.info("%s%d candidate(s): %s", mode, len(candidates), " ".join(candidates))

    cached = _cached_fetch(cache_dir, fetch, sleep)
    frames: dict[str, pd.DataFrame] = {}
    for i, symbol in enumerate(candidates, 1):
        prefix = f"[{i:>4}/{len(candidates)}] {symbol:<8}"
        try:
            fresh, breaks = splits.prepare_refetch(symbol, data_dir, fetch=cached)
        except splits.RefetchRejected as exc:
            logger.warning("%s  rejected: %s", prefix, exc)
            summary["rejected"].append(symbol)
        except Exception:  # per-symbol failure, reported in the summary
            logger.exception("%s  download failed", prefix)
            summary["failed"].append(symbol)
        else:
            if breaks == 0:
                logger.info("%s  consistent", prefix)
                summary["consistent"].append(symbol)
            else:
                logger.info("%s  %d break(s), %d rows to replace", prefix, breaks, len(fresh))
                summary["replace"].append(symbol)
                frames[symbol] = fresh

    logger.info(
        "%sreplace %d: %s | consistent %d | rejected %d: %s | failed %d: %s",
        mode, len(summary["replace"]), " ".join(summary["replace"]),
        len(summary["consistent"]), len(summary["rejected"]), " ".join(summary["rejected"]),
        len(summary["failed"]), " ".join(summary["failed"]),
    )
    if not apply or not frames:
        return summary

    _backup(data_dir, frames)
    rows = splits.apply_refetches(frames, data_dir)
    logger.info("replaced %d rows for %d symbol(s)", rows, len(frames))
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    from market_data.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Re-download OHLCV history for symbols with an unadjusted stock split "
            "and replace the stored rows. Dry run unless --apply is given."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Back up the touched partitions, then replace the rows (default: dry run).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help=f"Root data directory (default: {DATA_DIR}).",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        help="Check these symbols instead of searching the store for split-shaped jumps.",
    )
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        help="Only look for jumps on or after this date (YYYY-MM-DD; default: all history).",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=CACHE_DIR,
        help=f"Download cache (default: {CACHE_DIR}).",
    )
    args = parser.parse_args()

    run(
        args.data_dir,
        apply=args.apply,
        symbols=args.symbols,
        since=args.since,
        cache_dir=args.cache_dir,
    )


if __name__ == "__main__":
    main()

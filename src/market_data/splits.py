"""
splits.py
---------
Re-download a symbol's OHLCV history after a stock split (ADR 0002).

Bars are stored split-adjusted as of their download and later runs only
append, so a split leaves the older bars on the old price basis.  The fix is
to re-download the symbol's whole stored date range, which yfinance returns
on one consistent basis, and replace the stored rows with it.

Functions
---------
count_breaks(stored, fresh)          Jumps in the stored/fresh close ratio.
check_refetch(stored, fresh)         Gate a re-download; raise RefetchRejected.
prepare_refetch(symbol, data_dir)    Re-download the stored range and gate it.
apply_refetches(frames, data_dir)    Replace the stored rows in one write.
shows_unadjusted_split(fresh, ...)   Whether a download still jumps at a split.
split_candidates(flat_df, since)     Symbols whose stored closes jump by a
                                     split-like factor (for the repair script).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from market_data.config import cfg as _cfg
from market_data.fetch import fetch_date_range, load_ticker_data
from market_data.flat_tables import from_flat
from market_data.storage import replace_rows

logger = logging.getLogger(__name__)

# Share of the stored dates a re-download must cover.
MIN_COVERAGE: float = _cfg.get("splits.min_coverage", 0.98)
# More breaks than this means the ticker now points at a different security.
MAX_BREAKS: int = _cfg.get("splits.max_breaks", 3)
# |day-over-day change| in log(stored close / fresh close) that counts as a break.
BREAK_THRESHOLD: float = _cfg.get("splits.break_threshold", 0.15)
# Relative tolerance when matching a price jump to a split ratio.
RATIO_TOLERANCE: float = _cfg.get("splits.ratio_tolerance", 0.05)
# Days either side of yfinance's split date to look for the price jump.
JUMP_WINDOW_DAYS: int = _cfg.get("splits.jump_window_days", 5)
# Split factors (and their reverses) the repair script looks for.
CANDIDATE_FACTORS: list[float] = _cfg.get(
    "splits.candidate_factors",
    [1.25, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 25, 30, 40, 50, 75, 100],
)


class RefetchRejected(Exception):
    """A re-download does not match the stored history well enough to replace it."""


def _common_closes(stored: pd.DataFrame, fresh: pd.DataFrame) -> pd.DataFrame:
    merged = stored[["date", "close"]].merge(
        fresh[["date", "close"]], on="date", suffixes=("_stored", "_fresh")
    )
    merged = merged[(merged["close_stored"] > 0) & (merged["close_fresh"] > 0)]
    return merged.sort_values("date")


def count_breaks(stored: pd.DataFrame, fresh: pd.DataFrame) -> int:
    """Count the jumps in log(stored close / fresh close) on their common dates.

    One unadjusted split in the stored rows is one break; a constant dividend
    drift is none.
    """
    common = _common_closes(stored, fresh)
    log_ratio = np.log(common["close_stored"] / common["close_fresh"])
    return int((log_ratio.diff().abs() > BREAK_THRESHOLD).sum())


def check_refetch(stored: pd.DataFrame, fresh: pd.DataFrame) -> int:
    """Return the break count, or raise ``RefetchRejected`` if *fresh* fails the gate."""
    if fresh.empty:
        raise RefetchRejected("empty download")
    coverage = stored["date"].isin(set(fresh["date"])).mean()
    if coverage < MIN_COVERAGE:
        raise RefetchRejected(
            f"coverage {coverage:.1%} of {len(stored)} stored dates, below {MIN_COVERAGE:.0%}"
        )
    breaks = count_breaks(stored, fresh)
    if breaks > MAX_BREAKS:
        raise RefetchRejected(f"{breaks} breaks against the stored closes, above {MAX_BREAKS}")
    return breaks


def prepare_refetch(
    symbol: str,
    data_dir: Path,
    fetch: Callable[..., pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, int]:
    """Re-download *symbol*'s stored date range and gate it against the stored rows.

    Returns the re-downloaded bars (flat OHLCV layout, clipped to the stored
    range) and their break count.  Stored dates the re-download lacks are
    logged; ``apply_refetches`` leaves them in place.
    """
    stored = load_ticker_data(symbol, data_dir)
    if stored is None:
        raise RefetchRejected("nothing stored")
    start, end = stored["date"].min(), stored["date"].max()

    fresh = (fetch or fetch_date_range)(symbol, start, end)
    fresh = fresh[(fresh["date"] >= start) & (fresh["date"] <= end)].reset_index(drop=True)

    breaks = check_refetch(stored, fresh)
    missing = stored.loc[~stored["date"].isin(set(fresh["date"])), "date"]
    if not missing.empty:
        logger.warning(
            "%s: %d stored date(s) not in the re-download, kept as stored: %s",
            symbol, len(missing), ", ".join(str(d) for d in missing.head(10)),
        )
    return fresh, breaks


def apply_refetches(frames: dict[str, pd.DataFrame], data_dir: Path) -> int:
    """Replace the stored rows of every re-downloaded symbol in one write.

    Only the re-downloaded ``(symbol, date)`` keys are touched.  Returns the
    number of rows written.
    """
    if not frames:
        return 0
    df = pd.concat(frames.values(), ignore_index=True)
    collected_at = pd.Timestamp.now(tz="UTC").floor("us")
    return replace_rows(from_flat(df, "ohlcv", collected_at), "ohlcv", data_dir)


def _matches(day_ratio: pd.Series, factor: float) -> pd.Series:
    return (day_ratio * factor - 1).abs() < RATIO_TOLERANCE


def shows_unadjusted_split(fresh: pd.DataFrame, split_date: date, ratio: float) -> bool:
    """Whether *fresh* still drops by *ratio* near *split_date*.

    True means Yahoo has not adjusted the history for the split yet, so the
    re-download is no better than the stored rows.
    """
    if abs(ratio - 1) < RATIO_TOLERANCE:
        return False
    bars = fresh[fresh["close"] > 0].sort_values("date")
    day_ratio = bars["close"] / bars["close"].shift()
    window = timedelta(days=JUMP_WINDOW_DAYS)
    near = (bars["date"] >= split_date - window) & (bars["date"] <= split_date + window)
    return bool((_matches(day_ratio, ratio) & near).any())


def split_candidates(flat_df: pd.DataFrame, since: date | None = None) -> list[str]:
    """Return the symbols whose close jumps by a split-like factor (on or after *since*).

    *flat_df* needs ``symbol``, ``date`` and ``close`` columns.  A real move
    can match too; the re-download's break count tells them apart.
    """
    bars = flat_df[flat_df["close"] > 0].sort_values(["symbol", "date"])
    day_ratio = bars["close"] / bars.groupby("symbol")["close"].shift()
    hit = pd.Series(False, index=bars.index)
    for factor in CANDIDATE_FACTORS:
        hit |= _matches(day_ratio, factor) | _matches(day_ratio, 1 / factor)
    if since is not None:
        hit &= bars["date"] >= since
    return sorted(bars.loc[hit, "symbol"].unique())

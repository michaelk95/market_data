"""
fetch_macro.py
--------------
Collect macroeconomic time series from the FRED API (Federal Reserve Bank
of St. Louis) with full vintage (realtime) history.

Each row carries the vintage date (``report_date = FRED realtime_start``) and
the supersession date (``valid_to_date = FRED realtime_end``; ``9999-12-31``
for the currently-active value), enabling point-in-time queries that are free
of look-ahead bias from data revisions (e.g., GDP advance vs. final estimates).

``revision_rank`` (1 = advance, 2 = second estimate, …) is stored on every row
and kept consistent across the whole series after each incremental update via
``_recompute_revision_ranks``.

Series collected by default
----------------------------
Daily
  DFF       Effective Federal Funds Rate
  T10Y2Y    10-year minus 2-year Treasury spread

Monthly
  CPIAUCSL  CPI — All Urban Consumers (headline)
  CPILFESL  Core CPI (ex food & energy)
  PCEPI     PCE Price Index
  PCEPILFE  Core PCE
  UNRATE    Unemployment Rate
  PAYEMS    Nonfarm Payrolls (thousands of jobs)

Quarterly
  GDPC1     Real GDP (chained 2017 dollars)
  GDP       Nominal GDP

Data is stored under data/macro/data.parquet (single unpartitioned file).

Requires
--------
  FRED_API_KEY environment variable (or .env file at project root).

Usage
-----
    market-data-fetch-macro                        # update all default series
    market-data-fetch-macro --series DFF T10Y2Y    # update specific series
    market-data-fetch-macro --start 1990-01-01     # custom bootstrap start
    market-data-fetch-macro --data-dir /path/to/data  # custom data directory
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg
from market_data.resilience import fred_retry
from market_data.schema import PARTITION_COLS, DataSource, ReportTimeMarker
from market_data.storage import read_table, write_table

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))

# Default bootstrap start date — FRED has data going back decades for most series
DEFAULT_START: str = _cfg.get("collection.macro_start", "1990-01-01")

# Series to collect by default — keys of the macro.series mapping in config.yaml
DEFAULT_SERIES: list[str] = list(
    _cfg.get(
        "macro.series",
        {
            # Daily
            "DFF":      "Effective Federal Funds Rate",
            "T10Y2Y":   "10yr minus 2yr Treasury yield spread",
            # Monthly
            "CPIAUCSL": "CPI All Urban Consumers (headline)",
            "CPILFESL": "Core CPI (ex food & energy)",
            "PCEPI":    "PCE Price Index",
            "PCEPILFE": "Core PCE",
            "UNRATE":   "Unemployment Rate",
            "PAYEMS":   "Nonfarm Payrolls",
            # Quarterly
            "GDPC1":    "Real GDP (chained 2017 dollars)",
            "GDP":      "Nominal GDP",
        },
    ).keys()
)

# Incremental lookback window per series.  Some FRED series are subject to
# periodic revisions that can silently rewrite observations from years prior:
#   - GDP / GDPC1: annual revisions every July
#   - PAYEMS / UNRATE: annual BLS benchmark revisions every February
#   - CPIAUCSL / CPILFESL: periodic BLS methodological revisions
#   - PCEPI / PCEPILFE: annual BEA comprehensive revisions every July
# A 400-day window covers one full annual revision cycle for each.  Series
# not listed here use the 7-day default.
SERIES_LOOKBACK_DAYS: dict[str, int] = {
    "GDPC1":    400,  # annual GDP revisions each July
    "GDP":      400,  # annual GDP revisions each July
    "PAYEMS":   400,  # annual BLS benchmark revisions each February
    "UNRATE":   400,  # annual BLS benchmark revisions each February
    "CPIAUCSL": 400,  # periodic BLS methodological revisions
    "CPILFESL": 400,  # periodic BLS methodological revisions
    "PCEPI":    400,  # annual BEA comprehensive revisions each July
    "PCEPILFE": 400,  # annual BEA comprehensive revisions each July
}
_DEFAULT_LOOKBACK_DAYS = 7
# FRED limits get_series_all_releases to 2000 vintage dates per request. Daily series like DFF
# accumulate ~140 vintage dates/year, so a full bootstrap from 1990 hits ~5000. Chunk requests
# into 4-year windows to stay safely under the limit for all series frequencies.
_VINTAGE_CHUNK_YEARS = 4

# Static FRED release names for default series (best-effort; None for unknowns)
_SERIES_RELEASE_NAMES: dict[str, str] = {
    "GDPC1":    "Gross Domestic Product",
    "GDP":      "Gross Domestic Product",
    "CPIAUCSL": "Consumer Price Index for All Urban Consumers",
    "CPILFESL": "Consumer Price Index for All Urban Consumers: All Items Less Food and Energy",
    "PCEPI":    "Personal Income and Outlays",
    "PCEPILFE": "Personal Income and Outlays",
    "UNRATE":   "Employment Situation",
    "PAYEMS":   "Employment Situation",
    "DFF":      "H.15 Selected Interest Rates",
    "T10Y2Y":   "H.15 Selected Interest Rates",
}

_EMPTY_COLS: list[str] = [
    "series_id", "value", "valid_to_date",
    "revision_rank", "release_name",
    "period_start_date", "period_end_date",
    "report_date", "report_time_marker", "source", "collected_at",
]

_FAR_FUTURE = date(9999, 12, 31)


# ---------------------------------------------------------------------------
# API key loading
# ---------------------------------------------------------------------------

def _load_api_key() -> str:
    """
    Load the FRED API key from the environment (or .env file).

    Raises RuntimeError with a clear message if the key is missing so the
    user knows exactly what to fix.
    """
    try:
        from dotenv import load_dotenv  # type: ignore[import]
        load_dotenv()
    except ImportError:
        pass  # python-dotenv not installed; rely on the real environment

    key = os.environ.get("FRED_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "FRED_API_KEY is not set. Add it to your .env file:\n"
            "  FRED_API_KEY=your_key_here\n"
            "Get a free key at https://fred.stlouisfed.org/"
        )
    return key


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _derive_realtime_end(df: pd.DataFrame) -> pd.Series:
    """
    Reconstruct realtime_end for each vintage row.

    fredapi's get_series_all_releases does not return realtime_end, so we derive
    it: for each observation date, the realtime_end of vintage N is one day before
    the realtime_start of vintage N+1. The last vintage for each date gets 9999-12-31.
    """
    rs = pd.to_datetime(df["realtime_start"]).dt.date
    ob = pd.to_datetime(df["date"]).dt.date
    tmp = pd.DataFrame({"ob": ob, "rs": rs}, index=df.index).sort_values(["ob", "rs"])
    tmp["next_rs"] = tmp.groupby("ob")["rs"].shift(-1)
    return (
        tmp["next_rs"]
        .apply(lambda d: (d - timedelta(days=1)) if pd.notna(d) else _FAR_FUTURE)
        .reindex(df.index)
    )


def _fetch_all_releases_chunked(
    fred: object,
    series_id: str,
    realtime_start: str,
    realtime_end: str,
) -> pd.DataFrame:
    """
    Call get_series_all_releases in 4-year windows to stay under FRED's 2000
    vintage-date-per-request limit. Results are concatenated and deduplicated.
    """
    start = date.fromisoformat(realtime_start)
    end = date.fromisoformat(realtime_end)

    frames: list[pd.DataFrame] = []
    chunk_start = start
    while chunk_start <= end:
        try:
            chunk_end = (
                date(chunk_start.year + _VINTAGE_CHUNK_YEARS, chunk_start.month, chunk_start.day)
                - timedelta(days=1)
            )
        except ValueError:
            # chunk_start is Feb 29 in a leap year; next same date may not exist
            chunk_end = date(chunk_start.year + _VINTAGE_CHUNK_YEARS, 2, 28)
        chunk_end = min(chunk_end, end)

        raw = fred.get_series_all_releases(  # type: ignore[union-attr]
            series_id,
            realtime_start=str(chunk_start),
            realtime_end=str(chunk_end),
        )
        if raw is not None and not (hasattr(raw, "empty") and raw.empty):
            frames.append(raw)
        chunk_start = chunk_end + timedelta(days=1)

    if not frames:
        return pd.DataFrame()

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["realtime_start", "date"])
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

@fred_retry
def fetch_series_vintages(
    series_id: str,
    realtime_start: str,
    api_key: str,
) -> pd.DataFrame:
    """
    Pull all vintages of a FRED series from *realtime_start* to today.

    Uses ``get_series_all_releases`` (chunked into 4-year windows to stay under
    FRED's 2000-vintage-date limit) so each row carries the vintage date
    (``report_date``) and derived supersession date (``valid_to_date``), enabling
    point-in-time queries that are free of look-ahead bias from revisions.

    Also populates:
    - ``revision_rank``: ordinal position within the revision chain for each
      observation period (1 = advance estimate, 2 = second, …).  The rank is
      computed across the returned slice only; ``update_series`` recomputes it
      across the full stored series after each write.
    - ``release_name``: human-readable FRED release name (static mapping for
      default series; ``None`` for others).

    Returns a DataFrame ready for ``storage.write_table("macro", ...)``.
    Returns an empty DataFrame (with correct columns) if no data is available.
    """
    try:
        import fredapi  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "fredapi is not installed. Run: pip install fredapi"
        ) from exc

    fred = fredapi.Fred(api_key=api_key)
    df = _fetch_all_releases_chunked(fred, series_id, realtime_start, str(date.today()))

    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    df = df.dropna(subset=["value"])
    if df.empty:
        return pd.DataFrame(columns=_EMPTY_COLS)

    df["period_start_date"] = pd.to_datetime(df["date"]).dt.date
    df["period_end_date"] = df["period_start_date"]
    df["report_date"] = pd.to_datetime(df["realtime_start"]).dt.date
    df["valid_to_date"] = _derive_realtime_end(df)
    df["series_id"] = series_id
    df["report_time_marker"] = ReportTimeMarker.POST_MARKET
    df["source"] = DataSource.FRED
    df["collected_at"] = datetime.now(timezone.utc)
    df["value"] = df["value"].astype(float)

    # revision_rank: ordinal within each (series, period) group ordered by report_date.
    # Computed over this fetch slice; _recompute_revision_ranks corrects it later for
    # incremental runs that only cover a lookback window.
    df["revision_rank"] = (
        df.sort_values("report_date")
          .groupby("period_start_date", sort=False)
          .cumcount()
        + 1
    )

    df["release_name"] = _SERIES_RELEASE_NAMES.get(series_id)

    return df[_EMPTY_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Revision-rank maintenance
# ---------------------------------------------------------------------------

def _macro_partition_paths(data_dir: Path) -> list[Path]:
    """Return all existing parquet paths holding macro data.

    Handles both layouts:

    - Unpartitioned (current):   ``data/macro/data.parquet``
    - Year-partitioned (future): ``data/macro/year=YYYY/data.parquet``

    Layout is determined from ``PARTITION_COLS["macro"]`` so this stays in
    sync if the macro table is ever partitioned alongside ``ohlcv`` et al.
    """
    table_dir = data_dir / "macro"
    if not table_dir.exists():
        return []

    if PARTITION_COLS.get("macro"):
        return [
            entry / "data.parquet"
            for entry in sorted(table_dir.iterdir())
            if entry.is_dir()
            and entry.name.startswith("year=")
            and (entry / "data.parquet").exists()
        ]

    single = table_dir / "data.parquet"
    return [single] if single.exists() else []


def _recompute_revision_ranks(series_id: str, data_dir: Path) -> None:
    """Recompute ``revision_rank`` for *series_id* across the entire stored dataset.

    Called after ``write_table`` to ensure ranks are correct even when
    incremental fetches (which only cover a lookback window) add new vintages
    to observations that already exist in storage.

    Works for both the unpartitioned macro layout (single ``data.parquet``) and
    a year-partitioned layout; partition files are discovered via
    ``_macro_partition_paths`` rather than hardcoded.  Each affected partition
    file is rewritten atomically (tmp + rename).
    """
    paths = _macro_partition_paths(data_dir)
    if not paths:
        return

    # Load every partition once; collect this series' rows across all of them
    # so ranks can be recomputed globally.
    frames: dict[Path, pd.DataFrame] = {p: pd.read_parquet(p) for p in paths}
    series_slices = [
        part.loc[part["series_id"] == series_id]
        for part in frames.values()
        if "series_id" in part.columns and (part["series_id"] == series_id).any()
    ]
    if not series_slices:
        return

    combined = (
        pd.concat(series_slices, ignore_index=True)
        .sort_values(["period_start_date", "report_date"])
    )
    combined["revision_rank"] = (
        combined.groupby("period_start_date", sort=False).cumcount() + 1
    )

    # Join key includes series_id so the rank map cannot collide with rows
    # from other series that happen to share a (period, report) pair.
    rank_map = combined[
        ["series_id", "period_start_date", "report_date", "revision_rank"]
    ].rename(columns={"revision_rank": "_new_rank"})

    for path, part in frames.items():
        if "series_id" not in part.columns or not (part["series_id"] == series_id).any():
            continue

        merged = part.merge(
            rank_map,
            on=["series_id", "period_start_date", "report_date"],
            how="left",
        )
        updated_mask = merged["_new_rank"].notna()
        merged.loc[updated_mask, "revision_rank"] = (
            merged.loc[updated_mask, "_new_rank"].astype(part["revision_rank"].dtype)
        )
        merged = merged.drop(columns=["_new_rank"])

        tmp = path.with_name(path.stem + ".tmp.parquet")
        merged.to_parquet(tmp, index=False)
        tmp.replace(path)


# ---------------------------------------------------------------------------
# Revision detection
# ---------------------------------------------------------------------------

def _detect_revisions(
    series_id: str,
    existing: pd.DataFrame,
    new_df: pd.DataFrame,
) -> int:
    """Detect new vintages that revise an already-seen observation period.

    Logs one INFO line per revision and returns the total count.  A "revision"
    is a row whose ``(period_start_date, report_date)`` key is not in *existing*
    but whose ``period_start_date`` IS — meaning the observation was already
    released and this is a subsequent estimate.

    Parameters
    ----------
    series_id:
        FRED series ID (used in log messages only).
    existing:
        Rows already in storage for this series (may be empty).
    new_df:
        Rows returned from the current fetch.
    """
    if existing.empty or new_df.empty:
        return 0

    existing_keys = set(
        zip(
            pd.to_datetime(existing["period_start_date"]).dt.date,
            pd.to_datetime(existing["report_date"]).dt.date,
        )
    )
    existing_periods = set(pd.to_datetime(existing["period_start_date"]).dt.date)

    count = 0
    for _, row in new_df.iterrows():
        period = pd.Timestamp(row["period_start_date"]).date()
        report = pd.Timestamp(row["report_date"]).date()
        if (period, report) not in existing_keys and period in existing_periods:
            logger.info(
                "[macro] Revision detected: %s period=%s new_value=%.4f (report_date=%s)",
                series_id,
                period,
                float(row["value"]),
                report,
            )
            count += 1

    return count


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def update_series(series_id: str, api_key: str, start: str, data_dir: Path) -> int:
    """
    Bootstrap or incrementally update a single FRED series with vintage data.

    On first run: pulls all vintages from *start* to today.
    On subsequent runs: pulls from (max report_date − lookback) to today.
    The lookback window is series-specific (see ``SERIES_LOOKBACK_DAYS``);
    quarterly GDP uses 400 days to catch annual benchmark revisions.

    Returns the number of new rows added.
    """
    existing = read_table("macro", data_dir, series_ids=[series_id])

    if existing.empty:
        realtime_start = start
        action = f"bootstrap from {start}"
    else:
        latest_report = pd.to_datetime(existing["report_date"]).dt.date.max()
        lookback_days = SERIES_LOOKBACK_DAYS.get(series_id, _DEFAULT_LOOKBACK_DAYS)
        lookback = latest_report - timedelta(days=lookback_days)
        realtime_start = str(lookback)
        action = f"incremental since {lookback} ({lookback_days}d window)"

    df = fetch_series_vintages(series_id, realtime_start=realtime_start, api_key=api_key)

    if df.empty:
        logger.info("%s  no data  (%s)", series_id, action)
        return 0

    n_revisions = _detect_revisions(series_id, existing, df)
    added = write_table(df, "macro", data_dir)
    if added > 0:
        _recompute_revision_ranks(series_id, data_dir)

    logger.info(
        "%s  +%d rows  %d revisions  (%s)",
        series_id, added, n_revisions, action,
    )
    return added


def run(
    series_ids: list[str] | None = None,
    start: str = DEFAULT_START,
    data_dir: Path = DATA_DIR,
) -> None:
    """
    Update all macro series (or a custom subset) with full vintage history.
    """
    targets = series_ids or DEFAULT_SERIES
    today = date.today()

    api_key = _load_api_key()

    logger.info("market_data macro  —  %s", today)
    logger.info("Series: %s", ", ".join(targets))

    total_added = 0
    for series_id in targets:
        try:
            added = update_series(series_id, api_key=api_key, start=start, data_dir=data_dir)
            total_added += added
        except Exception as exc:
            logger.error("%s  ERROR: %s", series_id, exc, exc_info=True)

    logger.info("macro done: %d new rows across %d series", total_added, len(targets))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Fetch/update macroeconomic series from FRED with full vintage history."
    )
    parser.add_argument(
        "--series",
        nargs="+",
        metavar="SERIES_ID",
        help="Override the default series list (e.g. --series DFF CPIAUCSL).",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        metavar="YYYY-MM-DD",
        help=f"Bootstrap start date (default: {DEFAULT_START}).",
    )
    parser.add_argument(
        "--data-dir",
        default=str(DATA_DIR),
        metavar="DIR",
        help="Root data directory (default: data/).",
    )
    args = parser.parse_args()

    run(series_ids=args.series, start=args.start, data_dir=Path(args.data_dir))


if __name__ == "__main__":
    main()

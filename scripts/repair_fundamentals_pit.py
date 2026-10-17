"""
repair_fundamentals_pit.py
--------------------------
One-shot repair: move every fundamentals row to its snapshot date.

Rows written by `fetch_fundamentals.py` and `migrate_fundamentals.py` before
this fix used the latest EDGAR 10-K/10-Q filing date as `report_date` (and
`period_start_date`/`period_end_date`) with `report_date_known=True`. The
values are a yfinance `.info` snapshot (market cap, P/E, ...) taken on a later
day, so point-in-time readers saw them months or years before they existed.

For each row the repair:
  1. Recovers the snapshot date:
     - rows migrated from the old per-ticker files: the `as_of` of the matching
       row in `fundamentals/<SYMBOL>.parquet.bak` (same symbol, market_cap and
       total_revenue, as_of not after the collection date; if several rows
       match, only the earliest-collected one);
     - every other row: the UTC date of `collected_at` (the collector runs in
       the early morning, so this is the local collection date too). It is
       never earlier than the true snapshot date, so the fallback is safe.
  2. Sets `report_date`, `period_start_date` and `period_end_date` to the
     snapshot date and `report_date_known=True`.
  3. Keeps the old `report_date` as `last_filing_date` when it was an EDGAR
     filing date (`report_date_known=True` in a table not yet repaired).

All other columns are left as they are. Rows move between year partitions, so
the whole table is rewritten rather than merged through `storage.write_table`.
Before writing it checks that `(symbol, period_start_date)` is unique and that
no `report_date` is before the snapshot date, and copies the old partitions to
`<data_dir>/fundamentals_pit_backup_<timestamp>/`. Safe to re-run: rows that
are already on their snapshot date are left alone. Do not run it while the
daily collector is writing.

Usage
-----
    python scripts/repair_fundamentals_pit.py                 # dry run (default): report only
    python scripts/repair_fundamentals_pit.py --apply         # back up, then rewrite the table
    python scripts/repair_fundamentals_pit.py --data-dir D:/market_data/data
"""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from market_data.config import cfg as _cfg
from market_data.schema import DEDUP_KEYS, SORT_KEYS

# Under "market_data" so setup_logging() handlers see it (__name__ is "__main__" when run as a script).
logger = logging.getLogger("market_data.repair_fundamentals_pit")

DATA_DIR = Path(_cfg.get("paths.data_dir", "data"))
TABLE = "fundamentals"
_BAK_SUFFIX = ".parquet.bak"
_BAK_COLUMNS = ["symbol", "as_of", "market_cap", "total_revenue"]
_DATE_COLUMNS = ["period_start_date", "period_end_date", "report_date"]


# ---------------------------------------------------------------------------
# Snapshot dates
# ---------------------------------------------------------------------------


def _to_date(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values).dt.date


def load_bak_snapshots(fund_dir: Path) -> pd.DataFrame:
    """Return symbol/as_of/market_cap/total_revenue from the old `.parquet.bak` files."""
    frames = []
    for path in sorted(fund_dir.glob(f"*{_BAK_SUFFIX}")):
        old = pd.read_parquet(path)
        old["symbol"] = path.name[: -len(_BAK_SUFFIX)]
        frames.append(old.reindex(columns=_BAK_COLUMNS))
    if not frames:
        return pd.DataFrame(columns=_BAK_COLUMNS)
    bak = pd.concat(frames, ignore_index=True)
    bak["as_of"] = _to_date(bak["as_of"])
    return bak


def _same(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a == b) | (a.isna() & b.isna())


def snapshot_dates(df: pd.DataFrame, bak: pd.DataFrame | None) -> tuple[pd.Series, pd.Series]:
    """Return (snapshot date per row, mask of rows dated from a .bak file)."""
    collected = pd.to_datetime(df["collected_at"], utc=True).dt.date
    snapshot = collected.copy()
    from_bak = pd.Series(False, index=df.index)
    if bak is None or bak.empty:
        return snapshot, from_bak

    left = df[["symbol", "market_cap", "total_revenue", "collected_at"]].assign(
        _row=df.index, _collected=collected
    )
    pairs = left.merge(bak, on="symbol", suffixes=("", "_bak"))
    match = (
        _same(pairs["market_cap"], pairs["market_cap_bak"])
        & _same(pairs["total_revenue"], pairs["total_revenue_bak"])
        & (pairs["as_of"] <= pairs["_collected"])
    )
    # A stale quote can repeat the .bak values in later snapshots: only the
    # earliest-collected matching row per .bak row is the migrated one.
    first = (
        pairs[match]
        .sort_values("collected_at")
        .drop_duplicates(subset=["symbol", "as_of"], keep="first")
    )
    as_of = first.groupby("_row")["as_of"].min()
    snapshot.loc[as_of.index] = as_of
    from_bak.loc[as_of.index] = True
    return snapshot, from_bak


# ---------------------------------------------------------------------------
# Transform and checks
# ---------------------------------------------------------------------------


def repair_frame(df: pd.DataFrame, bak: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return a copy of *df* with every row dated at its snapshot date."""
    out = df.copy()
    snapshot, _ = snapshot_dates(out, bak)
    old_report = _to_date(out["report_date"])
    known = out["report_date_known"].astype(bool)

    if "last_filing_date" not in out.columns:
        # Nothing repaired yet: every known=True report_date is a filing date.
        out["last_filing_date"] = None
        was_filing_date = known
    else:
        # Rows already on their snapshot date (repaired, or written by the fixed
        # fetcher) keep their last_filing_date.
        was_filing_date = known & (old_report != snapshot) & out["last_filing_date"].isna()
    out["last_filing_date"] = out["last_filing_date"].astype(object)

    out.loc[was_filing_date, "last_filing_date"] = old_report[was_filing_date]
    out["last_filing_date"] = out["last_filing_date"].where(out["last_filing_date"].notna(), None)

    for col in _DATE_COLUMNS:
        out[col] = snapshot
    out["report_date_known"] = True
    return out


def check_invariants(df: pd.DataFrame, bak: pd.DataFrame | None = None) -> None:
    """Raise ValueError if *df* has duplicate keys or a row dated before its snapshot."""
    keys = DEDUP_KEYS[TABLE]
    dupes = df[df.duplicated(subset=keys, keep=False)]
    if not dupes.empty:
        raise ValueError(
            f"{len(dupes)} rows share a duplicate {keys} key after repair, e.g.\n"
            f"{dupes[keys + ['collected_at']].head(10)}"
        )

    snapshot, _ = snapshot_dates(df, bak)
    report = _to_date(df["report_date"])
    early = df[report < snapshot]
    if not early.empty:
        raise ValueError(
            f"{len(early)} rows have a report_date before their snapshot date, e.g.\n"
            f"{early[['symbol', 'report_date', 'collected_at']].head(10)}"
        )
    for col in ("period_start_date", "period_end_date"):
        if (_to_date(df[col]) != report).any():
            raise ValueError(f"{col} differs from report_date")


# ---------------------------------------------------------------------------
# Whole-table rewrite
# ---------------------------------------------------------------------------


def _partition_dirs(table_dir: Path) -> list[Path]:
    if not table_dir.exists():
        return []
    return sorted(
        p for p in table_dir.iterdir()
        if p.is_dir() and p.name.startswith("year=") and (p / "data.parquet").exists()
    )


def _write_partitions(df: pd.DataFrame, table_dir: Path, old_dirs: list[Path]) -> list[str]:
    years = pd.to_datetime(df["period_start_date"]).dt.year
    written: list[str] = []
    for year, group in df.groupby(years):
        part_dir = table_dir / f"year={int(year)}"
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / "data.parquet"
        tmp = path.with_name(path.stem + ".tmp.parquet")
        group.sort_values(SORT_KEYS[TABLE]).reset_index(drop=True).to_parquet(tmp, index=False)
        tmp.replace(path)
        written.append(part_dir.name)
    for old in old_dirs:
        if old.name not in written:
            shutil.rmtree(old)
            logger.info("removed emptied partition %s", old.name)
    return written


def run(*, data_dir: Path = DATA_DIR, apply: bool = False) -> dict:
    """Repair the fundamentals table under *data_dir*; write only when *apply* is True.

    Returns a summary dict with row counts.
    """
    table_dir = data_dir / TABLE
    old_dirs = _partition_dirs(table_dir)
    summary = {"rows": 0, "rows_moved": 0, "from_bak": 0, "filing_dates_kept": 0, "known_flipped": 0}
    if not old_dirs:
        logger.info("No fundamentals partitions under %s, nothing to do", table_dir)
        return summary

    df = pd.concat([pd.read_parquet(d / "data.parquet") for d in old_dirs], ignore_index=True)
    bak = load_bak_snapshots(table_dir)
    repaired = repair_frame(df, bak)
    check_invariants(repaired, bak)

    _, from_bak = snapshot_dates(df, bak)
    moved = _to_date(df["report_date"]) != _to_date(repaired["report_date"])
    old_filing = df["last_filing_date"] if "last_filing_date" in df.columns else pd.Series(None, index=df.index)
    summary.update({
        "rows": len(df),
        "rows_moved": int(moved.sum()),
        "from_bak": int(from_bak.sum()),
        "filing_dates_kept": int((old_filing.isna() & repaired["last_filing_date"].notna()).sum()),
        "known_flipped": int((~df["report_date_known"].astype(bool)).sum()),
    })

    mode = "" if apply else "DRY RUN - "
    logger.info(
        "%srows=%d  moved=%d  dated_from_bak=%d  filing_dates_kept=%d  known_flipped=%d  (.bak files: %d)",
        mode, summary["rows"], summary["rows_moved"], summary["from_bak"],
        summary["filing_dates_kept"], summary["known_flipped"], len(bak),
    )
    old_years = pd.to_datetime(df["report_date"]).dt.year.value_counts().sort_index()
    new_years = pd.to_datetime(repaired["report_date"]).dt.year.value_counts().sort_index()
    logger.info("%srows per year before: %s  after: %s", mode, old_years.to_dict(), new_years.to_dict())

    changed = summary["rows_moved"] or summary["filing_dates_kept"] or summary["known_flipped"]
    if not apply or not changed:
        if apply:
            logger.info("Already repaired, nothing written")
        return summary

    backup = data_dir / f"{TABLE}_pit_backup_{datetime.now():%Y%m%d_%H%M%S_%f}"
    for d in old_dirs:
        shutil.copytree(d, backup / d.name)
    logger.info("backed up %d partition(s) to %s", len(old_dirs), backup)

    written = _write_partitions(repaired, table_dir, old_dirs)
    logger.info("wrote partitions %s", written)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    parser = argparse.ArgumentParser(
        description=(
            "Move every fundamentals row to its snapshot date (report_date was the "
            "EDGAR filing date, which leaked later .info values into the past). "
            "Dry run unless --apply is given."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Back up the old partitions, then rewrite the table (default: dry run).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help=f"Root data directory (default: {DATA_DIR}).",
    )
    args = parser.parse_args()

    run(data_dir=args.data_dir, apply=args.apply)


if __name__ == "__main__":
    main()

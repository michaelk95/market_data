"""Unified read/write utilities for the bitemporal market data store.

Storage layout
--------------
Tables with high row counts (ohlcv, fundamentals, options) are partitioned
by calendar year using Hive-style directory naming:

    data/<table>/year=2024/data.parquet
    data/<table>/year=2023/data.parquet
    ...

Small, reference-sized tables (indices, macro) are stored as a single file:

    data/<table>/data.parquet

All writes are atomic (write to .tmp, then rename) and idempotent
(deduplication on each table's DEDUP_KEYS before every save).

Public API
----------
write_table(df, table_name, data_dir) -> int
    Merge df into the table; return number of net new rows written.

read_table(table_name, data_dir, *, start_date, end_date,
           symbols, series_ids, columns) -> pd.DataFrame
    Read rows from the table, with optional pushdown-style filters.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

import pandas as pd

from .schema import (
    DEDUP_KEYS,
    PARTITION_COLS,
    SORT_KEYS,
    TABLE_SCHEMAS,
    validate_bitemporal_columns,
)

logger = logging.getLogger(__name__)

__all__ = ["write_table", "read_table", "read_macro_as_of", "read_macro_revisions"]

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def write_table(
    df: pd.DataFrame,
    table_name: str,
    data_dir: Path,
) -> int:
    """Merge *df* into the unified table and return the number of net new rows.

    Parameters
    ----------
    df:
        DataFrame conforming to the bitemporal schema for *table_name*.  Must
        contain all columns listed in ``schema.BITEMPORAL_COLUMNS``.
    table_name:
        One of ``"ohlcv"``, ``"indices"``, ``"fundamentals"``, ``"macro"``,
        ``"options"``.
    data_dir:
        Root data directory (e.g. ``Path("data")``).

    Returns
    -------
    int
        Number of rows that were genuinely new (i.e. not already present).
    """
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(
            f"Unknown table '{table_name}'. Valid names: {sorted(TABLE_SCHEMAS)}"
        )
    if df.empty:
        return 0

    validate_bitemporal_columns(df)

    dedup_keys = DEDUP_KEYS[table_name]
    sort_keys = SORT_KEYS[table_name]
    partition_cols = PARTITION_COLS[table_name]
    table_dir = data_dir / table_name

    df = df.copy()
    total_new = 0

    if not partition_cols:
        # Non-partitioned table: single data.parquet
        path = table_dir / "data.parquet"
        total_new = _merge_write(path, df, dedup_keys, sort_keys)
    else:
        # Partitioned by year extracted from period_start_date
        df["_year"] = pd.to_datetime(df["period_start_date"]).dt.year
        for year, group in df.groupby("_year"):
            part_path = table_dir / f"year={int(year)}" / "data.parquet"
            rows_to_write = group.drop(columns=["_year"])
            total_new += _merge_write(part_path, rows_to_write, dedup_keys, sort_keys)

    return total_new


def read_table(
    table_name: str,
    data_dir: Path,
    *,
    start_date: datetime.date | None = None,
    end_date: datetime.date | None = None,
    symbols: list[str] | None = None,
    series_ids: list[str] | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Read rows from *table_name*, optionally filtered.

    Parameters
    ----------
    table_name:
        One of ``"ohlcv"``, ``"indices"``, ``"fundamentals"``, ``"macro"``,
        ``"options"``.
    data_dir:
        Root data directory.
    start_date:
        Inclusive lower bound on ``period_start_date``.
    end_date:
        Inclusive upper bound on ``period_start_date``.
    symbols:
        If provided, return only rows whose ``symbol`` is in this list.
        Applies to ``ohlcv``, ``indices``, ``fundamentals``, ``options``.
    series_ids:
        If provided, return only rows whose ``series_id`` is in this list.
        Applies to ``macro``.
    columns:
        If provided, return only these columns (passed to ``read_parquet``).

    Returns
    -------
    pd.DataFrame
        Empty DataFrame if the table does not exist yet.
    """
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(
            f"Unknown table '{table_name}'. Valid names: {sorted(TABLE_SCHEMAS)}"
        )

    table_dir = data_dir / table_name
    if not table_dir.exists():
        return _empty_df(table_name, columns)

    partition_cols = PARTITION_COLS[table_name]

    if not partition_cols:
        path = table_dir / "data.parquet"
        if not path.exists():
            return _empty_df(table_name, columns)
        df = pd.read_parquet(path, columns=columns)
    else:
        files = _get_partition_files(table_dir, start_date, end_date)
        if not files:
            # Warn if flat (old-format) files exist but no year=* partitions — a
            # likely sign that market-data-migrate-fundamentals hasn't been run.
            flat_files = [p for p in table_dir.iterdir() if p.suffix == ".parquet" and p.is_file()]
            if flat_files:
                logger.warning(
                    "read_table('%s'): directory %s contains %d flat .parquet file(s) "
                    "but no year=YYYY partitions. Run the migration script to convert "
                    "old-format files to the bitemporal layout.",
                    table_name, table_dir, len(flat_files),
                )
            return _empty_df(table_name, columns)
        df = pd.concat(
            [pd.read_parquet(f, columns=columns) for f in files],
            ignore_index=True,
        )

    # --- in-memory row filters ---
    df = _apply_filters(
        df,
        start_date=start_date,
        end_date=end_date,
        symbols=symbols,
        series_ids=series_ids,
    )

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Macro-specific query helpers
# ---------------------------------------------------------------------------

_FAR_FUTURE = datetime.date(9999, 12, 31)


def read_macro_as_of(
    series_ids: list[str],
    as_of_date: datetime.date,
    data_dir: Path,
) -> pd.DataFrame:
    """Return the vintage of each (series_id, period) that was current on *as_of_date*.

    A vintage is "current" on a given date when:

    - ``report_date <= as_of_date``  — it had been released by then
    - ``valid_to_date > as_of_date`` — it had not yet been superseded
      OR ``valid_to_date == 9999-12-31`` — it is the currently-active value

    This is the primary point-in-time query for the backtest engine: given any
    historical date, reconstruct the information set available at that moment,
    with no look-ahead bias from subsequent data revisions.

    Parameters
    ----------
    series_ids:
        FRED series IDs to query (e.g. ``["GDPC1", "UNRATE"]``).
    as_of_date:
        The historical date to query as of.
    data_dir:
        Root data directory.

    Returns
    -------
    pd.DataFrame
        Rows from the macro table matching the point-in-time filter.
        Empty DataFrame if no data is available.
    """
    df = read_table("macro", data_dir, series_ids=series_ids)
    if df.empty:
        return df

    # date32 parquet columns return as object dtype with Python date objects.
    # Avoid pd.to_datetime: it overflows on 9999-12-31 in pandas < 2.0.
    report_date = df["report_date"]
    valid_to_date = df["valid_to_date"]

    mask = (
        (report_date <= as_of_date)
        & ((valid_to_date > as_of_date) | (valid_to_date == _FAR_FUTURE))
    )
    return df[mask].reset_index(drop=True)


def read_macro_revisions(
    series_id: str,
    period_start_date: datetime.date,
    data_dir: Path,
) -> pd.DataFrame:
    """Return all vintages of a single macro observation, ordered by report_date.

    Adds three computed columns to help trace the revision chain:

    - ``revision_rank``     — 1-based ordinal (1 = first/advance estimate, …)
    - ``value_change``      — absolute change from the previous vintage (NaN for first)
    - ``value_change_pct``  — percent change from the previous vintage (NaN for first)

    Parameters
    ----------
    series_id:
        FRED series ID (e.g. ``"GDPC1"``).
    period_start_date:
        Start date of the observation period to inspect
        (e.g. ``datetime.date(2019, 10, 1)`` for GDP Q4 2019).
    data_dir:
        Root data directory.

    Returns
    -------
    pd.DataFrame
        All vintages for the given observation, sorted by ``report_date``,
        with ``revision_rank``, ``value_change``, ``value_change_pct`` appended.
        Empty DataFrame if no data is found.
    """
    df = read_table("macro", data_dir, series_ids=[series_id])
    if df.empty:
        return df

    obs_dates = pd.to_datetime(df["period_start_date"]).dt.date
    df = df[obs_dates == period_start_date].copy()
    if df.empty:
        return df

    df = df.sort_values("report_date").reset_index(drop=True)
    df["revision_rank"] = range(1, len(df) + 1)
    df["value_change"] = df["value"].diff()
    df["value_change_pct"] = df["value"].pct_change() * 100

    return df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _empty_df(table_name: str, columns: list[str] | None) -> pd.DataFrame:
    """Return an empty DataFrame whose columns match the table schema (or the
    requested *columns* projection).  This avoids KeyError when callers do
    ``df[["col1", "col2"]]`` on a no-data result."""
    schema_cols = TABLE_SCHEMAS[table_name].names
    cols = columns if columns is not None else schema_cols
    return pd.DataFrame(columns=cols)


def _merge_write(
    path: Path,
    new_df: pd.DataFrame,
    dedup_keys: list[str],
    sort_keys: list[str],
) -> int:
    """Merge *new_df* into the parquet at *path*, dedup, sort, and save atomically.

    Returns the number of net new rows (after - before).
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        existing = pd.read_parquet(path)
        before = len(existing)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        before = 0
        combined = new_df.copy()

    combined = (
        combined
        .drop_duplicates(subset=dedup_keys)
        .sort_values(sort_keys)
        .reset_index(drop=True)
    )
    after = len(combined)

    tmp = path.with_name(path.stem + ".tmp.parquet")
    combined.to_parquet(tmp, index=False)
    tmp.replace(path)

    return after - before


def _get_partition_files(
    table_dir: Path,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
) -> list[Path]:
    """Return the data.parquet paths for year partitions that overlap the range."""
    start_year = start_date.year if start_date is not None else None
    end_year = end_date.year if end_date is not None else None

    files: list[Path] = []
    for entry in sorted(table_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("year="):
            continue
        try:
            year = int(entry.name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue

        if start_year is not None and year < start_year:
            continue
        if end_year is not None and year > end_year:
            continue

        part_file = entry / "data.parquet"
        if part_file.exists():
            files.append(part_file)

    return files


def _apply_filters(
    df: pd.DataFrame,
    *,
    start_date: datetime.date | None,
    end_date: datetime.date | None,
    symbols: list[str] | None,
    series_ids: list[str] | None,
) -> pd.DataFrame:
    """Apply in-memory row filters to *df* and return the result."""
    if df.empty:
        return df

    if start_date is not None and "period_start_date" in df.columns:
        df = df[
            pd.to_datetime(df["period_start_date"]).dt.date >= start_date
        ]
    if end_date is not None and "period_start_date" in df.columns:
        df = df[
            pd.to_datetime(df["period_start_date"]).dt.date <= end_date
        ]
    if symbols is not None and "symbol" in df.columns:
        df = df[df["symbol"].isin(symbols)]
    if series_ids is not None and "series_id" in df.columns:
        df = df[df["series_id"].isin(series_ids)]

    return df

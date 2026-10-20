"""
flat_tables.py
--------------
Conversion between the flat frames the collectors build and the bitemporal
schema of the ``ohlcv``, ``indices`` and ``options`` tables.

The collectors (``fetch.py``, ``fetch_indices.py``, ``fetch_options.py``) work
with flat frames keyed on ``date`` (or ``snapshot_date`` for options).  The
same conversion is used by the live writers and by
``scripts/migrate_flat_tables.py``, so rows migrated from the old flat files
and rows written live have identical keys and dtypes and deduplicate cleanly.

Column mapping
--------------
- ``date`` / ``snapshot_date`` becomes ``period_start_date``,
  ``period_end_date`` and ``report_date``: a daily bar or a chain snapshot is
  known on the day it describes.
- ``report_time_marker`` is ``post-market`` for daily bars and ``pre-market``
  for options chains (fetched before the open).
- ``source`` is ``yfinance``; ``collected_at`` is supplied by the caller.
- Values are cast to ``schema.TABLE_SCHEMAS`` (e.g. ``volume`` int64 → float64).
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa

from market_data.schema import TABLE_SCHEMAS, DataSource, ReportTimeMarker

OHLCV_FLAT_COLS = ["date", "symbol", "open", "high", "low", "close", "volume"]
OPTIONS_FLAT_COLS = [
    "snapshot_date", "symbol", "expiry", "strike", "option_type",
    "last_price", "bid", "ask", "volume", "open_interest", "implied_vol",
    "in_the_money",
]

# Column order of each table's flat frame.
FLAT_COLUMNS: dict[str, list[str]] = {
    "ohlcv": OHLCV_FLAT_COLS,
    "indices": OHLCV_FLAT_COLS,
    "options": OPTIONS_FLAT_COLS,
}

# Date column in the flat layout, per table.
FLAT_DATE_COLUMN: dict[str, str] = {
    "ohlcv": "date",
    "indices": "date",
    "options": "snapshot_date",
}

REPORT_TIME_MARKER: dict[str, ReportTimeMarker] = {
    "ohlcv": ReportTimeMarker.POST_MARKET,
    "indices": ReportTimeMarker.POST_MARKET,
    "options": ReportTimeMarker.PRE_MARKET,
}


def _check_table(table: str) -> None:
    if table not in FLAT_DATE_COLUMN:
        raise ValueError(
            f"Unknown flat table '{table}'. Valid: {sorted(FLAT_DATE_COLUMN)}"
        )


def to_bitemporal(flat: pd.DataFrame, table: str, collected_at: pd.Timestamp) -> pd.DataFrame:
    """Add the bitemporal columns to *flat* (no casting; see ``conform_to_schema``)."""
    _check_table(table)
    date_col = FLAT_DATE_COLUMN[table]
    if date_col not in flat.columns:
        raise ValueError(f"{table}: flat rows are missing columns ['{date_col}']")
    df = flat.copy()
    day = pd.to_datetime(df.pop(date_col)).dt.date
    df["period_start_date"] = day
    df["period_end_date"] = day
    df["report_date"] = day
    df["report_time_marker"] = REPORT_TIME_MARKER[table].value
    df["source"] = DataSource.YFINANCE.value
    df["collected_at"] = collected_at
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"]).dt.date
    return df


def conform_to_schema(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Cast *df* to ``TABLE_SCHEMAS[table]``: column order, types and units."""
    schema = TABLE_SCHEMAS[table]
    missing = [name for name in schema.names if name not in df.columns]
    if missing:
        raise ValueError(f"{table}: flat rows are missing columns {missing}")
    arrow = pa.Table.from_pandas(df[schema.names], schema=schema, preserve_index=False)
    return arrow.to_pandas(date_as_object=True)


def from_flat(flat: pd.DataFrame, table: str, collected_at: pd.Timestamp) -> pd.DataFrame:
    """Convert a flat collector frame to *table*'s bitemporal schema."""
    return conform_to_schema(to_bitemporal(flat, table, collected_at), table)


def to_flat(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Project bitemporal rows of *table* back to the flat collector layout."""
    _check_table(table)
    flat = df.rename(columns={"period_start_date": FLAT_DATE_COLUMN[table]})
    return flat[FLAT_COLUMNS[table]].reset_index(drop=True)

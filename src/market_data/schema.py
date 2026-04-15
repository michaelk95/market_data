"""Bitemporal schema definitions for all market data types.

Every observation carries six temporal/provenance fields:

  period_start_date  – start of the business period this row covers
  period_end_date    – end of the business period (= period_start_date for
                       daily data; end-of-quarter for quarterly earnings)
  report_date        – date the data was publicly available / reported
  report_time_marker – when in the trading day the data became available
  source             – originating data provider
  collected_at       – UTC timestamp when our pipeline collected this row

These fields enable point-in-time (look-ahead-bias-free) queries: given any
as-of date, filter to rows where report_date <= as_of to see exactly what was
known at that moment.
"""

from __future__ import annotations

from enum import Enum

import pyarrow as pa


class ReportTimeMarker(str, Enum):
    """When in the trading day a data point was first available."""
    PRE_MARKET = "pre-market"
    DURING_HOURS = "during-hours"
    POST_MARKET = "post-market"


class DataSource(str, Enum):
    """Originating data provider."""
    YFINANCE = "yfinance"
    FRED = "fred"
    ISHARES = "ishares"


# ---------------------------------------------------------------------------
# Bitemporal column definitions
# ---------------------------------------------------------------------------

BITEMPORAL_COLUMNS: list[str] = [
    "period_start_date",
    "period_end_date",
    "report_date",
    "report_time_marker",
    "source",
    "collected_at",
]

_BITEMPORAL_FIELDS: list[pa.Field] = [
    pa.field("period_start_date", pa.date32()),
    pa.field("period_end_date", pa.date32()),
    pa.field("report_date", pa.date32()),
    pa.field("report_time_marker", pa.string()),
    pa.field("source", pa.string()),
    pa.field("collected_at", pa.timestamp("us", tz="UTC")),
]

# ---------------------------------------------------------------------------
# Per-table PyArrow schemas (data columns first, bitemporal fields appended)
# ---------------------------------------------------------------------------

OHLCV_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.float64()),
    *_BITEMPORAL_FIELDS,
])

INDICES_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("open", pa.float64()),
    pa.field("high", pa.float64()),
    pa.field("low", pa.float64()),
    pa.field("close", pa.float64()),
    pa.field("volume", pa.float64()),
    *_BITEMPORAL_FIELDS,
])

FUNDAMENTALS_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("market_cap", pa.float64()),
    pa.field("enterprise_value", pa.float64()),
    pa.field("trailing_pe", pa.float64()),
    pa.field("forward_pe", pa.float64()),
    pa.field("price_to_book", pa.float64()),
    pa.field("trailing_eps", pa.float64()),
    pa.field("forward_eps", pa.float64()),
    pa.field("total_revenue", pa.float64()),
    pa.field("profit_margin", pa.float64()),
    pa.field("analyst_target_mean", pa.float64()),
    pa.field("analyst_target_low", pa.float64()),
    pa.field("analyst_target_high", pa.float64()),
    pa.field("analyst_recommendation", pa.string()),
    pa.field("analyst_count", pa.int64()),
    *_BITEMPORAL_FIELDS,
])

MACRO_SCHEMA = pa.schema([
    pa.field("series_id", pa.string()),
    pa.field("value", pa.float64()),
    *_BITEMPORAL_FIELDS,
])

OPTIONS_SCHEMA = pa.schema([
    pa.field("symbol", pa.string()),
    pa.field("expiry", pa.date32()),
    pa.field("strike", pa.float64()),
    pa.field("option_type", pa.string()),
    pa.field("last_price", pa.float64()),
    pa.field("bid", pa.float64()),
    pa.field("ask", pa.float64()),
    pa.field("volume", pa.float64()),
    pa.field("open_interest", pa.float64()),
    pa.field("implied_vol", pa.float64()),
    pa.field("in_the_money", pa.bool_()),
    *_BITEMPORAL_FIELDS,
])

# ---------------------------------------------------------------------------
# Lookup tables keyed by table name
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, pa.Schema] = {
    "ohlcv": OHLCV_SCHEMA,
    "indices": INDICES_SCHEMA,
    "fundamentals": FUNDAMENTALS_SCHEMA,
    "macro": MACRO_SCHEMA,
    "options": OPTIONS_SCHEMA,
}

# Columns that uniquely identify one observation (used for deduplication)
DEDUP_KEYS: dict[str, list[str]] = {
    "ohlcv": ["symbol", "period_start_date"],
    "indices": ["symbol", "period_start_date"],
    "fundamentals": ["symbol", "period_start_date"],
    "macro": ["series_id", "period_start_date"],
    "options": ["symbol", "period_start_date", "expiry", "strike", "option_type"],
}

# Columns used to order rows within each stored file
SORT_KEYS: dict[str, list[str]] = {
    "ohlcv": ["period_start_date", "symbol"],
    "indices": ["period_start_date", "symbol"],
    "fundamentals": ["period_start_date", "symbol"],
    "macro": ["period_start_date", "series_id"],
    "options": ["period_start_date", "symbol", "expiry", "strike", "option_type"],
}

# Tables partitioned by year (Hive-style: year=YYYY/data.parquet).
# Tables not listed here are stored as a single data.parquet file.
PARTITION_COLS: dict[str, list[str]] = {
    "ohlcv": ["year"],
    "fundamentals": ["year"],
    "options": ["year"],
    "indices": [],
    "macro": [],
}

# ---------------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------------


def validate_bitemporal_columns(df: "pd.DataFrame") -> None:  # noqa: F821
    """Raise ValueError if any required bitemporal column is absent from df."""
    import pandas as pd  # local import to keep schema.py import-light

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame, got {type(df)}")
    missing = [c for c in BITEMPORAL_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"DataFrame is missing required bitemporal columns: {missing}"
        )

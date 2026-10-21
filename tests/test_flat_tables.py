"""Tests for flat_tables.py: conversion between the flat collector frames and
the bitemporal table schema, shared by the live writers and the migration."""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from market_data.flat_tables import FLAT_COLUMNS, drop_null_keys, from_flat, to_flat
from market_data.schema import TABLE_SCHEMAS

_AT = pd.Timestamp("2026-10-19 21:00:00", tz="UTC")


class TestFromFlat:
    def test_ohlcv_columns_match_schema(self, ohlcv_df):
        df = from_flat(ohlcv_df, "ohlcv", _AT)
        assert list(df.columns) == TABLE_SCHEMAS["ohlcv"].names

    def test_ohlcv_date_becomes_period_and_report_date(self, ohlcv_df):
        df = from_flat(ohlcv_df, "ohlcv", _AT)
        assert list(df["period_start_date"]) == list(ohlcv_df["date"])
        assert list(df["period_end_date"]) == list(ohlcv_df["date"])
        assert list(df["report_date"]) == list(ohlcv_df["date"])
        assert (df["report_time_marker"] == "post-market").all()
        assert (df["source"] == "yfinance").all()
        assert (df["collected_at"] == _AT).all()

    def test_dates_are_date_objects(self, ohlcv_df):
        """Live rows must match migrated rows in dtype, or dedup misses them."""
        flat = ohlcv_df.assign(date=pd.to_datetime(ohlcv_df["date"]))
        df = from_flat(flat, "ohlcv", _AT)
        assert all(type(d) is datetime.date for d in df["period_start_date"])

    def test_int_volume_cast_to_float(self, ohlcv_df):
        flat = ohlcv_df.assign(volume=ohlcv_df["volume"].astype("int64"))
        df = from_flat(flat, "ohlcv", _AT)
        assert df["volume"].dtype == "float64"

    def test_options_snapshot_date_is_pre_market(self, options_df):
        df = from_flat(options_df, "options", _AT)
        assert list(df.columns) == TABLE_SCHEMAS["options"].names
        assert df["period_start_date"].iloc[0] == datetime.date(2024, 1, 2)
        assert df["report_time_marker"].iloc[0] == "pre-market"
        assert df["expiry"].iloc[0] == datetime.date(2024, 2, 16)

    def test_missing_column_raises(self, ohlcv_df):
        with pytest.raises(ValueError, match="missing columns"):
            from_flat(ohlcv_df.drop(columns=["close"]), "ohlcv", _AT)

    def test_unknown_table_raises(self, ohlcv_df):
        with pytest.raises(ValueError, match="Unknown"):
            from_flat(ohlcv_df, "macro", _AT)


class TestToFlat:
    @pytest.mark.parametrize("table", ["ohlcv", "indices"])
    def test_round_trip_ohlcv_like(self, ohlcv_df, table):
        back = to_flat(from_flat(ohlcv_df, table, _AT), table)
        assert list(back.columns) == FLAT_COLUMNS[table]
        pd.testing.assert_frame_equal(back, ohlcv_df)

    def test_round_trip_options(self, options_df):
        back = to_flat(from_flat(options_df, "options", _AT), "options")
        assert list(back.columns) == FLAT_COLUMNS["options"]
        pd.testing.assert_frame_equal(back, options_df)

    def test_empty_frame_keeps_flat_columns(self):
        empty = pd.DataFrame(columns=TABLE_SCHEMAS["ohlcv"].names)
        back = to_flat(empty, "ohlcv")
        assert back.empty
        assert list(back.columns) == FLAT_COLUMNS["ohlcv"]


class TestDropNullKeys:
    """Rows with a null dedup key are dropped before writing (issue #74, ADR 0001)."""

    @pytest.mark.parametrize("column", ["snapshot_date", "symbol", "expiry", "strike", "option_type"])
    def test_options_null_key_row_dropped(self, options_df, column):
        bad = options_df.assign(strike=191.0).assign(**{column: None})
        out = drop_null_keys(pd.concat([options_df, bad], ignore_index=True), "options")
        assert len(out) == 1
        assert out["strike"].iloc[0] == 190.0

    def test_ohlcv_null_date_dropped(self, ohlcv_df):
        flat = ohlcv_df.astype({"date": object})
        flat.loc[flat.index[0], "date"] = None
        out = drop_null_keys(flat, "ohlcv")
        assert len(out) == len(ohlcv_df) - 1

    def test_null_value_columns_kept(self, options_df):
        out = drop_null_keys(options_df.assign(bid=None, in_the_money=None), "options")
        assert len(out) == 1

    def test_logs_count_per_symbol(self, options_df, caplog):
        bad = options_df.assign(expiry=None, strike=[191.0])
        no_symbol = options_df.assign(symbol=None, strike=[192.0])
        with caplog.at_level("WARNING", logger="market_data.flat_tables"):
            drop_null_keys(pd.concat([options_df, bad, no_symbol], ignore_index=True), "options")
        assert "options: dropped 2 row(s) with a null key" in caplog.text
        assert "AAPL=1" in caplog.text
        assert "<no symbol>=1" in caplog.text

    def test_clean_frame_logs_nothing(self, options_df, caplog):
        with caplog.at_level("WARNING", logger="market_data.flat_tables"):
            out = drop_null_keys(options_df, "options")
        assert len(out) == 1
        assert caplog.text == ""

    def test_from_flat_drops_null_keys(self, options_df):
        bad = options_df.assign(symbol=None, strike=[191.0])
        out = from_flat(pd.concat([options_df, bad], ignore_index=True), "options", _AT)
        assert out["symbol"].tolist() == ["AAPL"]

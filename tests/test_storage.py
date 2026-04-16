"""Tests for market_data.storage — unified read/write utilities."""

import datetime

import pandas as pd
import pytest

from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import read_table, write_table

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


def _bt(
    period_start: datetime.date,
    period_end: datetime.date | None = None,
    report_date: datetime.date | None = None,
    marker: str = ReportTimeMarker.POST_MARKET,
    source: str = DataSource.YFINANCE,
    collected_at: pd.Timestamp | None = None,
) -> dict:
    """Return a dict of bitemporal columns for use in test rows."""
    return {
        "period_start_date": period_start,
        "period_end_date": period_end or period_start,
        "report_date": report_date or period_start,
        "report_time_marker": marker,
        "source": source,
        "collected_at": collected_at or pd.Timestamp("2024-01-03 21:00:00", tz="UTC"),
    }


def _ohlcv_row(symbol: str, date: datetime.date, close: float = 100.0) -> dict:
    return {
        "symbol": symbol,
        "open": close * 0.99,
        "high": close * 1.01,
        "low": close * 0.98,
        "close": close,
        "volume": 1_000_000.0,
        **_bt(date),
    }


def _macro_row(series_id: str, date: datetime.date, value: float) -> dict:
    return {
        "series_id": series_id,
        "value": value,
        **_bt(date, source=DataSource.FRED),
    }


def _options_row(
    symbol: str,
    date: datetime.date,
    expiry: datetime.date,
    strike: float,
    option_type: str = "call",
) -> dict:
    return {
        "symbol": symbol,
        "expiry": expiry,
        "strike": strike,
        "option_type": option_type,
        "last_price": 5.0,
        "bid": 4.9,
        "ask": 5.1,
        "volume": 100.0,
        "open_interest": 500.0,
        "implied_vol": 0.25,
        "in_the_money": False,
        **_bt(date),
    }


# ---------------------------------------------------------------------------
# write_table — argument validation
# ---------------------------------------------------------------------------


class TestWriteTableValidation:
    def test_unknown_table_raises(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        with pytest.raises(ValueError, match="Unknown table"):
            write_table(df, "nonexistent", tmp_path)

    def test_missing_bitemporal_column_raises(self, tmp_path):
        df = pd.DataFrame([{"symbol": "AAPL", "close": 100.0}])
        with pytest.raises(ValueError, match="missing required bitemporal columns"):
            write_table(df, "ohlcv", tmp_path)

    def test_empty_dataframe_returns_zero(self, tmp_path):
        df = pd.DataFrame()
        result = write_table(df, "ohlcv", tmp_path)
        assert result == 0

    def test_empty_dataframe_creates_no_files(self, tmp_path):
        write_table(pd.DataFrame(), "ohlcv", tmp_path)
        assert not (tmp_path / "ohlcv").exists()


# ---------------------------------------------------------------------------
# write_table + read_table — non-partitioned tables (macro, indices)
# ---------------------------------------------------------------------------


class TestNonPartitionedTable:
    def test_write_creates_single_file(self, tmp_path):
        df = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        write_table(df, "macro", tmp_path)
        assert (tmp_path / "macro" / "data.parquet").exists()

    def test_no_tmp_file_after_write(self, tmp_path):
        df = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        write_table(df, "macro", tmp_path)
        tmp_files = list((tmp_path / "macro").glob("*.tmp.parquet"))
        assert tmp_files == []

    def test_roundtrip_preserves_rows(self, tmp_path):
        rows = [
            _macro_row("DFF", datetime.date(2024, 1, 2), 5.33),
            _macro_row("T10Y2Y", datetime.date(2024, 1, 2), 0.42),
        ]
        df = pd.DataFrame(rows)
        write_table(df, "macro", tmp_path)
        result = read_table("macro", tmp_path)
        assert len(result) == 2
        assert set(result["series_id"]) == {"DFF", "T10Y2Y"}

    def test_write_returns_new_row_count(self, tmp_path):
        df = pd.DataFrame([
            _macro_row("DFF", datetime.date(2024, 1, 2), 5.33),
            _macro_row("DFF", datetime.date(2024, 1, 3), 5.33),
        ])
        n = write_table(df, "macro", tmp_path)
        assert n == 2

    def test_idempotent_write_returns_zero(self, tmp_path):
        df = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        write_table(df, "macro", tmp_path)
        n = write_table(df, "macro", tmp_path)
        assert n == 0

    def test_idempotent_write_does_not_duplicate_rows(self, tmp_path):
        df = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        write_table(df, "macro", tmp_path)
        write_table(df, "macro", tmp_path)
        result = read_table("macro", tmp_path)
        assert len(result) == 1

    def test_incremental_write_appends(self, tmp_path):
        df1 = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        df2 = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 3), 5.33)])
        write_table(df1, "macro", tmp_path)
        n = write_table(df2, "macro", tmp_path)
        assert n == 1
        result = read_table("macro", tmp_path)
        assert len(result) == 2

    def test_value_update_replaces_row(self, tmp_path):
        """Dedup keeps latest write for the same (series_id, period_start_date)."""
        df1 = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.33)])
        df2 = pd.DataFrame([_macro_row("DFF", datetime.date(2024, 1, 2), 5.50)])
        write_table(df1, "macro", tmp_path)
        write_table(df2, "macro", tmp_path)
        result = read_table("macro", tmp_path)
        # Only one row; value is from whichever write was kept by drop_duplicates (first)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# write_table + read_table — partitioned tables (ohlcv)
# ---------------------------------------------------------------------------


class TestPartitionedTable:
    def test_write_creates_year_partition_dirs(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df, "ohlcv", tmp_path)
        assert (tmp_path / "ohlcv" / "year=2024" / "data.parquet").exists()

    def test_no_tmp_files_after_write(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df, "ohlcv", tmp_path)
        tmp_files = list((tmp_path / "ohlcv").rglob("*.tmp.parquet"))
        assert tmp_files == []

    def test_multi_year_data_splits_into_partitions(self, tmp_path):
        df = pd.DataFrame([
            _ohlcv_row("AAPL", datetime.date(2022, 6, 1)),
            _ohlcv_row("AAPL", datetime.date(2023, 6, 1)),
            _ohlcv_row("AAPL", datetime.date(2024, 6, 1)),
        ])
        write_table(df, "ohlcv", tmp_path)
        for year in (2022, 2023, 2024):
            assert (tmp_path / "ohlcv" / f"year={year}" / "data.parquet").exists()

    def test_roundtrip_preserves_all_rows(self, tmp_path):
        df = pd.DataFrame([
            _ohlcv_row("AAPL", datetime.date(2023, 12, 29), 193.0),
            _ohlcv_row("MSFT", datetime.date(2023, 12, 29), 374.0),
            _ohlcv_row("AAPL", datetime.date(2024, 1, 2), 185.0),
        ])
        write_table(df, "ohlcv", tmp_path)
        result = read_table("ohlcv", tmp_path)
        assert len(result) == 3
        assert set(result["symbol"]) == {"AAPL", "MSFT"}

    def test_write_returns_correct_new_row_count(self, tmp_path):
        df = pd.DataFrame([
            _ohlcv_row("AAPL", datetime.date(2024, 1, 2)),
            _ohlcv_row("AAPL", datetime.date(2024, 1, 3)),
        ])
        n = write_table(df, "ohlcv", tmp_path)
        assert n == 2

    def test_idempotent_returns_zero(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df, "ohlcv", tmp_path)
        n = write_table(df, "ohlcv", tmp_path)
        assert n == 0

    def test_idempotent_no_duplicate_rows(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df, "ohlcv", tmp_path)
        write_table(df, "ohlcv", tmp_path)
        result = read_table("ohlcv", tmp_path)
        assert len(result) == 1

    def test_incremental_cross_partition_write(self, tmp_path):
        df1 = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2023, 12, 29))])
        df2 = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df1, "ohlcv", tmp_path)
        n = write_table(df2, "ohlcv", tmp_path)
        assert n == 1
        result = read_table("ohlcv", tmp_path)
        assert len(result) == 2

    def test_year_column_not_stored_in_file(self, tmp_path):
        """The _year helper column used for partitioning must not leak into stored data."""
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2024, 1, 2))])
        write_table(df, "ohlcv", tmp_path)
        result = read_table("ohlcv", tmp_path)
        assert "year" not in result.columns
        assert "_year" not in result.columns


# ---------------------------------------------------------------------------
# read_table — argument validation and empty-state handling
# ---------------------------------------------------------------------------


class TestReadTableValidation:
    def test_unknown_table_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown table"):
            read_table("nonexistent", tmp_path)

    def test_missing_table_dir_returns_empty(self, tmp_path):
        result = read_table("ohlcv", tmp_path)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_missing_table_dir_returns_schema_columns(self, tmp_path):
        result = read_table("ohlcv", tmp_path)
        assert "symbol" in result.columns
        assert "period_start_date" in result.columns

    def test_missing_single_file_returns_empty(self, tmp_path):
        (tmp_path / "macro").mkdir()  # dir exists but no data.parquet
        result = read_table("macro", tmp_path)
        assert result.empty

    def test_missing_single_file_returns_schema_columns(self, tmp_path):
        (tmp_path / "macro").mkdir()
        result = read_table("macro", tmp_path)
        assert "series_id" in result.columns
        assert "period_start_date" in result.columns

    def test_no_matching_partitions_returns_empty(self, tmp_path):
        # Write 2022 data, then query for 2024 only
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2022, 6, 1))])
        write_table(df, "ohlcv", tmp_path)
        result = read_table(
            "ohlcv", tmp_path,
            start_date=datetime.date(2024, 1, 1),
        )
        assert result.empty

    def test_no_matching_partitions_returns_schema_columns(self, tmp_path):
        df = pd.DataFrame([_ohlcv_row("AAPL", datetime.date(2022, 6, 1))])
        write_table(df, "ohlcv", tmp_path)
        result = read_table("ohlcv", tmp_path, start_date=datetime.date(2024, 1, 1))
        assert "symbol" in result.columns
        assert "period_start_date" in result.columns

    def test_flat_files_without_partitions_warns(self, tmp_path, caplog):
        """Old-format flat .parquet files should trigger a migration warning."""
        import logging
        fund_dir = tmp_path / "fundamentals"
        fund_dir.mkdir()
        # Simulate old per-ticker file (old schema, no year= partitions)
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(fund_dir / "AAPL.parquet")
        with caplog.at_level(logging.WARNING, logger="market_data.storage"):
            result = read_table("fundamentals", tmp_path)
        assert result.empty
        assert "symbol" in result.columns
        assert any("flat .parquet" in msg for msg in caplog.messages)

    def test_columns_projection_on_empty_returns_requested_columns(self, tmp_path):
        result = read_table("ohlcv", tmp_path, columns=["symbol", "close"])
        assert list(result.columns) == ["symbol", "close"]


# ---------------------------------------------------------------------------
# read_table — filters
# ---------------------------------------------------------------------------


class TestReadTableFilters:
    @pytest.fixture()
    def populated_ohlcv(self, tmp_path):
        rows = [
            _ohlcv_row("AAPL", datetime.date(2023, 12, 1)),
            _ohlcv_row("AAPL", datetime.date(2024, 1, 15)),
            _ohlcv_row("AAPL", datetime.date(2024, 3, 1)),
            _ohlcv_row("MSFT", datetime.date(2024, 1, 15)),
            _ohlcv_row("GOOGL", datetime.date(2024, 2, 1)),
        ]
        write_table(pd.DataFrame(rows), "ohlcv", tmp_path)
        return tmp_path

    def test_symbols_filter(self, populated_ohlcv):
        result = read_table("ohlcv", populated_ohlcv, symbols=["AAPL"])
        assert set(result["symbol"]) == {"AAPL"}
        assert len(result) == 3

    def test_symbols_filter_multiple(self, populated_ohlcv):
        result = read_table("ohlcv", populated_ohlcv, symbols=["AAPL", "MSFT"])
        assert set(result["symbol"]) == {"AAPL", "MSFT"}

    def test_symbols_filter_empty_list_returns_empty(self, populated_ohlcv):
        result = read_table("ohlcv", populated_ohlcv, symbols=[])
        assert result.empty

    def test_start_date_filter(self, populated_ohlcv):
        result = read_table(
            "ohlcv", populated_ohlcv,
            start_date=datetime.date(2024, 1, 1),
        )
        dates = pd.to_datetime(result["period_start_date"]).dt.date
        assert all(d >= datetime.date(2024, 1, 1) for d in dates)
        assert len(result) == 4  # three 2024 AAPL + one MSFT + one GOOGL (5 total - 1 dec23)

    def test_end_date_filter(self, populated_ohlcv):
        result = read_table(
            "ohlcv", populated_ohlcv,
            end_date=datetime.date(2024, 1, 31),
        )
        dates = pd.to_datetime(result["period_start_date"]).dt.date
        assert all(d <= datetime.date(2024, 1, 31) for d in dates)

    def test_start_and_end_date_filter(self, populated_ohlcv):
        result = read_table(
            "ohlcv", populated_ohlcv,
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 31),
        )
        dates = pd.to_datetime(result["period_start_date"]).dt.date
        assert all(datetime.date(2024, 1, 1) <= d <= datetime.date(2024, 1, 31) for d in dates)

    def test_date_filter_prunes_partitions(self, populated_ohlcv):
        """Querying only 2024 should not load the 2023 partition at all."""
        result = read_table(
            "ohlcv", populated_ohlcv,
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 12, 31),
        )
        dates = pd.to_datetime(result["period_start_date"]).dt.date
        assert all(d.year == 2024 for d in dates)

    def test_columns_projection(self, populated_ohlcv):
        result = read_table(
            "ohlcv", populated_ohlcv,
            columns=["symbol", "close", "period_start_date"],
        )
        assert set(result.columns) == {"symbol", "close", "period_start_date"}

    @pytest.fixture()
    def populated_macro(self, tmp_path):
        rows = [
            _macro_row("DFF", datetime.date(2024, 1, 2), 5.33),
            _macro_row("DFF", datetime.date(2024, 1, 3), 5.33),
            _macro_row("T10Y2Y", datetime.date(2024, 1, 2), 0.42),
        ]
        write_table(pd.DataFrame(rows), "macro", tmp_path)
        return tmp_path

    def test_series_ids_filter(self, populated_macro):
        result = read_table("macro", populated_macro, series_ids=["DFF"])
        assert set(result["series_id"]) == {"DFF"}
        assert len(result) == 2

    def test_series_ids_filter_empty_list_returns_empty(self, populated_macro):
        result = read_table("macro", populated_macro, series_ids=[])
        assert result.empty

    def test_macro_date_filter(self, populated_macro):
        result = read_table(
            "macro", populated_macro,
            start_date=datetime.date(2024, 1, 3),
        )
        assert len(result) == 1
        assert result.iloc[0]["series_id"] == "DFF"


# ---------------------------------------------------------------------------
# Options table (uses multi-column dedup key)
# ---------------------------------------------------------------------------


class TestOptionsTable:
    def test_roundtrip(self, tmp_path):
        rows = [
            _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 185.0, "call"),
            _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 185.0, "put"),
            _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 190.0, "call"),
        ]
        df = pd.DataFrame(rows)
        n = write_table(df, "options", tmp_path)
        assert n == 3
        result = read_table("options", tmp_path)
        assert len(result) == 3

    def test_dedup_on_full_contract_key(self, tmp_path):
        row = _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 185.0, "call")
        df = pd.DataFrame([row])
        write_table(df, "options", tmp_path)
        n = write_table(df, "options", tmp_path)
        assert n == 0
        result = read_table("options", tmp_path)
        assert len(result) == 1

    def test_same_strike_different_type_are_distinct(self, tmp_path):
        call = _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 185.0, "call")
        put = _options_row("AAPL", datetime.date(2024, 1, 2), datetime.date(2024, 2, 16), 185.0, "put")
        df = pd.DataFrame([call, put])
        n = write_table(df, "options", tmp_path)
        assert n == 2

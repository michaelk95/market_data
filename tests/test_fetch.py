"""
Tests for fetch.py: _normalize(), save_ticker_data(), load_ticker_data().

No network calls are made; all yfinance interactions are tested via fixtures
that replicate the real DataFrame shapes yfinance returns.
"""

from datetime import date

import pandas as pd
import pytest

from market_data.fetch import (
    OHLCV_COLS,
    _normalize,
    load_ticker_data,
    save_ticker_data,
)


# ---------------------------------------------------------------------------
# _normalize()
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_standard_yfinance_output(self, raw_yfinance_df):
        """_normalize handles a TZ-aware DatetimeTZIndex and mixed-case columns."""
        result = _normalize(raw_yfinance_df, "AAPL")

        assert list(result.columns) == OHLCV_COLS
        assert len(result) == 2
        assert (result["symbol"] == "AAPL").all()
        assert result["close"].iloc[0] == pytest.approx(186.5)
        assert result["close"].iloc[1] == pytest.approx(187.0)

    def test_dates_are_plain_date_objects(self, raw_yfinance_df):
        """After normalization every value in the date column is a datetime.date."""
        result = _normalize(raw_yfinance_df, "AAPL")
        assert all(isinstance(d, date) for d in result["date"])

    def test_extra_columns_stripped(self, raw_yfinance_df):
        """Columns like 'Dividends' and 'Stock Splits' are dropped."""
        result = _normalize(raw_yfinance_df, "AAPL")
        assert set(result.columns) == set(OHLCV_COLS)

    def test_empty_returns_empty_with_schema(self):
        """_normalize of an empty DataFrame returns empty with correct columns."""
        result = _normalize(pd.DataFrame(), "AAPL")
        assert result.empty
        assert list(result.columns) == OHLCV_COLS

    def test_multiindex_columns_flattened(self):
        """MultiIndex columns from yf.download multi-ticker calls are flattened."""
        dates = pd.DatetimeIndex(["2024-01-02"], tz="UTC")
        cols = pd.MultiIndex.from_tuples(
            [
                ("Open", "SPY"),
                ("High", "SPY"),
                ("Low", "SPY"),
                ("Close", "SPY"),
                ("Volume", "SPY"),
            ]
        )
        df = pd.DataFrame(
            [[450.0, 452.0, 449.0, 451.0, 80_000_000.0]],
            index=dates,
            columns=cols,
        )
        result = _normalize(df, "SPY")
        assert list(result.columns) == OHLCV_COLS
        assert result["close"].iloc[0] == pytest.approx(451.0)

    def test_drops_rows_without_close(self):
        """Rows where Close is NaN are removed."""
        dates = pd.DatetimeIndex(["2024-01-02", "2024-01-03"])
        df = pd.DataFrame(
            {
                "Open": [100.0, 101.0],
                "High": [102.0, 103.0],
                "Low": [99.0, 100.0],
                "Close": [101.0, float("nan")],
                "Volume": [1_000_000.0, 1_100_000.0],
            },
            index=dates,
        )
        result = _normalize(df, "TEST")
        assert len(result) == 1
        assert result["close"].iloc[0] == pytest.approx(101.0)

    def test_tolerates_missing_volume_column(self):
        """_normalize works when Volume is absent — only available columns returned."""
        dates = pd.DatetimeIndex(["2024-01-02"])
        df = pd.DataFrame(
            {"Open": [100.0], "High": [102.0], "Low": [99.0], "Close": [101.0]},
            index=dates,
        )
        result = _normalize(df, "TEST")
        assert "close" in result.columns
        assert "volume" not in result.columns

    def test_strips_timezone_from_index(self):
        """Dates have no timezone info after normalization."""
        dates = pd.DatetimeIndex(["2024-01-02"], tz="US/Eastern")
        df = pd.DataFrame(
            {"Open": [100.0], "High": [102.0], "Low": [99.0], "Close": [101.0], "Volume": [1e6]},
            index=dates,
        )
        result = _normalize(df, "TEST")
        # date objects have no tzinfo attribute — just confirm they are plain dates
        assert isinstance(result["date"].iloc[0], date)

    def test_symbol_column_set_correctly(self, raw_yfinance_df):
        """The symbol column is populated with the provided symbol string."""
        result = _normalize(raw_yfinance_df, "XLF")
        assert (result["symbol"] == "XLF").all()

    def test_index_reset_after_normalize(self, raw_yfinance_df):
        """The resulting DataFrame has a clean 0-based integer index."""
        result = _normalize(raw_yfinance_df, "AAPL")
        assert list(result.index) == list(range(len(result)))


# ---------------------------------------------------------------------------
# save_ticker_data() + load_ticker_data()
# ---------------------------------------------------------------------------


class TestSaveTickerData:
    def test_creates_new_file(self, ohlcv_df, tmp_path):
        data_dir = tmp_path / "ohlcv"
        rows_added = save_ticker_data("AAPL", ohlcv_df, data_dir)

        assert rows_added == 2
        assert (data_dir / "AAPL.parquet").exists()

    def test_returns_zero_for_empty_df(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        empty = pd.DataFrame(columns=OHLCV_COLS)
        assert save_ticker_data("AAPL", empty, data_dir) == 0
        assert not (data_dir / "AAPL.parquet").exists()

    def test_idempotent_on_duplicate_rows(self, ohlcv_df, tmp_path):
        """Saving the same data twice results in zero additional rows."""
        data_dir = tmp_path / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)
        added = save_ticker_data("AAPL", ohlcv_df, data_dir)

        assert added == 0
        stored = load_ticker_data("AAPL", data_dir)
        assert len(stored) == 2

    def test_appends_new_rows(self, ohlcv_df, tmp_path):
        data_dir = tmp_path / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)

        new_row = pd.DataFrame(
            {
                "date": [date(2024, 1, 4)],
                "symbol": ["AAPL"],
                "open": [188.0],
                "high": [190.0],
                "low": [187.0],
                "close": [189.0],
                "volume": [55_000_000.0],
            }
        )
        added = save_ticker_data("AAPL", new_row, data_dir)

        assert added == 1
        stored = load_ticker_data("AAPL", data_dir)
        assert len(stored) == 3

    def test_deduplicates_overlap(self, ohlcv_df, tmp_path):
        """When new_df overlaps existing on (date, symbol), duplicates are dropped."""
        data_dir = tmp_path / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)

        # 2024-01-02 already stored; 2024-01-05 is genuinely new
        overlap = pd.DataFrame(
            {
                "date": [date(2024, 1, 2), date(2024, 1, 5)],
                "symbol": ["AAPL", "AAPL"],
                "open": [999.0, 190.0],
                "high": [999.0, 192.0],
                "low": [999.0, 189.0],
                "close": [999.0, 191.0],
                "volume": [1.0, 60_000_000.0],
            }
        )
        added = save_ticker_data("AAPL", overlap, data_dir)

        assert added == 1
        stored = load_ticker_data("AAPL", data_dir)
        assert len(stored) == 3

    def test_creates_parent_dir(self, ohlcv_df, tmp_path):
        """save_ticker_data creates missing parent directories automatically."""
        data_dir = tmp_path / "deep" / "nested" / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)
        assert (data_dir / "AAPL.parquet").exists()

    def test_sorted_by_date_ascending(self, tmp_path):
        """Stored data is always sorted by date ascending."""
        data_dir = tmp_path / "ohlcv"
        df = pd.DataFrame(
            {
                "date": [date(2024, 1, 3), date(2024, 1, 2)],  # out of order
                "symbol": ["AAPL", "AAPL"],
                "open": [186.0, 185.0],
                "high": [188.0, 187.0],
                "low": [185.0, 184.0],
                "close": [187.0, 186.5],
                "volume": [45e6, 50e6],
            }
        )
        save_ticker_data("AAPL", df, data_dir)
        stored = load_ticker_data("AAPL", data_dir)
        dates = list(stored["date"])
        assert dates == sorted(dates)


class TestLoadTickerData:
    def test_returns_none_for_missing_file(self, tmp_path):
        result = load_ticker_data("NONEXISTENT", tmp_path / "ohlcv")
        assert result is None

    def test_round_trip_columns(self, ohlcv_df, tmp_path):
        data_dir = tmp_path / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)
        loaded = load_ticker_data("AAPL", data_dir)

        assert loaded is not None
        assert list(loaded.columns) == list(ohlcv_df.columns)

    def test_round_trip_values(self, ohlcv_df, tmp_path):
        data_dir = tmp_path / "ohlcv"
        save_ticker_data("AAPL", ohlcv_df, data_dir)
        loaded = load_ticker_data("AAPL", data_dir)

        assert len(loaded) == 2
        assert loaded["close"].iloc[0] == pytest.approx(186.5)
        assert loaded["close"].iloc[1] == pytest.approx(187.0)

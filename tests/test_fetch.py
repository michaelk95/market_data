"""
Tests for fetch.py: _normalize(), save_ticker_data(), load_ticker_data().

save/load go through the bitemporal store (storage.write_table/read_table).

No network calls are made; all yfinance interactions are tested via fixtures
that replicate the real DataFrame shapes yfinance returns.
"""

from datetime import date

import migrate_flat_tables
import pandas as pd
import pytest

from market_data import fetch
from market_data.fetch import (
    OHLCV_COLS,
    BatchWriteError,
    TickerBatchWriter,
    _normalize,
    load_ticker_data,
    save_ticker_data,
)
from market_data.schema import TABLE_SCHEMAS
from market_data.storage import read_table

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
# save_ticker_data() + load_ticker_data()  (bitemporal store)
# ---------------------------------------------------------------------------


def _bar(day: date, close: float, symbol: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [day],
            "symbol": [symbol],
            "open": [close - 1.0],
            "high": [close + 1.0],
            "low": [close - 2.0],
            "close": [close],
            "volume": [55_000_000.0],
        }
    )


class TestSaveTickerData:
    def test_writes_year_partition_under_data_root(self, ohlcv_df, tmp_path):
        rows_added = save_ticker_data("AAPL", ohlcv_df, tmp_path)

        assert rows_added == 2
        assert (tmp_path / "ohlcv" / "year=2024" / "data.parquet").exists()
        assert not (tmp_path / "ohlcv" / "AAPL.parquet").exists()
        assert not (tmp_path / "ohlcv" / "ohlcv").exists()

    def test_rows_follow_bitemporal_schema(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        stored = read_table("ohlcv", tmp_path)
        assert list(stored.columns) == TABLE_SCHEMAS["ohlcv"].names
        assert list(stored["period_start_date"]) == list(ohlcv_df["date"])
        assert (stored["report_time_marker"] == "post-market").all()

    def test_rows_split_across_year_partitions(self, tmp_path):
        df = pd.concat([_bar(date(2023, 12, 29), 180.0), _bar(date(2024, 1, 2), 185.0)])
        assert save_ticker_data("AAPL", df, tmp_path) == 2
        assert (tmp_path / "ohlcv" / "year=2023" / "data.parquet").exists()
        assert (tmp_path / "ohlcv" / "year=2024" / "data.parquet").exists()

    def test_indices_table_is_single_file(self, ohlcv_df, tmp_path):
        vix = ohlcv_df.assign(symbol="^VIX")
        assert save_ticker_data("^VIX", vix, tmp_path, table="indices") == 2
        assert (tmp_path / "indices" / "data.parquet").exists()
        assert not (tmp_path / "ohlcv").exists()

    def test_table_dir_instead_of_root_raises(self, ohlcv_df, tmp_path):
        """Passing data/ohlcv (the old argument) would nest ohlcv/ohlcv/."""
        with pytest.raises(ValueError, match="data root"):
            save_ticker_data("AAPL", ohlcv_df, tmp_path / "ohlcv")

    def test_returns_zero_for_empty_df(self, tmp_path):
        empty = pd.DataFrame(columns=OHLCV_COLS)
        assert save_ticker_data("AAPL", empty, tmp_path) == 0
        assert not (tmp_path / "ohlcv").exists()

    def test_idempotent_on_duplicate_rows(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        assert save_ticker_data("AAPL", ohlcv_df, tmp_path) == 0
        assert len(load_ticker_data("AAPL", tmp_path)) == 2

    def test_appends_new_rows(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        assert save_ticker_data("AAPL", _bar(date(2024, 1, 4), 189.0), tmp_path) == 1
        assert len(load_ticker_data("AAPL", tmp_path)) == 3

    def test_overlap_keeps_existing_row(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        overlap = pd.concat([_bar(date(2024, 1, 2), 999.0), _bar(date(2024, 1, 5), 191.0)])
        assert save_ticker_data("AAPL", overlap, tmp_path) == 1
        stored = load_ticker_data("AAPL", tmp_path)
        assert len(stored) == 3
        assert stored.loc[stored["date"] == date(2024, 1, 2), "close"].item() == pytest.approx(186.5)

    def test_timestamp_dates_dedup_against_date_objects(self, ohlcv_df, tmp_path):
        """A bar whose date arrives as a Timestamp must still match the stored date."""
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        again = ohlcv_df.assign(date=pd.to_datetime(ohlcv_df["date"]))
        assert save_ticker_data("AAPL", again, tmp_path) == 0

    def test_other_symbols_untouched(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        save_ticker_data("MSFT", ohlcv_df.assign(symbol="MSFT"), tmp_path)
        assert len(load_ticker_data("AAPL", tmp_path)) == 2
        assert len(load_ticker_data("MSFT", tmp_path)) == 2

    def test_live_write_after_migration_adds_nothing(self, ohlcv_df, tmp_path):
        """Rows migrated from a flat file and the same bars saved live are identical keys."""
        flat = ohlcv_df.assign(volume=ohlcv_df["volume"].astype("int64"))
        (tmp_path / "ohlcv").mkdir()
        flat.to_parquet(tmp_path / "ohlcv" / "AAPL.parquet", index=False)
        assert migrate_flat_tables.run("ohlcv", tmp_path) == 2

        assert save_ticker_data("AAPL", ohlcv_df, tmp_path) == 0
        assert len(read_table("ohlcv", tmp_path)) == 2


class TestLoadTickerData:
    def test_returns_none_when_symbol_absent(self, ohlcv_df, tmp_path):
        assert load_ticker_data("NONEXISTENT", tmp_path) is None
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        assert load_ticker_data("NONEXISTENT", tmp_path) is None

    def test_round_trip_flat_columns(self, ohlcv_df, tmp_path):
        save_ticker_data("AAPL", ohlcv_df, tmp_path)
        loaded = load_ticker_data("AAPL", tmp_path)
        assert list(loaded.columns) == OHLCV_COLS
        pd.testing.assert_frame_equal(loaded, ohlcv_df)

    def test_sorted_by_date_ascending(self, tmp_path):
        df = pd.concat([_bar(date(2024, 1, 3), 187.0), _bar(date(2024, 1, 2), 186.5)])
        save_ticker_data("AAPL", df, tmp_path)
        dates = list(load_ticker_data("AAPL", tmp_path)["date"])
        assert dates == sorted(dates)

    def test_reads_indices_table(self, ohlcv_df, tmp_path):
        save_ticker_data("^VIX", ohlcv_df.assign(symbol="^VIX"), tmp_path, table="indices")
        loaded = load_ticker_data("^VIX", tmp_path, table="indices")
        assert loaded["date"].max() == date(2024, 1, 3)
        assert load_ticker_data("^VIX", tmp_path) is None  # not in ohlcv


# ---------------------------------------------------------------------------
# TickerBatchWriter  (one write_table call per batch of symbols)
# ---------------------------------------------------------------------------


@pytest.fixture()
def write_calls(monkeypatch):
    """Record every storage write the batch writer makes."""
    calls: list[pd.DataFrame] = []
    real = fetch.write_table_counts

    def spy(df, table, data_dir):
        calls.append(df)
        return real(df, table, data_dir)

    monkeypatch.setattr(fetch, "write_table_counts", spy)
    return calls


class TestTickerBatchWriter:
    def test_one_write_for_many_symbols(self, tmp_path, write_calls):
        writer = TickerBatchWriter(tmp_path, max_symbols=10)
        for sym in ("AAA", "BBB", "CCC"):
            writer.add(sym, pd.concat([
                _bar(date(2023, 12, 29), 10.0, sym), _bar(date(2024, 1, 2), 11.0, sym),
            ]))
        assert write_calls == []

        counts = writer.flush()

        assert len(write_calls) == 1
        assert counts == {"AAA": 2, "BBB": 2, "CCC": 2}
        assert len(read_table("ohlcv", tmp_path)) == 6

    def test_counts_exclude_rows_already_stored(self, tmp_path, write_calls):
        save_ticker_data("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"), tmp_path)
        writer = TickerBatchWriter(tmp_path)
        writer.add("AAA", pd.concat([
            _bar(date(2024, 1, 2), 99.0, "AAA"), _bar(date(2024, 1, 3), 11.0, "AAA"),
        ]))
        writer.add("BBB", _bar(date(2024, 1, 2), 20.0, "BBB"))
        assert writer.flush() == {"AAA": 1, "BBB": 1}
        stored = load_ticker_data("AAA", tmp_path)
        assert stored.loc[stored["date"] == date(2024, 1, 2), "close"].item() == 10.0

    def test_symbol_with_nothing_new_reports_zero(self, tmp_path, write_calls):
        save_ticker_data("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"), tmp_path)
        writer = TickerBatchWriter(tmp_path)
        writer.add("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"))
        writer.add("EMPTY", pd.DataFrame(columns=OHLCV_COLS))
        assert writer.flush() == {"AAA": 0, "EMPTY": 0}

    def test_full_after_max_symbols(self, tmp_path):
        writer = TickerBatchWriter(tmp_path, max_symbols=2)
        writer.add("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"))
        assert not writer.full
        writer.add("BBB", _bar(date(2024, 1, 2), 10.0, "BBB"))
        assert writer.full
        assert writer.symbols == ["AAA", "BBB"]
        writer.flush()
        assert not writer.full
        assert writer.symbols == []

    def test_flush_of_empty_buffer_writes_nothing(self, tmp_path, write_calls):
        assert TickerBatchWriter(tmp_path).flush() == {}
        assert write_calls == []

    def test_bad_frame_fails_at_add_and_is_not_buffered(self, tmp_path):
        writer = TickerBatchWriter(tmp_path)
        with pytest.raises(ValueError):
            writer.add("BAD", pd.DataFrame({"symbol": ["BAD"], "close": [1.0]}))
        writer.add("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"))
        assert writer.symbols == ["AAA"]
        assert writer.flush() == {"AAA": 1}

    def test_failed_flush_raises_with_symbols_and_clears_buffer(self, tmp_path, monkeypatch):
        def boom(df, table, data_dir):
            raise OSError("disk full")

        monkeypatch.setattr(fetch, "write_table_counts", boom)
        writer = TickerBatchWriter(tmp_path)
        writer.add("AAA", _bar(date(2024, 1, 2), 10.0, "AAA"))
        writer.add("BBB", _bar(date(2024, 1, 2), 10.0, "BBB"))
        with pytest.raises(BatchWriteError) as excinfo:
            writer.flush()
        assert excinfo.value.symbols == ["AAA", "BBB"]
        assert isinstance(excinfo.value.__cause__, OSError)
        assert writer.symbols == []

    def test_indices_table(self, tmp_path):
        writer = TickerBatchWriter(tmp_path, table="indices")
        writer.add("^VIX", _bar(date(2024, 1, 2), 15.0, "^VIX"))
        assert writer.flush() == {"^VIX": 1}
        assert (tmp_path / "indices" / "data.parquet").exists()

    def test_rejects_table_dir(self, tmp_path):
        with pytest.raises(ValueError, match="data root"):
            TickerBatchWriter(tmp_path / "ohlcv")

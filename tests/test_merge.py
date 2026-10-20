"""
Tests for merge.py: run() builds merged.parquet from the bitemporal ohlcv table.

The output keeps the flat ``date, symbol, open, high, low, close, volume``
schema that paper_trading's MarketData reads.  All data lives under tmp_path.
"""

from datetime import date

import pandas as pd

from market_data import merge
from market_data.fetch import OHLCV_COLS, save_ticker_data

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ohlcv(symbol: str, dates: list, prices: list) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * len(dates),
            "open": prices,
            "high": [p + 1.0 for p in prices],
            "low": [p - 1.0 for p in prices],
            "close": prices,
            "volume": [1_000_000.0] * len(dates),
        }
    )


def _save(data_dir, symbol, dates, prices):
    save_ticker_data(symbol, _make_ohlcv(symbol, dates, prices), data_dir)


DATES_2 = [date(2024, 1, 2), date(2024, 1, 3)]
DATES_3 = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMergeRun:
    def test_merges_two_symbols(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])
        _save(tmp_path, "MSFT", DATES_2, [375.0, 376.0])

        merge.run(data_dir=tmp_path, out=out)

        assert len(pd.read_parquet(out)) == 4  # 2 symbols × 2 dates

    def test_combined_row_count_three_symbols(self, tmp_path):
        out = tmp_path / "merged.parquet"
        for sym in ["AAPL", "MSFT", "GOOGL"]:
            _save(tmp_path, sym, DATES_3, [100.0, 101.0, 102.0])

        merge.run(data_dir=tmp_path, out=out)
        assert len(pd.read_parquet(out)) == 3 * len(DATES_3)

    def test_reads_all_year_partitions(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", [date(2023, 12, 29), date(2024, 1, 2)], [180.0, 185.0])

        merge.run(data_dir=tmp_path, out=out)
        result = pd.read_parquet(out)
        assert list(result["date"]) == [date(2023, 12, 29), date(2024, 1, 2)]

    def test_output_has_flat_schema_in_order(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])

        merge.run(data_dir=tmp_path, out=out)
        result = pd.read_parquet(out)
        assert list(result.columns) == OHLCV_COLS

    def test_date_column_holds_dates(self, tmp_path):
        """paper_trading selects on df["date"] == datetime.date."""
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])

        merge.run(data_dir=tmp_path, out=out)
        result = pd.read_parquet(out)
        assert all(type(d) is date for d in result["date"])
        assert len(result[result["date"] == DATES_2[0]]) == 1

    def test_values_survive_round_trip(self, tmp_path):
        out = tmp_path / "merged.parquet"
        expected = _make_ohlcv("AAPL", DATES_2, [185.0, 186.0])
        save_ticker_data("AAPL", expected, tmp_path)

        merge.run(data_dir=tmp_path, out=out)
        pd.testing.assert_frame_equal(pd.read_parquet(out), expected)

    def test_sorted_by_date_then_symbol(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "ZZZ", DATES_2, [100.0, 101.0])
        _save(tmp_path, "AAA", DATES_2, [200.0, 201.0])

        merge.run(data_dir=tmp_path, out=out)
        result = pd.read_parquet(out)

        assert list(result["symbol"]) == ["AAA", "ZZZ", "AAA", "ZZZ"]
        assert list(result["date"]) == sorted(result["date"])

    def test_ignores_stale_flat_files(self, tmp_path):
        """Leftover data/ohlcv/<SYMBOL>.parquet files are not a merge source."""
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])
        _make_ohlcv("OLD", DATES_2, [1.0, 2.0]).to_parquet(
            tmp_path / "ohlcv" / "OLD.parquet", index=False
        )

        merge.run(data_dir=tmp_path, out=out)
        assert set(pd.read_parquet(out)["symbol"]) == {"AAPL"}

    def test_empty_table_does_not_create_output(self, tmp_path):
        out = tmp_path / "merged.parquet"
        merge.run(data_dir=tmp_path, out=out)
        assert not out.exists()

    def test_rerun_is_stable(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])

        merge.run(data_dir=tmp_path, out=out)
        first = pd.read_parquet(out)
        merge.run(data_dir=tmp_path, out=out)
        pd.testing.assert_frame_equal(pd.read_parquet(out), first)

    def test_creates_parent_dir_for_output(self, tmp_path):
        out = tmp_path / "deep" / "nested" / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])

        merge.run(data_dir=tmp_path, out=out)
        assert out.exists()

    def test_no_tmp_file_left_behind(self, tmp_path):
        out = tmp_path / "merged.parquet"
        _save(tmp_path, "AAPL", DATES_2, [185.0, 186.0])

        merge.run(data_dir=tmp_path, out=out)
        assert not list(tmp_path.glob("*.tmp.parquet"))

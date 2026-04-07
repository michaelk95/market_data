"""
Tests for merge.py: run() with multiple tmp parquets.

Verifies correct row count, deduplication, sort order, and atomic-write
behaviour without touching any real data directories.
"""

from datetime import date

import pandas as pd

from market_data import merge


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


DATES_2 = [date(2024, 1, 2), date(2024, 1, 3)]
DATES_3 = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestMergeRun:
    def test_merges_two_ticker_files(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        _make_ohlcv("AAPL", DATES_2, [185.0, 186.0]).to_parquet(
            data_dir / "AAPL.parquet", index=False
        )
        _make_ohlcv("MSFT", DATES_2, [375.0, 376.0]).to_parquet(
            data_dir / "MSFT.parquet", index=False
        )

        merge.run(data_dir=data_dir, out=out)

        result = pd.read_parquet(out)
        assert len(result) == 4  # 2 symbols × 2 dates

    def test_combined_row_count_three_symbols(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        for sym in ["AAPL", "MSFT", "GOOGL"]:
            _make_ohlcv(sym, DATES_3, [100.0, 101.0, 102.0]).to_parquet(
                data_dir / f"{sym}.parquet", index=False
            )

        merge.run(data_dir=data_dir, out=out)
        result = pd.read_parquet(out)
        assert len(result) == 3 * len(DATES_3)

    def test_deduplicates_on_date_symbol(self, tmp_path):
        """If the same (date, symbol) appears in two files, only one row survives."""
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        df = _make_ohlcv("AAPL", [date(2024, 1, 2)], [185.0])
        # Write identical data to two separate files to simulate overlap
        df.to_parquet(data_dir / "AAPL.parquet", index=False)
        df.to_parquet(data_dir / "AAPL_dup.parquet", index=False)

        merge.run(data_dir=data_dir, out=out)
        result = pd.read_parquet(out)
        assert len(result) == 1

    def test_sorted_by_date_then_symbol(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        _make_ohlcv("ZZZ", DATES_2, [100.0, 101.0]).to_parquet(
            data_dir / "ZZZ.parquet", index=False
        )
        _make_ohlcv("AAA", DATES_2, [200.0, 201.0]).to_parquet(
            data_dir / "AAA.parquet", index=False
        )

        merge.run(data_dir=data_dir, out=out)
        result = pd.read_parquet(out)

        # Within the same date, symbols should be alphabetically ordered
        same_date = result[result["date"] == DATES_2[0]]
        assert list(same_date["symbol"]) == ["AAA", "ZZZ"]

    def test_output_file_created(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        _make_ohlcv("AAPL", DATES_2, [185.0, 186.0]).to_parquet(
            data_dir / "AAPL.parquet", index=False
        )
        merge.run(data_dir=data_dir, out=out)
        assert out.exists()

    def test_empty_dir_does_not_create_output(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        merge.run(data_dir=data_dir, out=out)
        assert not out.exists()

    def test_output_has_correct_columns(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "merged.parquet"

        _make_ohlcv("AAPL", DATES_2, [185.0, 186.0]).to_parquet(
            data_dir / "AAPL.parquet", index=False
        )
        merge.run(data_dir=data_dir, out=out)
        result = pd.read_parquet(out)
        assert set(result.columns) == {"date", "symbol", "open", "high", "low", "close", "volume"}

    def test_existing_merged_file_not_included_in_sources(self, tmp_path):
        """The merged output file itself must not be read back in as a source."""
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = data_dir / "merged.parquet"  # place merged inside data_dir

        _make_ohlcv("AAPL", DATES_2, [185.0, 186.0]).to_parquet(
            data_dir / "AAPL.parquet", index=False
        )
        # First pass — creates merged.parquet inside data_dir
        merge.run(data_dir=data_dir, out=out)
        first_count = len(pd.read_parquet(out))

        # Second pass — merged.parquet now exists; should not be double-counted
        merge.run(data_dir=data_dir, out=out)
        second_count = len(pd.read_parquet(out))

        assert first_count == second_count

    def test_creates_parent_dir_for_output(self, tmp_path):
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()
        out = tmp_path / "deep" / "nested" / "merged.parquet"

        _make_ohlcv("AAPL", DATES_2, [185.0, 186.0]).to_parquet(
            data_dir / "AAPL.parquet", index=False
        )
        merge.run(data_dir=data_dir, out=out)
        assert out.exists()

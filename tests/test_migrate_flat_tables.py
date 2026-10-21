"""Tests for scripts/migrate_flat_tables.py (MD-09).

The migration converts the flat per-symbol files written by the old collector
(data/ohlcv/<SYMBOL>.parquet, data/indices/<SYMBOL>.parquet,
data/options/<SYMBOL>.parquet) into the bitemporal layout that
``storage.read_table`` reads.
"""

from __future__ import annotations

import datetime
import os
from pathlib import Path

import migrate_flat_tables
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from market_data.fetch import save_ticker_data
from market_data.schema import TABLE_SCHEMAS
from market_data.storage import read_table

# A fixed mtime for the flat files: 2026-10-14 08:30:00 UTC.
_MTIME = datetime.datetime(2026, 10, 14, 8, 30, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Helpers: build flat files in the old collector's format
# ---------------------------------------------------------------------------


def _write_flat(path: Path, df: pd.DataFrame, mtime: datetime.datetime = _MTIME) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    ts = mtime.timestamp()
    os.utime(path, (ts, ts))


def _flat_ohlcv(symbol: str, dates: list[datetime.date], start: float = 100.0) -> pd.DataFrame:
    """Old fetch.py layout: date (object of date), symbol, OHLC float, volume int64."""
    n = len(dates)
    closes = [start + i for i in range(n)]
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": [symbol] * n,
            "open": [c - 0.5 for c in closes],
            "high": [c + 1.0 for c in closes],
            "low": [c - 1.0 for c in closes],
            "close": closes,
            "volume": pd.array([1_000_000 + i for i in range(n)], dtype="int64"),
        }
    )


def _flat_options(symbol: str, snapshot: datetime.date) -> pd.DataFrame:
    """Old fetch_options.py layout keyed on snapshot_date."""
    return pd.DataFrame(
        {
            "snapshot_date": [snapshot, snapshot, snapshot],
            "symbol": [symbol] * 3,
            "expiry": [
                datetime.date(2026, 4, 17),
                datetime.date(2026, 4, 17),
                datetime.date(2026, 5, 15),
            ],
            "strike": [150.0, 150.0, 160.0],
            "option_type": ["call", "put", "call"],
            "last_price": [5.0, 2.0, 1.5],
            "bid": [4.9, 1.9, 1.4],
            "ask": [5.1, 2.1, 1.6],
            "volume": [10.0, 5.0, 1.0],
            "open_interest": [100.0, 50.0, 10.0],
            "implied_vol": [0.3, 0.32, 0.28],
            "in_the_money": [True, False, False],
        }
    )


_DATES_2023_2024 = [
    datetime.date(2023, 12, 28),
    datetime.date(2023, 12, 29),
    datetime.date(2024, 1, 2),
    datetime.date(2024, 1, 3),
]


@pytest.fixture
def flat_data(tmp_path: Path) -> Path:
    """A data dir with old-format ohlcv, indices and options files."""
    _write_flat(tmp_path / "ohlcv" / "AAPL.parquet", _flat_ohlcv("AAPL", _DATES_2023_2024))
    _write_flat(tmp_path / "ohlcv" / "MSFT.parquet", _flat_ohlcv("MSFT", _DATES_2023_2024, 300.0))
    _write_flat(tmp_path / "indices" / "^VIX.parquet", _flat_ohlcv("^VIX", _DATES_2023_2024, 15.0))
    _write_flat(tmp_path / "indices" / "^TNX.parquet", _flat_ohlcv("^TNX", _DATES_2023_2024, 4.0))
    _write_flat(tmp_path / "options" / "AAPL.parquet", _flat_options("AAPL", datetime.date(2026, 4, 7)))
    return tmp_path


def _flat_files(table_dir: Path) -> list[str]:
    return sorted(p.name for p in table_dir.glob("*.parquet"))


def _normalize_type(t: pa.DataType) -> pa.DataType:
    # pandas 3 writes str columns as large_string; on disk it is the same
    # Parquet BYTE_ARRAY/UTF8 column as string, and the existing fundamentals
    # and macro tables already store it that way.
    return pa.string() if t == pa.large_string() else t


def _assert_matches_schema(path: Path, table: str) -> None:
    actual = pq.read_schema(path).remove_metadata()
    expected = TABLE_SCHEMAS[table]
    actual_types = {f.name: _normalize_type(f.type) for f in actual}
    expected_types = {f.name: f.type for f in expected}
    assert actual_types == expected_types


# ---------------------------------------------------------------------------
# OHLCV
# ---------------------------------------------------------------------------


class TestMigrateOhlcv:
    def test_rows_readable_via_read_table(self, flat_data: Path) -> None:
        written = migrate_flat_tables.run("ohlcv", flat_data)

        df = read_table("ohlcv", flat_data)
        assert written == 8
        assert len(df) == 8
        assert set(df["symbol"]) == {"AAPL", "MSFT"}

    def test_bitemporal_columns_for_daily_bars(self, flat_data: Path) -> None:
        migrate_flat_tables.run("ohlcv", flat_data)

        df = read_table("ohlcv", flat_data, symbols=["AAPL"])
        df = df.sort_values("period_start_date").reset_index(drop=True)
        assert list(df["period_start_date"]) == _DATES_2023_2024
        assert (df["period_end_date"] == df["period_start_date"]).all()
        assert (df["report_date"] == df["period_start_date"]).all()
        assert (df["report_time_marker"] == "post-market").all()
        assert (df["source"] == "yfinance").all()
        assert list(df["close"]) == [100.0, 101.0, 102.0, 103.0]

    def test_partitioned_by_year(self, flat_data: Path) -> None:
        migrate_flat_tables.run("ohlcv", flat_data)

        parts = sorted(p.name for p in (flat_data / "ohlcv").glob("year=*"))
        assert parts == ["year=2023", "year=2024"]

    def test_partitions_match_table_schema(self, flat_data: Path) -> None:
        """volume int64 → float64; dates → date32; collected_at → timestamp[us, UTC]."""
        migrate_flat_tables.run("ohlcv", flat_data)

        parts = list((flat_data / "ohlcv").glob("year=*/data.parquet"))
        assert len(parts) == 2
        for part in parts:
            _assert_matches_schema(part, "ohlcv")

    def test_collected_at_is_flat_file_mtime(self, flat_data: Path) -> None:
        later = datetime.datetime(2026, 10, 15, 4, 0, tzinfo=datetime.timezone.utc)
        _write_flat(
            flat_data / "ohlcv" / "MSFT.parquet",
            _flat_ohlcv("MSFT", _DATES_2023_2024, 300.0),
            mtime=later,
        )

        migrate_flat_tables.run("ohlcv", flat_data)

        df = read_table("ohlcv", flat_data)
        assert set(df["symbol"]) == {"AAPL", "MSFT"}
        by_symbol = df.groupby("symbol")["collected_at"].agg(["min", "max"])
        assert by_symbol.loc["AAPL", "min"] == pd.Timestamp(_MTIME)
        assert by_symbol.loc["AAPL", "max"] == pd.Timestamp(_MTIME)
        assert by_symbol.loc["MSFT", "min"] == pd.Timestamp(later)

    def test_keeps_flat_files(self, flat_data: Path) -> None:
        migrate_flat_tables.run("ohlcv", flat_data)

        assert _flat_files(flat_data / "ohlcv") == ["AAPL.parquet", "MSFT.parquet"]

    def test_idempotent(self, flat_data: Path) -> None:
        migrate_flat_tables.run("ohlcv", flat_data)
        second = migrate_flat_tables.run("ohlcv", flat_data)

        assert second == 0
        assert len(read_table("ohlcv", flat_data)) == 8

    def test_rerun_picks_up_rows_added_to_flat_files(self, flat_data: Path) -> None:
        """Rows the old collector adds to flat files after a first migration are picked up."""
        migrate_flat_tables.run("ohlcv", flat_data)
        _write_flat(
            flat_data / "ohlcv" / "AAPL.parquet",
            _flat_ohlcv("AAPL", [*_DATES_2023_2024, datetime.date(2024, 1, 4)]),
        )

        added = migrate_flat_tables.run("ohlcv", flat_data)

        assert added == 1
        assert len(read_table("ohlcv", flat_data, symbols=["AAPL"])) == 5

    def test_resync_after_live_writes_adds_only_flat_only_rows(self, flat_data: Path) -> None:
        """Cutover re-sync: rows the new writer already stored are not duplicated."""
        migrate_flat_tables.run("ohlcv", flat_data)
        jan4, jan5 = datetime.date(2024, 1, 4), datetime.date(2024, 1, 5)
        live = _flat_ohlcv("AAPL", [jan4], start=104.0).assign(volume=1_000_004.0)
        assert save_ticker_data("AAPL", live, flat_data) == 1
        # The old collector also wrote jan4, plus jan5 the new writer never saw.
        _write_flat(
            flat_data / "ohlcv" / "AAPL.parquet",
            _flat_ohlcv("AAPL", [*_DATES_2023_2024, jan4, jan5]),
        )

        assert migrate_flat_tables.run("ohlcv", flat_data) == 1
        stored = read_table("ohlcv", flat_data, symbols=["AAPL"])
        assert sorted(stored["period_start_date"])[-2:] == [jan4, jan5]
        assert migrate_flat_tables.run("ohlcv", flat_data) == 0

    def test_dry_run_writes_nothing(self, flat_data: Path) -> None:
        count = migrate_flat_tables.run("ohlcv", flat_data, dry_run=True)

        assert count == 8
        assert not list((flat_data / "ohlcv").glob("year=*"))

    def test_chunk_size_does_not_change_result(self, flat_data: Path, tmp_path_factory) -> None:
        other = tmp_path_factory.mktemp("chunked")
        for name in ("AAPL", "MSFT"):
            _write_flat(other / "ohlcv" / f"{name}.parquet", pd.read_parquet(flat_data / "ohlcv" / f"{name}.parquet"))

        migrate_flat_tables.run("ohlcv", flat_data, chunk_size=1000)
        migrate_flat_tables.run("ohlcv", other, chunk_size=1)

        key = ["symbol", "period_start_date"]
        a = read_table("ohlcv", flat_data).sort_values(key).reset_index(drop=True)
        b = read_table("ohlcv", other).sort_values(key).reset_index(drop=True)
        assert len(a) == 8
        pd.testing.assert_frame_equal(a, b)

    def test_symbols_filter(self, flat_data: Path) -> None:
        migrate_flat_tables.run("ohlcv", flat_data, symbols=["msft"])

        assert set(read_table("ohlcv", flat_data)["symbol"]) == {"MSFT"}

    def test_ignores_leftover_temp_files(self, flat_data: Path) -> None:
        """An interrupted atomic write leaves <SYMBOL>.tmp.parquet behind."""
        _write_flat(flat_data / "ohlcv" / "AAPL.tmp.parquet", _flat_ohlcv("AAPL", [datetime.date(2030, 1, 2)]))

        migrate_flat_tables.run("ohlcv", flat_data)

        df = read_table("ohlcv", flat_data)
        assert len(df) == 8
        assert datetime.date(2030, 1, 2) not in set(df["period_start_date"])

    @pytest.mark.parametrize("name", ["merged.parquet", "0_stale.parquet"])
    def test_skips_files_not_named_for_their_symbol(self, flat_data: Path, name: str) -> None:
        """The live ohlcv dir holds a stale multi-symbol merge.py output.

        Its rows must not be migrated, whether it sorts before or after the
        ticker files (sorting first would let its stale prices win dedup).
        """
        before = _flat_ohlcv("AAPL", [datetime.date(2024, 1, 2)], start=999.0)
        stray = pd.concat([before, _flat_ohlcv("ZZZ", [datetime.date(2024, 1, 2)])], ignore_index=True)
        _write_flat(flat_data / "ohlcv" / name, stray)

        written = migrate_flat_tables.run("ohlcv", flat_data)

        df = read_table("ohlcv", flat_data)
        assert written == 8
        assert set(df["symbol"]) == {"AAPL", "MSFT"}
        assert (df["close"] < 999.0).all()

    def test_no_flat_files_is_a_noop(self, tmp_path: Path) -> None:
        assert migrate_flat_tables.run("ohlcv", tmp_path) == 0


# ---------------------------------------------------------------------------
# Indices
# ---------------------------------------------------------------------------


class TestMigrateIndices:
    def test_single_unpartitioned_file(self, flat_data: Path) -> None:
        migrate_flat_tables.run("indices", flat_data)

        assert (flat_data / "indices" / "data.parquet").exists()
        assert not list((flat_data / "indices").glob("year=*"))
        _assert_matches_schema(flat_data / "indices" / "data.parquet", "indices")

    def test_rows_readable_via_read_table(self, flat_data: Path) -> None:
        migrate_flat_tables.run("indices", flat_data)

        df = read_table("indices", flat_data)
        assert len(df) == 8
        assert set(df["symbol"]) == {"^VIX", "^TNX"}
        assert (df["report_date"] == df["period_start_date"]).all()
        assert (df["report_time_marker"] == "post-market").all()

    def test_keeps_flat_files(self, flat_data: Path) -> None:
        migrate_flat_tables.run("indices", flat_data)

        # data.parquet is the migrated table, not a flat per-symbol file
        assert _flat_files(flat_data / "indices") == ["^TNX.parquet", "^VIX.parquet", "data.parquet"]

    def test_rerun_ignores_its_own_output(self, flat_data: Path) -> None:
        """data.parquet lives next to the flat files and must not be re-read as a symbol."""
        migrate_flat_tables.run("indices", flat_data)

        assert migrate_flat_tables.run("indices", flat_data) == 0
        assert len(read_table("indices", flat_data)) == 8


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


class TestMigrateOptions:
    def test_snapshot_date_becomes_period_start_date(self, flat_data: Path) -> None:
        migrate_flat_tables.run("options", flat_data)

        df = read_table("options", flat_data)
        assert len(df) == 3
        assert set(df["period_start_date"]) == {datetime.date(2026, 4, 7)}
        assert (df["report_date"] == df["period_start_date"]).all()
        assert "snapshot_date" not in df.columns

    def test_marker_is_pre_market(self, flat_data: Path) -> None:
        """Historical chains were fetched around 04:00, before the open (MD-14)."""
        migrate_flat_tables.run("options", flat_data)

        df = read_table("options", flat_data)
        assert len(df) == 3
        assert (df["report_time_marker"] == "pre-market").all()

    def test_keeps_contract_keys(self, flat_data: Path) -> None:
        migrate_flat_tables.run("options", flat_data)

        df = read_table("options", flat_data)
        keys = set(zip(df["expiry"], df["strike"], df["option_type"]))
        assert keys == {
            (datetime.date(2026, 4, 17), 150.0, "call"),
            (datetime.date(2026, 4, 17), 150.0, "put"),
            (datetime.date(2026, 5, 15), 160.0, "call"),
        }

    def test_partitions_match_table_schema(self, flat_data: Path) -> None:
        migrate_flat_tables.run("options", flat_data)

        parts = list((flat_data / "options").glob("year=*/data.parquet"))
        assert [p.parent.name for p in parts] == ["year=2026"]
        _assert_matches_schema(parts[0], "options")


def test_unknown_table_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="fundamentals"):
        migrate_flat_tables.run("fundamentals", tmp_path)

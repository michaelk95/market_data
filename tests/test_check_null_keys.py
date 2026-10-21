"""Tests for scripts/check_null_keys.py: a read-only audit of null-key rows
and of flat-file symbols missing from the bitemporal tables (issue #74)."""

from __future__ import annotations

import datetime
from pathlib import Path

import check_null_keys
import pandas as pd
import pytest

from market_data.flat_tables import conform_to_schema, to_bitemporal
from market_data.storage import write_table

_AT = pd.Timestamp("2026-10-20 08:00:00", tz="UTC")
_DAY = datetime.date(2026, 4, 7)


def _flat_options(symbol: str | None, strikes: list[float], day: datetime.date | None = _DAY) -> pd.DataFrame:
    n = len(strikes)
    return pd.DataFrame(
        {
            "snapshot_date": [day] * n,
            "symbol": [symbol] * n,
            "expiry": [datetime.date(2026, 5, 15)] * n,
            "strike": strikes,
            "option_type": ["call"] * n,
            "last_price": [1.0] * n,
            "bid": [0.9] * n,
            "ask": [1.1] * n,
            "volume": [1.0] * n,
            "open_interest": [10.0] * n,
            "implied_vol": [0.3] * n,
            "in_the_money": [False] * n,
        }
    )


def _store(df: pd.DataFrame, data_dir: Path, table: str = "options") -> None:
    """Write rows as-is, bypassing the live writers' null-key guard."""
    write_table(conform_to_schema(to_bitemporal(df, table, _AT), table), table, data_dir)


def _write_flat(data_dir: Path, name: str, df: pd.DataFrame, table: str = "options") -> None:
    path = data_dir / table / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


@pytest.fixture
def clean(tmp_path: Path) -> Path:
    """AAPL in the table and in its flat file, no null keys anywhere."""
    _store(_flat_options("AAPL", [150.0, 160.0]), tmp_path)
    _write_flat(tmp_path, "AAPL", _flat_options("AAPL", [150.0, 160.0]))
    return tmp_path


def _snapshot(root: Path) -> dict[str, tuple[int, int]]:
    return {
        str(p.relative_to(root)): (p.stat().st_size, p.stat().st_mtime_ns)
        for p in root.rglob("*") if p.is_file()
    }


class TestRun:
    def test_clean_data_has_no_problems(self, clean: Path) -> None:
        report = check_null_keys.run("options", clean)

        assert report.table_null_rows == {}
        assert report.flat_null_rows == {}
        assert report.missing_symbols == []
        assert not report.has_problems

    def test_counts_null_key_rows_in_table(self, clean: Path) -> None:
        _store(_flat_options(None, [170.0, 180.0]), clean)

        report = check_null_keys.run("options", clean)

        assert report.table_null_rows == {"<no symbol>": 2}
        assert report.has_problems

    def test_null_strike_counted_under_its_symbol(self, clean: Path) -> None:
        _store(_flat_options("AAPL", [float("nan")]), clean)

        assert check_null_keys.run("options", clean).table_null_rows == {"AAPL": 1}

    def test_missing_symbol_with_null_rows_is_recoverable(self, clean: Path) -> None:
        """The #74 case: legacy null rows made the first migration skip the file."""
        legacy = pd.concat(
            [_flat_options("MSFT", [100.0, 110.0]), _flat_options(None, [120.0], day=None)],
            ignore_index=True,
        )
        _write_flat(clean, "MSFT", legacy)

        report = check_null_keys.run("options", clean)

        assert report.flat_null_rows == {"MSFT": 1}
        assert report.missing_symbols == ["MSFT"]
        assert report.recoverable == ["MSFT"]
        assert report.has_problems

    def test_missing_symbol_without_null_rows_not_recoverable(self, clean: Path) -> None:
        _write_flat(clean, "NVDA", _flat_options("NVDA", [500.0]))

        report = check_null_keys.run("options", clean)

        assert report.missing_symbols == ["NVDA"]
        assert report.recoverable == []

    def test_flat_file_with_only_null_rows_is_not_missing(self, clean: Path) -> None:
        _write_flat(clean, "ZZZ", _flat_options(None, [1.0], day=None))

        report = check_null_keys.run("options", clean)

        assert report.flat_null_rows == {"ZZZ": 1}
        assert report.missing_symbols == []

    def test_file_not_named_for_its_symbol_is_skipped(self, clean: Path) -> None:
        """Migration skips these too (e.g. a stale merged.parquet)."""
        stray = pd.concat([_flat_options("AAA", [1.0]), _flat_options("BBB", [2.0])], ignore_index=True)
        _write_flat(clean, "merged", stray)

        report = check_null_keys.run("options", clean)

        assert report.skipped_files == ["merged.parquet"]
        assert report.missing_symbols == []

    def test_ohlcv_table(self, tmp_path: Path) -> None:
        bars = pd.DataFrame(
            {"date": [_DAY], "symbol": [None], "open": [1.0], "high": [1.0],
             "low": [1.0], "close": [1.0], "volume": [1.0]}
        )
        _store(bars, tmp_path, "ohlcv")
        _write_flat(tmp_path, "AAPL", bars.assign(symbol="AAPL"), "ohlcv")

        report = check_null_keys.run("ohlcv", tmp_path)

        assert report.table_null_rows == {"<no symbol>": 1}
        assert report.missing_symbols == ["AAPL"]

    def test_empty_data_dir(self, tmp_path: Path) -> None:
        report = check_null_keys.run("options", tmp_path)

        assert not report.has_problems
        assert not (tmp_path / "options").exists()

    def test_writes_nothing(self, clean: Path) -> None:
        _store(_flat_options(None, [170.0]), clean)
        _write_flat(clean, "MSFT", _flat_options("MSFT", [100.0]))
        before = _snapshot(clean)

        check_null_keys.run("options", clean)

        assert _snapshot(clean) == before

    def test_unknown_table_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="macro"):
            check_null_keys.run("macro", tmp_path)


class TestMain:
    def _main(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr("sys.argv", ["check_null_keys.py", *args])
        monkeypatch.setattr(check_null_keys, "setup_logging", lambda: None)
        with pytest.raises(SystemExit) as exc:
            check_null_keys.main()
        return exc.value.code

    def test_exit_zero_when_clean(self, clean: Path, monkeypatch) -> None:
        assert self._main(monkeypatch, "--data-dir", str(clean)) == 0

    def test_exit_one_when_problems(self, clean: Path, monkeypatch, caplog) -> None:
        _write_flat(clean, "MSFT", _flat_options("MSFT", [100.0]))
        with caplog.at_level("INFO", logger="market_data"):
            assert self._main(monkeypatch, "--data-dir", str(clean), "--table", "all") == 1
        assert "MSFT" in caplog.text

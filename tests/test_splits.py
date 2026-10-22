"""Tests for market_data.splits — re-downloading OHLCV history after a split."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from market_data import fetch, splits
from market_data.fetch import load_ticker_data, save_ticker_data
from market_data.storage import read_table


def _bars(symbol: str, closes: list[float], start: str = "2026-01-05") -> pd.DataFrame:
    days = pd.date_range(start, periods=len(closes), freq="B").date
    return pd.DataFrame({
        "date": days, "symbol": symbol, "open": closes, "high": closes,
        "low": closes, "close": closes, "volume": 100.0,
    })


# A 2:1 split on the 4th bar: stored bars before it are on the old basis,
# a fresh download has them halved.
STORED = [100.0, 102.0, 104.0, 52.0, 53.0]
FRESH = [50.0, 51.0, 52.0, 52.0, 53.0]


class TestCountBreaks:
    def test_one_split_is_one_break(self):
        assert splits.count_breaks(_bars("A", STORED), _bars("A", FRESH)) == 1

    def test_consistent_series_has_no_breaks(self):
        assert splits.count_breaks(_bars("A", FRESH), _bars("A", FRESH)) == 0

    def test_dividend_drift_is_not_a_break(self):
        drift = [c * 0.99 for c in FRESH]
        assert splits.count_breaks(_bars("A", FRESH), _bars("A", drift)) == 0

    def test_ignores_non_positive_closes(self):
        stored = _bars("A", [100.0, 0.0, 104.0, 52.0, 53.0])
        assert splits.count_breaks(stored, _bars("A", FRESH)) == 1


class TestCheckRefetch:
    def test_returns_break_count(self):
        assert splits.check_refetch(_bars("A", STORED), _bars("A", FRESH)) == 1

    def test_rejects_low_coverage(self):
        with pytest.raises(splits.RefetchRejected, match="coverage"):
            splits.check_refetch(_bars("A", STORED), _bars("A", FRESH[:3]))

    def test_rejects_empty_download(self):
        with pytest.raises(splits.RefetchRejected):
            splits.check_refetch(_bars("A", STORED), _bars("A", []))

    def test_rejects_different_security(self, monkeypatch):
        monkeypatch.setattr(splits, "MAX_BREAKS", 1)
        other = [50.0, 100.0, 50.0, 100.0, 50.0]
        with pytest.raises(splits.RefetchRejected, match="breaks"):
            splits.check_refetch(_bars("A", STORED), _bars("A", other))


class TestPrepareAndApply:
    def test_replaces_only_the_symbols_rows(self, tmp_path):
        save_ticker_data("AAA", _bars("AAA", STORED), tmp_path)
        save_ticker_data("BBB", _bars("BBB", STORED), tmp_path)
        calls = []

        def fake_fetch(symbol, start, end=None):
            calls.append((symbol, start, end))
            return _bars(symbol, FRESH)

        fresh, breaks = splits.prepare_refetch("AAA", tmp_path, fetch=fake_fetch)
        assert breaks == 1
        assert calls == [("AAA", date(2026, 1, 5), date(2026, 1, 9))]

        assert splits.apply_refetches({"AAA": fresh}, tmp_path) == 5
        assert load_ticker_data("AAA", tmp_path)["close"].tolist() == FRESH
        assert load_ticker_data("BBB", tmp_path)["close"].tolist() == STORED

    def test_keeps_stored_dates_the_download_lacks(self, tmp_path, caplog):
        save_ticker_data("AAA", _bars("AAA", STORED + [54.0] * 45), tmp_path)
        fresh_rows = _bars("AAA", FRESH + [54.0] * 45).drop(index=10)

        fresh, _ = splits.prepare_refetch(
            "AAA", tmp_path, fetch=lambda s, start, end=None: fresh_rows
        )
        splits.apply_refetches({"AAA": fresh}, tmp_path)

        stored = load_ticker_data("AAA", tmp_path)
        assert len(stored) == 50
        assert stored["close"].iloc[10] == 54.0
        assert "not in the re-download" in caplog.text

    def test_drops_rows_outside_the_stored_range(self, tmp_path):
        save_ticker_data("AAA", _bars("AAA", STORED), tmp_path)
        longer = _bars("AAA", [49.0] + FRESH, start="2026-01-02")

        fresh, _ = splits.prepare_refetch(
            "AAA", tmp_path, fetch=lambda s, start, end=None: longer
        )
        assert fresh["date"].min() == date(2026, 1, 5)

    def test_nothing_stored_is_rejected(self, tmp_path):
        with pytest.raises(splits.RefetchRejected):
            splits.prepare_refetch("AAA", tmp_path, fetch=lambda *a, **k: _bars("AAA", FRESH))

    def test_new_rows_record_collection_time(self, tmp_path):
        save_ticker_data("AAA", _bars("AAA", STORED), tmp_path)
        before = read_table("ohlcv", tmp_path)["collected_at"].max()
        splits.apply_refetches({"AAA": _bars("AAA", FRESH)}, tmp_path)
        assert (read_table("ohlcv", tmp_path)["collected_at"] >= before).all()


class TestUnadjustedSplit:
    def test_detects_jump_matching_the_split(self):
        assert splits.shows_unadjusted_split(_bars("A", STORED), date(2026, 1, 8), 2.0)

    def test_adjusted_history_has_no_jump(self):
        assert not splits.shows_unadjusted_split(_bars("A", FRESH), date(2026, 1, 8), 2.0)

    def test_reverse_split(self):
        closes = [1.0, 1.0, 30.0, 30.0]
        assert splits.shows_unadjusted_split(_bars("A", closes), date(2026, 1, 7), 1 / 30)


class TestSplitCandidates:
    def test_flags_split_shaped_jumps_only(self):
        df = pd.concat([
            _bars("SPLIT", STORED),
            _bars("MOVE", [100.0, 140.0, 141.0]),
            _bars("REV", [1.0, 1.0, 10.1]),
        ])
        assert splits.split_candidates(df) == ["REV", "SPLIT"]

    def test_since_ignores_older_jumps(self):
        df = _bars("SPLIT", STORED)
        assert splits.split_candidates(df, since=date(2026, 1, 9)) == []
        assert splits.split_candidates(df, since=date(2026, 1, 8)) == ["SPLIT"]


class TestFetchIncrementalWithSplits:
    def test_returns_bars_and_splits(self):
        idx = pd.DatetimeIndex(["2026-09-01", "2026-09-02"], tz="America/New_York")
        raw = pd.DataFrame({
            "Open": [1.0, 2.0], "High": [1.0, 2.0], "Low": [1.0, 2.0],
            "Close": [1.0, 2.0], "Volume": [10, 20],
            "Dividends": [0.0, 0.0], "Stock Splits": [0.0, 2.0],
        }, index=idx)
        ticker = MagicMock()
        ticker.history.return_value = raw
        with patch.object(fetch.yf, "Ticker", return_value=ticker):
            df, found = fetch.fetch_incremental_with_splits("APH", since=date(2026, 9, 1))

        assert ticker.history.call_args.kwargs["actions"] is True
        assert ticker.history.call_args.kwargs["auto_adjust"] is True
        assert list(df.columns) == fetch.OHLCV_COLS
        assert len(df) == 2
        assert found == [(date(2026, 9, 2), 2.0)]

    def test_no_actions_columns(self):
        ticker = MagicMock()
        ticker.history.return_value = pd.DataFrame()
        with patch.object(fetch.yf, "Ticker", return_value=ticker):
            df, found = fetch.fetch_incremental_with_splits("X", since=date(2026, 9, 1))
        assert df.empty and found == []

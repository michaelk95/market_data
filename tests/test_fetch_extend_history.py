"""
Tests for fetch_extend_history.py:
  - pending_tickers()  — classification of needs-fetch vs already-deep vs skip
  - run()              — fetch/save orchestration, state persistence, dry-run,
                          batch size, already-deep fast-path
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from market_data.fetch import save_ticker_data
from market_data.fetch_extend_history import EARLIEST_TARGET, pending_tickers, run
from market_data.storage import read_table

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

def _ohlcv(ticker: str, start: str, n: int = 5) -> pd.DataFrame:
    """Return a minimal OHLCV DataFrame with dates starting at *start*."""
    dates = pd.date_range(start, periods=n, freq="B").date
    return pd.DataFrame({
        "date": dates,
        "symbol": ticker,
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 1_000_000.0,
    })


def _store(data_dir: Path, df: pd.DataFrame) -> None:
    """Write *df* into the bitemporal ohlcv table under *data_dir*."""
    save_ticker_data(df["symbol"].iloc[0], df, data_dir)


# ---------------------------------------------------------------------------
# pending_tickers()
# ---------------------------------------------------------------------------

class TestPendingTickers:
    def test_ticker_after_target_needs_fetch(self, tmp_path):
        df = _ohlcv("AAPL", "2015-01-02")
        _store(tmp_path, df)
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), set())
        assert "AAPL" in needs
        assert "AAPL" not in deep

    def test_ticker_at_target_is_already_deep(self, tmp_path):
        df = _ohlcv("AAPL", str(EARLIEST_TARGET))
        _store(tmp_path, df)
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), set())
        assert "AAPL" not in needs
        assert "AAPL" in deep

    def test_ticker_before_target_is_already_deep(self, tmp_path):
        df = _ohlcv("AAPL", "1988-01-04")
        _store(tmp_path, df)
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), set())
        assert "AAPL" not in needs
        assert "AAPL" in deep

    def test_completed_ticker_is_skipped(self, tmp_path):
        df = _ohlcv("AAPL", "2015-01-02")
        _store(tmp_path, df)
        needs, deep = pending_tickers({"AAPL"}, tmp_path, {"AAPL"}, set())
        assert needs == []
        assert deep == []

    def test_failed_ticker_is_skipped(self, tmp_path):
        df = _ohlcv("AAPL", "2015-01-02")
        _store(tmp_path, df)
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), {"AAPL"})
        assert needs == []
        assert deep == []

    def test_ticker_with_no_file_is_skipped(self, tmp_path):
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), set())
        assert needs == []
        assert deep == []

    def test_mixed_set_classified_correctly(self, tmp_path):
        _store(tmp_path, _ohlcv("SHALLOW", "2018-01-02"))
        _store(tmp_path, _ohlcv("DEEP", "1989-01-03"))
        _store(tmp_path, _ohlcv("DONE", "2010-01-04"))

        needs, deep = pending_tickers(
            {"SHALLOW", "DEEP", "DONE"}, tmp_path, {"DONE"}, set()
        )
        assert needs == ["SHALLOW"]
        assert deep == ["DEEP"]

    def test_custom_earliest_target(self, tmp_path):
        # Ticker starts in 2010; custom target is 2005 → needs fetching
        df = _ohlcv("AAPL", "2010-01-04")
        _store(tmp_path, df)
        custom_target = date(2005, 1, 1)
        needs, _deep = pending_tickers({"AAPL"}, tmp_path, set(), set(), custom_target)
        assert "AAPL" in needs

    def test_earliest_date_spans_partitions(self, tmp_path):
        """A ticker with rows in several year partitions uses its overall minimum."""
        _store(tmp_path, _ohlcv("AAPL", "2015-01-02"))
        _store(tmp_path, _ohlcv("AAPL", "1989-01-03"))
        needs, deep = pending_tickers({"AAPL"}, tmp_path, set(), set())
        assert needs == []
        assert deep == ["AAPL"]

    def test_flat_file_alone_is_not_history(self, tmp_path):
        """Leftover data/ohlcv/<SYMBOL>.parquet files are no longer read."""
        (tmp_path / "ohlcv").mkdir()
        _ohlcv("AAPL", "2015-01-02").to_parquet(tmp_path / "ohlcv" / "AAPL.parquet", index=False)
        assert pending_tickers({"AAPL"}, tmp_path, set(), set()) == ([], [])

    def test_table_read_once_for_many_tickers(self, tmp_path):
        for t in ["AAA", "BBB", "CCC"]:
            _store(tmp_path, _ohlcv(t, "2015-01-02"))
        with patch("market_data.fetch_extend_history.read_table", wraps=read_table) as spy:
            needs, _ = pending_tickers({"AAA", "BBB", "CCC"}, tmp_path, set(), set())
        assert needs == ["AAA", "BBB", "CCC"]
        assert spy.call_count == 1


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.fixture()
    def data_dir(self, tmp_path) -> Path:
        d = tmp_path / "data"
        d.mkdir()
        return d

    @pytest.fixture()
    def state_file(self, tmp_path) -> Path:
        return tmp_path / "state.json"

    def _base_state(self, tickers: list[str]) -> dict:
        return {"onboarded": tickers, "extend_history_completed": [], "extend_history_failures": {}}

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_fetches_shallow_tickers(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        _store(data_dir, _ohlcv("AAPL", "2015-01-02"))

        state_file.write_text(json.dumps(self._base_state(["AAPL"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text

        mock_fetch.return_value = _ohlcv("AAPL", "1993-01-29", n=100)
        mock_save.return_value = 80

        result = run(data_dir=data_dir, batch_size=0, dry_run=False)

        mock_fetch.assert_called_once_with("AAPL")
        assert result["fetched"] == 1
        assert result["failed"] == 0

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_deep_tickers_marked_complete_without_fetch(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        _store(data_dir, _ohlcv("SPY", "1988-01-04"))

        state_file.write_text(json.dumps(self._base_state(["SPY"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text

        result = run(data_dir=data_dir, batch_size=0, dry_run=False)

        mock_fetch.assert_not_called()
        assert result["skipped"] == 1
        saved_state = json.loads(state_file.read_text())
        assert "SPY" in saved_state["extend_history_completed"]

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_batch_size_limits_fetches(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        for t in ["AAA", "BBB", "CCC"]:
            _store(data_dir, _ohlcv(t, "2015-01-02"))

        state_file.write_text(json.dumps(self._base_state(["AAA", "BBB", "CCC"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("AAA", "1993-01-29", n=50)
        mock_save.return_value = 40

        result = run(data_dir=data_dir, batch_size=1, dry_run=False)

        assert mock_fetch.call_count == 1
        assert result["fetched"] == 1
        assert result["remaining"] == 2

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_empty_data_recorded_as_failure(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        _store(data_dir, _ohlcv("AAPL", "2015-01-02"))

        state_file.write_text(json.dumps(self._base_state(["AAPL"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = pd.DataFrame()

        result = run(data_dir=data_dir, batch_size=0, dry_run=False)

        assert result["failed"] == 1
        mock_save.assert_not_called()
        saved_state = json.loads(state_file.read_text())
        assert "AAPL" in saved_state["extend_history_failures"]

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_dry_run_makes_no_network_calls(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        _store(data_dir, _ohlcv("AAPL", "2015-01-02"))

        state_file.write_text(json.dumps(self._base_state(["AAPL"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text

        run(data_dir=data_dir, batch_size=0, dry_run=True)

        mock_fetch.assert_not_called()
        mock_save.assert_not_called()

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_completed_tickers_persisted_to_state(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        _store(data_dir, _ohlcv("AAPL", "2015-01-02"))

        state_file.write_text(json.dumps(self._base_state(["AAPL"])))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("AAPL", "1993-01-29", n=100)
        mock_save.return_value = 80

        run(data_dir=data_dir, batch_size=0, dry_run=False)

        saved_state = json.loads(state_file.read_text())
        assert "AAPL" in saved_state["extend_history_completed"]

    @patch("market_data.fetch_extend_history.fetch_max_history")
    @patch("market_data.fetch_extend_history.save_ticker_data")
    @patch("market_data.fetch_extend_history.STATE_FILE")
    def test_no_onboarded_tickers_returns_early(
        self, mock_state_path, mock_save, mock_fetch, data_dir, tmp_path, state_file
    ):
        state_file.write_text(json.dumps({"onboarded": [], "extend_history_completed": [], "extend_history_failures": {}}))
        mock_state_path.exists.return_value = True
        mock_state_path.read_text = state_file.read_text
        mock_state_path.write_text = state_file.write_text

        result = run(data_dir=data_dir, batch_size=0, dry_run=False)

        mock_fetch.assert_not_called()
        assert result == {"fetched": 0, "skipped": 0, "failed": 0, "remaining": 0}

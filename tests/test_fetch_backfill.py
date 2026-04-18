"""
Tests for fetch_backfill.py:
  - pending_tickers()  — skipping logic (completed, failures, existing files,
                          active tickers, multi-period collapse)
  - run()              — fetch/save orchestration, state persistence, dry-run,
                          empty-data handling
"""

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from market_data.fetch_backfill import pending_tickers, run


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

def _hist(tickers_and_periods: list[tuple]) -> pd.DataFrame:
    """Build a minimal constituent_history DataFrame."""
    rows = []
    for ticker, date_added, date_removed in tickers_and_periods:
        rows.append({
            "ticker": ticker,
            "index": "SP500",
            "date_added": pd.Timestamp(date_added),
            "date_removed": pd.Timestamp(date_removed) if date_removed else pd.NaT,
        })
    return pd.DataFrame(rows)


def _ohlcv(ticker: str, n: int = 5) -> pd.DataFrame:
    """Return a minimal OHLCV DataFrame for a ticker."""
    dates = pd.date_range("2000-01-03", periods=n, freq="B").date
    return pd.DataFrame({
        "date": dates,
        "symbol": ticker,
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 102.0,
        "volume": 1_000_000.0,
    })


# ---------------------------------------------------------------------------
# pending_tickers()
# ---------------------------------------------------------------------------

class TestPendingTickers:
    def test_delisted_ticker_with_no_file_is_pending(self, tmp_path):
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert len(jobs) == 1
        assert jobs[0]["ticker"] == "DEAD"

    def test_active_ticker_is_skipped(self, tmp_path):
        hist = _hist([("AAPL", "2000-01-01", None)])
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert jobs == []

    def test_completed_ticker_is_skipped(self, tmp_path):
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        jobs = pending_tickers(hist, tmp_path, {"DEAD"}, set())
        assert jobs == []

    def test_failed_ticker_is_skipped(self, tmp_path):
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        jobs = pending_tickers(hist, tmp_path, set(), {"DEAD"})
        assert jobs == []

    def test_ticker_with_existing_file_is_skipped(self, tmp_path):
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        (tmp_path / "DEAD.parquet").touch()
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert jobs == []

    def test_date_range_uses_earliest_start_latest_end(self, tmp_path):
        # Ticker with two membership periods
        hist = _hist([
            ("AAL", "1996-01-02", "1997-01-15"),
            ("AAL", "2015-03-23", "2024-09-23"),
        ])
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert len(jobs) == 1
        assert jobs[0]["start"] == date(1996, 1, 2)
        assert jobs[0]["end"] == date(2024, 9, 23)

    def test_multiple_pending_tickers_all_returned(self, tmp_path):
        hist = _hist([
            ("DEAD1", "2000-01-01", "2010-01-01"),
            ("DEAD2", "2005-01-01", "2015-01-01"),
            ("ALIVE", "2000-01-01", None),
        ])
        jobs = pending_tickers(hist, tmp_path, set(), set())
        tickers = {j["ticker"] for j in jobs}
        assert tickers == {"DEAD1", "DEAD2"}

    def test_job_contains_correct_date_fields(self, tmp_path):
        hist = _hist([("DEAD", "2003-07-14", "2018-11-30")])
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert jobs[0]["start"] == date(2003, 7, 14)
        assert jobs[0]["end"] == date(2018, 11, 30)


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.fixture()
    def hist_file(self, tmp_path) -> Path:
        df = _hist([
            ("DEAD1", "2000-01-01", "2010-06-15"),
            ("DEAD2", "2005-01-01", "2015-03-01"),
        ])
        path = tmp_path / "constituent_history.parquet"
        df.to_parquet(path, index=False)
        return path

    @pytest.fixture()
    def state_file(self, tmp_path) -> Path:
        return tmp_path / "state.json"

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_fetches_and_saves_pending_tickers(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.__str__ = lambda s: str(state_file)
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD1")
        mock_save.return_value = 5

        result = run(
            constituent_path=hist_file,
            ohlcv_dir=tmp_path / "ohlcv",
            batch_size=0,
            dry_run=False,
        )

        assert mock_fetch.call_count == 2
        assert result["fetched"] == 2
        assert result["failed"] == 0

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_batch_size_limits_fetches(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD1")
        mock_save.return_value = 5

        result = run(
            constituent_path=hist_file,
            ohlcv_dir=tmp_path / "ohlcv",
            batch_size=1,
            dry_run=False,
        )

        assert mock_fetch.call_count == 1
        assert result["fetched"] == 1
        assert result["remaining"] == 1

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_empty_data_recorded_as_failure(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = pd.DataFrame()  # yfinance returned nothing

        result = run(
            constituent_path=hist_file,
            ohlcv_dir=tmp_path / "ohlcv",
            batch_size=0,
            dry_run=False,
        )

        assert result["failed"] == 2
        assert result["fetched"] == 0
        mock_save.assert_not_called()

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_dry_run_makes_no_network_calls(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text

        run(
            constituent_path=hist_file,
            ohlcv_dir=tmp_path / "ohlcv",
            batch_size=0,
            dry_run=True,
        )

        mock_fetch.assert_not_called()
        mock_save.assert_not_called()

    def test_missing_constituent_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="market-data-fetch-constituent-history"):
            run(constituent_path=tmp_path / "nonexistent.parquet")

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_completed_tickers_persisted_to_state(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        written = {}

        def _write(text):
            written["state"] = json.loads(text)

        mock_state_path.exists.return_value = False
        mock_state_path.write_text = _write
        mock_fetch.return_value = _ohlcv("DEAD1")
        mock_save.return_value = 5

        run(
            constituent_path=hist_file,
            ohlcv_dir=tmp_path / "ohlcv",
            batch_size=0,
            dry_run=False,
        )

        assert "DEAD1" in written["state"]["backfill_completed"]
        assert "DEAD2" in written["state"]["backfill_completed"]

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.save_ticker_data")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_existing_ohlcv_file_skipped(
        self, mock_state_path, mock_save, mock_fetch, hist_file, tmp_path, state_file
    ):
        ohlcv_dir = tmp_path / "ohlcv"
        ohlcv_dir.mkdir()
        (ohlcv_dir / "DEAD1.parquet").touch()

        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD2")
        mock_save.return_value = 5

        result = run(
            constituent_path=hist_file,
            ohlcv_dir=ohlcv_dir,
            batch_size=0,
            dry_run=False,
        )

        assert result["fetched"] == 1
        fetched_tickers = [c.args[0] for c in mock_fetch.call_args_list]
        assert "DEAD1" not in fetched_tickers
        assert "DEAD2" in fetched_tickers

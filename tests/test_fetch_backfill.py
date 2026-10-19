"""
Tests for fetch_backfill.py:
  - pending_tickers()  — skipping logic (completed, failures, already-stored rows,
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

from market_data.fetch import save_ticker_data
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

    def test_ticker_with_stored_rows_is_skipped(self, tmp_path):
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        save_ticker_data("DEAD", _ohlcv("DEAD"), tmp_path)
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert jobs == []

    def test_flat_file_alone_does_not_count_as_stored(self, tmp_path):
        """Leftover data/ohlcv/<SYMBOL>.parquet files are no longer read."""
        hist = _hist([("DEAD", "2000-01-01", "2010-06-15")])
        (tmp_path / "ohlcv").mkdir()
        _ohlcv("DEAD").to_parquet(tmp_path / "ohlcv" / "DEAD.parquet", index=False)
        jobs = pending_tickers(hist, tmp_path, set(), set())
        assert [j["ticker"] for j in jobs] == ["DEAD"]

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
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_fetches_and_saves_pending_tickers(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.__str__ = lambda s: str(state_file)
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD1")

        result = run(
            constituent_path=hist_file,
            data_dir=tmp_path / "data",
            batch_size=0,
            dry_run=False,
        )

        assert mock_fetch.call_count == 2
        assert result["fetched"] == 2
        assert result["failed"] == 0

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_batch_size_limits_fetches(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD1")

        result = run(
            constituent_path=hist_file,
            data_dir=tmp_path / "data",
            batch_size=1,
            dry_run=False,
        )

        assert mock_fetch.call_count == 1
        assert result["fetched"] == 1
        assert result["remaining"] == 1

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_empty_data_recorded_as_failure(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = pd.DataFrame()  # yfinance returned nothing

        result = run(
            constituent_path=hist_file,
            data_dir=tmp_path / "data",
            batch_size=0,
            dry_run=False,
        )

        assert result["failed"] == 2
        assert result["fetched"] == 0
        assert not (tmp_path / "data" / "ohlcv").exists()

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_dry_run_makes_no_network_calls(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text

        run(
            constituent_path=hist_file,
            data_dir=tmp_path / "data",
            batch_size=0,
            dry_run=True,
        )

        mock_fetch.assert_not_called()
        assert not (tmp_path / "data" / "ohlcv").exists()

    def test_missing_constituent_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="market-data-fetch-constituent-history"):
            run(constituent_path=tmp_path / "nonexistent.parquet")

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_completed_tickers_persisted_to_state(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        written = {}

        def _write(text):
            written["state"] = json.loads(text)

        mock_state_path.exists.return_value = False
        mock_state_path.write_text = _write
        mock_fetch.return_value = _ohlcv("DEAD1")

        run(
            constituent_path=hist_file,
            data_dir=tmp_path / "data",
            batch_size=0,
            dry_run=False,
        )

        assert "DEAD1" in written["state"]["backfill_completed"]
        assert "DEAD2" in written["state"]["backfill_completed"]

    @patch("market_data.fetch_backfill.fetch_date_range")
    @patch("market_data.fetch_backfill.STATE_FILE")
    def test_existing_ohlcv_file_skipped(
        self, mock_state_path, mock_fetch, hist_file, tmp_path, state_file
    ):
        data_dir = tmp_path / "data"
        save_ticker_data("DEAD1", _ohlcv("DEAD1"), data_dir)

        mock_state_path.exists.return_value = False
        mock_state_path.write_text = state_file.write_text
        mock_fetch.return_value = _ohlcv("DEAD2")

        result = run(
            constituent_path=hist_file,
            data_dir=data_dir,
            batch_size=0,
            dry_run=False,
        )

        assert result["fetched"] == 1
        fetched_tickers = [c.args[0] for c in mock_fetch.call_args_list]
        assert "DEAD1" not in fetched_tickers
        assert "DEAD2" in fetched_tickers


# ---------------------------------------------------------------------------
# run() — batched ohlcv writes
# ---------------------------------------------------------------------------

class TestRunBatchedWrites:
    @pytest.fixture()
    def env(self, tmp_path, monkeypatch):
        from market_data import fetch, fetch_backfill

        hist_file = tmp_path / "constituent_history.parquet"
        _hist([
            ("DEAD1", "2000-01-01", "2010-06-15"),
            ("DEAD2", "2005-01-01", "2015-03-01"),
            ("DEAD3", "2001-01-01", "2012-03-01"),
        ]).to_parquet(hist_file, index=False)
        state_file = tmp_path / "state.json"
        env: dict = {
            "hist_file": hist_file, "data_dir": tmp_path / "data",
            "state_file": state_file, "writes": [],
        }
        monkeypatch.setattr(fetch_backfill, "STATE_FILE", state_file)
        monkeypatch.setattr(fetch_backfill, "SLEEP_BETWEEN_CALLS", 0)
        monkeypatch.setattr(fetch, "DEFAULT_WRITE_BATCH_SYMBOLS", 2)
        monkeypatch.setattr(fetch_backfill, "fetch_date_range", lambda t, s, e: _ohlcv(t))
        real_write = fetch.write_table_counts

        def spy(df, table, data_dir):
            env["writes"].append(sorted(df["symbol"].unique()))
            if env.get("fail_write"):
                raise OSError("disk full")
            return real_write(df, table, data_dir)

        monkeypatch.setattr(fetch, "write_table_counts", spy)
        return env

    def _run(self, env):
        return run(
            constituent_path=env["hist_file"], data_dir=env["data_dir"],
            batch_size=0, dry_run=False,
        )

    def test_one_write_per_batch_of_symbols(self, env):
        result = self._run(env)

        assert [len(w) for w in env["writes"]] == [2, 1]
        assert result["fetched"] == 3
        saved = json.loads(env["state_file"].read_text())
        assert sorted(saved["backfill_completed"]) == ["DEAD1", "DEAD2", "DEAD3"]

    def test_failed_write_leaves_batch_incomplete(self, env):
        env["fail_write"] = True

        result = self._run(env)

        assert result["fetched"] == 0
        assert result["failed"] == 3
        saved = json.loads(env["state_file"].read_text())
        assert saved["backfill_completed"] == []
        assert set(saved["backfill_failures"]) == {"DEAD1", "DEAD2", "DEAD3"}

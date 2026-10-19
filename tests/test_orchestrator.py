"""
Tests for orchestrator.py: load_state(), save_state(), load_ordered_tickers().

All tests use monkeypatching and tmp_path to avoid touching real files.
No network calls or pipeline steps are exercised here.
"""

import json
from datetime import date

import pandas as pd
import pytest

from market_data import orchestrator

# ---------------------------------------------------------------------------
# load_state() / save_state()
# ---------------------------------------------------------------------------


class TestLoadState:
    def test_returns_defaults_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(orchestrator, "STATE_FILE", tmp_path / "state.json")
        state = orchestrator.load_state()

        assert state["onboarded"] == []
        assert state["last_run"] is None
        assert state["last_ticker_refresh"] is None
        assert state["last_fundamentals_run"] is None
        assert state["options_cycle"] == []

    def test_reads_onboarded_list(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)
        state_file.write_text(json.dumps({"onboarded": ["AAPL", "MSFT"]}))

        state = orchestrator.load_state()
        assert state["onboarded"] == ["AAPL", "MSFT"]

    def test_handles_partial_state_file(self, tmp_path, monkeypatch):
        """Missing keys in state.json get sensible defaults."""
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)
        state_file.write_text(json.dumps({"onboarded": ["AAPL"], "last_run": "2024-01-10"}))

        state = orchestrator.load_state()
        assert state["onboarded"] == ["AAPL"]
        assert state["last_run"] == "2024-01-10"
        assert state["last_ticker_refresh"] is None
        assert state["options_cycle"] == []

    def test_handles_empty_json_object(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)
        state_file.write_text(json.dumps({}))

        state = orchestrator.load_state()
        assert state["onboarded"] == []
        assert state["last_run"] is None


class TestSaveState:
    def test_creates_file(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)

        orchestrator.save_state({"onboarded": ["AAPL"], "last_run": "2024-01-01"})
        assert state_file.exists()

    def test_written_as_valid_json(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)

        orchestrator.save_state({"onboarded": ["AAPL"], "last_run": "2024-01-01"})
        raw = json.loads(state_file.read_text())
        assert raw["onboarded"] == ["AAPL"]
        assert raw["last_run"] == "2024-01-01"

    def test_round_trip_full_state(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)

        original = {
            "onboarded": ["AAPL", "MSFT", "GOOGL"],
            "last_run": "2024-03-15",
            "last_ticker_refresh": "2024-02-10",
            "last_fundamentals_run": "2024-02-15",
            "options_cycle": ["AAPL", "MSFT"],
        }
        orchestrator.save_state(original)
        loaded = orchestrator.load_state()

        assert loaded["onboarded"] == original["onboarded"]
        assert loaded["last_run"] == original["last_run"]
        assert loaded["last_ticker_refresh"] == original["last_ticker_refresh"]
        assert loaded["last_fundamentals_run"] == original["last_fundamentals_run"]
        assert loaded["options_cycle"] == original["options_cycle"]

    def test_overwrites_previous_state(self, tmp_path, monkeypatch):
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)

        orchestrator.save_state({"onboarded": ["AAPL"], "last_run": "2024-01-01"})
        orchestrator.save_state({"onboarded": ["AAPL", "MSFT"], "last_run": "2024-01-02"})

        loaded = orchestrator.load_state()
        assert loaded["onboarded"] == ["AAPL", "MSFT"]
        assert loaded["last_run"] == "2024-01-02"

    def test_date_objects_serialised_as_strings(self, tmp_path, monkeypatch):
        """save_state uses default=str so date objects don't raise TypeError."""
        state_file = tmp_path / "state.json"
        monkeypatch.setattr(orchestrator, "STATE_FILE", state_file)

        # Should not raise even with a date value
        orchestrator.save_state({"last_run": date(2024, 1, 15), "onboarded": []})
        raw = json.loads(state_file.read_text())
        assert raw["last_run"] == "2024-01-15"


# ---------------------------------------------------------------------------
# load_ordered_tickers()
# ---------------------------------------------------------------------------


class TestLoadOrderedTickers:
    def test_raises_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tmp_path / "nonexistent.csv")
        with pytest.raises(FileNotFoundError):
            orchestrator.load_ordered_tickers()

    def test_raises_when_symbol_column_absent(self, tmp_path, monkeypatch):
        tickers_file = tmp_path / "tickers.csv"
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tickers_file)
        pd.DataFrame({"ticker": ["AAPL"]}).to_csv(tickers_file, index=False)

        with pytest.raises(ValueError, match="symbol"):
            orchestrator.load_ordered_tickers()

    def test_returns_list_of_strings(self, tmp_path, monkeypatch):
        tickers_file = tmp_path / "tickers.csv"
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tickers_file)
        pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "market_value": [3e12, 2.8e12],
                "index": ["SP500", "SP500"],
            }
        ).to_csv(tickers_file, index=False)

        result = orchestrator.load_ordered_tickers()
        assert isinstance(result, list)
        assert all(isinstance(s, str) for s in result)

    def test_preserves_csv_row_order(self, tmp_path, monkeypatch):
        """load_ordered_tickers returns symbols in the order they appear in the CSV."""
        tickers_file = tmp_path / "tickers.csv"
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tickers_file)

        symbols = ["BIG", "MEDIUM", "SMALL"]
        pd.DataFrame(
            {
                "symbol": symbols,
                "market_value": [1_000_000.0, 500_000.0, 100_000.0],
                "index": ["SP500"] * 3,
                "date_added": ["2024-01-01"] * 3,
            }
        ).to_csv(tickers_file, index=False)

        result = orchestrator.load_ordered_tickers()
        assert result == symbols

    def test_drops_nan_symbols(self, tmp_path, monkeypatch):
        tickers_file = tmp_path / "tickers.csv"
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tickers_file)

        # Write a CSV where one symbol cell is blank
        tickers_file.write_text("symbol,market_value\nAAPL,1000\n,500\nMSFT,900\n")
        result = orchestrator.load_ordered_tickers()
        assert "" not in result
        assert "AAPL" in result
        assert "MSFT" in result

    def test_all_symbols_present(self, tmp_path, monkeypatch):
        tickers_file = tmp_path / "tickers.csv"
        monkeypatch.setattr(orchestrator, "TICKERS_FILE", tickers_file)

        expected = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
        pd.DataFrame({"symbol": expected, "market_value": range(len(expected))}).to_csv(
            tickers_file, index=False
        )

        result = orchestrator.load_ordered_tickers()
        assert set(result) == set(expected)
        assert len(result) == len(expected)


# ---------------------------------------------------------------------------
# step_options()
# ---------------------------------------------------------------------------


class TestStepOptions:
    @pytest.fixture()
    def options_calls(self, tmp_path, monkeypatch):
        from market_data import fetch_options

        calls: dict = {"results": [], "run": {}}
        monkeypatch.setattr(orchestrator, "DATA_DIR", tmp_path)
        monkeypatch.setattr(fetch_options, "get_sp500_symbols", lambda onboarded: ["AAA", "BBB"])
        monkeypatch.setattr(fetch_options, "get_etf_symbols", lambda onboarded: [])
        monkeypatch.setattr(orchestrator.metrics, "start_run", lambda name: None)
        monkeypatch.setattr(orchestrator.metrics, "finish_run", lambda: None)
        monkeypatch.setattr(
            orchestrator.metrics,
            "record_symbol_result",
            lambda s, success, **kw: calls["results"].append((s, success)),
        )

        def fake_run(symbols, data_dir, max_expiries):
            calls["run"] = {"symbols": symbols, "data_dir": data_dir}
            return calls["rows"]

        monkeypatch.setattr(fetch_options, "run", fake_run)
        return calls

    def test_success_comes_from_rows_fetched(self, options_calls, tmp_path):
        options_calls["rows"] = {"AAA": 12, "BBB": 0}
        orchestrator.step_options({}, {"AAA", "BBB"}, batch_size=10, max_expiries=2)

        assert options_calls["run"]["data_dir"] == tmp_path
        assert options_calls["results"] == [("AAA", True), ("BBB", False)]

    def test_all_empty_logs_silent_failure(self, options_calls, caplog):
        options_calls["rows"] = {"AAA": 0, "BBB": 0}
        orchestrator.step_options({}, {"AAA", "BBB"}, batch_size=10, max_expiries=2)

        assert "OPTIONS silent failure" in caplog.text

    def test_stale_flat_file_does_not_count_as_success(self, options_calls, tmp_path):
        (tmp_path / "options").mkdir()
        (tmp_path / "options" / "AAA.parquet").touch()
        options_calls["rows"] = {"AAA": 0, "BBB": 0}
        orchestrator.step_options({}, {"AAA", "BBB"}, batch_size=10, max_expiries=2)

        assert ("AAA", False) in options_calls["results"]

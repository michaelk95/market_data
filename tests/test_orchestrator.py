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
from market_data.fetch import save_ticker_data
from market_data.storage import read_table

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
        assert state["split_refetch_pending"] == {}

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


# ---------------------------------------------------------------------------
# step_onboard() / step_update()  — batched ohlcv writes
# ---------------------------------------------------------------------------


def _bars(symbol: str, start: str, n: int = 3) -> pd.DataFrame:
    days = pd.date_range(start, periods=n, freq="B").date
    return pd.DataFrame({
        "date": days, "symbol": symbol, "open": 1.0, "high": 2.0,
        "low": 0.5, "close": 1.5, "volume": 100.0,
    })


@pytest.fixture()
def ohlcv_env(tmp_path, monkeypatch):
    """Point the orchestrator at tmp_path and record metrics and storage writes."""
    from market_data import fetch

    env: dict = {"results": [], "writes": [], "frames": {}}
    monkeypatch.setattr(orchestrator, "DATA_DIR", tmp_path)
    monkeypatch.setattr(orchestrator, "SLEEP_BETWEEN_CALLS", 0)
    monkeypatch.setattr(fetch, "DEFAULT_WRITE_BATCH_SYMBOLS", 2)
    monkeypatch.setattr(orchestrator.metrics, "start_run", lambda name: None)
    monkeypatch.setattr(orchestrator.metrics, "finish_run", lambda: None)
    monkeypatch.setattr(
        orchestrator.metrics,
        "record_symbol_result",
        lambda s, success, rows_written=0, reason=None: env["results"].append(
            (s, success, rows_written)
        ),
    )
    real_write = fetch.write_table_counts

    def spy(df, table, data_dir):
        env["writes"].append(sorted(df["symbol"].unique()))
        if env.get("fail_write"):
            raise OSError("disk full")
        return real_write(df, table, data_dir)

    monkeypatch.setattr(fetch, "write_table_counts", spy)

    def fake_fetch(symbol, **kwargs):
        frame = env["frames"][symbol]
        if isinstance(frame, Exception):
            raise frame
        return frame

    def fake_fetch_with_splits(symbol, **kwargs):
        return fake_fetch(symbol, **kwargs), env.get("splits", {}).get(symbol, [])

    monkeypatch.setattr(orchestrator, "fetch_history", fake_fetch)
    monkeypatch.setattr(orchestrator, "fetch_incremental_with_splits", fake_fetch_with_splits)
    return env


class TestStepOnboard:
    def test_writes_once_per_batch_of_symbols(self, ohlcv_env, tmp_path):
        ohlcv_env["frames"] = {s: _bars(s, "2023-12-28") for s in ("AAA", "BBB", "CCC")}
        state: dict = {}

        onboarded, failed = orchestrator.step_onboard(["AAA", "BBB", "CCC"], 10, set(), state)

        assert ohlcv_env["writes"] == [["AAA", "BBB"], ["CCC"]]
        assert onboarded == {"AAA", "BBB", "CCC"}
        assert failed == set()
        assert ohlcv_env["results"] == [("AAA", True, 3), ("BBB", True, 3), ("CCC", True, 3)]
        assert len(read_table("ohlcv", tmp_path)) == 9

    def test_fetch_error_and_empty_frame_do_not_block_batch(self, ohlcv_env):
        ohlcv_env["frames"] = {
            "AAA": _bars("AAA", "2024-01-02"),
            "ERR": RuntimeError("boom"),
            "NONE": _bars("NONE", "2024-01-02", n=0),
            "BBB": _bars("BBB", "2024-01-02"),
        }
        state: dict = {}

        onboarded, failed = orchestrator.step_onboard(["AAA", "ERR", "NONE", "BBB"], 10, set(), state)

        assert onboarded == {"AAA", "BBB"}
        assert failed == {"ERR"}
        assert ohlcv_env["writes"] == [["AAA", "BBB"]]
        assert "ERR" in state["fetch_failures"] and "NONE" in state["fetch_failures"]

    def test_failed_write_marks_whole_batch_failed(self, ohlcv_env):
        ohlcv_env["frames"] = {s: _bars(s, "2024-01-02") for s in ("AAA", "BBB")}
        ohlcv_env["fail_write"] = True
        state: dict = {}

        onboarded, failed = orchestrator.step_onboard(["AAA", "BBB"], 10, set(), state)

        assert onboarded == set()
        assert failed == {"AAA", "BBB"}
        assert set(state["fetch_failures"]) == {"AAA", "BBB"}
        assert [r[:2] for r in ohlcv_env["results"]] == [("AAA", False), ("BBB", False)]


class TestStepUpdate:
    def test_per_symbol_counts_with_one_write_per_batch(self, ohlcv_env, tmp_path):
        save_ticker_data("AAA", _bars("AAA", "2024-01-02", n=2), tmp_path)
        ohlcv_env["writes"].clear()
        ohlcv_env["frames"] = {
            "AAA": _bars("AAA", "2024-01-02", n=4),  # 2 already stored
            "BBB": _bars("BBB", "2024-01-02", n=1),
        }

        results = orchestrator.step_update(["AAA", "BBB"], date(2024, 1, 2), {})

        assert results == {"AAA": 2, "BBB": 1}
        assert ohlcv_env["writes"] == [["AAA", "BBB"]]
        assert ohlcv_env["results"] == [("AAA", True, 2), ("BBB", True, 1)]

    def test_failed_write_records_failures(self, ohlcv_env):
        ohlcv_env["frames"] = {"AAA": _bars("AAA", "2024-01-02")}
        ohlcv_env["fail_write"] = True
        state: dict = {}

        results = orchestrator.step_update(["AAA"], date(2024, 1, 2), state)

        assert results == {"AAA": 0}
        assert "AAA" in state["fetch_failures"]
        assert ohlcv_env["results"] == [("AAA", False, 0)]


# ---------------------------------------------------------------------------
# Splits: step_update() queues them, step_split_refetch() re-downloads
# ---------------------------------------------------------------------------

# A 2:1 split on the 4th bar (2026-01-08): stored bars before it are on the
# old basis, a fresh download has them halved.
STORED_CLOSES = [100.0, 102.0, 104.0, 52.0, 53.0]
FRESH_CLOSES = [50.0, 51.0, 52.0, 52.0, 53.0]


def _closes(symbol: str, closes: list[float], start: str = "2026-01-05") -> pd.DataFrame:
    frame = _bars(symbol, start, n=len(closes))
    for col in ("open", "high", "low", "close"):
        frame[col] = closes
    return frame


def _pending(symbol: str, added: str) -> dict:
    return {symbol: {"split_date": "2026-01-08", "ratio": 2.0, "added": added}}


class TestStepUpdateSplits:
    def test_split_queues_symbol(self, ohlcv_env):
        ohlcv_env["frames"] = {s: _bars(s, "2026-01-08", n=1) for s in ("AAA", "BBB")}
        ohlcv_env["splits"] = {"AAA": [(date(2026, 1, 8), 2.0)]}
        state: dict = {}

        orchestrator.step_update(["AAA", "BBB"], date(2026, 1, 8), state)

        assert state["split_refetch_pending"] == {
            "AAA": {"split_date": "2026-01-08", "ratio": 2.0, "added": str(date.today())},
        }

    def test_requeue_keeps_first_added_date(self, ohlcv_env):
        ohlcv_env["frames"] = {"AAA": _bars("AAA", "2026-01-08", n=1)}
        ohlcv_env["splits"] = {"AAA": [(date(2026, 1, 8), 2.0)]}
        state: dict = {"split_refetch_pending": _pending("AAA", added="2026-01-08")}

        orchestrator.step_update(["AAA"], date(2026, 1, 8), state)

        assert state["split_refetch_pending"]["AAA"]["added"] == "2026-01-08"


@pytest.fixture()
def refetch_env(tmp_path, monkeypatch):
    """Store AAA and BBB with an unadjusted split; fake the re-download."""
    from market_data import splits

    monkeypatch.setattr(orchestrator, "DATA_DIR", tmp_path)
    monkeypatch.setattr(orchestrator, "SLEEP_BETWEEN_CALLS", 0)
    save_ticker_data("AAA", _closes("AAA", STORED_CLOSES), tmp_path)
    save_ticker_data("BBB", _closes("BBB", STORED_CLOSES), tmp_path)
    env: dict = {"frames": {}, "calls": []}

    def fake_range(symbol, start, end=None):
        env["calls"].append(symbol)
        frame = env["frames"][symbol]
        if isinstance(frame, Exception):
            raise frame
        return frame

    monkeypatch.setattr(splits, "fetch_date_range", fake_range)
    return env


def _stored_closes(symbol: str, data_dir) -> list[float]:
    df = read_table("ohlcv", data_dir, symbols=[symbol]).sort_values("period_start_date")
    return df["close"].tolist()


TODAY = str(date.today())


class TestStepSplitRefetch:
    def test_replaces_history_and_clears_pending(self, refetch_env, tmp_path):
        refetch_env["frames"] = {"AAA": _closes("AAA", FRESH_CLOSES)}
        state = {"split_refetch_pending": _pending("AAA", TODAY)}

        assert orchestrator.step_split_refetch(state) == {"AAA"}

        assert state["split_refetch_pending"] == {}
        assert _stored_closes("AAA", tmp_path) == FRESH_CLOSES
        assert _stored_closes("BBB", tmp_path) == STORED_CLOSES

    def test_download_error_stays_pending(self, refetch_env, tmp_path):
        refetch_env["frames"] = {"AAA": RuntimeError("timeout")}
        state = {"split_refetch_pending": _pending("AAA", TODAY)}

        assert orchestrator.step_split_refetch(state) == set()

        assert "AAA" in state["split_refetch_pending"]
        assert _stored_closes("AAA", tmp_path) == STORED_CLOSES

    def test_write_error_stays_pending(self, refetch_env, monkeypatch):
        from market_data import splits

        refetch_env["frames"] = {"AAA": _closes("AAA", FRESH_CLOSES)}
        state = {"split_refetch_pending": _pending("AAA", TODAY)}

        def boom(df, table, data_dir):
            raise OSError("disk full")

        monkeypatch.setattr(splits, "replace_rows", boom)
        assert orchestrator.step_split_refetch(state) == set()
        assert "AAA" in state["split_refetch_pending"]

    def test_gate_rejection_leaves_data_and_clears_pending(self, refetch_env, tmp_path, caplog):
        refetch_env["frames"] = {"AAA": _closes("AAA", FRESH_CLOSES[:2])}
        state = {"split_refetch_pending": _pending("AAA", TODAY)}

        assert orchestrator.step_split_refetch(state) == set()

        assert state["split_refetch_pending"] == {}
        assert _stored_closes("AAA", tmp_path) == STORED_CLOSES
        assert "repair_split_adjustment" in caplog.text

    def test_unadjusted_download_stays_pending(self, refetch_env, tmp_path):
        # Yahoo has not adjusted the history yet: the download still jumps.
        refetch_env["frames"] = {"AAA": _closes("AAA", STORED_CLOSES)}
        state = {"split_refetch_pending": _pending("AAA", TODAY)}

        assert orchestrator.step_split_refetch(state) == set()
        assert "AAA" in state["split_refetch_pending"]

    def test_already_consistent_is_cleared_without_write(self, refetch_env, tmp_path, monkeypatch):
        from market_data import splits

        save_ticker_data("CCC", _closes("CCC", FRESH_CLOSES), tmp_path)
        refetch_env["frames"] = {"CCC": _closes("CCC", FRESH_CLOSES)}
        state = {"split_refetch_pending": _pending("CCC", TODAY)}
        monkeypatch.setattr(splits, "replace_rows", lambda *a: pytest.fail("no write expected"))

        assert orchestrator.step_split_refetch(state) == set()
        assert state["split_refetch_pending"] == {}

    def test_expired_entry_is_dropped_without_download(self, refetch_env, caplog):
        state = {"split_refetch_pending": _pending("AAA", added="2020-01-01")}

        assert orchestrator.step_split_refetch(state) == set()

        assert state["split_refetch_pending"] == {}
        assert refetch_env["calls"] == []
        assert "repair_split_adjustment" in caplog.text

    def test_one_write_for_all_symbols(self, refetch_env, monkeypatch):
        from market_data import splits

        refetch_env["frames"] = {s: _closes(s, FRESH_CLOSES) for s in ("AAA", "BBB")}
        state = {"split_refetch_pending": {**_pending("AAA", TODAY), **_pending("BBB", TODAY)}}
        writes = []
        real = splits.replace_rows

        def spy(df, table, data_dir):
            writes.append(sorted(df["symbol"].unique()))
            return real(df, table, data_dir)

        monkeypatch.setattr(splits, "replace_rows", spy)
        assert orchestrator.step_split_refetch(state) == {"AAA", "BBB"}
        assert writes == [["AAA", "BBB"]]

    def test_nothing_pending_does_nothing(self, refetch_env):
        assert orchestrator.step_split_refetch({}) == set()
        assert refetch_env["calls"] == []

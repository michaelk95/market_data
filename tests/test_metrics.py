"""
Tests for metrics.py: start_run(), record_symbol_result(), finish_run(),
persistence to metrics.json, load_history(), and 90-day retention pruning.
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import market_data.metrics as metrics_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_entry(days_ago: int, data_type: str = "onboard") -> dict:
    """Return a fake run dict with start_time `days_ago` days in the past."""
    ts = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    return {
        "data_type": data_type,
        "start_time": ts,
        "end_time": ts,
        "duration_seconds": 1.0,
        "symbols_attempted": 0,
        "symbols_succeeded": 0,
        "symbols_failed": [],
        "rows_written": {},
    }


# ---------------------------------------------------------------------------
# start_run / finish_run lifecycle
# ---------------------------------------------------------------------------

class TestRunLifecycle:
    def setup_method(self):
        """Reset module state before each test."""
        metrics_mod._current_run = None

    def test_start_run_initialises_state(self):
        metrics_mod.start_run("onboard")
        assert metrics_mod._current_run is not None
        assert metrics_mod._current_run["data_type"] == "onboard"
        assert metrics_mod._current_run["symbols_attempted"] == 0
        assert metrics_mod._current_run["symbols_succeeded"] == 0
        assert metrics_mod._current_run["symbols_failed"] == []
        assert metrics_mod._current_run["rows_written"] == {}
        assert metrics_mod._current_run["end_time"] is None

    def test_start_run_sets_iso_start_time(self):
        metrics_mod.start_run("update")
        ts = metrics_mod._current_run["start_time"]
        parsed = datetime.fromisoformat(ts)
        assert parsed is not None

    def test_finish_run_clears_state(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()
        assert metrics_mod._current_run is None

    def test_finish_run_without_start_is_noop(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod._current_run = None
        metrics_mod.finish_run()  # should not raise
        assert not (tmp_path / "metrics.json").exists()

    def test_finish_run_computes_duration(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads((tmp_path / "metrics.json").read_text())
        run = data["runs"][0]
        assert run["duration_seconds"] >= 0
        assert run["end_time"] is not None

    def test_finish_run_writes_data_type(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("fundamentals")
        metrics_mod.finish_run()

        data = json.loads((tmp_path / "metrics.json").read_text())
        assert data["runs"][0]["data_type"] == "fundamentals"


# ---------------------------------------------------------------------------
# record_symbol_result
# ---------------------------------------------------------------------------

class TestRecordSymbolResult:
    def setup_method(self):
        metrics_mod._current_run = None

    def test_record_success_increments_counts(self):
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=100)

        assert metrics_mod._current_run["symbols_attempted"] == 1
        assert metrics_mod._current_run["symbols_succeeded"] == 1
        assert metrics_mod._current_run["rows_written"]["onboard"] == 100

    def test_record_failure_adds_to_failed_list(self):
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("BADTICKER", success=False, reason="no data")

        assert metrics_mod._current_run["symbols_attempted"] == 1
        assert metrics_mod._current_run["symbols_succeeded"] == 0
        failed = metrics_mod._current_run["symbols_failed"]
        assert len(failed) == 1
        assert failed[0]["symbol"] == "BADTICKER"
        assert failed[0]["reason"] == "no data"

    def test_record_failure_without_reason_uses_unknown(self):
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("X", success=False)
        assert metrics_mod._current_run["symbols_failed"][0]["reason"] == "unknown"

    def test_rows_written_accumulates_across_symbols(self):
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=100)
        metrics_mod.record_symbol_result("MSFT", success=True, rows_written=200)

        assert metrics_mod._current_run["rows_written"]["onboard"] == 300

    def test_rows_written_uses_run_data_type(self):
        metrics_mod.start_run("options")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=500)

        assert "options" in metrics_mod._current_run["rows_written"]
        assert metrics_mod._current_run["rows_written"]["options"] == 500

    def test_multiple_failures_accumulate(self):
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("A", success=False, reason="err1")
        metrics_mod.record_symbol_result("B", success=False, reason="err2")

        failed = metrics_mod._current_run["symbols_failed"]
        assert len(failed) == 2
        symbols = {f["symbol"] for f in failed}
        assert symbols == {"A", "B"}

    def test_record_without_active_run_is_noop(self):
        metrics_mod._current_run = None
        # Should not raise
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=100)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def setup_method(self):
        metrics_mod._current_run = None

    def test_finish_run_creates_metrics_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()
        assert (tmp_path / "metrics.json").exists()

    def test_metrics_file_is_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()
        data = json.loads((tmp_path / "metrics.json").read_text())
        assert "runs" in data
        assert isinstance(data["runs"], list)

    def test_run_data_persisted_correctly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=250)
        metrics_mod.record_symbol_result("BAD", success=False, reason="err")
        metrics_mod.finish_run()

        data = json.loads((tmp_path / "metrics.json").read_text())
        run = data["runs"][0]
        assert run["symbols_attempted"] == 2
        assert run["symbols_succeeded"] == 1
        assert run["symbols_failed"] == [{"symbol": "BAD", "reason": "err"}]
        assert run["rows_written"]["onboard"] == 250

    def test_multiple_runs_accumulate(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)

        for _ in range(3):
            metrics_mod.start_run("update")
            metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 3

    def test_creates_logs_subdirectory(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "logs" / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()
        assert metrics_file.exists()

    def test_corrupt_existing_file_starts_fresh(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        metrics_file.write_text("not valid json {{{{")

        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 1


# ---------------------------------------------------------------------------
# 90-day retention pruning
# ---------------------------------------------------------------------------

class TestRetentionPruning:
    def setup_method(self):
        metrics_mod._current_run = None

    def test_old_runs_are_pruned(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        monkeypatch.setattr(metrics_mod, "RETENTION_DAYS", 90)

        old_run = _make_run_entry(days_ago=100)
        metrics_file.write_text(json.dumps({"runs": [old_run]}))

        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 1
        assert data["runs"][0]["duration_seconds"] >= 0

    def test_recent_runs_are_kept(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        monkeypatch.setattr(metrics_mod, "RETENTION_DAYS", 90)

        runs = [_make_run_entry(days_ago=10), _make_run_entry(days_ago=50)]
        metrics_file.write_text(json.dumps({"runs": runs}))

        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 3

    def test_exactly_at_boundary_is_kept(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        monkeypatch.setattr(metrics_mod, "RETENTION_DAYS", 90)

        # A run exactly 89 days ago should survive
        runs = [_make_run_entry(days_ago=89)]
        metrics_file.write_text(json.dumps({"runs": runs}))

        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 2

    def test_custom_retention_days(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        monkeypatch.setattr(metrics_mod, "RETENTION_DAYS", 7)

        old = _make_run_entry(days_ago=10)
        recent = _make_run_entry(days_ago=3)
        metrics_file.write_text(json.dumps({"runs": [old, recent]}))

        metrics_mod.start_run("onboard")
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        # old run pruned, recent + new run survive
        assert len(data["runs"]) == 2


# ---------------------------------------------------------------------------
# load_history
# ---------------------------------------------------------------------------

class TestLoadHistory:
    def setup_method(self):
        metrics_mod._current_run = None

    def test_returns_empty_when_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        result = metrics_mod.load_history()
        assert result == {"runs": []}

    def test_loads_existing_file(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        metrics_file.write_text(json.dumps({"runs": [{"start_time": "2024-01-01"}]}))
        result = metrics_mod.load_history()
        assert len(result["runs"]) == 1

    def test_returns_empty_on_corrupt_file(self, tmp_path, monkeypatch):
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)
        metrics_file.write_text("not valid json {{{{")
        result = metrics_mod.load_history()
        assert result == {"runs": []}

    def test_load_history_reflects_completed_runs(self, tmp_path, monkeypatch):
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", tmp_path / "metrics.json")
        metrics_mod.start_run("macro")
        metrics_mod.finish_run()

        history = metrics_mod.load_history()
        assert len(history["runs"]) == 1
        assert history["runs"][0]["data_type"] == "macro"
        assert "start_time" in history["runs"][0]
        assert "end_time" in history["runs"][0]
        assert "duration_seconds" in history["runs"][0]


# ---------------------------------------------------------------------------
# File isolation (concurrent runs don't corrupt)
# ---------------------------------------------------------------------------

class TestFileIsolation:
    def setup_method(self):
        metrics_mod._current_run = None

    def test_interleaved_writes_use_separate_paths(self, tmp_path, monkeypatch):
        """Two separate METRICS_FILE paths never cross-contaminate."""
        file_a = tmp_path / "a" / "metrics.json"
        file_b = tmp_path / "b" / "metrics.json"
        file_a.parent.mkdir()
        file_b.parent.mkdir()

        # Write to file A
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", file_a)
        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=100)
        metrics_mod.finish_run()

        # Write to file B
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", file_b)
        metrics_mod.start_run("update")
        metrics_mod.record_symbol_result("MSFT", success=True, rows_written=50)
        metrics_mod.finish_run()

        data_a = json.loads(file_a.read_text())
        data_b = json.loads(file_b.read_text())

        assert len(data_a["runs"]) == 1
        assert data_a["runs"][0]["data_type"] == "onboard"
        assert len(data_b["runs"]) == 1
        assert data_b["runs"][0]["data_type"] == "update"

    def test_second_run_appends_not_overwrites(self, tmp_path, monkeypatch):
        """Each finish_run appends to the existing file rather than replacing it."""
        metrics_file = tmp_path / "metrics.json"
        monkeypatch.setattr(metrics_mod, "METRICS_FILE", metrics_file)

        metrics_mod.start_run("onboard")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=100)
        metrics_mod.finish_run()

        metrics_mod.start_run("update")
        metrics_mod.record_symbol_result("AAPL", success=True, rows_written=5)
        metrics_mod.finish_run()

        data = json.loads(metrics_file.read_text())
        assert len(data["runs"]) == 2
        assert data["runs"][0]["data_type"] == "onboard"
        assert data["runs"][1]["data_type"] == "update"

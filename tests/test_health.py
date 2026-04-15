"""
Tests for health.py — health_check() freshness/staleness detection.

All tests use tmp_path and never touch the real data/ directory.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from market_data.health import health_check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(path: Path, age_days: float = 0.0) -> None:
    """Write a minimal parquet file and backdate its mtime by age_days."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": [1]}).to_parquet(path, index=False)

    if age_days != 0.0:
        offset_seconds = age_days * 86400.0
        stat = path.stat()
        new_mtime = stat.st_mtime - offset_seconds
        os.utime(path, (new_mtime, new_mtime))


def _single_type_dir(tmp_path: Path, data_type: str, age_days: float) -> Path:
    """Create a data dir with one parquet file at the given age and return the root."""
    root = tmp_path / "data"
    _write_parquet(root / data_type / "TEST.parquet", age_days=age_days)
    return root


# ---------------------------------------------------------------------------
# Report structure
# ---------------------------------------------------------------------------

class TestReportStructure:
    EXPECTED_KEYS = {"last_updated", "expected_within_days", "is_stale", "age_days"}

    def test_all_four_data_types_present(self, tmp_path):
        root = tmp_path / "data"
        result = health_check(data_dir=root)
        assert set(result.keys()) == {"ohlcv", "options", "fundamentals", "macro"}

    def test_each_entry_has_required_keys(self, tmp_path):
        root = tmp_path / "data"
        result = health_check(data_dir=root)
        for data_type, report in result.items():
            assert self.EXPECTED_KEYS == set(report.keys()), (
                f"{data_type} missing keys: {self.EXPECTED_KEYS - set(report.keys())}"
            )

    def test_fresh_entry_types(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=0.5)
        result = health_check(data_dir=root)
        r = result["ohlcv"]
        assert isinstance(r["last_updated"], datetime)
        assert r["last_updated"].tzinfo is not None  # timezone-aware
        assert isinstance(r["expected_within_days"], int)
        assert isinstance(r["is_stale"], bool)
        assert isinstance(r["age_days"], float)

    def test_missing_entry_types(self, tmp_path):
        root = tmp_path / "data"  # no subdirs created
        result = health_check(data_dir=root)
        r = result["ohlcv"]
        assert r["last_updated"] is None
        assert r["age_days"] is None
        assert r["is_stale"] is True


# ---------------------------------------------------------------------------
# Missing directory / no parquet files
# ---------------------------------------------------------------------------

class TestMissingData:
    def test_missing_directory_is_stale(self, tmp_path):
        root = tmp_path / "data"  # ohlcv subdir never created
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is True

    def test_missing_directory_logs_warning(self, tmp_path, caplog):
        root = tmp_path / "data"
        with caplog.at_level(logging.WARNING, logger="market_data.health"):
            health_check(data_dir=root)
        assert any("not found" in r.message or "missing" in r.message.lower()
                   for r in caplog.records)

    def test_empty_directory_is_stale(self, tmp_path):
        root = tmp_path / "data"
        (root / "ohlcv").mkdir(parents=True)  # dir exists but no parquet files
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is True

    def test_missing_all_four_dirs(self, tmp_path):
        root = tmp_path / "data"
        result = health_check(data_dir=root)
        for dt in ("ohlcv", "options", "fundamentals", "macro"):
            assert result[dt]["is_stale"] is True


# ---------------------------------------------------------------------------
# Fresh data
# ---------------------------------------------------------------------------

class TestFreshData:
    def test_ohlcv_fresh_within_2_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=1.0)
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is False

    def test_options_fresh_within_14_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "options", age_days=10.0)
        result = health_check(data_dir=root)
        assert result["options"]["is_stale"] is False

    def test_fundamentals_fresh_within_35_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "fundamentals", age_days=30.0)
        result = health_check(data_dir=root)
        assert result["fundamentals"]["is_stale"] is False

    def test_macro_fresh_within_7_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "macro", age_days=5.0)
        result = health_check(data_dir=root)
        assert result["macro"]["is_stale"] is False

    def test_fresh_expected_within_days_correct(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=0.5)
        result = health_check(data_dir=root)
        assert result["ohlcv"]["expected_within_days"] == 2

    def test_fresh_age_days_is_reasonable(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=1.0)
        result = health_check(data_dir=root)
        assert 0.9 < result["ohlcv"]["age_days"] < 1.1

    def test_fresh_data_logs_info(self, tmp_path, caplog):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=0.5)
        with caplog.at_level(logging.INFO, logger="market_data.health"):
            health_check(data_dir=root)
        assert any("ok" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# Stale data
# ---------------------------------------------------------------------------

class TestStaleData:
    def test_ohlcv_stale_beyond_2_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=3.0)
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is True

    def test_options_stale_beyond_14_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "options", age_days=20.0)
        result = health_check(data_dir=root)
        assert result["options"]["is_stale"] is True

    def test_fundamentals_stale_beyond_35_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "fundamentals", age_days=40.0)
        result = health_check(data_dir=root)
        assert result["fundamentals"]["is_stale"] is True

    def test_macro_stale_beyond_7_days(self, tmp_path):
        root = _single_type_dir(tmp_path, "macro", age_days=10.0)
        result = health_check(data_dir=root)
        assert result["macro"]["is_stale"] is True

    def test_stale_age_days_is_reasonable(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=5.0)
        result = health_check(data_dir=root)
        assert 4.9 < result["ohlcv"]["age_days"] < 5.1

    def test_stale_expected_within_days_correct(self, tmp_path):
        root = _single_type_dir(tmp_path, "macro", age_days=10.0)
        result = health_check(data_dir=root)
        assert result["macro"]["expected_within_days"] == 7

    def test_stale_last_updated_is_datetime(self, tmp_path):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=5.0)
        result = health_check(data_dir=root)
        assert isinstance(result["ohlcv"]["last_updated"], datetime)

    def test_stale_data_logs_warning(self, tmp_path, caplog):
        root = _single_type_dir(tmp_path, "ohlcv", age_days=5.0)
        with caplog.at_level(logging.WARNING, logger="market_data.health"):
            health_check(data_dir=root)
        assert any("stale" in r.message.lower() for r in caplog.records)

    def test_most_recent_file_is_used(self, tmp_path):
        """health_check picks the newest file, so one fresh file among old ones = ok."""
        root = tmp_path / "data"
        _write_parquet(root / "ohlcv" / "OLD.parquet", age_days=20.0)
        _write_parquet(root / "ohlcv" / "NEW.parquet", age_days=0.5)
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is False


# ---------------------------------------------------------------------------
# Summary logging
# ---------------------------------------------------------------------------

class TestSummaryLogging:
    def test_summary_warning_when_any_stale(self, tmp_path, caplog):
        root = tmp_path / "data"  # all missing = all stale
        with caplog.at_level(logging.WARNING, logger="market_data.health"):
            health_check(data_dir=root)
        assert any("summary" in r.message.lower() for r in caplog.records)

    def test_summary_info_when_all_fresh(self, tmp_path, caplog):
        root = tmp_path / "data"
        for dt in ("ohlcv", "options", "fundamentals", "macro"):
            _write_parquet(root / dt / "TEST.parquet", age_days=0.1)
        with caplog.at_level(logging.INFO, logger="market_data.health"):
            health_check(data_dir=root)
        messages = [r.message for r in caplog.records]
        assert any("summary" in m.lower() and "fresh" in m.lower() for m in messages)

    def test_mixed_statuses(self, tmp_path, caplog):
        root = tmp_path / "data"
        _write_parquet(root / "ohlcv" / "A.parquet", age_days=0.5)     # fresh
        _write_parquet(root / "options" / "B.parquet", age_days=20.0)  # stale
        result = health_check(data_dir=root)
        assert result["ohlcv"]["is_stale"] is False
        assert result["options"]["is_stale"] is True

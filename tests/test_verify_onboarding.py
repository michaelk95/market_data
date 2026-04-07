"""
Tests for verify_onboarding.py: check() and fix().

Uses tmp_path so no real state.json or data/ directory is touched.
Parquet files are only touched as placeholders (zero-byte touch is sufficient
because check() only inspects filenames, not file contents).
"""

import json
from pathlib import Path

import pytest

from market_data.verify_onboarding import check, fix


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_state(tmp_path: Path, onboarded: list[str]) -> Path:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"onboarded": onboarded}))
    return state_file


def _make_data_dir(tmp_path: Path, symbols: list[str]) -> Path:
    data_dir = tmp_path / "ohlcv"
    data_dir.mkdir(exist_ok=True)
    for sym in symbols:
        (data_dir / f"{sym}.parquet").touch()
    return data_dir


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------


class TestCheck:
    def test_clean_state_no_ghosts_no_orphans(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "MSFT"])
        data_dir = _make_data_dir(tmp_path, ["AAPL", "MSFT"])
        ghosts, orphans = check(state_file, data_dir)

        assert ghosts == []
        assert orphans == []

    def test_ghost_detected_when_file_missing(self, tmp_path):
        """Ticker in state but no file on disk is a ghost."""
        state_file = _make_state(tmp_path, ["AAPL", "GHOST"])
        data_dir = _make_data_dir(tmp_path, ["AAPL"])

        ghosts, orphans = check(state_file, data_dir)
        assert "GHOST" in ghosts
        assert "AAPL" not in ghosts

    def test_orphan_detected_when_file_not_in_state(self, tmp_path):
        """File on disk but absent from state is an orphan."""
        state_file = _make_state(tmp_path, ["AAPL"])
        data_dir = _make_data_dir(tmp_path, ["AAPL", "ORPHAN"])

        ghosts, orphans = check(state_file, data_dir)
        assert "ORPHAN" in orphans
        assert "AAPL" not in orphans

    def test_ghost_and_orphan_simultaneously(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "GHOST"])
        data_dir = _make_data_dir(tmp_path, ["AAPL", "ORPHAN"])

        ghosts, orphans = check(state_file, data_dir)
        assert ghosts == ["GHOST"]
        assert orphans == ["ORPHAN"]

    def test_empty_onboarded_list(self, tmp_path):
        """With no onboarded tickers, any file on disk is an orphan."""
        state_file = _make_state(tmp_path, [])
        data_dir = _make_data_dir(tmp_path, ["AAPL"])

        ghosts, orphans = check(state_file, data_dir)
        assert ghosts == []
        assert "AAPL" in orphans

    def test_empty_data_dir(self, tmp_path):
        """With no files on disk, every onboarded ticker is a ghost."""
        state_file = _make_state(tmp_path, ["AAPL", "MSFT"])
        data_dir = tmp_path / "ohlcv"
        data_dir.mkdir()

        ghosts, orphans = check(state_file, data_dir)
        assert set(ghosts) == {"AAPL", "MSFT"}
        assert orphans == []

    def test_both_lists_are_sorted(self, tmp_path):
        """Ghosts and orphans are returned in sorted order."""
        state_file = _make_state(tmp_path, ["ZZZZ", "AAPL", "GHOST_B", "GHOST_A"])
        data_dir = _make_data_dir(tmp_path, ["AAPL", "ORPHAN_B", "ORPHAN_A"])

        ghosts, orphans = check(state_file, data_dir)
        assert ghosts == sorted(ghosts)
        assert orphans == sorted(orphans)

    def test_raises_when_state_file_missing(self, tmp_path):
        data_dir = _make_data_dir(tmp_path, [])
        with pytest.raises(FileNotFoundError):
            check(tmp_path / "nonexistent.json", data_dir)

    def test_multiple_ghosts(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "GHOST1", "GHOST2", "GHOST3"])
        data_dir = _make_data_dir(tmp_path, ["AAPL"])

        ghosts, _ = check(state_file, data_dir)
        assert set(ghosts) == {"GHOST1", "GHOST2", "GHOST3"}

    def test_multiple_orphans(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL"])
        data_dir = _make_data_dir(tmp_path, ["AAPL", "ORP1", "ORP2", "ORP3"])

        _, orphans = check(state_file, data_dir)
        assert set(orphans) == {"ORP1", "ORP2", "ORP3"}

    def test_large_onboarded_list(self, tmp_path):
        """Correctness holds with many symbols."""
        all_syms = [f"SYM{i:04d}" for i in range(200)]
        ghost_syms = all_syms[:10]   # first 10 have no file
        disk_syms = all_syms[10:]    # rest have a file

        state_file = _make_state(tmp_path, all_syms)
        data_dir = _make_data_dir(tmp_path, disk_syms)

        ghosts, orphans = check(state_file, data_dir)
        assert set(ghosts) == set(ghost_syms)
        assert orphans == []


# ---------------------------------------------------------------------------
# fix()
# ---------------------------------------------------------------------------


class TestFix:
    def test_removes_ghosts_from_state(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "GHOST1", "GHOST2"])
        fix(state_file, ["GHOST1", "GHOST2"])

        raw = json.loads(state_file.read_text())
        assert "GHOST1" not in raw["onboarded"]
        assert "GHOST2" not in raw["onboarded"]
        assert "AAPL" in raw["onboarded"]

    def test_returns_count_removed(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "MSFT", "GHOST"])
        removed = fix(state_file, ["GHOST"])
        assert removed == 1

    def test_returns_zero_for_empty_ghosts_list(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "MSFT"])
        removed = fix(state_file, [])
        assert removed == 0

    def test_state_file_updated_on_disk(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "GHOST"])
        fix(state_file, ["GHOST"])

        raw = json.loads(state_file.read_text())
        assert raw["onboarded"] == ["AAPL"]

    def test_onboarded_list_remains_sorted(self, tmp_path):
        state_file = _make_state(tmp_path, ["AAPL", "GHOST", "MSFT", "TSLA"])
        fix(state_file, ["GHOST"])

        raw = json.loads(state_file.read_text())
        onboarded = raw["onboarded"]
        assert onboarded == sorted(onboarded)

    def test_fix_is_idempotent(self, tmp_path):
        """Calling fix twice with the same ghosts list has the same net effect."""
        state_file = _make_state(tmp_path, ["AAPL", "GHOST"])
        fix(state_file, ["GHOST"])
        fix(state_file, ["GHOST"])  # second call is a no-op

        raw = json.loads(state_file.read_text())
        assert "GHOST" not in raw["onboarded"]
        assert "AAPL" in raw["onboarded"]

    def test_non_ghost_fields_preserved(self, tmp_path):
        """fix() must not destroy other fields in state.json."""
        state_file = tmp_path / "state.json"
        state_file.write_text(
            json.dumps(
                {
                    "onboarded": ["AAPL", "GHOST"],
                    "last_run": "2024-01-15",
                    "options_cycle": ["AAPL"],
                }
            )
        )
        fix(state_file, ["GHOST"])

        raw = json.loads(state_file.read_text())
        assert raw["last_run"] == "2024-01-15"
        assert raw["options_cycle"] == ["AAPL"]

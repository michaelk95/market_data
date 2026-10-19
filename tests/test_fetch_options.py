"""Tests for fetch_options.py storage: save_options_snapshot() and run().

Snapshots go to the bitemporal options table (data/options/year=YYYY/).
No network calls: fetch_option_chain is patched.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from market_data import fetch_options
from market_data.fetch_options import save_options_snapshot
from market_data.schema import TABLE_SCHEMAS
from market_data.storage import read_table


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(fetch_options, "SLEEP_BETWEEN_CALLS", 0)


class TestSaveOptionsSnapshot:
    def test_writes_year_partition(self, options_df, tmp_path):
        assert save_options_snapshot(options_df, tmp_path) == 1
        assert (tmp_path / "options" / "year=2024" / "data.parquet").exists()
        assert not (tmp_path / "options" / "AAPL.parquet").exists()

    def test_rows_follow_bitemporal_schema(self, options_df, tmp_path):
        save_options_snapshot(options_df, tmp_path)
        stored = read_table("options", tmp_path)
        assert list(stored.columns) == TABLE_SCHEMAS["options"].names
        assert stored["period_start_date"].iloc[0] == date(2024, 1, 2)
        assert stored["report_time_marker"].iloc[0] == "pre-market"

    def test_same_day_rerun_is_idempotent(self, options_df, tmp_path):
        save_options_snapshot(options_df, tmp_path)
        assert save_options_snapshot(options_df, tmp_path) == 0
        assert len(read_table("options", tmp_path)) == 1

    def test_next_snapshot_appends(self, options_df, tmp_path):
        save_options_snapshot(options_df, tmp_path)
        nxt = options_df.assign(snapshot_date=date(2024, 1, 3))
        assert save_options_snapshot(nxt, tmp_path) == 1

    def test_empty_frame_writes_nothing(self, tmp_path):
        empty = pd.DataFrame(columns=fetch_options.OPTIONS_COLS)
        assert save_options_snapshot(empty, tmp_path) == 0
        assert not (tmp_path / "options").exists()

    def test_table_dir_instead_of_root_raises(self, options_df, tmp_path):
        with pytest.raises(ValueError, match="data root"):
            save_options_snapshot(options_df, tmp_path / "options")


class TestRun:
    def test_returns_rows_per_symbol(self, options_df, tmp_path):
        chains = {
            "AAPL": options_df,
            "MSFT": options_df.assign(symbol="MSFT"),
            "EMPTY": pd.DataFrame(columns=fetch_options.OPTIONS_COLS),
        }
        with patch.object(fetch_options, "fetch_option_chain", side_effect=lambda s, **_: chains[s]):
            result = fetch_options.run(["AAPL", "MSFT", "EMPTY"], data_dir=tmp_path)

        assert result == {"AAPL": 1, "MSFT": 1, "EMPTY": 0}
        stored = read_table("options", tmp_path)
        assert sorted(stored["symbol"]) == ["AAPL", "MSFT"]

    def test_failed_symbol_reports_zero_and_others_saved(self, options_df, tmp_path):
        def fake(symbol, **_):
            if symbol == "BAD":
                raise RuntimeError("boom")
            return options_df

        with patch.object(fetch_options, "fetch_option_chain", side_effect=fake):
            result = fetch_options.run(["BAD", "AAPL"], data_dir=tmp_path)

        assert result == {"BAD": 0, "AAPL": 1}
        assert len(read_table("options", tmp_path)) == 1

    def test_batch_written_once(self, options_df, tmp_path):
        """One write per run, not one partition rewrite per symbol."""
        chains = {s: options_df.assign(symbol=s) for s in ["AAA", "BBB", "CCC"]}
        with (
            patch.object(fetch_options, "fetch_option_chain", side_effect=lambda s, **_: chains[s]),
            patch.object(fetch_options, "save_options_snapshot", wraps=save_options_snapshot) as spy,
        ):
            fetch_options.run(list(chains), data_dir=tmp_path)
        assert spy.call_count == 1

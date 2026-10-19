"""Tests for fetch_indices.update_symbol() against the bitemporal indices table.

No network calls: fetch_history / fetch_incremental are patched.
"""

from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from market_data import fetch_indices
from market_data.storage import read_table


def _bars(days: list[date], symbol: str = "^VIX") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": days,
            "symbol": symbol,
            "open": 15.0,
            "high": 16.0,
            "low": 14.0,
            "close": 15.5,
            "volume": 0.0,
        }
    )


@pytest.fixture(autouse=True)
def _data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(fetch_indices, "DATA_DIR", tmp_path)


def test_bootstrap_writes_indices_table(tmp_path):
    with patch.object(fetch_indices, "fetch_history", return_value=_bars([date(2024, 1, 2)])) as hist:
        assert fetch_indices.update_symbol("^VIX", history_years=1) == 1
    hist.assert_called_once()
    assert (tmp_path / "indices" / "data.parquet").exists()
    assert not (tmp_path / "indices" / "^VIX.parquet").exists()


def test_incremental_since_last_stored_date(tmp_path):
    with patch.object(fetch_indices, "fetch_history", return_value=_bars([date(2024, 1, 2), date(2024, 1, 3)])):
        fetch_indices.update_symbol("^VIX", history_years=1)

    new = _bars([date(2024, 1, 3), date(2024, 1, 4)])
    with (
        patch.object(fetch_indices, "fetch_history") as hist,
        patch.object(fetch_indices, "fetch_incremental", return_value=new) as inc,
    ):
        assert fetch_indices.update_symbol("^VIX", history_years=1) == 1

    hist.assert_not_called()
    assert inc.call_args.kwargs["since"] == date(2024, 1, 3)
    assert len(read_table("indices", tmp_path, symbols=["^VIX"])) == 3


def test_other_symbol_still_bootstraps(tmp_path):
    with patch.object(fetch_indices, "fetch_history", return_value=_bars([date(2024, 1, 2)])):
        fetch_indices.update_symbol("^VIX", history_years=1)
    with patch.object(fetch_indices, "fetch_history", return_value=_bars([date(2024, 1, 2)], "^TNX")) as hist:
        fetch_indices.update_symbol("^TNX", history_years=1)
    hist.assert_called_once()

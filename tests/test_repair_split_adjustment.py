"""Tests for scripts/repair_split_adjustment.py.

Bars stored before a split stay on the old price basis. The repair finds
split-shaped jumps in the store, re-downloads those symbols and replaces
their rows when the re-download passes the same gate as the nightly run.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
import repair_split_adjustment as repair

from market_data.fetch import load_ticker_data, save_ticker_data

STORED = [100.0, 102.0, 104.0, 52.0, 53.0]
FRESH = [50.0, 51.0, 52.0, 52.0, 53.0]


def _bars(symbol: str, closes: list[float]) -> pd.DataFrame:
    days = pd.date_range("2026-01-05", periods=len(closes), freq="B").date
    return pd.DataFrame({
        "date": days, "symbol": symbol, "open": closes, "high": closes,
        "low": closes, "close": closes, "volume": 100.0,
    })


@pytest.fixture()
def store(tmp_path):
    data_dir = tmp_path / "data"
    save_ticker_data("AAA", _bars("AAA", STORED), data_dir)   # unadjusted split
    save_ticker_data("BBB", _bars("BBB", FRESH), data_dir)    # consistent
    save_ticker_data("CCC", _bars("CCC", STORED), data_dir)   # different security
    return data_dir


def _fake_fetch(calls: list):
    downloads = {
        "AAA": _bars("AAA", FRESH),
        "BBB": _bars("BBB", FRESH),
        "CCC": _bars("CCC", FRESH[:2]),
    }

    def fetch(symbol, start, end=None):
        calls.append(symbol)
        return downloads[symbol]

    return fetch


def _run(store, tmp_path, calls, **kwargs):
    return repair.run(
        store, cache_dir=tmp_path / "cache", fetch=_fake_fetch(calls), sleep=0, **kwargs
    )


def test_dry_run_reports_and_writes_nothing(store, tmp_path):
    before = {p: p.read_bytes() for p in store.rglob("*.parquet")}
    calls: list = []

    summary = _run(store, tmp_path, calls, apply=False, symbols=["AAA", "BBB", "CCC"])

    assert summary["replace"] == ["AAA"]
    assert summary["consistent"] == ["BBB"]
    assert summary["rejected"] == ["CCC"]
    assert {p: p.read_bytes() for p in store.rglob("*.parquet")} == before
    assert not list(store.glob("ohlcv_split_backup_*"))


def test_candidates_come_from_split_shaped_jumps(store, tmp_path):
    calls: list = []
    summary = _run(store, tmp_path, calls, apply=False)
    assert summary["candidates"] == ["AAA", "CCC"]
    assert calls == ["AAA", "CCC"]


def test_since_limits_candidates(store, tmp_path):
    calls: list = []
    summary = _run(store, tmp_path, calls, apply=False, since=date(2026, 1, 9))
    assert summary["candidates"] == []
    assert calls == []


def test_apply_backs_up_and_replaces(store, tmp_path):
    calls: list = []

    summary = _run(store, tmp_path, calls, apply=True, symbols=["AAA", "BBB", "CCC"])

    assert summary["replace"] == ["AAA"]
    assert load_ticker_data("AAA", store)["close"].tolist() == FRESH
    assert load_ticker_data("CCC", store)["close"].tolist() == STORED
    backups = list(store.glob("ohlcv_split_backup_*"))
    assert len(backups) == 1
    assert (backups[0] / "year=2026" / "data.parquet").exists()


def test_cached_downloads_are_reused(store, tmp_path):
    _run(store, tmp_path, [], apply=False, symbols=["AAA"])
    calls: list = []

    summary = _run(store, tmp_path, calls, apply=True, symbols=["AAA"])

    assert calls == []
    assert summary["replace"] == ["AAA"]


def test_download_error_is_reported_and_skipped(store, tmp_path):
    def fetch(symbol, start, end=None):
        raise RuntimeError("timeout")

    summary = repair.run(
        store, cache_dir=tmp_path / "cache", fetch=fetch, sleep=0,
        apply=True, symbols=["AAA"],
    )
    assert summary["failed"] == ["AAA"]
    assert load_ticker_data("AAA", store)["close"].tolist() == STORED

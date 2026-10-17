"""Tests for scripts/migrate_fundamentals.py — point-in-time dates of migrated rows."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import migrate_fundamentals
import pandas as pd

from market_data.storage import read_table

_AS_OF = date(2026, 4, 5)
_FILING = date(2026, 2, 27)


def _write_old_file(fund_dir, symbol: str, as_of: date) -> None:
    fund_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "as_of": [as_of],
        "symbol": [symbol],
        "market_cap": [1.0e12],
        "trailing_pe": [30.0],
    }).to_parquet(fund_dir / f"{symbol}.parquet", index=False)


@patch("migrate_fundamentals.time.sleep")
@patch("migrate_fundamentals.edgar.get_latest_filing_date", return_value=_FILING)
def test_report_date_is_snapshot_as_of_not_filing_date(_mock_edgar, _mock_sleep, tmp_path):
    _write_old_file(tmp_path / "fundamentals", "AAPL", _AS_OF)

    migrate_fundamentals.run(data_dir=tmp_path)

    df = read_table("fundamentals", tmp_path)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["report_date"] == _AS_OF
    assert row["period_start_date"] == _AS_OF
    assert row["period_end_date"] == _AS_OF
    assert row["last_filing_date"] == _FILING
    assert bool(row["report_date_known"]) is True


@patch("migrate_fundamentals.time.sleep")
@patch("migrate_fundamentals.edgar.get_latest_filing_date", return_value=None)
def test_no_edgar_date_leaves_last_filing_date_empty(_mock_edgar, _mock_sleep, tmp_path):
    _write_old_file(tmp_path / "fundamentals", "XYZ", _AS_OF)

    migrate_fundamentals.run(data_dir=tmp_path)

    row = read_table("fundamentals", tmp_path).iloc[0]
    assert row["report_date"] == _AS_OF
    assert pd.isna(row["last_filing_date"])

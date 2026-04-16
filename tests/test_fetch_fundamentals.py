"""Tests for market_data.fetch_fundamentals — bitemporal fundamentals collection."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd

from market_data.fetch_fundamentals import fetch_fundamentals, run
from market_data.schema import validate_bitemporal_columns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TODAY = date(2024, 2, 1)
_EDGAR_DATE = date(2024, 1, 25)

_FULL_INFO = {
    "marketCap": 3_000_000_000_000,
    "enterpriseValue": 3_100_000_000_000,
    "trailingPE": 29.5,
    "forwardPE": 27.0,
    "priceToBook": 45.0,
    "trailingEps": 6.30,
    "forwardEps": 6.90,
    "totalRevenue": 400_000_000_000,
    "profitMargins": 0.25,
}


# ---------------------------------------------------------------------------
# fetch_fundamentals — bitemporal field population
# ---------------------------------------------------------------------------

class TestFetchFundamentals:
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_returns_dict_with_all_bitemporal_fields(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record is not None
        df = pd.DataFrame([record])
        validate_bitemporal_columns(df)  # raises if any field missing

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_uses_edgar_date_as_report_date(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record["report_date"] == _EDGAR_DATE
        assert record["period_start_date"] == _EDGAR_DATE
        assert record["period_end_date"] == _EDGAR_DATE
        assert record["report_date_known"] is True

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=None)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_falls_back_to_today_when_no_edgar_date(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record["report_date"] == _TODAY
        assert record["period_start_date"] == _TODAY
        assert record["report_date_known"] is False

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=None)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value={"marketCap": 0})
    def test_returns_none_when_no_market_cap(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("XLF", today=_TODAY)
        assert record is None

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=None)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value={})
    def test_returns_none_for_empty_info(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("DELISTED", today=_TODAY)
        assert record is None

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_source_is_yfinance(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record["source"] == "yfinance"

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_report_time_marker_is_post_market(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record["report_time_marker"] == "post-market"

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_collected_at_is_utc_timestamp(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        ts = record["collected_at"]
        assert isinstance(ts, datetime)
        assert ts.tzinfo is not None
        assert ts.tzinfo == timezone.utc

    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch(
        "market_data.fetch_fundamentals._fetch_ticker_info",
        return_value={**_FULL_INFO, "trailingPE": None, "forwardPE": None},
    )
    def test_missing_optional_fields_stored_as_none(self, _mock_info, _mock_edgar):
        record = fetch_fundamentals("AAPL", today=_TODAY)
        assert record["trailing_pe"] is None
        assert record["forward_pe"] is None


# ---------------------------------------------------------------------------
# run() — integration with storage.write_table
# ---------------------------------------------------------------------------

class TestRun:
    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_writes_to_fundamentals_table(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 1
        assert (tmp_path / "fundamentals").exists()

    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_idempotent(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        run(["AAPL"], data_dir=tmp_path)
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 0  # already present, no new rows

    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=None)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value={})
    def test_run_skips_no_data_tickers(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        saved = run(["XLF"], data_dir=tmp_path)
        assert saved == 0
        assert not (tmp_path / "fundamentals").exists()

    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", side_effect=RuntimeError("network error"))
    def test_run_continues_after_per_ticker_error(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        # Should not raise; just returns 0 (failed silently)
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 0

    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_returns_int(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        result = run(["AAPL"], data_dir=tmp_path)
        assert isinstance(result, int)

    @patch("market_data.fetch_fundamentals.time.sleep")
    @patch("market_data.fetch_fundamentals.edgar.get_latest_filing_date", return_value=_EDGAR_DATE)
    @patch("market_data.fetch_fundamentals._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_uses_year_partitioned_layout(self, _mock_info, _mock_edgar, _mock_sleep, tmp_path):
        run(["AAPL"], data_dir=tmp_path)
        year = _EDGAR_DATE.year
        assert (tmp_path / "fundamentals" / f"year={year}" / "data.parquet").exists()

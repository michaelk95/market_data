"""Tests for market_data.fetch_analyst_estimates — daily point-in-time collection."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd

from market_data.fetch_analyst_estimates import fetch_analyst_estimates, run
from market_data.schema import validate_bitemporal_columns


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TODAY = date(2024, 2, 1)

_FULL_INFO = {
    "targetMeanPrice": 200.0,
    "targetLowPrice": 170.0,
    "targetHighPrice": 230.0,
    "recommendationMean": 1.8,
    "numberOfAnalystOpinions": 42,
}


# ---------------------------------------------------------------------------
# fetch_analyst_estimates — bitemporal field population
# ---------------------------------------------------------------------------

class TestFetchAnalystEstimates:
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_returns_dict_with_all_bitemporal_fields(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record is not None
        df = pd.DataFrame([record])
        validate_bitemporal_columns(df)  # raises if any field missing

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_uses_today_as_report_date(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record["report_date"] == _TODAY
        assert record["period_start_date"] == _TODAY
        assert record["period_end_date"] == _TODAY

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_report_date_known_is_always_false(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record["report_date_known"] is False

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_analyst_recommendation_is_string(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert isinstance(record["analyst_recommendation"], str)
        assert record["analyst_recommendation"] == "1.8"

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_analyst_count_is_int(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert isinstance(record["analyst_count"], int)
        assert record["analyst_count"] == 42

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_price_targets_are_floats(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert isinstance(record["analyst_target_mean"], float)
        assert isinstance(record["analyst_target_low"], float)
        assert isinstance(record["analyst_target_high"], float)
        assert record["analyst_target_mean"] == 200.0
        assert record["analyst_target_low"] == 170.0
        assert record["analyst_target_high"] == 230.0

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_source_is_yfinance(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record["source"] == "yfinance"

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_report_time_marker_is_post_market(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record["report_time_marker"] == "post-market"

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_collected_at_is_utc_timestamp(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        ts = record["collected_at"]
        assert isinstance(ts, datetime)
        assert ts.tzinfo is not None
        assert ts.tzinfo == timezone.utc

    @patch(
        "market_data.fetch_analyst_estimates._fetch_ticker_info",
        return_value={"targetMeanPrice": 0},
    )
    def test_returns_none_when_no_target_price(self, _mock_info):
        record = fetch_analyst_estimates("XLF", today=_TODAY)
        assert record is None

    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value={})
    def test_returns_none_for_empty_info(self, _mock_info):
        record = fetch_analyst_estimates("DELISTED", today=_TODAY)
        assert record is None

    @patch(
        "market_data.fetch_analyst_estimates._fetch_ticker_info",
        return_value={**_FULL_INFO, "recommendationMean": None, "numberOfAnalystOpinions": None},
    )
    def test_missing_optional_fields_stored_as_none(self, _mock_info):
        record = fetch_analyst_estimates("AAPL", today=_TODAY)
        assert record["analyst_recommendation"] is None
        assert record["analyst_count"] is None


# ---------------------------------------------------------------------------
# run() — integration with storage.write_table
# ---------------------------------------------------------------------------

class TestRun:
    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_writes_to_analyst_estimates_table(self, _mock_info, _mock_sleep, tmp_path):
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 1
        assert (tmp_path / "analyst_estimates").exists()

    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_idempotent(self, _mock_info, _mock_sleep, tmp_path):
        run(["AAPL"], data_dir=tmp_path)
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 0  # already present for today, no new rows

    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value={})
    def test_run_skips_no_coverage_tickers(self, _mock_info, _mock_sleep, tmp_path):
        saved = run(["XLF"], data_dir=tmp_path)
        assert saved == 0
        assert not (tmp_path / "analyst_estimates").exists()

    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch(
        "market_data.fetch_analyst_estimates._fetch_ticker_info",
        side_effect=RuntimeError("network error"),
    )
    def test_run_continues_after_per_ticker_error(self, _mock_info, _mock_sleep, tmp_path):
        saved = run(["AAPL"], data_dir=tmp_path)
        assert saved == 0

    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_returns_int(self, _mock_info, _mock_sleep, tmp_path):
        result = run(["AAPL"], data_dir=tmp_path)
        assert isinstance(result, int)

    @patch("market_data.fetch_analyst_estimates.time.sleep")
    @patch("market_data.fetch_analyst_estimates._fetch_ticker_info", return_value=_FULL_INFO)
    def test_run_uses_year_partitioned_layout(self, _mock_info, _mock_sleep, tmp_path):
        run(["AAPL"], data_dir=tmp_path)
        year = date.today().year  # run() uses date.today() for period_start_date
        assert (tmp_path / "analyst_estimates" / f"year={year}" / "data.parquet").exists()

"""Tests for fetch_macro.py vintage / realtime-API fetch path."""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from market_data.fetch_macro import DEFAULT_START, fetch_series_vintages, update_series
from market_data.schema import DataSource, ReportTimeMarker


# ---------------------------------------------------------------------------
# Shared fixture — mimics what fredapi.get_series_all_releases returns
# (index=realtime_start, columns=realtime_end/date/value)
# ---------------------------------------------------------------------------

_VINTAGE_INDEX = pd.Index(
    [
        datetime.date(2020, 1, 30),  # first vintage of 2019-Q4
        datetime.date(2020, 3, 26),  # revised vintage of 2019-Q4
        datetime.date(2020, 1, 30),  # first vintage of 2019-Q3
    ],
    name="realtime_start",
)

_VINTAGE_DF = pd.DataFrame(
    {
        "realtime_end": [
            datetime.date(2020, 3, 25),   # superseded
            datetime.date(9999, 12, 31),  # currently active
            datetime.date(9999, 12, 31),  # currently active
        ],
        "date": [
            datetime.date(2019, 10, 1),
            datetime.date(2019, 10, 1),
            datetime.date(2019, 7, 1),
        ],
        "value": [19.1, 19.2, 18.9],
    },
    index=_VINTAGE_INDEX,
)


def _make_fred_mock(df: pd.DataFrame) -> MagicMock:
    fred = MagicMock()
    fred.get_series_all_releases.return_value = df
    return fred


# ---------------------------------------------------------------------------
# TestFetchSeriesVintages
# ---------------------------------------------------------------------------


class TestFetchSeriesVintages:
    """Unit tests for fetch_series_vintages()."""

    def _call(self, df: pd.DataFrame = _VINTAGE_DF) -> pd.DataFrame:
        with patch("fredapi.Fred", return_value=_make_fred_mock(df)):
            return fetch_series_vintages("GDPC1", realtime_start="2020-01-01", api_key="test")

    def test_all_bitemporal_columns_present(self):
        result = self._call()
        for col in [
            "period_start_date", "period_end_date", "report_date",
            "report_time_marker", "source", "collected_at",
        ]:
            assert col in result.columns, f"missing column: {col}"

    def test_realtime_start_maps_to_report_date(self):
        result = self._call()
        assert set(result["report_date"].tolist()) == {
            datetime.date(2020, 1, 30),
            datetime.date(2020, 3, 26),
        }

    def test_realtime_end_maps_to_valid_to_date(self):
        result = self._call()
        assert "valid_to_date" in result.columns
        active = result[result["valid_to_date"] == datetime.date(9999, 12, 31)]
        assert len(active) == 2

    def test_date_maps_to_period_start_and_end(self):
        result = self._call()
        assert (result["period_start_date"] == result["period_end_date"]).all()
        obs_dates = set(result["period_start_date"].tolist())
        assert datetime.date(2019, 10, 1) in obs_dates
        assert datetime.date(2019, 7, 1) in obs_dates

    def test_source_is_fred(self):
        result = self._call()
        assert (result["source"] == DataSource.FRED).all()

    def test_report_time_marker_is_post_market(self):
        result = self._call()
        assert (result["report_time_marker"] == ReportTimeMarker.POST_MARKET).all()

    def test_drops_nan_value_rows(self):
        df_with_nan = pd.DataFrame(
            {
                "realtime_end": _VINTAGE_DF["realtime_end"].tolist(),
                "date": _VINTAGE_DF["date"].tolist(),
                "value": [19.1, float("nan"), 18.9],
            },
            index=_VINTAGE_INDEX,
        )
        result = self._call(df=df_with_nan)
        assert len(result) == 2

    def test_returns_empty_df_on_empty_response(self):
        with patch("fredapi.Fred", return_value=_make_fred_mock(pd.DataFrame())):
            result = fetch_series_vintages("GDPC1", realtime_start="2020-01-01", api_key="test")
        assert result.empty
        assert "series_id" in result.columns


# ---------------------------------------------------------------------------
# TestUpdateSeries
# ---------------------------------------------------------------------------


class TestUpdateSeries:
    """Tests for update_series() using tmp_path."""

    def test_bootstrap_uses_default_start(self, tmp_path):
        """First run with no existing data uses DEFAULT_START as realtime_start."""
        captured: dict = {}

        def fake_fetch(series_id, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            result = update_series("GDPC1", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        assert captured["realtime_start"] == DEFAULT_START
        assert result == 0

    def test_incremental_uses_latest_report_date_minus_7_days(self, tmp_path):
        """Subsequent run keys off max(report_date) − 7 days."""
        from market_data.storage import write_table

        seed = pd.DataFrame([{
            "series_id": "GDPC1",
            "value": 19.1,
            "valid_to_date": datetime.date(9999, 12, 31),
            "period_start_date": datetime.date(2020, 1, 1),
            "period_end_date": datetime.date(2020, 1, 1),
            "report_date": datetime.date(2020, 3, 26),
            "report_time_marker": ReportTimeMarker.POST_MARKET,
            "source": DataSource.FRED,
            "collected_at": pd.Timestamp("2020-03-26", tz="UTC"),
        }])
        write_table(seed, "macro", tmp_path)

        captured: dict = {}

        def fake_fetch(series_id, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            update_series("GDPC1", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        expected = str(datetime.date(2020, 3, 26) - datetime.timedelta(days=7))
        assert captured["realtime_start"] == expected

    def test_returns_zero_on_empty_response(self, tmp_path):
        with patch("market_data.fetch_macro.fetch_series_vintages", return_value=pd.DataFrame()):
            result = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)
        assert result == 0

    def test_idempotent(self, tmp_path):
        """Writing the same data twice returns 0 on the second call."""
        row = pd.DataFrame([{
            "series_id": "DFF",
            "value": 5.33,
            "valid_to_date": datetime.date(9999, 12, 31),
            "period_start_date": datetime.date(2024, 1, 2),
            "period_end_date": datetime.date(2024, 1, 2),
            "report_date": datetime.date(2024, 1, 2),
            "report_time_marker": ReportTimeMarker.POST_MARKET,
            "source": DataSource.FRED,
            "collected_at": pd.Timestamp("2024-01-02", tz="UTC"),
        }])

        def fake_fetch(series_id, realtime_start, api_key):
            return row.copy()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            first = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)
            second = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        assert first == 1
        assert second == 0

"""Tests for fetch_macro.py vintage / realtime-API fetch path."""
from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock, patch

import pytest

import pandas as pd

from market_data.fetch_macro import (
    DEFAULT_START,
    SERIES_LOOKBACK_DAYS,
    _DEFAULT_LOOKBACK_DAYS,
    _VINTAGE_CHUNK_YEARS,
    _detect_revisions,
    _derive_realtime_end,
    _fetch_all_releases_chunked,
    _recompute_revision_ranks,
    fetch_series_vintages,
    update_series,
)
from market_data.schema import PARTITION_COLS, DataSource, ReportTimeMarker
from market_data.storage import write_table


# ---------------------------------------------------------------------------
# Shared fixture — mimics what fredapi.get_series_all_releases actually returns.
# fredapi returns a plain DataFrame with integer index and columns:
#   realtime_start, date, value
# (realtime_end is commented out in fredapi source and not returned)
# ---------------------------------------------------------------------------

_VINTAGE_DF = pd.DataFrame(
    {
        "realtime_start": [
            datetime.date(2020, 1, 30),  # first vintage of 2019-Q4
            datetime.date(2020, 3, 26),  # revised vintage of 2019-Q4
            datetime.date(2020, 1, 30),  # first vintage of 2019-Q3
        ],
        "date": [
            datetime.date(2019, 10, 1),
            datetime.date(2019, 10, 1),
            datetime.date(2019, 7, 1),
        ],
        "value": [19.1, 19.2, 18.9],
    }
)


def _make_fred_mock(df: pd.DataFrame) -> MagicMock:
    fred = MagicMock()
    fred.get_series_all_releases.return_value = df
    return fred


def _seed_row(series_id: str, period: datetime.date, report_date: datetime.date) -> dict:
    """Return a minimal valid macro row dict for seeding storage."""
    return {
        "series_id": series_id,
        "value": 19.1,
        "valid_to_date": datetime.date(9999, 12, 31),
        "revision_rank": 1,
        "release_name": None,
        "period_start_date": period,
        "period_end_date": period,
        "report_date": report_date,
        "report_time_marker": ReportTimeMarker.POST_MARKET,
        "source": DataSource.FRED,
        "collected_at": pd.Timestamp("2024-01-01", tz="UTC"),
    }


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
        df_with_nan = _VINTAGE_DF.copy()
        df_with_nan["value"] = [19.1, float("nan"), 18.9]
        result = self._call(df=df_with_nan)
        assert len(result) == 2

    def test_returns_empty_df_on_empty_response(self):
        with patch("fredapi.Fred", return_value=_make_fred_mock(pd.DataFrame())):
            result = fetch_series_vintages("GDPC1", realtime_start="2020-01-01", api_key="test")
        assert result.empty
        assert "series_id" in result.columns

    def test_revision_rank_column_present(self):
        result = self._call()
        assert "revision_rank" in result.columns

    def test_revision_rank_values(self):
        """Q4 2019 appears twice → ranks 1,2; Q3 2019 once → rank 1."""
        result = self._call()
        q4 = result[result["period_start_date"] == datetime.date(2019, 10, 1)].sort_values("report_date")
        assert list(q4["revision_rank"]) == [1, 2]
        q3 = result[result["period_start_date"] == datetime.date(2019, 7, 1)]
        assert list(q3["revision_rank"]) == [1]

    def test_release_name_column_present(self):
        result = self._call()
        assert "release_name" in result.columns

    def test_release_name_populated_for_known_series(self):
        result = self._call()
        assert (result["release_name"] == "Gross Domestic Product").all()

    def test_release_name_none_for_unknown_series(self):
        with patch("fredapi.Fred", return_value=_make_fred_mock(_VINTAGE_DF)):
            result = fetch_series_vintages("CUSTOM_XYZ", realtime_start="2020-01-01", api_key="test")
        assert result["release_name"].isna().all()


# ---------------------------------------------------------------------------
# TestDeriveRealtimeEnd
# ---------------------------------------------------------------------------


class TestDeriveRealtimeEnd:
    """Unit tests for _derive_realtime_end()."""

    def test_single_vintage_gets_far_future(self):
        df = pd.DataFrame({
            "realtime_start": [datetime.date(2020, 1, 30)],
            "date": [datetime.date(2019, 10, 1)],
            "value": [19.1],
        })
        result = _derive_realtime_end(df)
        assert result.iloc[0] == datetime.date(9999, 12, 31)

    def test_superseded_vintage_gets_next_start_minus_one_day(self):
        df = pd.DataFrame({
            "realtime_start": [datetime.date(2020, 1, 30), datetime.date(2020, 3, 26)],
            "date": [datetime.date(2019, 10, 1), datetime.date(2019, 10, 1)],
            "value": [19.1, 19.2],
        })
        result = _derive_realtime_end(df)
        assert result.iloc[0] == datetime.date(2020, 3, 25)
        assert result.iloc[1] == datetime.date(9999, 12, 31)

    def test_independent_observation_dates_do_not_cross(self):
        """Vintages for different observation dates are bucketed independently."""
        df = _VINTAGE_DF.copy()
        result = _derive_realtime_end(df)
        # 2019-Q4 first vintage: superseded by 2020-03-26 → realtime_end = 2020-03-25
        q4_first = df[(df["date"] == datetime.date(2019, 10, 1)) & (df["realtime_start"] == datetime.date(2020, 1, 30))].index[0]
        assert result.loc[q4_first] == datetime.date(2020, 3, 25)
        # 2019-Q3 only vintage → 9999-12-31
        q3 = df[df["date"] == datetime.date(2019, 7, 1)].index[0]
        assert result.loc[q3] == datetime.date(9999, 12, 31)

    def test_preserves_input_row_order(self):
        """Result is aligned to the original DataFrame index, not sorted order."""
        df = pd.DataFrame({
            "realtime_start": [datetime.date(2020, 3, 26), datetime.date(2020, 1, 30)],
            "date": [datetime.date(2019, 10, 1), datetime.date(2019, 10, 1)],
            "value": [19.2, 19.1],
        })
        result = _derive_realtime_end(df)
        # Row 0 is the later vintage → 9999-12-31
        assert result.iloc[0] == datetime.date(9999, 12, 31)
        # Row 1 is the earlier vintage → superseded day before row 0's realtime_start
        assert result.iloc[1] == datetime.date(2020, 3, 25)


# ---------------------------------------------------------------------------
# TestFetchAllReleasesChunked
# ---------------------------------------------------------------------------


class TestFetchAllReleasesChunked:
    """Unit tests for _fetch_all_releases_chunked()."""

    def test_single_chunk_when_range_fits(self):
        """A range shorter than _VINTAGE_CHUNK_YEARS issues exactly one API call."""
        fred = MagicMock()
        fred.get_series_all_releases.return_value = _VINTAGE_DF.copy()
        result = _fetch_all_releases_chunked(fred, "GDPC1", "2023-01-01", "2023-12-31")
        assert fred.get_series_all_releases.call_count == 1
        assert len(result) == len(_VINTAGE_DF)

    def test_multiple_chunks_for_long_range(self):
        """A range spanning more than _VINTAGE_CHUNK_YEARS issues multiple API calls."""
        fred = MagicMock()
        fred.get_series_all_releases.return_value = pd.DataFrame(
            {"realtime_start": [], "date": [], "value": []}
        )
        span_years = _VINTAGE_CHUNK_YEARS * 3
        start = datetime.date(2000, 1, 1)
        end = datetime.date(start.year + span_years, 1, 1)
        _fetch_all_releases_chunked(fred, "DFF", str(start), str(end))
        assert fred.get_series_all_releases.call_count >= 3

    def test_deduplicates_overlapping_rows(self):
        """Identical (realtime_start, date) rows from multiple chunks appear once."""
        fred = MagicMock()
        fred.get_series_all_releases.return_value = _VINTAGE_DF.copy()
        result = _fetch_all_releases_chunked(fred, "GDPC1", "2020-01-01", "2026-04-17")
        assert result.duplicated(subset=["realtime_start", "date"]).sum() == 0

    def test_empty_when_all_chunks_return_nothing(self):
        fred = MagicMock()
        fred.get_series_all_releases.return_value = pd.DataFrame()
        result = _fetch_all_releases_chunked(fred, "GDPC1", "2020-01-01", "2020-12-31")
        assert result.empty

    def test_chunk_realtime_end_never_exceeds_requested_end(self):
        """Each chunk's realtime_end kwarg must not exceed the overall end date."""
        fred = MagicMock()
        fred.get_series_all_releases.return_value = pd.DataFrame()
        overall_end = "2021-06-15"
        _fetch_all_releases_chunked(fred, "DFF", "2020-01-01", overall_end)
        for call in fred.get_series_all_releases.call_args_list:
            chunk_end = call.kwargs.get("realtime_end") or call.args[2]
            assert chunk_end <= overall_end

    def test_falls_back_to_get_series_when_all_chunks_not_in_alfred(self):
        """If every chunk returns 'not in ALFRED', fall back to fred.get_series()."""
        fred = MagicMock()
        fred.get_series_all_releases.side_effect = ValueError(
            "Bad Request.  The series does not exist in ALFRED but may exist in FRED."
        )
        fred.get_series.return_value = pd.Series(
            [1.5, 1.6],
            index=pd.to_datetime(["2024-01-01", "2024-01-02"]),
        )
        result = _fetch_all_releases_chunked(fred, "DFF", "2024-01-01", "2024-01-02")
        fred.get_series.assert_called_once()
        assert len(result) == 2
        assert "realtime_start" in result.columns
        assert "date" in result.columns
        assert "value" in result.columns

    def test_skips_alfred_missing_chunks_and_returns_successful_ones(self):
        """Chunks that return 'not in ALFRED' are skipped; successful chunks are kept."""
        call_count = 0
        def side_effect(series_id, realtime_start, realtime_end):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("The series does not exist in ALFRED but may exist in FRED.")
            return _VINTAGE_DF.copy()

        fred = MagicMock()
        fred.get_series_all_releases.side_effect = side_effect
        result = _fetch_all_releases_chunked(fred, "CPILFESL", "2020-01-01", "2028-01-01")
        assert not result.empty
        assert fred.get_series.call_count == 0  # fallback not triggered


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
        """Subsequent run for a non-GDP series keys off max(report_date) − 7 days."""
        seed = pd.DataFrame([_seed_row("DFF", datetime.date(2020, 1, 1), datetime.date(2020, 3, 26))])
        write_table(seed, "macro", tmp_path)

        captured: dict = {}

        def fake_fetch(series_id, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        expected = str(datetime.date(2020, 3, 26) - datetime.timedelta(days=7))
        assert captured["realtime_start"] == expected

    def test_gdpc1_uses_400_day_lookback(self, tmp_path):
        """GDPC1 uses a 400-day window to catch annual benchmark revisions."""
        report_date = datetime.date(2024, 7, 1)
        seed = pd.DataFrame([_seed_row("GDPC1", datetime.date(2024, 1, 1), report_date)])
        write_table(seed, "macro", tmp_path)

        captured: dict = {}

        def fake_fetch(series_id, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            update_series("GDPC1", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        expected_days = SERIES_LOOKBACK_DAYS["GDPC1"]
        expected = str(report_date - datetime.timedelta(days=expected_days))
        assert captured["realtime_start"] == expected

    @pytest.mark.parametrize("series_id", ["PAYEMS", "CPIAUCSL", "CPILFESL", "UNRATE", "PCEPI", "PCEPILFE"])
    def test_annually_revised_series_uses_400_day_lookback(self, series_id, tmp_path):
        """BLS/BEA benchmark-revised series use a 400-day window to catch annual revisions."""
        report_date = datetime.date(2024, 7, 1)
        seed = pd.DataFrame([_seed_row(series_id, datetime.date(2024, 6, 1), report_date)])
        write_table(seed, "macro", tmp_path)

        captured: dict = {}

        def fake_fetch(sid, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            update_series(series_id, api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        expected = str(report_date - datetime.timedelta(days=400))
        assert captured["realtime_start"] == expected

    def test_default_series_uses_7_day_lookback(self, tmp_path):
        """Series not in SERIES_LOOKBACK_DAYS fall back to _DEFAULT_LOOKBACK_DAYS."""
        report_date = datetime.date(2024, 7, 1)
        seed = pd.DataFrame([_seed_row("DFF", datetime.date(2024, 6, 1), report_date)])
        write_table(seed, "macro", tmp_path)

        captured: dict = {}

        def fake_fetch(series_id, realtime_start, api_key):
            captured["realtime_start"] = realtime_start
            return pd.DataFrame()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        expected = str(report_date - datetime.timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        assert captured["realtime_start"] == expected

    def test_returns_zero_on_empty_response(self, tmp_path):
        with patch("market_data.fetch_macro.fetch_series_vintages", return_value=pd.DataFrame()):
            result = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)
        assert result == 0

    def test_idempotent(self, tmp_path):
        """Writing the same data twice returns 0 on the second call."""
        row = pd.DataFrame([_seed_row("DFF", datetime.date(2024, 1, 2), datetime.date(2024, 1, 2))])

        def fake_fetch(series_id, realtime_start, api_key):
            return row.copy()

        with patch("market_data.fetch_macro.fetch_series_vintages", side_effect=fake_fetch):
            first = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)
            second = update_series("DFF", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        assert first == 1
        assert second == 0

    def test_revision_detected_logging(self, tmp_path, caplog):
        """A new vintage for an already-known observation period is logged as a revision."""
        period = datetime.date(2019, 10, 1)
        first_vintage = pd.DataFrame([_seed_row("GDPC1", period, datetime.date(2020, 1, 30))])
        write_table(first_vintage, "macro", tmp_path)

        second_vintage = pd.DataFrame([{
            **_seed_row("GDPC1", period, datetime.date(2020, 3, 26)),
            "value": 19.2,
            "valid_to_date": datetime.date(9999, 12, 31),
            "revision_rank": 2,
        }])

        with caplog.at_level(logging.INFO, logger="market_data.fetch_macro"):
            with patch(
                "market_data.fetch_macro.fetch_series_vintages",
                return_value=second_vintage,
            ):
                update_series("GDPC1", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        revision_logs = [r for r in caplog.records if "Revision detected" in r.message]
        assert len(revision_logs) >= 1
        assert "GDPC1" in revision_logs[0].message

    def test_revision_rank_recomputed_after_incremental(self, tmp_path):
        """After an incremental write adds a new vintage, stored ranks are corrected."""
        period = datetime.date(2019, 10, 1)
        first = pd.DataFrame([_seed_row("GDPC1", period, datetime.date(2020, 1, 30))])
        write_table(first, "macro", tmp_path)

        second = pd.DataFrame([{
            **_seed_row("GDPC1", period, datetime.date(2020, 3, 26)),
            "revision_rank": 99,  # deliberately wrong — should be fixed by recompute
        }])

        with patch("market_data.fetch_macro.fetch_series_vintages", return_value=second):
            update_series("GDPC1", api_key="test", start=DEFAULT_START, data_dir=tmp_path)

        stored = pd.read_parquet(tmp_path / "macro" / "data.parquet")
        stored = stored[stored["series_id"] == "GDPC1"].sort_values("report_date")
        assert list(stored["revision_rank"]) == [1, 2]

    def test_revision_rank_recomputed_partitioned_layout(self, tmp_path, monkeypatch):
        """If the macro table is ever year-partitioned, _recompute_revision_ranks
        still discovers and rewrites every partition file for the series."""
        # Simulate a partitioned macro layout.  PARTITION_COLS is a shared dict,
        # so in-place setitem is visible to fetch_macro's imported reference.
        monkeypatch.setitem(PARTITION_COLS, "macro", ["year"])

        period_2019 = datetime.date(2019, 10, 1)
        period_2020 = datetime.date(2020, 1, 1)

        part_2019 = pd.DataFrame([
            {**_seed_row("GDPC1", period_2019, datetime.date(2020, 1, 30)), "revision_rank": 99},
            {**_seed_row("GDPC1", period_2019, datetime.date(2020, 3, 26)), "revision_rank": 99},
        ])
        part_2020 = pd.DataFrame([
            {**_seed_row("GDPC1", period_2020, datetime.date(2020, 4, 30)), "revision_rank": 99},
        ])

        dir_2019 = tmp_path / "macro" / "year=2019"
        dir_2020 = tmp_path / "macro" / "year=2020"
        dir_2019.mkdir(parents=True)
        dir_2020.mkdir(parents=True)
        part_2019.to_parquet(dir_2019 / "data.parquet", index=False)
        part_2020.to_parquet(dir_2020 / "data.parquet", index=False)

        _recompute_revision_ranks("GDPC1", tmp_path)

        stored_2019 = pd.read_parquet(dir_2019 / "data.parquet").sort_values("report_date")
        stored_2020 = pd.read_parquet(dir_2020 / "data.parquet").sort_values("report_date")

        assert list(stored_2019["revision_rank"]) == [1, 2]
        assert list(stored_2020["revision_rank"]) == [1]


# ---------------------------------------------------------------------------
# TestDetectRevisions
# ---------------------------------------------------------------------------


class TestDetectRevisions:
    """Unit tests for _detect_revisions()."""

    def _make_df(self, period, report_date, value=19.0) -> pd.DataFrame:
        return pd.DataFrame([_seed_row("GDPC1", period, report_date)])

    def test_returns_zero_when_existing_empty(self):
        new_df = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        assert _detect_revisions("GDPC1", pd.DataFrame(), new_df) == 0

    def test_returns_zero_when_new_empty(self):
        existing = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        assert _detect_revisions("GDPC1", existing, pd.DataFrame()) == 0

    def test_detects_new_vintage_for_known_period(self):
        existing = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        new_df = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 3, 26))
        assert _detect_revisions("GDPC1", existing, new_df) == 1

    def test_ignores_new_periods(self):
        """A brand-new observation period is not a revision."""
        existing = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        new_df = self._make_df(datetime.date(2020, 1, 1), datetime.date(2020, 4, 29))
        assert _detect_revisions("GDPC1", existing, new_df) == 0

    def test_ignores_already_known_vintage(self):
        """Re-fetching an already-stored (period, report_date) pair is not a revision."""
        existing = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        new_df = self._make_df(datetime.date(2019, 10, 1), datetime.date(2020, 1, 30))
        assert _detect_revisions("GDPC1", existing, new_df) == 0

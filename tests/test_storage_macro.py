"""Tests for macro-specific storage query helpers: read_macro_as_of, read_macro_revisions."""
from __future__ import annotations

import datetime

import pandas as pd
import pytest

from market_data.schema import DataSource, ReportTimeMarker
from market_data.storage import read_macro_as_of, read_macro_revisions, write_table

FAR_FUTURE = datetime.date(9999, 12, 31)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _vintage(
    series_id: str,
    period: datetime.date,
    report_date: datetime.date,
    valid_to_date: datetime.date,
    value: float,
    revision_rank: int = 1,
) -> dict:
    return {
        "series_id": series_id,
        "value": value,
        "valid_to_date": valid_to_date,
        "revision_rank": revision_rank,
        "release_name": "Test Release",
        "period_start_date": period,
        "period_end_date": period,
        "report_date": report_date,
        "report_time_marker": ReportTimeMarker.POST_MARKET,
        "source": DataSource.FRED,
        "collected_at": pd.Timestamp("2024-01-01", tz="UTC"),
    }


Q4_2019 = datetime.date(2019, 10, 1)
Q3_2019 = datetime.date(2019, 7, 1)

ADVANCE   = datetime.date(2020, 1, 30)   # report_date for advance estimate
SECOND    = datetime.date(2020, 3, 26)   # report_date for second estimate
FINAL     = datetime.date(2020, 6, 25)   # report_date for final estimate

ADVANCE_SUPERSEDED = datetime.date(2020, 3, 25)
SECOND_SUPERSEDED  = datetime.date(2020, 6, 24)


@pytest.fixture
def macro_store(tmp_path):
    """Seed three vintages of GDP Q4 2019 and one vintage of Q3 2019."""
    rows = [
        _vintage("GDPC1", Q4_2019, ADVANCE, ADVANCE_SUPERSEDED, 19.1, revision_rank=1),
        _vintage("GDPC1", Q4_2019, SECOND,  SECOND_SUPERSEDED,  19.2, revision_rank=2),
        _vintage("GDPC1", Q4_2019, FINAL,   FAR_FUTURE,          19.3, revision_rank=3),
        _vintage("GDPC1", Q3_2019, ADVANCE, FAR_FUTURE,          18.9, revision_rank=1),
    ]
    write_table(pd.DataFrame(rows), "macro", tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# TestReadMacroAsOf
# ---------------------------------------------------------------------------


class TestReadMacroAsOf:

    def _q4_row(self, result: pd.DataFrame) -> pd.DataFrame:
        return result[
            pd.to_datetime(result["period_start_date"]).dt.date == Q4_2019
        ]

    def test_advance_estimate_on_release_date(self, macro_store):
        """As of the day after advance release, the advance estimate is current."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2020, 2, 1), macro_store)
        q4 = self._q4_row(result)
        assert len(q4) == 1
        assert float(q4.iloc[0]["value"]) == pytest.approx(19.1)

    def test_second_estimate_after_supersession(self, macro_store):
        """After the advance is superseded, the second estimate is current."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2020, 4, 1), macro_store)
        q4 = self._q4_row(result)
        assert len(q4) == 1
        assert float(q4.iloc[0]["value"]) == pytest.approx(19.2)

    def test_final_estimate_when_active(self, macro_store):
        """The final estimate (valid_to_date=9999-12-31) is returned for recent as-of dates."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2026, 1, 1), macro_store)
        q4 = self._q4_row(result)
        assert len(q4) == 1
        assert float(q4.iloc[0]["value"]) == pytest.approx(19.3)

    def test_no_row_before_any_release(self, macro_store):
        """As of before the advance release date, no row is visible."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2020, 1, 1), macro_store)
        q4 = self._q4_row(result)
        assert len(q4) == 0

    def test_includes_q3_observation(self, macro_store):
        """Q3 2019 has a single always-active vintage; it appears in any as-of after its release."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2020, 2, 1), macro_store)
        q3 = result[pd.to_datetime(result["period_start_date"]).dt.date == Q3_2019]
        assert len(q3) == 1
        assert float(q3.iloc[0]["value"]) == pytest.approx(18.9)

    def test_empty_for_missing_table(self, tmp_path):
        """No data directory → empty DataFrame, no error."""
        result = read_macro_as_of(["GDPC1"], datetime.date(2024, 1, 1), tmp_path)
        assert result.empty

    def test_empty_for_unknown_series(self, macro_store):
        result = read_macro_as_of(["NOPE"], datetime.date(2024, 1, 1), macro_store)
        assert result.empty

    def test_filters_to_requested_series(self, tmp_path):
        """Only the requested series_ids are returned."""
        rows = [
            _vintage("DFF", datetime.date(2024, 1, 2), datetime.date(2024, 1, 2), FAR_FUTURE, 5.33),
            _vintage("UNRATE", datetime.date(2024, 1, 1), datetime.date(2024, 2, 2), FAR_FUTURE, 3.7),
        ]
        write_table(pd.DataFrame(rows), "macro", tmp_path)
        result = read_macro_as_of(["DFF"], datetime.date(2024, 3, 1), tmp_path)
        assert set(result["series_id"].unique()) == {"DFF"}


# ---------------------------------------------------------------------------
# TestReadMacroRevisions
# ---------------------------------------------------------------------------


class TestReadMacroRevisions:

    def test_returns_all_three_vintages(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        assert len(result) == 3

    def test_sorted_by_report_date(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        dates = list(pd.to_datetime(result["report_date"]).dt.date)
        assert dates == sorted(dates)

    def test_revision_rank_sequence(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        assert list(result["revision_rank"]) == [1, 2, 3]

    def test_value_change_first_row_is_nan(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        assert pd.isna(result.iloc[0]["value_change"])

    def test_value_change_subsequent_rows(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        assert result.iloc[1]["value_change"] == pytest.approx(19.2 - 19.1)
        assert result.iloc[2]["value_change"] == pytest.approx(19.3 - 19.2)

    def test_value_change_pct_first_row_is_nan(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        assert pd.isna(result.iloc[0]["value_change_pct"])

    def test_value_change_pct_second_row(self, macro_store):
        result = read_macro_revisions("GDPC1", Q4_2019, macro_store)
        expected_pct = (19.2 - 19.1) / 19.1 * 100
        assert result.iloc[1]["value_change_pct"] == pytest.approx(expected_pct)

    def test_single_vintage_has_nan_changes(self, macro_store):
        """Q3 2019 has only one vintage; change columns are NaN."""
        result = read_macro_revisions("GDPC1", Q3_2019, macro_store)
        assert len(result) == 1
        assert result.iloc[0]["revision_rank"] == 1
        assert pd.isna(result.iloc[0]["value_change"])
        assert pd.isna(result.iloc[0]["value_change_pct"])

    def test_empty_for_missing_period(self, macro_store):
        result = read_macro_revisions("GDPC1", datetime.date(2000, 1, 1), macro_store)
        assert result.empty

    def test_empty_for_missing_series(self, macro_store):
        result = read_macro_revisions("NOPE", Q4_2019, macro_store)
        assert result.empty

    def test_empty_for_missing_table(self, tmp_path):
        result = read_macro_revisions("GDPC1", Q4_2019, tmp_path)
        assert result.empty

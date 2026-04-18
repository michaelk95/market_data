"""
Tests for fetch_constituent_history.py:
  - parse_ticker_start_end() — column mapping, date parsing, active/removed
                               distinction, multi-period tickers
"""

import textwrap

import pandas as pd
import pytest

from market_data.fetch_constituent_history import parse_ticker_start_end


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _csv(*rows: str) -> str:
    """Build a minimal sp500_ticker_start_end CSV string."""
    header = "ticker,start_date,end_date"
    return header + "\n" + "\n".join(rows)


# ---------------------------------------------------------------------------
# parse_ticker_start_end()
# ---------------------------------------------------------------------------

class TestParseTickerStartEnd:
    def test_output_columns(self):
        raw = _csv("AAPL,2000-01-01,")
        result = parse_ticker_start_end(raw)
        assert list(result.columns) == ["ticker", "index", "date_added", "date_removed"]

    def test_index_column_is_sp500(self):
        raw = _csv("AAPL,2000-01-01,", "MSFT,1999-01-01,")
        result = parse_ticker_start_end(raw)
        assert (result["index"] == "SP500").all()

    def test_active_ticker_has_nat_date_removed(self):
        raw = _csv("AAPL,2000-01-01,")
        result = parse_ticker_start_end(raw)
        assert pd.isna(result.loc[0, "date_removed"])

    def test_removed_ticker_has_date_removed(self):
        raw = _csv("DEAD,2000-01-01,2010-06-15")
        result = parse_ticker_start_end(raw)
        assert result.loc[0, "date_removed"] == pd.Timestamp("2010-06-15")

    def test_date_added_parsed(self):
        raw = _csv("AAPL,1976-03-01,")
        result = parse_ticker_start_end(raw)
        assert result.loc[0, "date_added"] == pd.Timestamp("1976-03-01")

    def test_multi_period_ticker_produces_multiple_rows(self):
        # AAL left and rejoined — should produce 2 rows
        raw = _csv(
            "AAL,1996-01-02,1997-01-15",
            "AAL,2015-03-23,2024-09-23",
        )
        result = parse_ticker_start_end(raw)
        aal = result[result["ticker"] == "AAL"]
        assert len(aal) == 2
        assert aal.iloc[0]["date_added"] == pd.Timestamp("1996-01-02")
        assert aal.iloc[1]["date_added"] == pd.Timestamp("2015-03-23")

    def test_sorted_by_ticker_then_date(self):
        raw = _csv(
            "MSFT,1999-01-01,",
            "AAPL,2000-01-01,",
            "AAPL,1997-01-01,1999-12-31",
        )
        result = parse_ticker_start_end(raw)
        assert list(result["ticker"]) == ["AAPL", "AAPL", "MSFT"]
        aapl = result[result["ticker"] == "AAPL"]
        assert aapl.iloc[0]["date_added"] < aapl.iloc[1]["date_added"]

    def test_strips_whitespace_from_tickers(self):
        raw = _csv("  AAPL  ,2000-01-01,")
        result = parse_ticker_start_end(raw)
        assert result.loc[0, "ticker"] == "AAPL"

    def test_strips_whitespace_from_column_names(self):
        raw = " ticker , start_date , end_date \nAAPL,2000-01-01,"
        result = parse_ticker_start_end(raw)
        assert result.loc[0, "ticker"] == "AAPL"

    def test_filters_empty_tickers(self):
        raw = _csv("AAPL,2000-01-01,", ",2001-01-01,", "MSFT,1999-01-01,")
        result = parse_ticker_start_end(raw)
        assert set(result["ticker"]) == {"AAPL", "MSFT"}

    def test_drops_rows_with_unparseable_date_added(self):
        raw = _csv("AAPL,not-a-date,", "MSFT,2000-01-01,")
        result = parse_ticker_start_end(raw)
        assert list(result["ticker"]) == ["MSFT"]

    def test_missing_required_column_raises(self):
        raw = "symbol,start_date,end_date\nAAPL,2000-01-01,"
        with pytest.raises(ValueError, match="missing"):
            parse_ticker_start_end(raw)

    def test_mixed_active_and_removed(self):
        raw = _csv(
            "AAPL,2000-01-01,",
            "DEAD,2005-01-01,2015-06-01",
            "MSFT,1999-01-01,",
        )
        result = parse_ticker_start_end(raw)
        assert result["date_removed"].isna().sum() == 2   # AAPL and MSFT
        assert result["date_removed"].notna().sum() == 1  # DEAD

    def test_real_format_sample(self):
        """Parse a sample that mirrors the actual fja05680 CSV format."""
        raw = textwrap.dedent("""\
            ticker,start_date,end_date
            A,2000-06-05,
            AABA,1999-12-08,2017-06-19
            AAL,1996-01-02,1997-01-15
            AAL,2015-03-23,2024-09-23
            AAPL,1996-01-02,
        """)
        result = parse_ticker_start_end(raw)
        assert len(result) == 5
        assert result["ticker"].nunique() == 4
        assert result[result["ticker"] == "AAPL"]["date_removed"].isna().all()
        aaba = result[result["ticker"] == "AABA"].iloc[0]
        assert aaba["date_removed"] == pd.Timestamp("2017-06-19")

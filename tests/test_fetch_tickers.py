"""
Tests for fetch_tickers.py:
  - clean_holdings()     — filtering, corrections, market-value parsing
  - apply_date_added()   — date preservation and new-ticker assignment
  - merge_holdings()     — dedup strategy for IWM/IVV overlap
  - _inject_etf_rows()   — idempotency and ETF label correctness
"""


from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from market_data.fetch_tickers import (
    IVV_CSV_URL,
    IWM_CSV_URL,
    _inject_etf_rows,
    apply_date_added,
    clean_holdings,
    fetch_etf_holdings,
    merge_holdings,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_holdings(tickers, names, market_values, asset_classes=None):
    """Build a minimal iShares-style raw holdings DataFrame."""
    n = len(tickers)
    return pd.DataFrame(
        {
            "Ticker": tickers,
            "Name": names,
            "Market Value": market_values,
            "Asset Class": asset_classes if asset_classes else ["Equity"] * n,
        }
    )


# ---------------------------------------------------------------------------
# clean_holdings()
# ---------------------------------------------------------------------------


class TestCleanHoldings:
    def test_output_columns(self):
        raw = _raw_holdings(["AAPL"], ["Apple Inc."], ["$1,000,000.00"])
        result = clean_holdings(raw, "SP500")
        assert list(result.columns) == ["symbol", "name", "market_value", "index"]

    def test_index_label_applied(self):
        raw = _raw_holdings(["AAPL", "MSFT"], ["Apple", "Microsoft"], ["$1,000.00", "$900.00"])
        result = clean_holdings(raw, "RUT2000")
        assert (result["index"] == "RUT2000").all()

    def test_excludes_non_equity_asset_class(self):
        raw = _raw_holdings(
            ["AAPL", "CASH"],
            ["Apple", "Cash Component"],
            ["$1,000,000.00", "$0.00"],
            asset_classes=["Equity", "Cash"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_excludes_dash_placeholder_tickers(self):
        raw = _raw_holdings(
            ["AAPL", "-", ""],
            ["Apple", "Placeholder", "Blank"],
            ["$1,000,000.00", "$0.00", "$0.00"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_excludes_cvr_names(self):
        raw = _raw_holdings(
            ["AAPL", "FAKECVR"],
            ["Apple Inc.", "CVR for SomeCo Acquisition"],
            ["$1,000,000.00", "$100.00"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_excludes_rights_names(self):
        raw = _raw_holdings(
            ["AAPL", "FAKERIGHTS"],
            ["Apple Inc.", "Common RIGHTS of XYZ Corp"],
            ["$1,000,000.00", "$50.00"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_excludes_warrant_names(self):
        raw = _raw_holdings(
            ["AAPL", "FAKEWT"],
            ["Apple Inc.", "WARRANT Series B"],
            ["$1,000,000.00", "$50.00"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_excludes_escrow_names(self):
        raw = _raw_holdings(
            ["AAPL", "FAKEESC"],
            ["Apple Inc.", "Escrow shares of OldCo"],
            ["$1,000,000.00", "$10.00"],
        )
        result = clean_holdings(raw, "SP500")
        assert set(result["symbol"]) == {"AAPL"}

    def test_ticker_correction_brkb(self):
        raw = _raw_holdings(["BRKB", "AAPL"], ["Berkshire", "Apple"], ["$500,000.00", "$1,000,000.00"])
        result = clean_holdings(raw, "SP500")
        assert "BRK-B" in result["symbol"].values
        assert "BRKB" not in result["symbol"].values

    def test_ticker_correction_bfb(self):
        raw = _raw_holdings(["BFB", "AAPL"], ["Brown-Forman B", "Apple"], ["$200,000.00", "$1,000,000.00"])
        result = clean_holdings(raw, "SP500")
        assert "BF-B" in result["symbol"].values

    def test_parses_market_value_with_dollar_and_commas(self):
        raw = _raw_holdings(["AAPL"], ["Apple"], ["$1,234,567.89"])
        result = clean_holdings(raw, "SP500")
        assert result["market_value"].iloc[0] == pytest.approx(1_234_567.89)

    def test_sorts_by_market_value_descending(self):
        raw = _raw_holdings(
            ["SMALL", "BIG", "MED"],
            ["Small", "Big", "Med"],
            ["$100.00", "$1,000,000.00", "$50,000.00"],
        )
        result = clean_holdings(raw, "SP500")
        values = list(result["market_value"])
        assert values == sorted(values, reverse=True)

    def test_missing_market_value_column_gives_nan(self):
        df = pd.DataFrame({"Ticker": ["AAPL"], "Name": ["Apple"], "Asset Class": ["Equity"]})
        result = clean_holdings(df, "SP500")
        assert pd.isna(result["market_value"].iloc[0])


# ---------------------------------------------------------------------------
# apply_date_added()
# ---------------------------------------------------------------------------


class TestApplyDateAdded:
    def test_all_new_get_today_when_no_existing_file(self, tmp_path):
        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "name": ["Apple", "Microsoft"],
                "market_value": [1_000_000.0, 900_000.0],
                "index": ["SP500", "SP500"],
            }
        )
        result = apply_date_added(new_df, tmp_path / "nonexistent.csv", "2024-01-15")
        assert (result["date_added"] == "2024-01-15").all()
        assert (result["date_removed"] == "").all()

    def test_existing_symbol_preserves_original_date(self, tmp_path):
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
                "date_added": ["2023-06-01"],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_100_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-01-15")
        assert result.loc[result["symbol"] == "AAPL", "date_added"].iloc[0] == "2023-06-01"

    def test_new_symbol_gets_today(self, tmp_path):
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
                "date_added": ["2023-06-01"],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],  # MSFT is brand-new
                "name": ["Apple", "Microsoft"],
                "market_value": [1_000_000.0, 900_000.0],
                "index": ["SP500", "SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-01-15")
        assert result.loc[result["symbol"] == "MSFT", "date_added"].iloc[0] == "2024-01-15"

    def test_dropped_ticker_carried_forward(self, tmp_path):
        """Tickers that left both indices are still present in the output."""
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL", "DROPPED"],
                "name": ["Apple", "Dropped Corp"],
                "market_value": [1_000_000.0, 50_000.0],
                "index": ["SP500", "SP500"],
                "date_added": ["2023-01-01", "2022-01-01"],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],  # DROPPED not included
                "name": ["Apple Inc."],
                "market_value": [1_100_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-01-15")
        assert "DROPPED" in result["symbol"].values
        dropped_row = result[result["symbol"] == "DROPPED"]
        assert dropped_row["date_removed"].iloc[0] == "2024-01-15"

    def test_already_dropped_ticker_keeps_date_removed(self, tmp_path):
        """A ticker dropped in a prior run keeps its original date_removed."""
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL", "DROPPED"],
                "name": ["Apple", "Dropped Corp"],
                "market_value": [1_000_000.0, 50_000.0],
                "index": ["SP500", "SP500"],
                "date_added": ["2023-01-01", "2022-01-01"],
                "date_removed": ["", "2024-01-15"],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_100_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-06-01")
        dropped_row = result[result["symbol"] == "DROPPED"]
        assert dropped_row["date_removed"].iloc[0] == "2024-01-15"

    def test_active_ticker_has_empty_date_removed(self, tmp_path):
        """Active tickers always have an empty date_removed."""
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
                "date_added": ["2023-01-01"],
                "date_removed": [""],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_100_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-06-01")
        assert result.loc[result["symbol"] == "AAPL", "date_removed"].iloc[0] == ""

    def test_backfill_missing_date_added_column(self, tmp_path):
        """Old CSV without date_added or date_removed columns gets backfilled."""
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
                # no date_added or date_removed columns
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-01-15")
        # AAPL was in old CSV, so it gets the backfilled date, not today
        assert result.loc[result["symbol"] == "AAPL", "date_added"].iloc[0] == "2000-01-01"
        assert result.loc[result["symbol"] == "AAPL", "date_removed"].iloc[0] == ""

    def test_no_duplicate_symbols_in_output(self, tmp_path):
        """The output should not contain duplicate symbols."""
        existing = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple"],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
                "date_added": ["2023-01-01"],
            }
        )
        csv_path = tmp_path / "tickers.csv"
        existing.to_csv(csv_path, index=False)

        new_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_100_000.0],
                "index": ["SP500"],
            }
        )
        result = apply_date_added(new_df, csv_path, "2024-01-15")
        assert result["symbol"].nunique() == len(result)


# ---------------------------------------------------------------------------
# merge_holdings()
# ---------------------------------------------------------------------------


class TestMergeHoldings:
    def test_each_symbol_appears_once(self):
        iwm = pd.DataFrame(
            {
                "symbol": ["SHARED", "SMALL"],
                "name": ["Shared Corp", "Small Corp"],
                "market_value": [500_000.0, 100_000.0],
                "index": ["RUT2000", "RUT2000"],
            }
        )
        ivv = pd.DataFrame(
            {
                "symbol": ["SHARED", "BIG"],
                "name": ["Shared Corp IVV", "Big Corp"],
                "market_value": [600_000.0, 1_000_000.0],
                "index": ["SP500", "SP500"],
            }
        )
        result = merge_holdings(iwm, ivv)
        assert result["symbol"].value_counts().max() == 1

    def test_overlap_uses_max_market_value(self):
        iwm = pd.DataFrame(
            {"symbol": ["SHARED"], "name": ["Shared"], "market_value": [400_000.0], "index": ["RUT2000"]}
        )
        ivv = pd.DataFrame(
            {"symbol": ["SHARED"], "name": ["Shared"], "market_value": [600_000.0], "index": ["SP500"]}
        )
        result = merge_holdings(iwm, ivv)
        mv = result.loc[result["symbol"] == "SHARED", "market_value"].iloc[0]
        assert mv == pytest.approx(600_000.0)

    def test_overlap_index_label_combined(self):
        iwm = pd.DataFrame(
            {"symbol": ["SHARED"], "name": ["S"], "market_value": [500_000.0], "index": ["RUT2000"]}
        )
        ivv = pd.DataFrame(
            {"symbol": ["SHARED"], "name": ["S"], "market_value": [600_000.0], "index": ["SP500"]}
        )
        result = merge_holdings(iwm, ivv)
        idx_label = result.loc[result["symbol"] == "SHARED", "index"].iloc[0]
        assert idx_label == "SP500,RUT2000"

    def test_non_overlap_keeps_original_index(self):
        iwm = pd.DataFrame(
            {"symbol": ["SMALL"], "name": ["Small"], "market_value": [100_000.0], "index": ["RUT2000"]}
        )
        ivv = pd.DataFrame(
            {"symbol": ["BIG"], "name": ["Big"], "market_value": [1_000_000.0], "index": ["SP500"]}
        )
        result = merge_holdings(iwm, ivv)
        assert result.loc[result["symbol"] == "SMALL", "index"].iloc[0] == "RUT2000"
        assert result.loc[result["symbol"] == "BIG", "index"].iloc[0] == "SP500"

    def test_sorted_by_market_value_descending(self):
        iwm = pd.DataFrame(
            {"symbol": ["SMALL"], "name": ["Small"], "market_value": [100_000.0], "index": ["RUT2000"]}
        )
        ivv = pd.DataFrame(
            {"symbol": ["BIG"], "name": ["Big"], "market_value": [1_000_000.0], "index": ["SP500"]}
        )
        result = merge_holdings(iwm, ivv)
        assert result["symbol"].iloc[0] == "BIG"

    def test_output_columns(self):
        iwm = pd.DataFrame(
            {"symbol": ["A"], "name": ["A Corp"], "market_value": [100.0], "index": ["RUT2000"]}
        )
        ivv = pd.DataFrame(
            {"symbol": ["B"], "name": ["B Corp"], "market_value": [200.0], "index": ["SP500"]}
        )
        result = merge_holdings(iwm, ivv)
        assert set(result.columns) == {"symbol", "name", "market_value", "index"}


# ---------------------------------------------------------------------------
# _inject_etf_rows()
# ---------------------------------------------------------------------------


class TestInjectEtfRows:
    def test_injects_sector_etf_rows(self):
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
            }
        )
        result = _inject_etf_rows(df)
        sector_rows = result[result["index"] == "SECTOR_ETF"]
        assert len(sector_rows) > 0

    def test_injects_broad_etf_rows(self):
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
            }
        )
        result = _inject_etf_rows(df)
        broad_rows = result[result["index"] == "BROAD_ETF"]
        assert len(broad_rows) > 0

    def test_idempotent(self):
        """Calling _inject_etf_rows twice does not create duplicate ETF symbols."""
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
            }
        )
        once = _inject_etf_rows(df)
        twice = _inject_etf_rows(once)

        assert twice["symbol"].nunique() == len(twice)
        assert len(twice) == len(once)

    def test_does_not_duplicate_already_present_etf(self):
        from market_data.etf_config import SECTOR_ETFS

        sym, name = SECTOR_ETFS[0]
        df = pd.DataFrame(
            {
                "symbol": [sym],
                "name": [name],
                "market_value": [float("nan")],
                "index": ["SECTOR_ETF"],
            }
        )
        result = _inject_etf_rows(df)
        assert (result["symbol"] == sym).sum() == 1

    def test_etf_rows_have_nan_market_value(self):
        df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "name": ["Apple Inc."],
                "market_value": [1_000_000.0],
                "index": ["SP500"],
            }
        )
        result = _inject_etf_rows(df)
        etf_rows = result[result["index"].isin(["SECTOR_ETF", "BROAD_ETF"])]
        assert etf_rows["market_value"].isna().all()


# ---------------------------------------------------------------------------
# fetch_etf_holdings() and the latest-holdings.csv format
# ---------------------------------------------------------------------------

_LATEST_HOLDINGS_CSV = """iShares Russell 2000 ETF
Fund Holdings as of,"Oct 19, 2026"
Inception Date,"May 22, 2000"
Shares Outstanding,"276,000,000.00"
Stock,"-"
Bond,"-"
Cash,"-"
Other,"-"

Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
"BRK B","BERKSHIRE HATHAWAY INC CLASS B","Financials","Equity","1,000,000.00","1.00","1,000,000.00","1.00","1.00","United States","NYSE","USD","1.00","USD","-"
"MOG A","MOOG INC CLASS A","Industrials","Equity","500,000.00","0.50","500,000.00","1.00","1.00","United States","NYSE","USD","1.00","USD","-"
"FROG","JFROG","Information Technology","Equity","250,000.00","0.25","250,000.00","1.00","1.00","United States","NASDAQ","USD","1.00","USD","-"
"-","RENT THE RUNWAY INC","Consumer Discretionary","Equity","2.51","0.00","2.51","1.00","0.00","United States","NO MARKET (E.G. UNLISTED)","USD","1.00","USD","-"
"RTYZ6","RUSSELL 2000 EMINI CME DEC 26","Cash and/or Derivatives","Futures","0.00","0.00","1.00","1.00","1.00","-","Chicago Mercantile Exchange","USD","1.00","USD","-"
"""


def _mock_response(text: str, content_type: str = "text/plain; charset=utf-8"):
    resp = MagicMock()
    resp.text = text
    resp.headers = {"Content-Type": content_type}
    resp.raise_for_status.return_value = None
    return resp


class TestFetchEtfHoldings:
    def test_urls_use_latest_holdings_csv(self):
        assert IWM_CSV_URL.endswith("/latest-holdings.csv")
        assert IVV_CSV_URL.endswith("/latest-holdings.csv")

    def test_parses_latest_holdings_format(self):
        with patch("market_data.fetch_tickers.requests.get",
                   return_value=_mock_response(_LATEST_HOLDINGS_CSV)):
            raw = fetch_etf_holdings(IWM_CSV_URL)
        result = clean_holdings(raw, "RUT2000")
        assert list(result["symbol"]) == ["BRK-B", "MOG-A", "FROG"]
        assert result["market_value"].iloc[0] == 1_000_000.0

    def test_html_response_raises_clear_error(self):
        html = "<!DOCTYPE html>\n<html><head></head><body>Holdings</body></html>"
        with patch("market_data.fetch_tickers.requests.get",
                   return_value=_mock_response(html, "text/csv;charset=UTF-8")), \
                pytest.raises(ValueError, match="HTML"):
            fetch_etf_holdings(IWM_CSV_URL)


class TestShareClassNormalization:
    @pytest.mark.parametrize("raw_symbol,expected", [
        ("BRK B", "BRK-B"),
        ("BF B", "BF-B"),
        ("BH A", "BH-A"),
        ("BRK.B", "BRK-B"),
        (" AAPL ", "AAPL"),
    ])
    def test_share_class_separator_becomes_hyphen(self, raw_symbol, expected):
        raw = _raw_holdings([raw_symbol], ["X"], ["1.00"])
        result = clean_holdings(raw, "SP500")
        assert result["symbol"].tolist() == [expected]

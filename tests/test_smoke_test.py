"""
Tests for smoke_test.py.

All external network calls are mocked — no live connections required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import smoke_test
from smoke_test import (
    check_fred,
    check_ishares_ivv,
    check_ishares_iwm,
    check_yfinance,
    run,
)


# ---------------------------------------------------------------------------
# check_yfinance
# ---------------------------------------------------------------------------

class TestCheckYfinance:
    def test_returns_ok_when_data_present(self):
        import pandas as pd
        mock_df = pd.DataFrame({"Close": [150.0]})
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = mock_df

        with patch("yfinance.Ticker", return_value=mock_ticker):
            ok, detail = check_yfinance()

        assert ok is True
        assert "1 row" in detail

    def test_returns_fail_when_empty_df(self):
        import pandas as pd
        mock_ticker = MagicMock()
        mock_ticker.history.return_value = pd.DataFrame()

        with patch("yfinance.Ticker", return_value=mock_ticker):
            ok, detail = check_yfinance()

        assert ok is False
        assert "empty" in detail.lower()

    def test_returns_fail_on_exception(self):
        with patch("yfinance.Ticker", side_effect=ConnectionError("unreachable")):
            ok, detail = check_yfinance()

        assert ok is False
        assert detail != ""


# ---------------------------------------------------------------------------
# check_fred
# ---------------------------------------------------------------------------

class TestCheckFred:
    def test_returns_fail_when_api_key_missing(self):
        with patch(
            "market_data.fetch_macro._load_api_key",
            side_effect=RuntimeError("FRED_API_KEY is not set"),
        ):
            ok, detail = check_fred()

        assert ok is False
        assert "FRED_API_KEY" in detail

    def test_returns_ok_when_data_present(self):
        import pandas as pd
        mock_series = pd.Series([5.33], index=["2026-04-01"])

        with patch("market_data.fetch_macro._load_api_key", return_value="testkey"):
            mock_fred = MagicMock()
            mock_fred.get_series.return_value = mock_series
            with patch("fredapi.Fred", return_value=mock_fred):
                ok, detail = check_fred()

        assert ok is True
        assert "1 observation" in detail

    def test_returns_fail_when_empty_series(self):
        import pandas as pd

        with patch("market_data.fetch_macro._load_api_key", return_value="testkey"):
            mock_fred = MagicMock()
            mock_fred.get_series.return_value = pd.Series([], dtype=float)
            with patch("fredapi.Fred", return_value=mock_fred):
                ok, detail = check_fred()

        assert ok is False

    def test_returns_fail_on_connection_error(self):
        with patch("market_data.fetch_macro._load_api_key", return_value="testkey"):
            with patch("fredapi.Fred", side_effect=ConnectionError("no route")):
                ok, detail = check_fred()

        assert ok is False
        assert detail != ""


# ---------------------------------------------------------------------------
# check_ishares_iwm / check_ishares_ivv
# ---------------------------------------------------------------------------

class TestCheckIshares:
    def _mock_ok_response(self):
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status.return_value = None
        return resp

    def test_iwm_ok(self):
        with patch("requests.get", return_value=self._mock_ok_response()):
            ok, detail = check_ishares_iwm()
        assert ok is True
        assert "200" in detail

    def test_ivv_ok(self):
        with patch("requests.get", return_value=self._mock_ok_response()):
            ok, detail = check_ishares_ivv()
        assert ok is True
        assert "200" in detail

    def test_iwm_fail_on_exception(self):
        import requests as req
        with patch("requests.get", side_effect=req.ConnectionError("timeout")):
            ok, detail = check_ishares_iwm()
        assert ok is False

    def test_ivv_fail_on_http_error(self):
        import requests as req
        resp = MagicMock()
        resp.status_code = 503
        with patch(
            "requests.get",
            side_effect=req.HTTPError(response=resp),
        ):
            ok, detail = check_ishares_ivv()
        assert ok is False


# ---------------------------------------------------------------------------
# run()
# ---------------------------------------------------------------------------

class TestRun:
    """Tests for run() — patch SOURCES directly since the list holds references."""

    def _sources(self, results: list[tuple[bool, str]]) -> list[tuple[str, object]]:
        return [(f"source_{i}", lambda ok=ok, detail=detail: (ok, detail))
                for i, (ok, detail) in enumerate(results)]

    def test_returns_true_when_all_ok(self):
        sources = self._sources([(True, "ok"), (True, "ok"), (True, "ok"), (True, "ok")])
        with patch.object(smoke_test, "SOURCES", sources):
            assert run() is True

    def test_returns_false_when_any_fail(self):
        sources = self._sources([(True, "ok"), (False, "key missing"), (True, "ok"), (True, "ok")])
        with patch.object(smoke_test, "SOURCES", sources):
            assert run() is False

    def test_returns_false_when_all_fail(self):
        sources = self._sources([(False, "err"), (False, "err"), (False, "err"), (False, "err")])
        with patch.object(smoke_test, "SOURCES", sources):
            assert run() is False

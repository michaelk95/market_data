"""
Tests for resilience.py: exception classification, failure tracking, and
quarantine helpers.

Network calls and tenacity's actual retry behaviour are not exercised here
(that would require a live server).  Instead we verify:

  - _is_transient() correctly classifies various exceptions
  - record_failure / clear_failure / is_quarantined / quarantined_symbols
    maintain correct state-dict entries
  - yf_retry / fred_retry / requests_retry return a callable (identity or
    wrapped) without raising at decoration time
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests

from market_data import resilience
from market_data.resilience import (
    _is_transient,
    clear_failure,
    is_quarantined,
    quarantined_symbols,
    record_failure,
)


# ---------------------------------------------------------------------------
# _is_transient()
# ---------------------------------------------------------------------------

class TestIsTransient:
    def test_timeout_is_transient(self):
        assert _is_transient(requests.Timeout())

    def test_connection_error_is_transient(self):
        assert _is_transient(requests.ConnectionError())

    def test_http_429_is_transient(self):
        resp = MagicMock()
        resp.status_code = 429
        exc = requests.HTTPError(response=resp)
        assert _is_transient(exc)

    def test_http_500_is_transient(self):
        resp = MagicMock()
        resp.status_code = 500
        exc = requests.HTTPError(response=resp)
        assert _is_transient(exc)

    def test_http_503_is_transient(self):
        resp = MagicMock()
        resp.status_code = 503
        exc = requests.HTTPError(response=resp)
        assert _is_transient(exc)

    def test_http_404_is_not_transient(self):
        resp = MagicMock()
        resp.status_code = 404
        exc = requests.HTTPError(response=resp)
        assert not _is_transient(exc)

    def test_http_400_is_not_transient(self):
        resp = MagicMock()
        resp.status_code = 400
        exc = requests.HTTPError(response=resp)
        assert not _is_transient(exc)

    def test_value_error_is_not_transient(self):
        assert not _is_transient(ValueError("bad series"))

    def test_runtime_error_is_not_transient(self):
        assert not _is_transient(RuntimeError("unexpected"))

    def test_transient_via_exception_chain(self):
        """_is_transient walks __cause__ to detect wrapped requests errors."""
        outer = RuntimeError("yfinance wrapper")
        outer.__cause__ = requests.Timeout()
        assert _is_transient(outer)

    def test_non_transient_chain(self):
        """Exception chain with no transient cause is not transient."""
        outer = RuntimeError("wrapper")
        outer.__cause__ = ValueError("inner")
        assert not _is_transient(outer)

    def test_http_error_with_no_response_is_not_transient(self):
        exc = requests.HTTPError(response=None)
        assert not _is_transient(exc)


# ---------------------------------------------------------------------------
# record_failure / clear_failure / is_quarantined / quarantined_symbols
# ---------------------------------------------------------------------------

class TestFailureTracking:
    def _state(self) -> dict:
        return {"fetch_failures": {}}

    def test_record_failure_increments_count(self):
        state = self._state()
        record_failure(state, "AAPL", "timeout")
        assert state["fetch_failures"]["AAPL"]["count"] == 1

    def test_record_failure_twice_increments(self):
        state = self._state()
        record_failure(state, "AAPL", "timeout")
        record_failure(state, "AAPL", "timeout")
        assert state["fetch_failures"]["AAPL"]["count"] == 2

    def test_record_failure_stores_reason(self):
        state = self._state()
        record_failure(state, "AAPL", "Connection reset")
        assert state["fetch_failures"]["AAPL"]["last_reason"] == "Connection reset"

    def test_record_failure_stores_date(self):
        from datetime import date
        state = self._state()
        record_failure(state, "AAPL", "timeout")
        assert state["fetch_failures"]["AAPL"]["last_failure"] == str(date.today())

    def test_record_failure_truncates_long_reason(self):
        state = self._state()
        long_reason = "x" * 300
        record_failure(state, "AAPL", long_reason)
        assert len(state["fetch_failures"]["AAPL"]["last_reason"]) <= 200

    def test_record_failure_creates_fetch_failures_key_if_absent(self):
        state: dict = {}
        record_failure(state, "AAPL", "err")
        assert "fetch_failures" in state

    def test_clear_failure_removes_entry(self):
        state = self._state()
        record_failure(state, "AAPL", "timeout")
        clear_failure(state, "AAPL")
        assert "AAPL" not in state["fetch_failures"]

    def test_clear_failure_no_op_when_not_present(self):
        state = self._state()
        clear_failure(state, "AAPL")  # should not raise

    def test_is_quarantined_false_when_below_threshold(self, monkeypatch):
        monkeypatch.setattr(resilience, "_quarantine_threshold", lambda _=None: 5)
        state = self._state()
        for _ in range(4):
            record_failure(state, "AAPL", "err")
        assert not is_quarantined(state, "AAPL")

    def test_is_quarantined_true_at_threshold(self, monkeypatch):
        monkeypatch.setattr(resilience, "_quarantine_threshold", lambda _=None: 5)
        state = self._state()
        for _ in range(5):
            record_failure(state, "AAPL", "err")
        assert is_quarantined(state, "AAPL")

    def test_is_quarantined_false_when_not_present(self):
        state = self._state()
        assert not is_quarantined(state, "AAPL")

    def test_quarantined_symbols_returns_sorted_list(self, monkeypatch):
        monkeypatch.setattr(resilience, "_quarantine_threshold", lambda _=None: 3)
        state = self._state()
        for sym in ["ZZZZ", "AAAA", "MMMM"]:
            for _ in range(3):
                record_failure(state, sym, "err")
        # One ticker below threshold
        record_failure(state, "BBBB", "err")

        result = quarantined_symbols(state)
        assert result == ["AAAA", "MMMM", "ZZZZ"]
        assert "BBBB" not in result

    def test_quarantined_symbols_empty_when_none_quarantined(self):
        state = self._state()
        assert quarantined_symbols(state) == []

    def test_clear_failure_removes_from_quarantine(self, monkeypatch):
        monkeypatch.setattr(resilience, "_quarantine_threshold", lambda _=None: 3)
        state = self._state()
        for _ in range(3):
            record_failure(state, "AAPL", "err")
        assert is_quarantined(state, "AAPL")
        clear_failure(state, "AAPL")
        assert not is_quarantined(state, "AAPL")


# ---------------------------------------------------------------------------
# Decorator smoke test — verify the decorators are callable at decoration time
# ---------------------------------------------------------------------------

class TestRetryDecorators:
    def test_yf_retry_returns_callable(self):
        @resilience.yf_retry
        def dummy():
            return 42

        assert callable(dummy)

    def test_fred_retry_returns_callable(self):
        @resilience.fred_retry
        def dummy():
            return 42

        assert callable(dummy)

    def test_requests_retry_returns_callable(self):
        @resilience.requests_retry
        def dummy():
            return 42

        assert callable(dummy)

    def test_decorated_function_executes_normally(self):
        @resilience.yf_retry
        def add(a, b):
            return a + b

        assert add(2, 3) == 5

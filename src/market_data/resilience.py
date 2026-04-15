"""
resilience.py
-------------
Retry decorators and per-ticker failure-tracking helpers for the market_data
pipeline.

Retry behaviour
---------------
Three decorator factories share the same exponential-backoff policy, differing
only in their label for log messages:

  yf_retry        — yfinance calls (``yf.Ticker(…).history``, ``.info``, etc.)
  fred_retry      — FRED API calls (``fredapi.Fred.get_series``)
  requests_retry  — raw ``requests`` calls (iShares CSV download)

All decorators retry on transient network failures only (timeouts, connection
resets, HTTP 429/5xx) and re-raise after exhausting attempts.  Requires
``tenacity>=8.2``; if tenacity is not installed the decorators are no-ops
(existing behaviour is preserved and a DEBUG message is emitted).

Failure tracking
----------------
The orchestrator calls these helpers to maintain a ``fetch_failures`` dict
inside the pipeline ``state`` dict (which is persisted to ``state.json``):

  record_failure(state, symbol, reason)  — increment consecutive-failure count
  clear_failure(state, symbol)           — reset count on a successful fetch
  is_quarantined(state, symbol)          — True when count >= threshold
  quarantined_symbols(state)             — sorted list of quarantined symbols

The quarantine threshold defaults to 5 and is configurable via
``resilience.quarantine_threshold`` in ``config.yaml``.
"""

from __future__ import annotations

import logging
from typing import Callable, TypeVar

import requests

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)

# ---------------------------------------------------------------------------
# Transient / permanent exception classification
# ---------------------------------------------------------------------------

#: HTTP status codes that warrant a retry (rate-limit, server errors)
TRANSIENT_HTTP_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


def _is_transient(exc: BaseException) -> bool:
    """Return True if *exc* is a transient, retryable network failure."""
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else None
        return code in TRANSIENT_HTTP_CODES
    # Walk the exception chain — yfinance and fredapi wrap requests errors
    cause = exc.__cause__ or exc.__context__
    if cause is not None and cause is not exc:
        return _is_transient(cause)
    return False


# ---------------------------------------------------------------------------
# Retry decorator factory
# ---------------------------------------------------------------------------

def _make_retry_decorator(label: str) -> Callable[[F], F]:
    """
    Build a tenacity retry decorator for *label* data source.

    Reads ``resilience.retry_attempts``, ``resilience.retry_min_wait``, and
    ``resilience.retry_max_wait`` from config (with sensible defaults).

    Falls back to an identity decorator if tenacity is not installed.
    """
    try:
        from tenacity import (  # noqa: PLC0415
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_exponential,
            before_sleep_log,
        )
        from market_data.config import cfg as _cfg  # noqa: PLC0415

        attempts: int = _cfg.get("resilience.retry_attempts", 3)
        min_wait: int = _cfg.get("resilience.retry_min_wait", 2)
        max_wait: int = _cfg.get("resilience.retry_max_wait", 60)

        return retry(  # type: ignore[return-value]
            retry=retry_if_exception(_is_transient),
            wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
            stop=stop_after_attempt(attempts),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
    except ImportError:
        logger.debug("tenacity not installed; %s retry is disabled", label)

        def _identity(func: F) -> F:
            return func

        return _identity  # type: ignore[return-value]


def yf_retry(func: F) -> F:
    """Retry a yfinance call on transient network failures."""
    return _make_retry_decorator("yfinance")(func)


def fred_retry(func: F) -> F:
    """Retry a FRED API call on transient network failures."""
    return _make_retry_decorator("FRED")(func)


def requests_retry(func: F) -> F:
    """Retry a raw requests call on transient network failures."""
    return _make_retry_decorator("requests")(func)


# ---------------------------------------------------------------------------
# Failure tracking helpers
# ---------------------------------------------------------------------------

def _failures(state: dict) -> dict:
    """Return the ``fetch_failures`` sub-dict from *state*, creating it if absent."""
    if "fetch_failures" not in state:
        state["fetch_failures"] = {}
    return state["fetch_failures"]


def record_failure(state: dict, symbol: str, reason: str) -> None:
    """
    Increment the consecutive-failure counter for *symbol* in *state*.

    Stores the date of the latest failure and a truncated reason string.
    """
    from datetime import date  # noqa: PLC0415

    failures = _failures(state)
    entry: dict = failures.get(symbol, {"count": 0})
    entry["count"] = entry.get("count", 0) + 1
    entry["last_failure"] = str(date.today())
    entry["last_reason"] = str(reason)[:200]
    failures[symbol] = entry


def clear_failure(state: dict, symbol: str) -> None:
    """Remove the failure record for *symbol* after a successful fetch."""
    _failures(state).pop(symbol, None)


def _quarantine_threshold(state: dict | None = None) -> int:
    """Return the configured quarantine threshold (default: 5)."""
    try:
        from market_data.config import cfg as _cfg  # noqa: PLC0415
        return int(_cfg.get("resilience.quarantine_threshold", 5))
    except Exception:  # noqa: BLE001
        return 5


def is_quarantined(state: dict, symbol: str) -> bool:
    """Return True if *symbol* has reached or exceeded the quarantine threshold."""
    entry = _failures(state).get(symbol)
    if entry is None:
        return False
    return entry.get("count", 0) >= _quarantine_threshold(state)


def quarantined_symbols(state: dict) -> list[str]:
    """Return a sorted list of all currently quarantined symbols."""
    threshold = _quarantine_threshold(state)
    return sorted(
        sym
        for sym, entry in _failures(state).items()
        if entry.get("count", 0) >= threshold
    )

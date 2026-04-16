"""
edgar.py
--------
Thin SEC EDGAR client for resolving ticker symbols to CIK numbers and
looking up the most recent 10-K / 10-Q filing date for a given company.

These filing dates are used as `report_date` in the fundamentals pipeline
— the official submission date is the authoritative source for when data
became publicly available, enabling look-ahead-bias-free backtesting.

Public API
----------
    get_cik(ticker) -> str | None
    get_latest_filing_date(ticker, *, before=None) -> date | None

No API key is required; SEC EDGAR endpoints are public.  The SEC asks
callers to include a User-Agent header with contact information.
Rate limit: 10 requests/second.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

# SEC requires a descriptive User-Agent with contact info.
_HEADERS = {
    "User-Agent": "market-data-pipeline contact@example.com",
    "Accept-Encoding": "gzip, deflate",
}

_EARNINGS_FORMS = {"10-K", "10-Q"}

# Module-level cache: ticker (uppercased) -> zero-padded 10-digit CIK string
_cik_cache: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_cik(ticker: str) -> str | None:
    """Return the zero-padded 10-digit CIK for *ticker*, or None if not found.

    Results are cached in-process after the first network call.
    """
    global _cik_cache
    if _cik_cache is None:
        _cik_cache = _load_cik_map()

    return _cik_cache.get(ticker.upper())


def get_latest_filing_date(
    ticker: str,
    *,
    before: date | None = None,
) -> date | None:
    """Return the most recent 10-K or 10-Q filing date for *ticker*.

    Parameters
    ----------
    ticker:
        Equity ticker symbol (case-insensitive).
    before:
        If provided, only consider filings whose ``filingDate`` is on or
        before this date.  Pass today's date to get the last filing whose
        data would have been publicly visible as of today.

    Returns
    -------
    date or None
        The filing date, or None if:
        - the ticker has no CIK in EDGAR
        - no 10-K/10-Q filings are found
        - a network error occurs (logged at WARNING level)
    """
    cik = get_cik(ticker)
    if cik is None:
        logger.debug("edgar: no CIK found for %s", ticker)
        return None

    try:
        submissions = _fetch_submissions(cik)
    except Exception as exc:
        logger.warning("edgar: failed to fetch submissions for %s (CIK %s): %s", ticker, cik, exc)
        return None

    return _latest_filing_date(submissions, before=before)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_cik_map() -> dict[str, str]:
    """Fetch company_tickers.json and return a ticker→CIK dict.

    The JSON is structured as:
        {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    """
    try:
        resp = requests.get(_TICKERS_URL, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
    except Exception as exc:
        logger.warning("edgar: failed to load company_tickers.json: %s", exc)
        return {}

    mapping: dict[str, str] = {}
    for entry in data.values():
        ticker = str(entry.get("ticker", "")).upper()
        cik_int = entry.get("cik_str")
        if ticker and cik_int is not None:
            mapping[ticker] = str(int(cik_int)).zfill(10)

    logger.debug("edgar: loaded %d ticker→CIK mappings", len(mapping))
    return mapping


def _fetch_submissions(cik: str) -> dict[str, Any]:
    """Fetch the EDGAR submissions JSON for a given CIK."""
    url = _SUBMISSIONS_URL.format(cik=cik)
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()


def _latest_filing_date(
    submissions: dict[str, Any],
    *,
    before: date | None,
) -> date | None:
    """Extract the most recent qualifying filing date from a submissions dict.

    EDGAR returns recent filings as parallel arrays inside
    ``submissions["filings"]["recent"]``.  Entries are sorted newest-first.
    """
    try:
        recent = submissions["filings"]["recent"]
        forms: list[str] = recent.get("form", [])
        filing_dates: list[str] = recent.get("filingDate", [])
    except (KeyError, TypeError):
        return None

    for form, date_str in zip(forms, filing_dates):
        if form not in _EARNINGS_FORMS:
            continue
        try:
            filing_date = date.fromisoformat(date_str)
        except (ValueError, TypeError):
            continue
        if before is not None and filing_date > before:
            continue
        return filing_date

    return None

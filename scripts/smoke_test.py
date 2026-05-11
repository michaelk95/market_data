"""
smoke_test.py
-------------
Lightweight connectivity check for all external data sources used by the
market_data pipeline.

Each check makes exactly one minimal request to its source and reports whether
the source is reachable.  Run this before a full pipeline run to surface
network or credential problems early.

Sources checked
---------------
  yfinance (Yahoo Finance)   — 1 day of AAPL OHLCV via yfinance
  FRED (Federal Reserve)     — 1 observation of DFF via fredapi
  iShares IWM holdings CSV   — HTTP GET (header-only) of the IWM CSV endpoint
  iShares IVV holdings CSV   — HTTP GET (header-only) of the IVV CSV endpoint

Exit codes
----------
  0  — all sources reachable
  1  — one or more sources unreachable

Usage
-----
    python scripts/smoke_test.py
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual source checks
# ---------------------------------------------------------------------------

def check_yfinance() -> tuple[bool, str]:
    """Fetch 1 day of AAPL data via yfinance."""
    try:
        import yfinance as yf  # noqa: PLC0415
        df = yf.Ticker("AAPL").history(period="1d")
        if df.empty:
            return False, "returned empty DataFrame"
        return True, f"{len(df)} row(s) returned"
    except Exception as exc:
        return False, str(exc)


def check_fred() -> tuple[bool, str]:
    """Fetch 1 observation of DFF (Fed Funds Rate) via fredapi."""
    try:
        from market_data.fetch_macro import _load_api_key  # noqa: PLC0415
        api_key = _load_api_key()
    except RuntimeError as exc:
        return False, str(exc)

    try:
        import fredapi  # noqa: PLC0415
        fred = fredapi.Fred(api_key=api_key)
        start = str(date.today() - timedelta(days=30))
        result = fred.get_series("DFF", observation_start=start)
        if result is None or result.empty:
            return False, "no data returned"
        return True, f"{len(result)} observation(s)"
    except Exception as exc:
        return False, str(exc)


def check_ishares_iwm() -> tuple[bool, str]:
    """HEAD request to the iShares IWM holdings CSV endpoint."""
    try:
        import requests  # noqa: PLC0415
        from market_data.fetch_tickers import IWM_CSV_URL, HEADERS  # noqa: PLC0415
        resp = requests.get(IWM_CSV_URL, headers=HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        return True, f"HTTP {resp.status_code}"
    except Exception as exc:
        return False, str(exc)


def check_ishares_ivv() -> tuple[bool, str]:
    """HEAD request to the iShares IVV holdings CSV endpoint."""
    try:
        import requests  # noqa: PLC0415
        from market_data.fetch_tickers import IVV_CSV_URL, HEADERS  # noqa: PLC0415
        resp = requests.get(IVV_CSV_URL, headers=HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        return True, f"HTTP {resp.status_code}"
    except Exception as exc:
        return False, str(exc)


# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------

#: (display_name, check_function)
SOURCES: list[tuple[str, object]] = [
    ("yfinance (Yahoo Finance)", check_yfinance),
    ("FRED (Federal Reserve API)", check_fred),
    ("iShares IWM holdings CSV", check_ishares_iwm),
    ("iShares IVV holdings CSV", check_ishares_ivv),
]


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def run() -> bool:
    """
    Run all source checks and log results.

    Returns True if every source is reachable, False otherwise.
    """
    all_ok = True
    for name, check_fn in SOURCES:
        ok, detail = check_fn()  # type: ignore[operator]
        status = "OK  " if ok else "FAIL"
        logger.info("  %-38s [%s]  %s", name, status, detail)
        if not ok:
            all_ok = False
    return all_ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415
    setup_logging()

    logger.info("market_data smoke test  —  %s", date.today())
    logger.info("Checking data sources...")

    ok = run()

    if ok:
        logger.info("All sources reachable. Safe to run the full pipeline.")
    else:
        logger.warning(
            "One or more sources unreachable. "
            "Resolve the issues above before running market-data-run."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Shared pytest fixtures for the market_data test suite.
All fixtures reflect the real production schemas used in the pipeline.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from datetime import date

import pandas as pd
import pytest


@pytest.fixture
def ohlcv_df():
    """Standard OHLCV DataFrame matching the fetch.py OHLCV_COLS schema."""
    return pd.DataFrame(
        {
            "date": [date(2024, 1, 2), date(2024, 1, 3)],
            "symbol": ["AAPL", "AAPL"],
            "open": [185.0, 186.0],
            "high": [187.0, 188.0],
            "low": [184.0, 185.0],
            "close": [186.5, 187.0],
            "volume": [50_000_000.0, 45_000_000.0],
        }
    )


@pytest.fixture
def fundamentals_df():
    """One-row fundamentals snapshot in the bitemporal schema."""
    return pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "market_cap": [3_000_000_000_000.0],
            "enterprise_value": [3_100_000_000_000.0],
            "trailing_pe": [29.5],
            "forward_pe": [27.0],
            "price_to_book": [45.0],
            "trailing_eps": [6.30],
            "forward_eps": [6.90],
            "total_revenue": [400_000_000_000.0],
            "profit_margin": [0.25],
            "analyst_target_mean": [200.0],
            "analyst_target_low": [170.0],
            "analyst_target_high": [230.0],
            "analyst_recommendation": ["1.8"],
            "analyst_count": [42],
            "report_date_known": [True],
            "period_start_date": [date(2024, 1, 31)],
            "period_end_date": [date(2024, 1, 31)],
            "report_date": [date(2024, 1, 31)],
            "report_time_marker": ["post-market"],
            "source": ["yfinance"],
            "collected_at": [pd.Timestamp("2024-01-31 21:00:00", tz="UTC")],
        }
    )


@pytest.fixture
def holdings_df():
    """Sample tickers.csv DataFrame matching the fetch_tickers.py schema."""
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "XLF"],
            "name": ["Apple Inc.", "Microsoft Corp.", "Financial Select Sector SPDR Fund"],
            "market_value": [3_000_000_000_000.0, 2_800_000_000_000.0, float("nan")],
            "index": ["SP500", "SP500", "SECTOR_ETF"],
            "date_added": ["2024-01-01", "2024-01-01", "2024-01-15"],
        }
    )


@pytest.fixture
def options_df():
    """One-row options snapshot matching fetch_options.py OPTIONS_COLS schema."""
    return pd.DataFrame(
        {
            "snapshot_date": [date(2024, 1, 2)],
            "symbol": ["AAPL"],
            "expiry": [date(2024, 2, 16)],
            "strike": [190.0],
            "option_type": ["call"],
            "last_price": [5.50],
            "bid": [5.40],
            "ask": [5.60],
            "volume": [1_000.0],
            "open_interest": [5_000.0],
            "implied_vol": [0.25],
            "in_the_money": [False],
        }
    )


@pytest.fixture
def raw_yfinance_df():
    """
    Simulates the output of yf.Ticker(symbol).history() — a DataFrame with a
    timezone-aware DatetimeTZIndex and mixed-case single-level columns.

    Used to test _normalize() against realistic yfinance output.
    """
    dates = pd.DatetimeIndex(
        ["2024-01-02", "2024-01-03"], tz="America/New_York"
    )
    return pd.DataFrame(
        {
            "Open": [185.0, 186.0],
            "High": [187.0, 188.0],
            "Low": [184.0, 185.0],
            "Close": [186.5, 187.0],
            "Volume": [50_000_000.0, 45_000_000.0],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 0.0],
        },
        index=dates,
    )

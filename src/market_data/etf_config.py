"""
etf_config.py
-------------
Static configuration for sector and broad-market ETFs tracked by the pipeline.

These symbols are injected into tickers.csv with index = "SECTOR_ETF" or
"BROAD_ETF" at ticker-refresh time (see fetch_tickers.py) and are treated
differently from index constituents:

  - OHLCV history and incremental updates:  yes (same as stocks)
  - Options chains:                          yes (liquid options markets)
  - Fundamentals snapshots:                 no (not meaningful for fund wrappers)

# TODO: Phase 2 — ETF holdings collection
# SSGA publishes daily holdings CSVs for all SPDR ETFs (and iShares does the
# same for their funds). A future fetch_etf_holdings.py module could snapshot
# each ETF's top-N holdings and sector weights over time, enabling
# holdings-drift analysis and rebalancing-signal features for backtests.
# Schema would be: (as_of_date, etf_symbol, holding_symbol, weight, shares,
# market_value), stored in data/etf_holdings/<SYMBOL>.parquet.
"""

# The 11 SPDR Select Sector ETFs — canonical sector decomposition of the S&P 500.
SECTOR_ETFS: list[tuple[str, str]] = [
    ("XLF",  "Financial Select Sector SPDR Fund"),
    ("XLK",  "Technology Select Sector SPDR Fund"),
    ("XLE",  "Energy Select Sector SPDR Fund"),
    ("XLV",  "Health Care Select Sector SPDR Fund"),
    ("XLI",  "Industrial Select Sector SPDR Fund"),
    ("XLP",  "Consumer Staples Select Sector SPDR Fund"),
    ("XLY",  "Consumer Discretionary Select Sector SPDR Fund"),
    ("XLU",  "Utilities Select Sector SPDR Fund"),
    ("XLB",  "Materials Select Sector SPDR Fund"),
    ("XLRE", "Real Estate Select Sector SPDR Fund"),
    ("XLC",  "Communication Services Select Sector SPDR Fund"),
]

# Broad-market and asset-class ETFs for macro/factor context.
BROAD_ETFS: list[tuple[str, str]] = [
    ("SPY", "SPDR S&P 500 ETF Trust"),
    ("QQQ", "Invesco QQQ Trust (Nasdaq-100)"),
    ("IWM", "iShares Russell 2000 ETF"),
    ("IVV", "iShares Core S&P 500 ETF"),
    ("DIA", "SPDR Dow Jones Industrial Average ETF Trust"),
    ("GLD", "SPDR Gold Shares"),
    ("TLT", "iShares 20+ Year Treasury Bond ETF"),
    ("HYG", "iShares iBoxx $ High Yield Corporate Bond ETF"),
]

# All ETFs tracked by the pipeline (sector + broad), as a flat symbol list.
ALL_ETFS: list[str] = [sym for sym, _ in SECTOR_ETFS + BROAD_ETFS]

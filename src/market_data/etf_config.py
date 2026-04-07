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

# Broad-market, factor, fixed-income, commodity, and international ETFs.
BROAD_ETFS: list[tuple[str, str]] = [
    # --- Broad Market ---
    ("SPY",  "SPDR S&P 500 ETF Trust"),
    ("VOO",  "Vanguard S&P 500 ETF"),
    ("IVV",  "iShares Core S&P 500 ETF"),
    ("QQQ",  "Invesco QQQ Trust (Nasdaq-100)"),
    ("VTI",  "Vanguard Total Stock Market ETF"),
    ("DIA",  "SPDR Dow Jones Industrial Average ETF Trust"),
    ("MDY",  "SPDR S&P MidCap 400 ETF Trust"),
    ("IJR",  "iShares Core S&P Small-Cap ETF"),
    ("IWM",  "iShares Russell 2000 ETF"),
    # --- Factors ---
    ("MTUM", "iShares MSCI USA Momentum Factor ETF"),
    ("QUAL", "iShares MSCI USA Quality Factor ETF"),
    ("VLUE", "iShares MSCI USA Value Factor ETF"),
    ("USMV", "iShares MSCI USA Min Vol Factor ETF"),
    ("SIZE", "iShares MSCI USA Size Factor ETF"),
    ("IWF",  "iShares Russell 1000 Growth ETF"),
    ("IWD",  "iShares Russell 1000 Value ETF"),
    ("IWO",  "iShares Russell 2000 Growth ETF"),
    ("IWN",  "iShares Russell 2000 Value ETF"),
    # --- Bonds ---
    ("AGG",  "iShares Core US Aggregate Bond ETF"),
    ("BND",  "Vanguard Total Bond Market ETF"),
    ("LQD",  "iShares iBoxx $ Investment Grade Corporate Bond ETF"),
    ("HYG",  "iShares iBoxx $ High Yield Corporate Bond ETF"),
    ("JNK",  "SPDR Bloomberg High Yield Bond ETF"),
    ("SHY",  "iShares 1-3 Year Treasury Bond ETF"),
    ("IEF",  "iShares 7-10 Year Treasury Bond ETF"),
    ("TLT",  "iShares 20+ Year Treasury Bond ETF"),
    ("TIP",  "iShares TIPS Bond ETF"),
    ("EMB",  "iShares JP Morgan USD Emerging Markets Bond ETF"),
    ("MUB",  "iShares National Muni Bond ETF"),
    # --- Commodities ---
    ("GLD",  "SPDR Gold Shares"),
    ("SLV",  "iShares Silver Trust"),
    ("USO",  "United States Oil Fund"),
    ("UNG",  "United States Natural Gas Fund"),
    ("PDBC", "Invesco Optimum Yield Diversified Commodity Strategy ETF"),
    ("CORN", "Teucrium Corn Fund"),
    ("WEAT", "Teucrium Wheat Fund"),
    ("DBA",  "Invesco DB Agriculture Fund"),
    # --- International ---
    ("EFA",  "iShares MSCI EAFE ETF (Developed Markets ex-US)"),
    ("IEFA", "iShares Core MSCI EAFE ETF"),
    ("EEM",  "iShares MSCI Emerging Markets ETF"),
    ("IEMG", "iShares Core MSCI Emerging Markets ETF"),
    ("VEA",  "Vanguard FTSE Developed Markets ETF"),
    ("VWO",  "Vanguard FTSE Emerging Markets ETF"),
    ("FXI",  "iShares China Large-Cap ETF"),
    ("EWJ",  "iShares MSCI Japan ETF"),
    ("EWZ",  "iShares MSCI Brazil ETF"),
    ("EWG",  "iShares MSCI Germany ETF"),
]

# All ETFs tracked by the pipeline (sector + broad), as a flat symbol list.
ALL_ETFS: list[str] = [sym for sym, _ in SECTOR_ETFS + BROAD_ETFS]

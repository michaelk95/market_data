# Changelog

All notable changes to this project will be documented here.

---

## [Unreleased]

### Added
- `fetch_tickers.py` — downloads current Russell 2000 constituents from the
  iShares IWM ETF holdings CSV; saves `tickers.csv` sorted by market value
  descending (largest market cap first). No API key required.
- `fetch.py` — core OHLCV fetch and storage library:
  - `fetch_history(symbol, years=10)` — full historical pull via yfinance
  - `fetch_incremental(symbol, since)` — incremental pull from a given date
  - `save_ticker_data(symbol, df, data_dir)` — atomic append with dedup
  - `load_ticker_data(symbol, data_dir)` — load existing per-ticker Parquet
- `orchestrator.py` — daily pipeline runner with two phases per run:
  1. Onboard next N pending tickers (10-year history, ordered by market cap)
  2. Incrementally update all previously onboarded tickers
  - Progress persisted in `state.json`; safe to interrupt and resume
  - CLI flags: `--batch-size`, `--no-update`, `--merge`
  - Default batch size: 50 tickers/day (~40 days to full Russell 2000 coverage)
  - 1-second sleep between API calls to stay within yfinance rate limits
- `merge.py` — merges all per-ticker `data/<SYMBOL>.parquet` files into a
  single `data/merged.parquet` ready for the `paper_trading` backtest engine
- `pyproject.toml` / `requirements.txt` — project metadata and dependencies
  (`yfinance`, `pandas`, `pyarrow`, `requests`)
- `README.md` — onboarding guide, usage examples, data schema, Task Scheduler
  setup instructions
- `backlog.md` — wishlist items deferred from MVP:
  - Extend history back to 1990
  - Survivorship bias mitigation (historical Russell 2000 membership)
  - Automated scheduler (replace Windows Task Scheduler)

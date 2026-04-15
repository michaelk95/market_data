# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

`market_data` is a Python package that collects and stores financial market data for a `paper_trading` backtest engine. It orchestrates a daily pipeline that fetches and stores:

- Equity OHLCV (S&P 500 + Russell 2000) via yfinance
- Market indices & rates (VIX, Treasury yields, Fed Funds futures) via yfinance
- Macro series (CPI, GDP, unemployment, etc.) via FRED API
- Per-ticker fundamentals (market cap, P/E, analyst estimates) via yfinance
- Options chains / IV (SP500 + sector ETFs) via yfinance

## Commands

```bash
# Install (registers CLI commands)
pip install -e ".[dev]"

# Run tests
python -m pytest

# Run a single test file or function
python -m pytest tests/test_fetch.py -v
python -m pytest tests/test_orchestrator.py::test_step_onboard -v

# Lint / autofix
ruff check src/ tests/
ruff check --fix src/ tests/

# Full daily pipeline
market-data-run --indices --macro --fundamentals --options --merge

# Other CLI commands
market-data-fetch-tickers       # refresh tickers.csv from iShares ETF holdings
market-data-merge               # merge data/ohlcv/*.parquet → data/merged.parquet
market-data-fetch-indices       # update VIX, Treasury yields, Fed Funds futures
market-data-fetch-macro         # update FRED series
market-data-fetch-fundamentals  # snapshot fundamentals for all onboarded tickers
market-data-fetch-options       # next batch of options chain snapshots
market-data-health              # check data freshness by subdirectory
```

`pytest` is configured in `pyproject.toml` to always run with coverage (`--cov=src/market_data`).

Requires `FRED_API_KEY` in a `.env` file at the project root. yfinance needs no API key.

## Architecture

### Orchestration flow (`src/market_data/orchestrator.py`)

The orchestrator is the main entry point (`market-data-run`). It runs in a fixed sequence:

1. **Ticker refresh** — `fetch_tickers.py`: pulls Russell 2000 + S&P 500 constituents from iShares ETF CSV endpoints → `tickers.csv`
2. **Onboard new tickers** — `fetch.fetch_history()`: fetches 10 years of history per new ticker → `data/ohlcv/<SYMBOL>.parquet`
3. **Update existing tickers** — `fetch.fetch_incremental()`: appends rows since `last_run` date
4. **Fundamentals** (monthly, auto-skips if <30 days since last run) — `fetch_fundamentals.py` → `data/fundamentals/<SYMBOL>.parquet`
5. **Options** (rolling 50-ticker batch across ~500 SP500 + ETF tickers, ~10-day cycle) — `fetch_options.py` → `data/options/<SYMBOL>.parquet`
6. **Indices** — `fetch_indices.py` → `data/indices/<SYMBOL>.parquet`
7. **Macro** — `fetch_macro.py` (FRED API) → `data/macro/<SERIES_ID>.parquet`
8. **Merge** — `merge.py`: concatenates all `data/ohlcv/*.parquet` → `data/merged.parquet`

State (onboarded tickers, last run date, options cycle position) is persisted to `state.json`. All writes are atomic (temp file + rename) and idempotent (deduplication on `(date, symbol)`).

### Key modules

| File | Role |
|------|------|
| `orchestrator.py` | Pipeline orchestration, state management, CLI arg parsing |
| `fetch.py` | Core OHLCV fetch/normalize/save; `fetch_history()` and `fetch_incremental()` |
| `fetch_tickers.py` | iShares ETF constituent scraping → `tickers.csv` |
| `etf_config.py` | Static registry of 11 sector ETFs + 39 broad-market ETFs used for priority onboarding and options cycle |
| `fetch_fundamentals.py` | Monthly yfinance `.info` snapshots; filters out ETFs |
| `fetch_options.py` | Rolling daily options chain snapshots; cycle state in `state.json` |
| `fetch_indices.py` | VIX, Treasury yields, Fed Funds futures |
| `fetch_macro.py` | FRED API series (DFF, CPI, PCE, unemployment, payrolls, GDP) |
| `merge.py` | Concatenates per-ticker parquets into `data/merged.parquet` |
| `metrics.py` | Per-run stats (duration, symbols succeeded/failed, rows written) → `logs/metrics.json`, 90-day rolling window |
| `health.py` | Data freshness checks by subdirectory (OHLCV ≤2d, options ≤14d, fundamentals ≤35d, macro ≤7d) |
| `verify_onboarding.py` | Detects ghost entries (in `state.json` but missing parquet); `--fix` removes them |
| `logging_config.py` | Rotating file handler (10 MB, 5 backups) + console; INFO to stderr, DEBUG to `logs/market_data.log` |

### Data storage layout

```
data/
├── ohlcv/<SYMBOL>.parquet          # columns: date, symbol, open, high, low, close, volume
├── merged.parquet                  # concatenation of all ohlcv/*.parquet
├── indices/<SYMBOL>.parquet        # same schema as ohlcv
├── macro/<SERIES_ID>.parquet       # columns: date, series_id, value
├── fundamentals/<SYMBOL>.parquet   # columns: as_of, symbol, market_cap, P/E, analyst fields...
└── options/<SYMBOL>.parquet        # columns: snapshot_date, symbol, expiry, strike, option_type, bid, ask, IV, OI...

logs/
├── market_data.log                 # rotating debug log
└── metrics.json                    # per-run statistics

state.json                          # orchestrator state (onboarded list, last_run, cycle position)
tickers.csv                         # symbol, name, market_value, index, date_added
```

### Key design patterns

- **Non-fatal failures**: Individual ticker/series failures are logged and tracked in metrics but do not stop the pipeline. Silent-failure detection warns if an entire batch produced no data.
- **Lazy imports**: Heavy modules (e.g. `fetch_fundamentals`, `fetch_macro`) are imported inside functions to keep orchestrator boot fast.
- **ETF priority**: Sector and broad-market ETFs (defined in `etf_config.py`) are onboarded before regular stock tickers and included in the options cycle.
- **Resumable batching**: `state.json` tracks onboarding progress and options cycle position so any run can be safely interrupted and resumed.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs on push to `main` and all PRs:
- Tests on Python 3.10 and 3.12 in parallel
- Lint with ruff on Python 3.12

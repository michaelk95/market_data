# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

`market_data` is a Python package that collects and stores financial market data (OHLCV, indices, macro, fundamentals, options) for a `paper_trading` backtest engine.

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

`pytest` is configured in `pyproject.toml` to always run with coverage. Requires `FRED_API_KEY` in `.env`. yfinance needs no API key.

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
| `etf_config.py` | Static registry of sector + broad-market ETFs used by orchestrator and options cycle |
| `fetch_fundamentals.py` | Monthly yfinance `.info` snapshots; filters out ETFs |
| `fetch_options.py` | Rolling daily options chain snapshots; cycle state in `state.json` |
| `fetch_indices.py` | VIX, Treasury yields, Fed Funds futures |
| `fetch_macro.py` | FRED API series (DFF, CPI, PCE, unemployment, payrolls, GDP) |
| `merge.py` | Concatenates per-ticker parquets into `data/merged.parquet` |
| `metrics.py` | Per-run stats → `logs/metrics.json`, 90-day rolling window |
| `health.py` | Data freshness checks (OHLCV ≤2d, options ≤14d, fundamentals ≤35d, macro ≤7d) |

### Key design patterns

- **Non-fatal failures**: Individual ticker/series failures are logged and tracked in metrics but do not stop the pipeline. Silent-failure detection warns if an entire batch produced no data.
- **Lazy imports**: Heavy modules are imported inside functions to keep orchestrator boot fast.
- **Resumable batching**: `state.json` tracks onboarding progress and options cycle position so any run can be safely interrupted and resumed.

## Git

Do not attribute commits or PRs to Claude. No `Co-Authored-By` lines, no mention of Claude in commit messages or PR bodies.

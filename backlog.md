# Backlog

Wishlist items — not needed for MVP. Revisit when the core pipeline is stable.

---

## Data History

- **Extend history back to 1990**
  Currently pulling 10 years of history per ticker. Eventually pull from 1990
  where data is available. yfinance supports `period="max"` for this.

## Data Quality

- **Survivorship bias mitigation**
  The current ticker list reflects the *current* Russell 2000 composition only.
  This means backtests will be biased toward companies that survived and grew
  large enough to remain in the index. To fix this properly:
  - Source historical Russell 2000 membership data (e.g. from FTSE Russell
    directly, a data vendor like Tiingo/Quandl/CRSP, or an academic dataset)
  - Track index entry/exit dates per ticker
  - Include delisted/acquired/bankrupt tickers with data up to their last
    trading day
  - Tag each ticker row with whether it was in the index on that date

## ETF Data

- **ETF holdings snapshots (Phase 2)**
  SSGA publishes daily holdings CSVs for all SPDR ETFs; iShares does the same.
  A `fetch_etf_holdings.py` module could snapshot each ETF's top-N holdings and
  sector weights over time, enabling holdings-drift analysis and rebalancing
  signals for backtests.
  - Schema: `(as_of_date, etf_symbol, holding_symbol, weight, shares, market_value)`
  - Storage: `data/etf_holdings/<SYMBOL>.parquet`
  - Orchestrator step: `maybe_run_etf_holdings()` (daily cadence, like indices)

## Options & Implied Volatility

- **Upgrade options source (post Phase 4)**
  The current `fetch_options.py` uses yfinance (unofficial scraper). If data
  quality becomes an issue, consider upgrading to:
  - Alpaca paid tier — provides real option chains with accurate IV and Greeks
  - CBOE DataShop — official source for historical options data
  Greek values (delta, gamma, theta, vega) are not available from yfinance
  and would require a paid source.

## Scheduling & Orchestration

- **Automated scheduler**
  Replace Windows Task Scheduler with a built-in scheduling solution so the
  pipeline is self-contained and portable (e.g. APScheduler, Prefect, or a
  Claude Code scheduled task).

## Maintainability

- **Observability and pipeline monitoring**
  The orchestrator currently logs everything to stdout with `print()` and there
  is no persistent run history beyond `state.json`. When a run fails partway
  through, diagnosing what happened requires scrolling terminal output. To fix:
  - Replace `print()` calls with structured logging (`logging` module) so log
    level can be controlled and output can be redirected to a file
  - Persist a per-run summary (tickers attempted, rows added, errors, duration)
    to a structured log file (e.g. `logs/runs.jsonl`)
  - Add a simple failure notification (e.g. write a `last_error.txt` or send a
    desktop toast) so silent failures don't go unnoticed for days
  - Consider a lightweight health-check command (`market-data-status`) that
    summarises the last N runs and flags any tickers that consistently fail

- **Schema evolution and parquet migration story**
  Parquet files are written with whatever columns the data source returns at
  fetch time. There is no schema version recorded, no migration tooling, and no
  guarantee that files written today will be readable by code written in three
  months. Adding a new column (e.g. `adj_close` or a new fundamental field)
  currently requires a manual re-fetch of all affected tickers. To fix:
  - Define an explicit schema per data type (OHLCV, fundamentals, options,
    indices, macro) using pandas dtype maps or a Pandera schema
  - Stamp each parquet file with a `schema_version` in its metadata
  - Write a `migrate.py` script (or CLI flag `--migrate`) that can rewrite
    existing files to the current schema without re-fetching from the source
  - Document the schema changelog so it's clear what changed and when

- **Dependency resilience and source failure handling**
  The pipeline depends on several unofficial or rate-limited external sources
  (yfinance, FRED, iShares CSV). When a source is slow, throttled, or returns
  malformed data, the current handling is a bare `except Exception` that prints
  a warning and moves on — with no retry, no backoff, and no way to know how
  much data was silently dropped. To fix:
  - Add per-source retry logic with exponential backoff (e.g. `tenacity`)
  - Distinguish transient failures (network timeout → retry) from permanent
    ones (ticker delisted → skip and mark in state.json)
  - Track fetch-failure counts per ticker in `state.json` so repeatedly failing
    tickers can be surfaced in the status report and eventually quarantined
  - Add a smoke-test command that hits each source with a single lightweight
    request and reports which sources are reachable before a full run

- **Centralized configuration**
  Runtime constants — `DATA_DIR`, `STATE_FILE`, `SLEEP_BETWEEN_CALLS`,
  `DEFAULT_BATCH_SIZE`, `FUNDAMENTALS_REFRESH_DAYS`, etc. — are scattered
  across `orchestrator.py`, `fetch.py`, `fetch_fundamentals.py`, and other
  modules. Changing the data directory or tuning a sleep interval requires
  hunting through multiple files. To fix:
  - Consolidate all tuneable constants into a single `config.py` (or a
    `[tool.market_data]` section in `pyproject.toml`)
  - Allow environment-variable overrides for the most important paths
    (`MARKET_DATA_DIR`, `MARKET_DATA_STATE`) so the pipeline can run against
    a test dataset without touching source code
  - Remove the duplicate path definitions that currently exist across modules

- **Data lineage tracking**
  There is currently no record of *when* a row was fetched, *which version* of
  the pipeline wrote it, or *which source* it came from. If yfinance silently
  returns bad data for a ticker one day, there is no way to identify and
  quarantine just those rows after the fact. To fix:
  - Add `fetched_at` (UTC timestamp) and `source` (e.g. `"yfinance"`) columns
    to all parquet files at write time
  - Optionally add a `pipeline_version` field derived from the package version
    in `pyproject.toml`
  - Update `merge.py` to preserve these lineage columns in `merged.parquet`
  - Document the lineage fields in a schema reference so downstream consumers
    know they can filter on `source` or `fetched_at` for data-quality audits

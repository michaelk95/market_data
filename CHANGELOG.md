# Changelog

All notable changes to this project will be documented here.

---

## [0.5.2] — 2026-04-16 ([#46](https://github.com/michaelk95/market_data/pull/46))

### Added
- `edgar.py` — SEC EDGAR client; maps tickers to CIK and fetches the most recent
  10-K/10-Q filing date, used as the authoritative `report_date` to eliminate
  look-ahead bias in fundamental queries.
- `migrate_fundamentals.py` — one-shot CLI (`market-data-migrate-fundamentals`) to
  convert existing per-ticker parquets to the year-partitioned bitemporal layout with
  EDGAR date backfill. Accepts `--backup` and `--dry-run` flags.
- `report_date_known` boolean field added to `FUNDAMENTALS_SCHEMA`.

### Changed
- `fetch_fundamentals.py`: rewrites fetcher to use the bitemporal schema via
  `storage.write_table()`. Falls back to collection date with
  `report_date_known=False` when EDGAR has no record for a ticker.

---

## [0.5.1] — 2026-04-15 ([#44](https://github.com/michaelk95/market_data/pull/44))

### Added
- `resilience.py` — tenacity-backed retry decorators (`yf_retry`, `fred_retry`,
  `requests_retry`) with exponential backoff (2–60s, up to 3 attempts). Transient
  errors (timeout, connection reset, HTTP 429/5xx) are retried; non-transient errors
  surface immediately.
- Failure tracking and quarantine: `fetch_failures` dict in `state.json` counts
  consecutive failures per ticker. Tickers reaching the threshold (default: 5) are
  quarantined — skipped from onboard and update queues until a successful fetch resets
  the counter. Quarantined tickers appear in the run banner and summary line.
- `smoke_test.py` — `market-data-smoke-test` CLI hits each data source with one
  minimal request and reports reachability. Exits 0 if all sources are up, 1 otherwise.
- `tenacity>=8.2` added to dependencies.
- `resilience:` section added to `config.yaml` exposing `retry_attempts`,
  `retry_min_wait`, `retry_max_wait`, and `quarantine_threshold`.

### Changed
- `fetch.py`: `@yf_retry` applied to `fetch_history` and `fetch_incremental`.
- `fetch_fundamentals.py`: extracted `_fetch_ticker_info` with `@yf_retry`; network
  errors now propagate to the outer loop so they are counted as failures rather than
  silently skipped.
- `fetch_macro.py`: `@fred_retry` applied to `fetch_series`.
- `fetch_options.py`: `@yf_retry` applied to `_get_expiry_dates` and
  `_get_option_chain`.
- `fetch_tickers.py`: `@requests_retry` applied to `fetch_etf_holdings`.
- `orchestrator.py`: quarantine filtering applied to onboard and update queues;
  quarantine summary added to run output.

---

## [0.5.0] — 2026-04-15 ([#43](https://github.com/michaelk95/market_data/pull/43))

### Added
- `schema.py` — `ReportTimeMarker`/`DataSource` enums, PyArrow schemas for all 5
  data types, and per-table `DEDUP_KEYS`, `SORT_KEYS`, and `PARTITION_COLS` maps.
- `storage.py` — `write_table()` and `read_table()` with Hive-style `year=`
  partitioning for high-volume tables (ohlcv, fundamentals, options) and single-file
  storage for small tables (indices, macro). Writes are atomic (`.tmp` → rename) and
  idempotent (dedup on table-specific keys).
- Bitemporal fields added to every schema: `period_start_date`, `period_end_date`,
  `report_date`, `report_time_marker`, `source`, `collected_at`.

### Notes
- Existing fetch modules are unchanged by this release. Individual pipeline stages
  are migrated to `storage.write_table()` in subsequent releases.

---

## [0.4.1] — 2026-04-15 ([#36](https://github.com/michaelk95/market_data/pull/36))

### Added
- `config.yaml` — repo-root configuration file with sections for `collection`,
  `macro`, `indices`, `paths`, `sources`, and `health`.
- `config.py` — `Config` class loaded once at import; exposes `cfg.get("dot.key",
  default)` and `cfg.resolve_path()` for path resolution relative to the repo root.
  Gracefully degrades to hardcoded defaults if PyYAML is missing.
- `pyyaml>=6.0` added to dependencies.

### Changed
- `orchestrator.py`, `fetch.py`, `fetch_macro.py`, `fetch_options.py`,
  `fetch_fundamentals.py`, `fetch_indices.py`, `health.py`: module-level constants
  now initialized from `cfg`. Behaviour unchanged.

---

## [0.4.0] — 2026-04-15 ([#31](https://github.com/michaelk95/market_data/pull/31), [#34](https://github.com/michaelk95/market_data/pull/34), [#35](https://github.com/michaelk95/market_data/pull/35))

### Added
- `logging_config.py` — `setup_logging()` configures a `market_data` package logger
  with a rotating file handler (`logs/market_data.log`, 10 MB, 5 backups) and stderr
  console handler.
- `metrics.py` — per-run statistics via `start_run()`, `record_symbol_result()`,
  `finish_run()`, and `load_history()`. Persists to `logs/metrics.json` with a 90-day
  rolling window (auto-pruned on `finish_run`). Wired into all orchestrator pipeline
  steps (onboard, update, options, fundamentals, indices, macro).
- `health.py` — `health_check(data_dir)` scans parquet file mtimes across `ohlcv/`,
  `options/`, `fundamentals/`, and `macro/` subdirectories. Returns a structured
  per-type staleness report (thresholds: ohlcv 2d, options 14d, fundamentals 35d,
  macro 7d). Missing or empty directories are treated as stale.
- `market-data-health` CLI entry point; exits with code 1 if any data type is stale.

### Changed
- `fetch_fundamentals.py`, `fetch_indices.py`, `fetch_macro.py`, `fetch_options.py`,
  `fetch_tickers.py`, `merge.py`, `orchestrator.py`, `verify_onboarding.py`: all
  `print()` calls replaced with structured `logger` calls at appropriate levels
  (DEBUG/INFO/WARNING/ERROR).

---

## [0.3.2] — 2026-04-09 ([#22](https://github.com/michaelk95/market_data/pull/22))

### Fixed
- `etf_config.py`: bonds entry in `BROAD_ETFS` was `TIPS` (not a valid symbol);
  corrected to `TIP` (iShares TIPS Bond ETF).

---

## [0.3.1] — 2026-04-07 ([#5](https://github.com/michaelk95/market_data/pull/5))

### Added
- GitHub Actions CI with two jobs: `test` (Python 3.10 + 3.12 matrix) and `lint`
  (ruff).
- `pytest-cov` wired into `pyproject.toml`; coverage prints on every local run.
- 92 tests across 5 modules — no network calls, all scoped to `tmp_path`:
  - `test_fetch.py`: `_normalize()`, `save_ticker_data()` idempotency/dedup/append,
    `load_ticker_data()` round-trip.
  - `test_fetch_tickers.py`: filtering, `apply_date_added()`, `merge_holdings()`,
    `_inject_etf_rows()` idempotency.
  - `test_merge.py`: multi-file merge, dedup on `(date, symbol)`, sort order,
    self-exclusion of merged output, parent-dir creation.
  - `test_orchestrator.py`: `load_state()` / `save_state()` round-trip, defaults for
    missing keys, `load_ordered_tickers()` order and error cases.
  - `test_verify_onboarding.py`: ghost/orphan detection, `fix()` ghost removal,
    idempotency, non-onboarded field preservation.

---

## [0.3.0] — 2026-04-07 ([#3](https://github.com/michaelk95/market_data/pull/3))

### Added
- `etf_config.py` — central registry of sector and broad-market ETFs tracked by the
  pipeline. Defines two groups:
  - `SECTOR_ETFS`: the 11 SPDR Select Sector ETFs (XLF, XLK, XLE, XLV, XLI, XLP,
    XLY, XLU, XLB, XLRE, XLC)
  - `BROAD_ETFS`: 8 broad-market / asset-class ETFs (SPY, QQQ, IWM, IVV, DIA, GLD,
    TLT, HYG)
  - `ALL_ETFS`: flat symbol list combining both groups

### Changed
- `fetch_tickers.py`: ETF rows are now injected into `tickers.csv` at ticker-refresh
  time via `_inject_etf_rows()`. Sector ETFs receive `index = "SECTOR_ETF"`;
  broad-market ETFs receive `index = "BROAD_ETF"`. `date_added` tracking works the
  same as for index constituents.
- `orchestrator.py`:
  - ETFs are priority-onboarded at the start of each run (outside the normal
    `--batch-size` batch limit) so they are not queued behind ~1,500 stock tickers.
  - Fundamentals snapshots now skip ETF symbols — yfinance `.info` fields for fund
    wrappers don't map to the equity fundamentals schema.
  - Options cycle now includes ETF symbols (via `fetch_options.get_etf_symbols()`)
    ahead of SP500 constituents, ensuring ETFs are covered at the start of every cycle.
- `fetch_options.py`: new `get_etf_symbols(onboarded)` function returns onboarded ETF
  symbols in `etf_config` order (sector first, then broad). Standalone CLI now
  includes ETFs in the cycle alongside SP500 tickers.

### Notes
- ETFs collect OHLCV history and options chains; fundamentals are intentionally
  skipped (not meaningful for fund wrappers).
- ETF holdings (top-N holdings, sector weights) are deferred to Phase 2 — see
  `backlog.md` for details.

---

## [0.2.4.3] — 2026-04-06 ([#4](https://github.com/michaelk95/market_data/pull/4))

### Fixed
- `fetch_options.py`: `snapshot_date`, `symbol`, and `expiry` columns were all-null in
  fetched option chain data. Root cause: constructing a `pd.DataFrame()` empty first, then
  assigning scalar values column-by-column before any indexed Series column existed.
  Pandas stored the scalars as zero-row columns; subsequent Series assignments (e.g.
  `strike`) aligned by index, leaving the scalar columns as NaN. Fixed by replacing the
  incremental assignments with a single `pd.DataFrame({...})` dict constructor, which
  correctly broadcasts scalars to the length of the Series values.

---

## [0.2.4.2] — 2026-04-06

### Added
- `verify_onboarding.py` — checks that every ticker marked as onboarded in `state.json`
  has a corresponding parquet file in `data/ohlcv/`. Ghost entries (onboarded in state
  but missing their file) are silently skipped by the pipeline and never re-fetched.
  - Reports ghosts and orphans (files on disk not tracked in state)
  - `--fix` flag removes ghost entries from `state.json` so the pipeline re-onboards them
  - CLI: `python -m market_data.verify_onboarding [--fix] [--state ...] [--data ...]`

### Fixed
- `fetch_tickers.py`: iShares ETF holdings use compact symbols for dual-class shares
  (e.g. `BRKB`, `BFB`) that strip the dot, but yfinance requires hyphens (`BRK-B`, `BF-B`).
  Added `TICKER_CORRECTIONS` map applied at fetch time. Affected tickers: `BRK-B`, `BF-B`,
  `GEF-B`, `CRD-A`, `MOG-A`.
- `fetch_tickers.py`: CVRs, warrants, rights, and escrow shares from iShares holdings are
  not tradeable equities and have no yfinance price data, causing persistent onboarding
  failures. Added `_SKIP_NAME_RE` name-based filter to exclude them at fetch time.

---

## [0.2.4.1] — 2026-04-06

### Fixed
- OHLCV parquet files were being written to `data/` root instead of `data/ohlcv/` on
  every pipeline run. Root cause: `DATA_DIR = Path("data")` in `orchestrator.py` and
  `DEFAULT_DATA_DIR = Path("data")` in `merge.py` both pointed at the wrong directory.
  - `orchestrator.py`: `DATA_DIR` changed to `Path("data/ohlcv")`
  - `merge.py`: `DEFAULT_DATA_DIR` changed to `Path("data/ohlcv")`; `DEFAULT_OUT`
    decoupled to remain `Path("data/merged.parquet")` so the merged file is unaffected

---

## [0.2.4] — 2026-04-05 ([#2](https://github.com/michaelk95/market_data/pull/2))

### Added
- `fetch_options.py` — daily option chain snapshots for SP500 tickers via yfinance:
  - Collects nearest 4 expiration dates per ticker (configurable via `--max-expiries`)
  - Fields per contract: `snapshot_date`, `symbol`, `expiry`, `strike`, `option_type`,
    `last_price`, `bid`, `ask`, `volume`, `open_interest`, `implied_vol`, `in_the_money`
  - Batched across days (default 50 tickers/run, ~10-day cycle for full SP500 coverage)
  - Cycle state tracked in `state.json` under `options_cycle`; auto-resets when all SP500
    tickers have been covered
  - Stores data in `data/options/<SYMBOL>.parquet`; deduped on
    `(snapshot_date, symbol, expiry, strike, option_type)`
  - CLI flags: `--batch-size`, `--max-expiries`, `--symbols`
- `market-data-fetch-options` — new CLI entry point
- `--options` and `--options-batch-size` flags added to `market-data-run` orchestrator
- `options_cycle` added to `state.json` schema
- `run_and_sleep.bat` updated to include `--options` in the daily run

### Notes
- yfinance option chains are an unofficial scraper; Greek values (delta, gamma, theta,
  vega) are not available. Implied volatility is available but carries the same
  reliability caveat. Suitable for research and forecasting, not production trading.

---

## [0.2.3] — 2026-04-05 ([#2](https://github.com/michaelk95/market_data/pull/2))

### Added
- `fetch_fundamentals.py` — monthly per-ticker fundamental snapshots via yfinance `.info`:
  - Valuation: `market_cap`, `enterprise_value`, `trailing_pe`, `forward_pe`, `price_to_book`
  - Earnings & revenue: `trailing_eps`, `forward_eps`, `total_revenue`, `profit_margin`
  - Analyst estimates: `analyst_target_mean/low/high`, `analyst_recommendation`, `analyst_count`
  - Each run appends one row tagged with `as_of` date; deduped on `(as_of, symbol)`
  - Stores data in `data/fundamentals/<SYMBOL>.parquet`
  - Standalone CLI defaults to all tickers in `state.json`; accepts `--symbols` override
- `market-data-fetch-fundamentals` — new CLI entry point
- `--fundamentals` flag added to `market-data-run` orchestrator
  - Auto-skipped if last fundamentals run was <30 days ago (same pattern as ticker refresh)
  - `last_fundamentals_run` persisted in `state.json`

---

## [0.2.2] — 2026-04-05 ([#2](https://github.com/michaelk95/market_data/pull/2))

### Added
- `fetch_macro.py` — collects macroeconomic time series from the FRED API:
  - 10 series configured by default (daily, monthly, and quarterly):
    - `DFF` — Effective Federal Funds Rate
    - `T10Y2Y` — 10-year minus 2-year Treasury yield spread
    - `CPIAUCSL` — CPI headline
    - `CPILFESL` — Core CPI (ex food & energy)
    - `PCEPI` — PCE Price Index
    - `PCEPILFE` — Core PCE
    - `UNRATE` — Unemployment Rate
    - `PAYEMS` — Nonfarm Payrolls
    - `GDPC1` — Real GDP (chained 2017 dollars)
    - `GDP` — Nominal GDP
  - Bootstraps from 1990-01-01 on first run; incremental updates thereafter
  - 7-day lookback on incremental pulls to capture FRED data revisions
  - Stores data in `data/macro/<SERIES_ID>.parquet` with schema `date, series_id, value`
  - Reads `FRED_API_KEY` from `.env` via `python-dotenv`
  - CLI flags: `--series`, `--start`
- `market-data-fetch-macro` — new CLI entry point
- `--macro` flag added to `market-data-run` orchestrator
- `fredapi>=0.5` and `python-dotenv>=1.0` added to dependencies
- `.env` — project-root secrets file for API keys (gitignored)
- `.env` added to `.gitignore`

---

## [0.2.1] — 2026-04-05 ([#2](https://github.com/michaelk95/market_data/pull/2))

### Added
- `fetch_indices.py` — collects daily OHLCV data for market index and rate symbols:
  - `^VIX` — CBOE Volatility Index
  - `^TNX` — 10-year Treasury yield
  - `^TYX` — 30-year Treasury yield
  - `^FVX` — 5-year Treasury yield
  - `^IRX` — 13-week T-bill yield
  - `ZQ=F` — 30-day Fed Funds Futures (front month)
  - `^GSPC` — S&P 500 index level
  - Bootstraps full history on first run; incremental updates thereafter
  - Stores data in `data/indices/<SYMBOL>.parquet` (same schema as equity OHLCV)
  - CLI flags: `--history`, `--symbols`
- `market-data-fetch-indices` — new CLI entry point
- `--indices` flag added to `market-data-run` orchestrator

---

## [0.1.0] — 2026-04-04 ([#1](https://github.com/michaelk95/market_data/pull/1))

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

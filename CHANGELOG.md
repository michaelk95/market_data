# Changelog

All notable changes to this project will be documented here.

---

## [0.12.2] — 2026-10-20 ([#86](https://github.com/michaelk95/market_data/pull/86))

### Added
- `docs/data-schemas.md`: every table's path, columns and types, dedup key,
  writer and how it fills the bitemporal fields, plus the files outside the
  tables (`merged.parquet`, `constituent_history.parquet`, `tickers.csv`,
  `state.json`) ([#72](https://github.com/michaelk95/market_data/issues/72)).
  `tests/test_data_schema_docs.py` fails when the doc and `schema.py` drift
  apart. Linked from the README.

---

## [0.12.1] — 2026-10-20 ([#85](https://github.com/michaelk95/market_data/pull/85))

### Fixed
- Rows with a null key no longer reach the ohlcv, indices or options tables
  ([#74](https://github.com/michaelk95/market_data/issues/74)). New
  `flat_tables.drop_null_keys` drops flat rows with a null dedup-key column
  (e.g. options `snapshot_date`, `symbol`, `expiry`, `strike`,
  `option_type`) and logs a warning with counts per symbol. `from_flat`
  applies it, so the live writers store the rest of the batch.
- `scripts/migrate_flat_tables.py` no longer skips a whole options file
  because it holds legacy rows with a null `symbol`. It drops those rows and
  migrates the good ones. Files skipped by an earlier migration are recovered
  by re-running `scripts/migrate_flat_tables.py --table options`.
- `fetch_option_chain` stores a missing `inTheMoney` as null, using a
  nullable boolean indexed like the yfinance chain, instead of an empty
  Series.

### Added
- `scripts/check_null_keys.py`: read-only audit of the ohlcv, indices and
  options tables. Reports null-key rows in each table and in the flat files,
  flat-file symbols missing from the table (and which of those a migration
  re-run recovers), and files the migration skips. Exits 1 when it finds a
  problem.
- `docs/research/options-null-keys.md` and ADR
  `docs/decisions/0001-drop-null-key-flat-rows.md`.

---

## [0.12.0] — 2026-10-19 ([#83](https://github.com/michaelk95/market_data/pull/83))

### Added
- `fetch.TickerBatchWriter` buffers conformed OHLCV rows for many symbols and
  writes them with one storage call per batch, so each `year=YYYY` partition
  is rewritten once per batch instead of once per symbol. `flush()` returns
  net-new rows per symbol. A failed write raises `fetch.BatchWriteError`,
  which names every symbol in the batch. Batch size comes from
  `collection.write_batch_symbols` (default 100).
- `storage.write_table_counts` merges like `write_table` but returns net-new
  rows per symbol.

### Changed
- The orchestrator's onboard and update steps, `fetch_extend_history.run` and
  `fetch_backfill.run` write through `TickerBatchWriter` instead of one
  `save_ticker_data` call per ticker. A ticker is marked onboarded or
  complete, and its metrics are recorded, only after the write holding its
  rows succeeds. If a batch write fails, every ticker in that batch is
  recorded as failed and the run continues.
- `write_table` now counts the incoming rows that survive deduplication,
  instead of the row count after the write minus the count before. The two
  differ only when a stored partition already held duplicate keys.

---

## [0.11.0] — 2026-10-19 ([#82](https://github.com/michaelk95/market_data/pull/82))

### Changed
- The ohlcv, indices and options collectors now write the bitemporal tables
  through `storage.write_table` instead of flat per-symbol files:
  `data/ohlcv/year=YYYY/data.parquet`, `data/options/year=YYYY/data.parquet`
  and `data/indices/data.parquet`. New module `flat_tables.py` holds the
  flat ↔ bitemporal conversion, shared with `scripts/migrate_flat_tables.py`
  so live and migrated rows have identical keys and deduplicate cleanly.
- Readers use the tables: `fetch_indices` (last stored date), `fetch_backfill`
  (which symbols have history), `fetch_extend_history` (earliest date per
  symbol, one table read for all tickers) and `verify_onboarding`.
- `merge.py` builds `data/merged.parquet` from `read_table("ohlcv")` and still
  emits the flat `date, symbol, open, high, low, close, volume` layout for
  paper_trading. `volume` is now float64 (the table schema type).
- Options step: `fetch_options.run` returns rows fetched per symbol, and the
  orchestrator records symbols with no rows as failures ("no options data")
  and warns when a whole batch returned nothing.

### Breaking
- Functions take the data root (`paths.data_dir`, `data/`) instead of a table
  directory and raise `ValueError` if given one: `fetch.load_ticker_data`,
  `fetch.save_ticker_data` (now returns rows written),
  `fetch_options.save_options_snapshot(new_df, data_dir)`,
  `fetch_backfill.run(data_dir=)`, `fetch_extend_history.run(data_dir=)`,
  `verify_onboarding`, and `market-data-merge --data-dir`.
- Flat `data/<table>/<SYMBOL>.parquet` files are no longer read. Re-run
  `scripts/migrate_flat_tables.py --table all` right before deploying so
  rows collected since the first migration reach the tables.

### Docs
- `AGENTS.md` describes the bitemporal ohlcv/indices/options writers.
- README storage table, layout note, data formats and `market-data-merge`
  description now show ohlcv, indices and options as bitemporal tables.

---

## [0.10.6] — 2026-10-19 ([#81](https://github.com/michaelk95/market_data/pull/81))

### Fixed
- `market-data-health` reported fundamentals as missing or stale even when
  current: its freshness scan used a non-recursive `glob("*.parquet")`, which
  never sees the `year=YYYY/data.parquet` partitions `storage.write_table`
  writes. The scan is now recursive, so partitioned tables are found while
  flat per-symbol files (ohlcv, options) and macro's single `data.parquet`
  keep working. Leftover `*.tmp.parquet` files from an interrupted write are
  ignored so they cannot make a table look fresh.
  
---
  
## [0.10.5] — 2026-10-19 ([#80](https://github.com/michaelk95/market_data/pull/80))

### Changed
- README storage paths and data formats now match what each collector
  writes: OHLCV is `data/ohlcv/<SYMBOL>.parquet` (not `data/<SYMBOL>.parquet`),
  macro is one bitemporal `data/macro/data.parquet`, fundamentals is
  `data/fundamentals/year=YYYY/data.parquet` with bitemporal columns, and
  analyst estimates are documented as their own table. Added a note on the
  two storage layouts, a bitemporal column reference, and the missing CLI
  commands (`fetch-analyst-estimates`, `extend-history`, `health`, `agent`).
- `merge.py` docstring describes the flat `data/ohlcv/` input and why the
  migrated `year=YYYY/` partitions are not read; new test pins that.
  
---

## [0.10.4] — 2026-10-19 ([#79](https://github.com/michaelk95/market_data/pull/79))

### Changed
- Agent instructions rewritten. `AGENTS.md` (new) holds the shared
  instructions: the task workflow (research, plan, failing tests, implement,
  test, lint, PR), hard rules (real data, point-in-time correctness,
  writes only through the existing atomic writers, polite caching and rate limiting), conventions and
  commands. Research goes in `docs/research/`, ADRs in `docs/decisions/`.
  `CLAUDE.md` now just imports `AGENTS.md`; the architecture overview
  and module table are removed.

---

## [0.10.3] — 2026-10-17 ([#78](https://github.com/michaelk95/market_data/pull/78))

### Changed
- Dev dependency `ruff` raised from `>=0.15,<0.16` to `>=0.17.1,<0.18`, and
  the lint debt the newer default rule set exposed (110 findings across
  `src/`, `tests/` and `scripts/`) is cleaned up:
  - Import sorting, unused `# noqa` directives (`PLC0415`, `PLW0603` and
    `BLE001` comments for rules that no longer fire) and a few
    modernisations (`collections.abc.Callable`, sorted `__all__`).
  - `logger.error(..., exc_info=True)` in `except` blocks is now
    `logger.exception(...)`; the exception text is no longer repeated in the
    message since the traceback already ends with it.
  - Blind `except Exception` narrowed where the failure types are known:
    EDGAR requests (`requests.RequestException`, `ValueError`) and
    `git rev-parse` in `version()` (`CalledProcessError`, `OSError`). An
    unexpected error in the EDGAR lookup now fails that ticker's fundamentals
    snapshot (logged with traceback) instead of being swallowed.
  - Catch-alls that are deliberate (per-expiry yfinance calls, smoke-test
    probes, the informational EDGAR lookup in `migrate_fundamentals.py`)
    keep `except Exception` with an explicit `# noqa: BLE001`; the skipped
    options expiry warning now includes the error.
  - `subprocess.run` in `agent.py` passes `check=False` explicitly; test
    fixes for unused unpacked variables, nested `with` and mutable class
    attributes.

---

## [0.10.2] — 2026-10-17 ([#77](https://github.com/michaelk95/market_data/pull/77))

### Fixed
- Scripts in `scripts/` logged nothing: they used `logging.getLogger(__name__)`,
  which is `__main__` when run directly, while `setup_logging()` only attaches
  handlers to the `market_data` logger. Each script now logs under
  `market_data.<script_name>`, and `tests/test_script_logging.py` checks that
  every script's records reach the configured handlers.

---

## [0.10.1] — 2026-10-17 ([#76](https://github.com/michaelk95/market_data/pull/76))

### Fixed
- Point-in-time lookahead in the `fundamentals` table: `fetch_fundamentals.py`
  and `scripts/migrate_fundamentals.py` set `report_date` (and
  `period_start_date`/`period_end_date`) to the latest EDGAR 10-K/10-Q filing
  date with `report_date_known=True`, although the values are a yfinance
  `.info` snapshot (market cap, P/E, ...) taken later. Point-in-time readers
  saw snapshot values months or years before they existed. `report_date` is
  now the snapshot (collection) date, with `report_date_known=True`.
- Snapshots silently dropped: rows were deduplicated on
  `(symbol, period_start_date)` with `period_start_date` = filing date, so a
  monthly snapshot taken before the ticker's next filing was discarded as a
  duplicate (e.g. the 2026-09-23 run fetched ~2,400 tickers and saved 249).
  Keying on the snapshot date keeps every snapshot from now on; lost ones
  cannot be recovered.
- CI lint failing on every branch: `ruff>=0.4` resolved to 0.17.1, which
  flags ~90 existing issues. The dev dependency is pinned to `ruff>=0.15,<0.16`
  (the version the code is linted with) until that lint debt is cleaned up.

### Added
- `last_filing_date` (date32, nullable) column in `FUNDAMENTALS_SCHEMA`: the
  latest 10-K/10-Q filing on or before the snapshot, informational only.
- `scripts/repair_fundamentals_pit.py`: one-shot repair of existing rows. Moves
  each row to its snapshot date (the `as_of` of the matching
  `fundamentals/*.parquet.bak` row for migrated rows, otherwise the
  `collected_at` date), keeps the old filing date as `last_filing_date`,
  checks keys are unique and no row is dated before its snapshot, backs up the
  old partitions and rewrites the table. Dry run unless `--apply`; safe to
  re-run.

---

## [0.10.0] — 2026-10-15 ([#75](https://github.com/michaelk95/market_data/pull/75))

### Added
- `scripts/migrate_flat_tables.py` (MD-09): one-shot migration of the flat
  per-symbol `ohlcv`, `indices` and `options` files into the bitemporal layout
  read by `storage.read_table` (`ohlcv`/`options` partitioned by year,
  `indices` as a single `data.parquet`). `date`/`snapshot_date` becomes
  `period_start_date`/`period_end_date`/`report_date`; `report_time_marker` is
  `post-market` for daily bars and `pre-market` for options; `collected_at` is
  the flat file's mtime; values are cast to `TABLE_SCHEMAS`. Writes in chunks
  (`--chunk-size`), supports `--symbols`, `--dry-run` and `--data-dir`, leaves
  the flat files in place, and is safe to re-run (picks up rows added since the
  last run). Files whose `symbol` column does not match the file name, such as
  the stale `data/ohlcv/merged.parquet`, are skipped with a warning.

---

## [0.9.1] — 2026-05-10 ([#80](https://github.com/michaelk95/market_data/pull/80))

### Changed
- Moved one-shot migration scripts (`migrate_fundamentals.py`, `migrate_macro.py`) and `smoke_test.py` out of the `market_data` package into a top-level `scripts/` directory; removed their `pyproject.toml` CLI entry points
- Updated `tests/conftest.py` to add `scripts/` to `sys.path` so `test_smoke_test.py` continues to work

---

## [0.9.0] — 2026-04-29 ([#70](https://github.com/michaelk95/market_data/issues/70))

### Added
- `src/market_data/agent.py`: Claude-powered log-parsing agent that reads
  `logs/metrics.json` and `state.json`, checks data freshness across all pipeline
  data types, detects quarantined tickers, high failure rates, and silent failures,
  then files GitHub issues for confirmed problems via the `gh` CLI.
- Three agent tools: `read_file`, `list_files`, `create_github_issue`.
- CLI: `market-data-agent` (accepts `--verbose` / `-v` to print tool calls).
- `anthropic>=0.50` added as a package dependency.

---

## [0.8.0] — 2026-04-18 ([#68](https://github.com/michaelk95/market_data/pull/68))

### Added
- `fetch_max_history()` in `fetch.py`: fetches the maximum available OHLCV
  history for a symbol using yfinance `period="max"`.
- `fetch_extend_history.py`: one-time backfill that extends all onboarded
  active tickers back to 1990 (or as far as yfinance has data). For each
  ticker, checks the earliest date on disk; if it's after 1990-01-01, fetches
  `period="max"` and merges via `save_ticker_data` (deduplication handled
  automatically). Tickers already at or before the target are marked complete
  immediately — no network call. Progress (`extend_history_completed`,
  `extend_history_failures`) is persisted to `state.json` so runs are safely
  resumable. Supports `--batch-size` and `--dry-run`. Addresses issue #6.
- CLI: `market-data-extend-history`

---

## [0.7.3] — 2026-04-18 ([#63](https://github.com/michaelk95/market_data/issues/63))

### Fixed
- `fetch_macro`: ALFRED chunks that return "does not exist in ALFRED" are now skipped silently, so series whose ALFRED coverage starts after the bootstrap date (e.g. CPILFESL, PCEPI) still collect whatever vintage history is available. If every chunk fails this way, the fetcher falls back to a plain `fred.get_series()` call — these series (e.g. DFF, T10Y2Y) are daily market rates that are never revised, so no vintage history exists and storing current values is correct. Rows fetched via the fallback carry `report_date = today` and `valid_to_date = 9999-12-31`.

---

## [0.7.2] — 2026-04-17 ([#65](https://github.com/michaelk95/market_data/pull/65))

### Added
- `fetch_constituent_history.py`: downloads historical S&P 500 constituent
  membership from the fja05680/sp500 dataset (`sp500_ticker_start_end.csv`)
  and writes `data/constituent_history.parquet` with columns `ticker`, `index`,
  `date_added`, `date_removed` (NaT = still active). Tickers that rejoined the
  index appear as multiple rows. Addresses MD-03 of the survivorship bias audit
  (issue #60).
- `fetch_backfill.py`: backfills historical OHLCV data for delisted S&P 500
  constituents. Reads `constituent_history.parquet`, finds tickers with no
  existing OHLCV file, and fetches their full membership date range via
  yfinance. Progress (completed/failures) is persisted to `state.json` so runs
  are safely resumable. Supports `--batch-size` and `--dry-run`.
- `fetch.fetch_date_range()`: bounded date-range variant of `fetch_history()`
  used by the backfill pipeline.
- `market-data-fetch-constituent-history` and `market-data-backfill-constituents`
  CLI commands.

---

## [0.7.1] — 2026-04-17 ([#63](https://github.com/michaelk95/market_data/issues/63))

### Fixed
- `fetch_macro`: chunked `get_series_all_releases` calls into 4-year windows to stay under FRED's 2000-vintage-date-per-request limit (was causing HTTP 400 for DFF and T10Y2Y on bootstrap and long lookback windows).
- `fetch_macro`: derived `realtime_end` / `valid_to_date` from the vintage chain instead of reading it from the fredapi response (fredapi never returned that column).
- `fetch_macro`: ALFRED chunks that return "does not exist in ALFRED" are now skipped silently, so series whose ALFRED coverage starts after the bootstrap date (e.g. CPILFESL, PCEPI) still collect whatever vintage history is available. If every chunk fails this way, the fetcher falls back to a plain `fred.get_series()` call — these series (e.g. DFF, T10Y2Y) are daily market rates that are never revised, so no vintage history exists and storing current values is correct. Rows fetched via the fallback carry `report_date = today` and `valid_to_date = 9999-12-31`.

---

## [0.7.0] — 2026-04-16 ([#62](https://github.com/michaelk95/market_data/pull/62))

### Added
- `tickers.csv` now includes a `date_removed` column. When a ticker drops out of
  both the Russell 2000 and S&P 500 on a refresh run, `date_removed` is set to
  that run's date and preserved on all subsequent runs. Active tickers have an
  empty `date_removed`. This enables point-in-time universe filtering in
  downstream consumers (e.g. `smelt`'s `ore.universe(as_of)`).

---

## [0.6.4] — 2026-04-16 ([#58](https://github.com/michaelk95/market_data/issues/58))

### Added
- `market_data.version()` returns `{"version": ..., "sha": ...}` for experiment provenance.
- `market_data.__version__` is a plain string of the installed package version.
- SHA falls back to `"unknown"` if git is unavailable.

---

## [0.6.3] — 2026-04-16 ([#59](https://github.com/michaelk95/market_data/pull/59))

### Changed
- `fetch_macro.SERIES_LOOKBACK_DAYS`: widened incremental lookback window for `UNRATE`,
  `PCEPI`, and `PCEPILFE` from 7 days to 400 days. `UNRATE` comes from the same BLS
  Employment Situation release as `PAYEMS` and is revised each February alongside it;
  `PCEPI`/`PCEPILFE` are subject to annual BEA comprehensive revisions every July that
  can silently rewrite years of history.

---

## [0.6.2] — 2026-04-16 ([#54](https://github.com/michaelk95/market_data/pull/54))

### Changed
- `fetch_macro.SERIES_LOOKBACK_DAYS`: widened incremental lookback window for `PAYEMS`,
  `CPIAUCSL`, and `CPILFESL` from 7 days to 400 days. These series are subject to annual
  benchmark revisions (PAYEMS each February; CPI periodically) that can silently rewrite
  years of history — a 7-day window would miss them entirely.

---

## [0.6.1] — 2026-04-16 ([#53](https://github.com/michaelk95/market_data/pull/53))

### Fixed
- `fetch_macro._recompute_revision_ranks()` now discovers partition files via
  `PARTITION_COLS["macro"]` rather than hardcoding `data/macro/data.parquet`.
  If the macro table is ever year-partitioned (like `ohlcv`, `fundamentals`,
  `options`), ranks are recomputed correctly across all partition files instead
  of silently doing nothing.
- Extracted `fetch_macro._macro_partition_paths()` to centralise file
  discovery for the macro table's current (unpartitioned) and any future
  (year-partitioned) layout.

---

## [0.6.0] — 2026-04-16 ([#42](https://github.com/michaelk95/market_data/pull/42))

### Added
- `storage.read_macro_as_of(series_ids, as_of_date, data_dir)` — point-in-time query
  returning the vintage of each (series_id, period) that was current on `as_of_date`.
  Primary primitive for look-ahead-bias-free backtest queries.
- `storage.read_macro_revisions(series_id, period_start_date, data_dir)` — returns all
  vintages of one observation ordered by `report_date`, with computed columns
  `revision_rank`, `value_change`, and `value_change_pct`.
- `fetch_macro.SERIES_LOOKBACK_DAYS` — per-series incremental lookback map.
  GDPC1 and GDP use a 400-day window to catch annual benchmark revisions (released
  each July); all other series keep the 7-day default.
- `fetch_macro._detect_revisions()` — detects and logs new vintages for already-seen
  observation periods (`[macro] Revision detected: …`).
- `fetch_macro._recompute_revision_ranks()` — after each incremental write, recomputes
  `revision_rank` across the full stored series so ranks are always correct.

### Changed
- `schema.py`: `MACRO_SCHEMA` gains `revision_rank` (`int32`) and `release_name`
  (`string`, nullable). `SORT_KEYS["macro"]` now includes `report_date` so stored rows
  are ordered by `(period_start_date, series_id, report_date)`.
- `fetch_macro.fetch_series_vintages()`: populates `revision_rank` (ordinal within the
  fetched slice) and `release_name` (static mapping for all default FRED series).
- `fetch_macro.update_series()`: uses `SERIES_LOOKBACK_DAYS` for the incremental window;
  calls `_detect_revisions` before writing and `_recompute_revision_ranks` after.

---

## [0.5.4] — 2026-04-15 ([#41](https://github.com/michaelk95/market_data/pull/41))

### Added
- `migrate_macro.py` — one-shot CLI (`market-data-migrate-macro`) to re-pull full
  FRED vintage history and write into `data/macro/data.parquet`. Flags: `--series`,
  `--start`, `--backup`, `--dry-run`.

### Changed
- `schema.py`: `MACRO_SCHEMA` gains a `valid_to_date` field (`FRED realtime_end`;
  `9999-12-31` marks currently-active vintages). `DEDUP_KEYS["macro"]` widened to
  `["series_id", "period_start_date", "report_date"]` to support multiple vintages
  per observation.
- `fetch_macro.py`: switched to `get_series_all_releases()` (FRED realtime API).
  Each row now carries `report_date = realtime_start` (vintage date) and
  `valid_to_date = realtime_end`. Incremental updates key off
  `max(report_date) − 7d`. Per-series Parquet helpers removed; all writes go
  through `storage.write_table()`.

### Migration
  Run `market-data-migrate-macro [--backup]` once to replace
  `data/macro/<SERIES_ID>.parquet` files with the bitemporal layout.
---
## [0.5.3] — 2026-04-15 ([#50](https://github.com/michaelk95/market_data/pull/50))

### Fixed
- `read_table` now returns an empty DataFrame with correct schema columns (instead of
  a bare `pd.DataFrame()` with zero columns) on all no-data paths, preventing
  `KeyError` when callers access specific columns on the result.
- `read_table` emits a `WARNING` log when a partitioned table directory contains flat
  `.parquet` files but no `year=YYYY` partitions, signalling that the migration script
  has not been run.


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

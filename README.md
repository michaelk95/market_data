# market_data

Market data collector for the `paper_trading` backtest engine.

Collects and stores six categories of data:

| Category | Source | Cadence | Storage |
|----------|--------|---------|---------|
| Equity OHLCV (SP500 + Russell 2000) | yfinance | Daily | `data/ohlcv/<SYMBOL>.parquet` |
| Market indices & rates (VIX, Treasury yields, Fed Funds futures) | yfinance | Daily | `data/indices/<SYMBOL>.parquet` |
| Macro series (CPI, GDP, unemployment, etc.) | FRED API | Daily/Monthly/Quarterly | `data/macro/data.parquet` |
| Per-ticker fundamentals (market cap, P/E, EPS, etc.) | yfinance | Monthly | `data/fundamentals/year=YYYY/data.parquet` |
| Analyst estimates (price targets, recommendation) | yfinance | Daily | `data/analyst_estimates/year=YYYY/data.parquet` |
| Options chains / IV (SP500 + sector/broad ETFs) | yfinance | Daily (batched ~10-day cycle) | `data/options/<SYMBOL>.parquet` |

There are two storage layouts. Macro, fundamentals and analyst estimates are
written by `storage.write_table` in the bitemporal layout from `schema.py`:
large tables are split into `year=YYYY/data.parquet` partitions, and small
ones go in a single `data.parquet`. The OHLCV, indices and options collectors
still write one flat Parquet file per symbol. `storage.read_table` and
`scripts/migrate_flat_tables.py` already use the bitemporal layout for those
three tables too, but the copies the migration creates are a one-off snapshot
that the collectors don't update yet. Read the flat files for current data.

---

## CLI commands

After `pip install -e .`, the following commands are available anywhere in your
virtual environment:

| Command | Description |
|---------|-------------|
| `market-data-fetch-tickers` | Download the current Russell 2000 + SP500 constituent list and save to `tickers.csv` |
| `market-data-run` | Run the daily pipeline — onboard new tickers, update existing ones, and optionally run all data types |
| `market-data-merge` | Merge the per-ticker files in `data/ohlcv/` into a single `data/merged.parquet` |
| `market-data-fetch-indices` | Update market index and rate symbols (VIX, Treasury yields, Fed Funds futures) |
| `market-data-fetch-macro` | Update FRED macro series (CPI, GDP, Fed Funds rate, Treasury spread, etc.) |
| `market-data-fetch-fundamentals` | Snapshot per-ticker fundamentals for all onboarded tickers (market cap, P/E, EPS, etc.) |
| `market-data-fetch-analyst-estimates` | Snapshot analyst price targets and recommendation for all onboarded equities |
| `market-data-fetch-options` | Run the next batch of option chain snapshots (IV, bid/ask, open interest) |
| `market-data-fetch-constituent-history` | Download historical S&P 500 constituent membership (1996–present) to `data/constituent_history.parquet` |
| `market-data-backfill-constituents` | Backfill OHLCV data for delisted S&P 500 tickers to mitigate survivorship bias |
| `market-data-extend-history` | Extend OHLCV history for onboarded tickers back to the earliest available date (targets 1990) |
| `market-data-health` | Check how recently each data directory was updated |
| `market-data-agent` | Read logs, check data freshness and file GitHub issues for confirmed problems (uses the Anthropic API) |

Each command accepts `--help` for full usage details.

---

## Onboarding

### 1. Install dependencies

Install as an editable package (recommended — registers the CLI commands):

```bash
pip install -e .
```

### 2. Set up API keys

Create a `.env` file at the project root with your FRED API key
(free at https://fred.stlouisfed.org/):

```
FRED_API_KEY=your_key_here
```

The `.env` file is gitignored. yfinance requires no API key.

### 3. Generate the ticker list

Fetches the current Russell 2000 and S&P 500 constituents from iShares ETF
holdings (no API key required) and saves them to `tickers.csv`, sorted by
market value largest-first.

```bash
market-data-fetch-tickers
```

Re-run this periodically to pick up index additions. New tickers will
automatically be queued for historical backfill the next time the
orchestrator runs.

### 4. Run the orchestrator

The orchestrator handles both historical backfill and daily incremental updates
in a single run. On each execution it:

1. **Onboards** the next N tickers (by market cap) that haven't been fetched yet,
   pulling 10 years of history each.
2. **Updates** all already-onboarded tickers with any new trading days since the
   last run.

Progress is saved to `state.json` so runs are safe to interrupt and resume.

```bash
market-data-run                                          # onboard + update OHLCV only
market-data-run --indices                                # also update indices & rates
market-data-run --macro                                  # also update FRED macro series
market-data-run --fundamentals                           # also snapshot fundamentals (monthly)
market-data-run --analyst-estimates                      # also snapshot analyst estimates
market-data-run --indices --macro --fundamentals --options --merge # full run (recommended daily)
market-data-run --batch-size 25                          # onboard fewer new tickers per day
market-data-run --batch-size 0                           # skip onboarding; updates only
market-data-run --no-update                              # skip updates; onboard only
```

At 50 tickers/day the full SP500 + Russell 2000 list takes ~60 days to onboard.

### 5. Bootstrap supplemental data

On first setup, run each supplemental command once to pull full history:

```bash
market-data-fetch-indices               # 10 years of VIX, Treasury yields, etc.
market-data-fetch-macro                 # FRED series back to 1990-01-01
market-data-fetch-fundamentals          # current snapshot for all onboarded tickers
```

### 6. Mitigate survivorship bias (one-time)

By default, the pipeline only tracks tickers in the **current** Russell 2000
and S&P 500 constituents. This introduces survivorship bias: companies that
were delisted, acquired, or removed from an index are invisible in the dataset,
making historical backtests appear more profitable than they would have been
in practice.

To reduce this bias, run the following two commands once after initial setup:

```bash
# Step 1 — download S&P 500 constituent history (1996–present, ~1,200 tickers)
market-data-fetch-constituent-history

# Step 2 — backfill OHLCV for the ~743 delisted tickers
# (runs in batches of 50; safe to interrupt and resume)
market-data-backfill-constituents --batch-size 50
market-data-backfill-constituents --batch-size 50  # re-run until "Nothing to backfill"
```

Use `--dry-run` to preview the scope before committing:

```bash
market-data-backfill-constituents --dry-run
```

**What this does:** `constituent_history.parquet` records every ticker's
S&P 500 membership span (`date_added` → `date_removed`). For each delisted
ticker with no existing OHLCV file, the backfill fetches that exact date range
from yfinance and saves it to `data/ohlcv/` using the same schema as the
regular pipeline. Progress (completed and failed tickers) is written to
`state.json` after each ticker so the run is safely resumable.

**Limitation:** yfinance has incomplete coverage for some older or
bankruptcy-reorganised tickers. Those are recorded in `state.json` under
`backfill_failures` and can be reviewed after the run.

### 7. Merge into a single file

Merge the per-ticker files in `data/ohlcv/` into a single Parquet for the backtest engine:

```bash
market-data-merge
```

This writes `data/merged.parquet`. Pass `--merge` to the orchestrator to do
this automatically at the end of each run.

### 8. Schedule daily runs

`run_and_sleep.bat` activates the virtual environment, runs the full pipeline,
logs output to `logs/runner.log`, and hibernates the PC. Point Windows Task
Scheduler at it:

```
Program:   C:\Windows\System32\cmd.exe
Arguments: /c D:\market_data\run_and_sleep.bat
Start in:  D:\market_data
```

The batch file runs:
```
market-data-run --indices --macro --fundamentals --options --merge
```

`--fundamentals` is self-throttling — it auto-skips if a snapshot was taken
less than 30 days ago, so it's safe to include in every daily run.

`--options` processes the next 50 tickers in the current cycle (ETFs first,
then SP500). After all ~500 tickers are covered (~10 days), the cycle resets
automatically.

---

## Data formats

### Equity OHLCV — `data/ohlcv/<SYMBOL>.parquet`

| Column | Type | Notes |
|--------|------|-------|
| `date` | date | Trading day |
| `symbol` | str | Ticker symbol |
| `open` | float | |
| `high` | float | |
| `low` | float | |
| `close` | float | Adjusted close |
| `volume` | float | |

`data/merged.parquet` concatenates all the per-ticker files (same columns), used as the
input to the `paper_trading` backtest engine:

```python
from paper_trading import MarketData, BacktestEngine

md = MarketData.from_parquet("../market_data/data/merged.parquet")
engine = BacktestEngine(market_data=md, initial_cash=100_000)
```

### Indices & rates — `data/indices/<SYMBOL>.parquet`

Same schema as equity OHLCV. Symbols collected:

| Symbol | Description |
|--------|-------------|
| `^VIX` | CBOE Volatility Index |
| `^TNX` | 10-year Treasury yield |
| `^TYX` | 30-year Treasury yield |
| `^FVX` | 5-year Treasury yield |
| `^IRX` | 13-week T-bill yield |
| `ZQ=F` | 30-day Fed Funds Futures (front month) |
| `^GSPC` | S&P 500 index level |

### Bitemporal columns

Every table written by `storage.write_table` (macro, fundamentals, analyst
estimates) has these columns in addition to its data columns:

| Column | Description |
|--------|-------------|
| `period_start_date` | Start of the period the row describes |
| `period_end_date` | End of that period |
| `report_date` | Date the value became publicly available |
| `report_time_marker` | `pre-market`, `during-hours` or `post-market` |
| `source` | Data provider (`yfinance`, `fred`, ...) |
| `collected_at` | UTC timestamp when the pipeline collected the row |

Filter on `report_date <= as_of` for point-in-time (look-ahead-free) queries.

### Macro series — `data/macro/data.parquet`

One file for all series. Every FRED vintage is kept, so revisions can be
replayed point-in-time (`storage.read_macro_as_of`,
`storage.read_macro_revisions`).

| Column | Description |
|--------|-------------|
| `series_id` | FRED series ID |
| `value` | Observed value in this vintage |
| `valid_to_date` | Last day this vintage was current (`9999-12-31` = still current) |
| `revision_rank` | 1 = first estimate, 2 = second, ... within a period |
| `release_name` | FRED release name (nullable) |
| *bitemporal columns* | `period_start_date` is the observation date |

Series collected by default:

| Series ID | Description | Frequency |
|-----------|-------------|-----------|
| `DFF` | Effective Federal Funds Rate | Daily |
| `T10Y2Y` | 10yr minus 2yr Treasury spread | Daily |
| `CPIAUCSL` | CPI — All Urban (headline) | Monthly |
| `CPILFESL` | Core CPI (ex food & energy) | Monthly |
| `PCEPI` | PCE Price Index | Monthly |
| `PCEPILFE` | Core PCE | Monthly |
| `UNRATE` | Unemployment Rate | Monthly |
| `PAYEMS` | Nonfarm Payrolls | Monthly |
| `GDPC1` | Real GDP (chained 2017 dollars) | Quarterly |
| `GDP` | Nominal GDP | Quarterly |

### Options chains — `data/options/<SYMBOL>.parquet`

Daily snapshots for SP500 tickers and ETFs. Each row is one option contract observed on `snapshot_date`.

| Column | Description |
|--------|-------------|
| `snapshot_date` | Date the snapshot was taken |
| `symbol` | Underlying ticker |
| `expiry` | Option expiration date |
| `strike` | Strike price |
| `option_type` | `"call"` or `"put"` |
| `last_price` | Last traded price |
| `bid` | Bid price |
| `ask` | Ask price |
| `volume` | Contracts traded on snapshot date |
| `open_interest` | Total open contracts |
| `implied_vol` | Implied volatility (annualised decimal, e.g. 0.25 = 25%) |
| `in_the_money` | `True` if the contract is currently ITM |

Covers the nearest 4 expiration dates per ticker. Data source is yfinance
(unofficial scraper) — suitable for research and forecasting, not production trading.
Greek values (delta, gamma, etc.) are not available from this source.

### Fundamentals — `data/fundamentals/year=YYYY/data.parquet`

One row per ticker per monthly snapshot, partitioned by the year of the
snapshot date.

| Column | Description |
|--------|-------------|
| `symbol` | Ticker symbol |
| `market_cap` | Market capitalisation |
| `enterprise_value` | Enterprise value |
| `trailing_pe` | Trailing P/E ratio |
| `forward_pe` | Forward P/E ratio |
| `price_to_book` | Price-to-book ratio |
| `trailing_eps` | EPS (trailing 12 months) |
| `forward_eps` | EPS (forward estimate) |
| `total_revenue` | Revenue (trailing 12 months) |
| `profit_margin` | Net profit margin |
| `report_date_known` | `True` if `report_date` comes from a known filing date |
| `last_filing_date` | Latest 10-K/10-Q filing on or before the snapshot (informational) |
| *bitemporal columns* | `period_start_date` is the snapshot date |

### Analyst estimates — `data/analyst_estimates/year=YYYY/data.parquet`

One row per ticker per daily snapshot, partitioned by year.

| Column | Description |
|--------|-------------|
| `symbol` | Ticker symbol |
| `analyst_target_mean` | Analyst mean price target |
| `analyst_target_low` | Analyst low price target |
| `analyst_target_high` | Analyst high price target |
| `analyst_recommendation` | Mean recommendation as a string, e.g. `"1.8"` (1=Strong Buy … 5=Strong Sell) |
| `analyst_count` | Number of analyst opinions |
| `report_date_known` | Always `False` (the consensus has no filing date) |
| *bitemporal columns* | `period_start_date` and `report_date` are the collection date |

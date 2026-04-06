# market_data

Market data collector for the `paper_trading` backtest engine.

Collects and stores four categories of data:

| Category | Source | Cadence | Storage |
|----------|--------|---------|---------|
| Equity OHLCV (SP500 + Russell 2000) | yfinance | Daily | `data/<SYMBOL>.parquet` |
| Market indices & rates (VIX, Treasury yields, Fed Funds futures) | yfinance | Daily | `data/indices/<SYMBOL>.parquet` |
| Macro series (CPI, GDP, unemployment, etc.) | FRED API | Daily/Monthly/Quarterly | `data/macro/<SERIES_ID>.parquet` |
| Per-ticker fundamentals (market cap, analyst estimates) | yfinance | Monthly | `data/fundamentals/<SYMBOL>.parquet` |
| Options chains / IV (SP500 only) | yfinance | Daily (batched ~10-day cycle) | `data/options/<SYMBOL>.parquet` |

---

## CLI commands

After `pip install -e .`, the following commands are available anywhere in your
virtual environment:

| Command | Description |
|---------|-------------|
| `market-data-fetch-tickers` | Download the current Russell 2000 + SP500 constituent list and save to `tickers.csv` |
| `market-data-run` | Run the daily pipeline — onboard new tickers, update existing ones, and optionally run all data types |
| `market-data-merge` | Merge all per-ticker OHLCV Parquet files into a single `data/merged.parquet` |
| `market-data-fetch-indices` | Update market index and rate symbols (VIX, Treasury yields, Fed Funds futures) |
| `market-data-fetch-macro` | Update FRED macro series (CPI, GDP, Fed Funds rate, Treasury spread, etc.) |
| `market-data-fetch-fundamentals` | Snapshot per-ticker fundamentals for all onboarded tickers (market cap, analyst targets, etc.) |
| `market-data-fetch-options` | Run the next batch of SP500 option chain snapshots (IV, bid/ask, open interest) |

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

### 6. Merge into a single file

Merge all per-ticker OHLCV files into a single Parquet for the backtest engine:

```bash
market-data-merge
```

This writes `data/merged.parquet`. Pass `--merge` to the orchestrator to do
this automatically at the end of each run.

### 7. Schedule daily runs

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

`--options` processes the next 50 SP500 tickers in the current cycle. After
all ~500 SP500 tickers are covered (~10 days), the cycle resets automatically.

---

## Data formats

### Equity OHLCV — `data/<SYMBOL>.parquet`

| Column | Type | Notes |
|--------|------|-------|
| `date` | date | Trading day |
| `symbol` | str | Ticker symbol |
| `open` | float | |
| `high` | float | |
| `low` | float | |
| `close` | float | Adjusted close |
| `volume` | float | |

`data/merged.parquet` is a concatenation of all per-ticker files, used as the
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

### Macro series — `data/macro/<SERIES_ID>.parquet`

| Column | Type | Notes |
|--------|------|-------|
| `date` | date | Observation date |
| `series_id` | str | FRED series ID |
| `value` | float | |

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

Daily snapshots for SP500 tickers. Each row is one option contract observed on `snapshot_date`.

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

### Fundamentals — `data/fundamentals/<SYMBOL>.parquet`

One row per ticker per monthly snapshot, tagged with `as_of` date.

| Column | Description |
|--------|-------------|
| `as_of` | Snapshot date |
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
| `analyst_target_mean` | Analyst mean price target |
| `analyst_target_low` | Analyst low price target |
| `analyst_target_high` | Analyst high price target |
| `analyst_recommendation` | Mean recommendation (1=Strong Buy … 5=Strong Sell) |
| `analyst_count` | Number of analyst opinions |

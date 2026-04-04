# market_data

Daily OHLCV data collector for the `paper_trading` backtest engine.

Pulls historical and incremental data for Russell 2000 tickers via yfinance,
stores per-ticker Parquet files, and produces a merged Parquet ready for the
backtest engine.

---

## Onboarding

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate the ticker list

Fetches the current Russell 2000 constituents from the iShares IWM ETF
holdings page (no API key required) and saves them to `tickers.csv`, sorted
by market value largest-first.

```bash
python fetch_tickers.py
```

Expected output:
```
Downloading IWM holdings from iShares...
Saved 1933 tickers to tickers.csv
symbol                               name  market_value
    BE          BLOOM ENERGY CLASS A CORP  703904509.44
   ...
```

Re-run this periodically to pick up index additions. New tickers will
automatically be queued for historical backfill the next time the
orchestrator runs.

### 3. Run the orchestrator

The orchestrator handles both historical backfill and daily incremental updates
in a single run. On each execution it:

1. **Onboards** the next N tickers (by market cap) that haven't been fetched yet,
   pulling 10 years of history each.
2. **Updates** all already-onboarded tickers with any new trading days since the
   last run.

Progress is saved to `state.json` so runs are safe to interrupt and resume.

```bash
python orchestrator.py                 # onboard next 50 tickers + update existing
python orchestrator.py --batch-size 25 # onboard fewer new tickers per day
python orchestrator.py --batch-size 0  # skip onboarding; updates only
python orchestrator.py --no-update     # skip updates; onboard only
python orchestrator.py --merge         # auto-run merge.py when done
```

At 50 tickers/day the full Russell 2000 takes ~40 days to onboard.

### 4. Merge into a single file

Once you have data for the tickers you want, merge them into a single Parquet
for the backtest engine:

```bash
python merge.py
```

This writes `data/merged.parquet`. Pass `--merge` to the orchestrator to do
this automatically at the end of each run.

### 5. Schedule daily runs

Use Windows Task Scheduler to run `orchestrator.py` once per day. Point it
at your Python interpreter and this project directory.

Example task action:
```
Program:   C:\path\to\.venv\Scripts\python.exe
Arguments: orchestrator.py --merge
Start in:  D:\market_data
```

---

## Data format

Per-ticker files are stored in `data/<SYMBOL>.parquet` with columns:

| Column | Type | Notes |
|--------|------|-------|
| `date` | date | Trading day |
| `symbol` | str | Ticker symbol |
| `open` | float | |
| `high` | float | |
| `low` | float | |
| `close` | float | Adjusted close |
| `volume` | float | |

The merged file (`data/merged.parquet`) is the file you pass to the
`paper_trading` backtest engine:

```python
from paper_trading import MarketData, BacktestEngine

md = MarketData.from_parquet("../market_data/data/merged.parquet")
engine = BacktestEngine(market_data=md, initial_cash=100_000)
```

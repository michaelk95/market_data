# Data schemas

What is stored under `data/`, column by column. `src/market_data/schema.py`
is the source of truth; `tests/test_data_schema_docs.py` fails when this file
and the schema drift apart.

## Reading the store

Read tables through `storage.read_table`, never by globbing parquet files:

```python
from datetime import date
from pathlib import Path

from market_data.storage import read_table

bars = read_table("ohlcv", Path("data"), symbols=["AAPL"],
                  start_date=date(2024, 1, 1), columns=["symbol", "period_start_date", "close"])
```

- `start_date` / `end_date` bound `period_start_date` (inclusive) and skip
  year partitions outside the range. `symbols` filters `symbol`;
  `series_ids` filters macro's `series_id`.
- A missing table returns an empty frame with the schema's columns.
- Dates (`date32`) come back as Python `datetime.date` objects in an `object`
  column, not `datetime64`.
- For macro, `storage.read_macro_as_of(series_ids, as_of_date, data_dir)`
  returns the vintage current on a date, and `read_macro_revisions` the full
  revision chain of one observation.

### Layout

- **Partitioned** tables are split by the year of `period_start_date`
  (Hive-style): `data/<table>/year=YYYY/data.parquet`.
- **Unpartitioned** tables are one `data/<table>/data.parquet`.
- Writes go through `storage.write_table`: rows are merged into the existing
  file, deduplicated on the table's dedup key and sorted. **The stored row
  wins**: re-collecting a key that is already stored changes nothing.
- Each file is written to `data.tmp.parquet` and renamed over `data.parquet`,
  so a crash leaves the old file intact. A leftover `*.tmp.parquet` is an
  interrupted write and can be deleted.
- Flat per-symbol `data/<table>/<SYMBOL>.parquet` files are the old
  layout. Nothing reads them; `scripts/migrate_flat_tables.py`,
  `migrate_fundamentals.py` and `migrate_macro.py` convert them.

## Bitemporal columns

Every table ends with these six columns, after its data columns. They make
point-in-time queries possible: to see what was known on date *D*, keep rows
with `report_date <= D`.

| Column | Type | Meaning |
|---|---|---|
| `period_start_date` | date32 | First day the row describes (the bar's date, the snapshot date, the macro observation date). Partition year comes from here. |
| `period_end_date` | date32 | Last day the row describes. Equal to `period_start_date` in every table today, including monthly and quarterly macro series. |
| `report_date` | date32 | First day the value was publicly knowable. Never earlier than the truth. |
| `report_time_marker` | string | When on `report_date` it became knowable: `pre-market`, `during-hours` or `post-market` (`schema.ReportTimeMarker`). |
| `source` | string | Provider: `yfinance`, `fred` or `ishares` (`schema.DataSource`). |
| `collected_at` | timestamp[us, UTC] | When our pipeline collected the row. For rows migrated from flat files it is the flat file's modification time. |

## `ohlcv`

Daily price bars for equities and ETFs, from yfinance.

- **Path:** `data/ohlcv/year=YYYY/data.parquet`
- **Dedup key:** `symbol`, `period_start_date`
- **Written by:** `fetch.save_ticker_data` / `fetch.TickerBatchWriter`
  (pipeline onboarding and updates, `market-data-extend-history`,
  `market-data-backfill-constituents`).
- **Bitemporal fields:** `period_start_date` = `period_end_date` =
  `report_date` = the bar's date, `post-market`.

| Column | Type | Notes |
|---|---|---|
| `symbol` | string | yfinance ticker, e.g. `AAPL`, `BRK-B`. |
| `open` | float64 | |
| `high` | float64 | |
| `low` | float64 | |
| `close` | float64 | |
| `volume` | float64 | Shares. Float so nulls survive. |

`market-data-merge` flattens this table into `data/merged.parquet`
(`date, symbol, open, high, low, close, volume`) for `paper_trading`.

## `indices`

Daily bars for the index and rate symbols in `config.yaml`
`indices.symbols` (e.g. `^VIX`, `^TNX`), from yfinance.

- **Path:** `data/indices/data.parquet`
- **Dedup key:** `symbol`, `period_start_date`
- **Written by:** `fetch_indices.py` via `fetch.save_ticker_data`.
- **Bitemporal fields:** as `ohlcv`.

| Column | Type | Notes |
|---|---|---|
| `symbol` | string | Includes the `^`, e.g. `^VIX`. |
| `open` | float64 | |
| `high` | float64 | |
| `low` | float64 | |
| `close` | float64 | For yield symbols (`^TNX`) this is the yield, not a price. |
| `volume` | float64 | Not meaningful for indices and yields. |

## `options`

Option chain snapshots for the nearest `collection.options_max_expiries`
expiries, from yfinance. Tickers rotate through a rolling cycle
(`state.json` `options_cycle`), so a symbol is not snapshotted every day.

- **Path:** `data/options/year=YYYY/data.parquet`
- **Dedup key:** `symbol`, `period_start_date`, `expiry`, `strike`, `option_type`
- **Written by:** `fetch_options.save_options_snapshot`.
- **Bitemporal fields:** `period_start_date` = `period_end_date` =
  `report_date` = the snapshot date, `pre-market` (chains are fetched
  around 04:00 local time, before the open).

| Column | Type | Notes |
|---|---|---|
| `symbol` | string | Underlying ticker. |
| `expiry` | date32 | Expiration date. |
| `strike` | float64 | |
| `option_type` | string | `call` or `put`. |
| `last_price` | float64 | Last trade price; may predate the snapshot for illiquid contracts. |
| `bid` | float64 | |
| `ask` | float64 | |
| `volume` | float64 | Contracts traded in the last session. |
| `open_interest` | float64 | |
| `implied_vol` | float64 | Yahoo's implied volatility, as a fraction (0.25 = 25%). |
| `in_the_money` | bool | Null when yfinance doesn't report it. |

Rows with a null key column are dropped before writing (ADR 0001);
`scripts/check_null_keys.py` audits for any stored before 0.12.1.

## `fundamentals`

Valuation and earnings snapshots from yfinance `.info`, taken every
`collection.fundamentals_refresh_days` days.

- **Path:** `data/fundamentals/year=YYYY/data.parquet`
- **Dedup key:** `symbol`, `period_start_date`
- **Written by:** `market-data-fetch-fundamentals` via `storage.write_table`.
- **Bitemporal fields:** `period_start_date` = `period_end_date` =
  `report_date` = the collection date, `post-market`. The values move with
  the price, so they weren't knowable before the snapshot; the filing date is
  *not* the `report_date`.

| Column | Type | Notes |
|---|---|---|
| `symbol` | string | |
| `market_cap` | float64 | `marketCap` |
| `enterprise_value` | float64 | `enterpriseValue` |
| `trailing_pe` | float64 | `trailingPE` |
| `forward_pe` | float64 | `forwardPE` |
| `price_to_book` | float64 | `priceToBook` |
| `trailing_eps` | float64 | `trailingEps` |
| `forward_eps` | float64 | `forwardEps` |
| `total_revenue` | float64 | `totalRevenue` |
| `profit_margin` | float64 | `profitMargins`, as a fraction. |
| `report_date_known` | bool | `True`: `report_date` is the exact collection date. |
| `last_filing_date` | date32 | Latest SEC EDGAR 10-K/10-Q on or before the snapshot. Informational; null when EDGAR has no record. |

## `analyst_estimates`

Daily analyst consensus snapshots from yfinance `.info`. Collected daily, so
the table accumulates a revision history of the consensus.

- **Path:** `data/analyst_estimates/year=YYYY/data.parquet`
- **Dedup key:** `symbol`, `period_start_date`
- **Written by:** `market-data-fetch-analyst-estimates` via
  `storage.write_table`.
- **Bitemporal fields:** `period_start_date` = `period_end_date` =
  `report_date` = the collection date, `post-market`.

| Column | Type | Notes |
|---|---|---|
| `symbol` | string | |
| `analyst_target_mean` | float64 | `targetMeanPrice` |
| `analyst_target_low` | float64 | `targetLowPrice` |
| `analyst_target_high` | float64 | `targetHighPrice` |
| `analyst_recommendation` | string | `recommendationMean` as a string, e.g. `"1.8"` (1 = strong buy, 5 = sell). |
| `analyst_count` | int64 | `numberOfAnalystOpinions` |
| `report_date_known` | bool | Always `False`: the consensus changes continuously, so the collection date is a proxy for when it became known. |

## `macro`

FRED series listed in `config.yaml` `macro.series`, with their full vintage
(ALFRED) history: one row per *vintage* of each observation, so revisions are
kept.

- **Path:** `data/macro/data.parquet`
- **Dedup key:** `series_id`, `period_start_date`, `report_date`
- **Written by:** `market-data-fetch-macro` via `storage.write_table`, then
  `fetch_macro._recompute_revision_ranks` rewrites `revision_rank`.
- **Bitemporal fields:** `period_start_date` = `period_end_date` = the FRED
  observation date (the first day of the month or quarter for monthly and
  quarterly series); `report_date` = FRED `realtime_start`, the day the
  vintage was published; `post-market`. Series that aren't in ALFRED fall
  back to current values only, with `report_date` = the day they were
  fetched.

| Column | Type | Notes |
|---|---|---|
| `series_id` | string | FRED id, e.g. `GDPC1`. |
| `value` | float64 | |
| `valid_to_date` | date32 | FRED `realtime_end`: the day this vintage was superseded. `9999-12-31` = still current. |
| `revision_rank` | int32 | 1 = first (advance) release, 2 = second estimate, … within the observation. |
| `release_name` | string | FRED release name; null for series without a mapping. |

## Files outside the tables

| Path | Format | Contents |
|---|---|---|
| `data/merged.parquet` | parquet | Flat OHLCV for `paper_trading` (`date, symbol, open, high, low, close, volume`), rebuilt by `market-data-merge`. Derived; safe to regenerate. |
| `data/constituent_history.parquet` | parquet | S&P 500 membership periods from fja05680/sp500: `ticker`, `index` (`SP500`), `date_added`, `date_removed` (NaT = still a member). A ticker that left and rejoined has several rows. |
| `tickers.csv` | csv | The ticker universe from the iShares IWM/IVV holdings: `symbol, name, market_value, index, date_added, date_removed`. Refreshed every `collection.ticker_refresh_days`. |
| `state.json` | json | Pipeline progress: `onboarded`, `last_run`, `last_ticker_refresh`, `last_fundamentals_run`, `options_cycle`, and the `backfill_*` / `extend_history_*` completed and failure lists. |
| `logs/metrics.json` | json | Per-run collection metrics (`metrics.py`). |

`tickers.csv`, `state.json` and `data/` are the only copy: back them up
before any manual change.

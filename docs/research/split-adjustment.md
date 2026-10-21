# OHLCV history is not re-adjusted after stock splits

## Question

`data/merged.parquet` shows BKNG falling 96% on 2026-04-06 and CRWD 75% in
June. Why, how widespread is it, and what can the store do about it?

## Findings

**Cause.** Every OHLCV fetch uses `auto_adjust=True`, so yfinance returns
prices adjusted for every split and dividend up to the moment of the fetch.
Yahoo adjusts by multiplying pre-split prices by the split factor, e.g. 0.5
for a 2-for-1 split ([Yahoo Help: adjusted close](https://help.yahoo.com/kb/SLN28256.html)).
The nightly run then only appends new bars. `storage._merge_write` keeps the
stored row when a key repeats, so bars stored before a split stay on the old
basis while bars after it are on the new one. The series jumps by the split
ratio at the ex-date.

**Scale.** On the deployed store (read only, 2026-10-20), 195 day-over-day
close ratios since April are below 0.55 or above 1.8. That cutoff misses 3:2
and 5:4 splits. For 7 of them, yfinance's `Ticker.splits` lists a split at
or near the jump: BKNG 25:1, CRWD 4:1, APH 2:1, CTVA 6.665:1, AVB 2.793:1,
DD 1:3 and BYND 1:30. Others could be real moves, e.g. ALIT ×19 and
FFAI ×106.

**Consumer.** `paper_trading`'s `MarketData.get_price` returns `close`
from `merged.parquet` as-is and has no corporate-action handling, so it
treats a split as a real loss or gain. `merge.py` does no adjusting either.

**yfinance split dates are approximate.** APH's split is listed for
2026-09-03, but the stored and the freshly fetched prices both halve on
2026-09-01. yfinance's price-repair docs also note that "sometimes Yahoo
fails to adjust prices on stock split day"
([yfinance price repair](https://ranaroussi.github.io/yfinance/advanced/price_repair.html)).
A freshly downloaded range, though, is consistent with itself: APH's
re-download from 09-01 to 09-05 has no jump.

**Stored vintages are unknown.** Every OHLCV row migrated from the flat files
carries `collected_at` = the 2026-10-17 migration time
(`scripts/migrate_flat_tables.py`), so the store can't tell which basis a row
is on.

**Detection costs no extra request.** `history(..., actions=True)` returns
`Dividends` and `Stock Splits` columns with the same bars. yfinance documents
`auto_adjust` as "Adjust all OHLC automatically?"
([yfinance `history` source](https://github.com/ranaroussi/yfinance/blob/main/yfinance/scrapers/history.py)).

**Size.** `data/ohlcv/` holds 65 year partitions of up to 18 MB each. A
rewrite of every partition a symbol spans takes seconds.

## Options

1. **Rescale stored rows in place** by the split ratio before the split date.
   It needs no download, but it relies on yfinance's split date, which can be
   wrong (APH), and it double-adjusts if run twice.
2. **Store unadjusted bars and a splits table, and adjust on read.** This is
   point-in-time clean, but Yahoo's unadjusted `Close` is itself
   split-adjusted, and the existing rows' basis can't be recovered from
   `collected_at`.
3. **Re-download and replace.** When a split appears, re-download the
   symbol's stored date range and replace those rows. One request per split,
   and the new range is consistent with itself whatever the split date says.

## Decision

Option 3, recorded in ADR 0002.

- The nightly update reads `Stock Splits` from the bars it already
  downloads.
- Each symbol with a split goes into a retry list in `state.json`. Its stored
  date range is re-downloaded, checked against the stored rows, and written
  with a replace that only touches that symbol's re-downloaded dates.
- `scripts/repair_split_adjustment.py` finds splits already in the store, and
  any the nightly run misses. It is a dry run by default.

Dividend-basis mixing is out of scope. It is a few percent a year, not a
step of 2× or more.

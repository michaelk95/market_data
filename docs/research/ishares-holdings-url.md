# iShares holdings CSV endpoint change

## Symptom

Since 2026-10-04 every daily run logs:

```
Ticker refresh failed: Could not locate the 'Ticker' header row in the ETF CSV.
The iShares page format may have changed.. Continuing with existing list.
```

The pipeline continues with the stale `tickers.csv` (last refreshed
2026-04-04), so index additions since April are never onboarded and removals
never get a `date_removed`.

## Findings

- The old endpoint
  `https://www.ishares.com/us/products/239710/ishares-russell-2000-etf/1467271812596.ajax?fileType=csv&fileName=IWM_holdings&dataType=fund`
  still answers `200` with `Content-Type: text/csv` and
  `content-disposition: attachment; filename=IWM_holdings.csv`, but the body
  is the HTML product page (`<!DOCTYPE html>...`). Hence no `Ticker,` line.
- The product page
  (https://www.ishares.com/us/products/239710/ishares-russell-2000-etf)
  now links "Download latest fund holdings CSV data" to
  `/us/products/239710/ishares-russell-2000-etf/latest-holdings.csv`. The same
  path works for IVV:
  https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/latest-holdings.csv
- The new CSV keeps the old layout: a few metadata lines, a blank line, then
  `Ticker,Name,Sector,Asset Class,Market Value,...` with quoted values.
- Market values no longer carry a `$` (`"268,154,162.25"`). The existing
  parser strips `$` and `,`, so it still works.
- Dual-class tickers now carry a space instead of being compacted:
  `"BRK B"`, `"BF B"`, `"GEF B"`, `"MOG A"`, `"CRD A"`, `"BH A"` (was
  `BRKB`, `BFB`, ...). Without normalisation these would appear as new symbols
  and the existing `BRK-B` etc. would be marked dropped.

## Decision

- Point `IWM_CSV_URL` / `IVV_CSV_URL` at `.../latest-holdings.csv`.
- Normalise any internal whitespace or `.` in a ticker to `-` (yfinance's
  share-class form) before applying `TICKER_CORRECTIONS`, which stays for the
  compact legacy form.
- When the response body is HTML, raise a `ValueError` that says so, so the
  next endpoint change is obvious from the log.

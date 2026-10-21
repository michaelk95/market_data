# Second OHLCV source for delisted tickers (issue #67)

Issue: <https://github.com/michaelk95/market_data/issues/67>

## Question

`market-data-backfill-constituents` got no yfinance data for ~500 of 625
delisted S&P 500 tickers. Do Stooq or Tiingo cover them? We checked four
well-known acquired names first: CELG, CBS, ATVI and TWX.

Research only: nothing was written to `data/` and no pipeline code was run.

## Membership periods

From `data/constituent_history.parquet` (read-only):

| Ticker | In the S&P 500 | Fate |
|---|---|---|
| ATVI | 2015-08-31 → 2023-10-18 | Acquired by Microsoft |
| CBS | 1996-01-02 → 2019-12-05 | Merged with Viacom → `VIAC` → `PARA` |
| CELG | 2006-11-06 → 2019-11-21 | Acquired by Bristol-Myers Squibb |
| TWX | 1996-01-02 → 2018-06-15 | Acquired by AT&T |

## Stooq

**Unusable.** Two separate problems:

- **The data isn't there.** Stooq's own historical-data page reports
  "Symbol nie istnieje w bazie" ("symbol not in the database") for
  [`celg.us`](https://stooq.com/q/d/?s=celg.us),
  [`cbs.us`](https://stooq.com/q/d/?s=cbs.us),
  [`atvi.us`](https://stooq.com/q/d/?s=atvi.us) and
  [`twx.us`](https://stooq.com/q/d/?s=twx.us). Stooq drops a ticker's history
  when it delists and reuses the ticker for whoever takes it next:
  [`para.us`](https://stooq.com/q/d/?s=para.us) is now Banzai International,
  with none of Paramount's history.
- **No programmatic access.** The CSV endpoint
  (`https://stooq.com/q/d/l/?s=<sym>&i=d`, which `pandas_datareader` uses)
  now returns an HTML page that asks the browser to solve a JavaScript
  proof-of-work bot check instead of the CSV. A pipeline would have to defeat
  that check, which is off the table.

## Tiingo

**Promising, but not yet verified with actual prices.** Tiingo publishes its
full ticker list, with first and last price date, as a public file that needs
no key:
<https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip>
(downloaded 2026-10-20).

| Ticker | Tiingo listing | Share of membership covered |
|---|---|---|
| ATVI | NASDAQ 1993-10-25 → 2023-10-13 | ~100% (ends at the last trading day) |
| CELG | NASDAQ 1990-03-26 → 2019-11-22 | 100% |
| TWX | NYSE 1992-03-19 → 2018-06-15 | 100% |
| CBS | not listed as `CBS` | see below |

**CBS** is listed under its current ticker: `PARA` (NASDAQ, 2006-01-03 →
2025-08-07) is the CBS → ViacomCBS → Paramount line. That covers 2006–2019
under a different symbol. 1996–2005 is missing, and it was a different company
anyway: Westinghouse/CBS until 2000, then old Viacom. The `CBS` row in
`constituent_history` merges several companies, so it needs its own mapping
whatever the source.

### All 503 tickers #67 lists

Run against the ticker list (#67's 493 "no data" plus 10 "exception"
tickers), offline. Coverage is the share of the ticker's S&P 500 membership
that falls inside Tiingo's start and end dates for the same ticker:

| Coverage | Tickers |
|---|---|
| ≥ 90% | 148 |
| 50–90% | 6 |
| < 50% | 20 |
| none | 329 |

- All 10 "exception" tickers are listed: 9 at ≥ 90%, KMG at 90%.
- Of the 329 with no coverage, 202 aren't in the list at all and 127 are
  listed only for a different period, i.e. the ticker now belongs to another
  company.
- Many of those are **renames, not gaps**. Tiingo files a company under its
  latest ticker, as with CBS → `PARA`. Examples: ABC → `COR`, ANTM → `ELV`,
  BLL → `BALL`, CDAY → `DAY`. Matching them needs an old-ticker → new-ticker
  map, so the real coverage is higher than 148. How much higher is unmeasured.

### Free tier

From <https://www.tiingo.com/about/pricing> (checked 2026-10-20), the free
Starter plan gives:

- 500 unique symbols a month
- 50 requests an hour
- 1,000 requests a day
- 30+ years of history
- a license limited to "internal use only", i.e. personal use. A personal
  backtest fits that.

That's about one request per ticker, so ~500 tickers fill a month's symbol
quota and take ~10 hours at 50 an hour. The page doesn't say whether
delisted tickers are on the free tier.

### Not verified

- **Actual prices.** The list gives only first and last dates. Checking the
  bars themselves (gaps, volume, how prices are adjusted) needs a free Tiingo
  account and API key, and only the user can create those.
- How Tiingo adjusts prices for splits and dividends compared with yfinance's
  `auto_adjust=True`. Mixing the two would give inconsistent adjusted prices
  across tickers.

## Not checked

- **EODHD** (paid, about $20 a month): its delisted-ticker list needs a key.
- **Alpha Vantage**, **Nasdaq Data Link** and **OpenBB**: not part of this
  check.

## Decision

Pending the user. Recommendation: **drop Stooq; take Tiingo forward.** Next
steps, in order:

1. The user creates a free Tiingo account and puts the key in `.env`.
2. Pull CELG, ATVI and TWX once each (3 requests) and compare against the
   membership dates and a known price. This also settles how Tiingo adjusts
   prices.
3. If the bars hold up, write an ADR covering the fallback design: yfinance
   first, Tiingo second, an old-ticker → new-ticker map, and how adjusted
   prices are reconciled. Then implement it.

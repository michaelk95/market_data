# Nightly analyst-estimate collection

## Findings

- Before PR #40 (2026-04-15) analyst fields (`analyst_target_mean/low/high`,
  `analyst_recommendation`, `analyst_count`) were part of the monthly
  fundamentals snapshot. The 2026-04-05 snapshot has them for 2,292 of 2,419
  tickers.
- [PR #40](https://github.com/michaelk95/market_data/pull/40) moved them into
  `fetch_analyst_estimates.py`, which writes the `analyst_estimates` table
  through `storage.write_table`. The orchestrator only runs it with
  `market-data-run --analyst-estimates`.
- `run_and_sleep.bat` was never given that flag. Since then no log mentions
  the `ANALYST-ESTIMATES` step, `data/analyst_estimates/` doesn't exist, and
  every fundamentals snapshot from May to September has the analyst columns
  but no values.
- The fetcher still works. A run for AAPL and BRK-B on 2026-10-20 against a
  temporary data dir saved both rows with the bitemporal fields set
  (`report_date` = collection date, `report_date_known` False).
- yfinance's `Ticker.info` only returns the current consensus
  (https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.html),
  so the April–October gap can't be backfilled.
- Cost: one `.info` call per onboarded equity ticker (about 2,400) plus the
  configured sleep, roughly 90 minutes a night at a 2s sleep.

## Decision

Add `--analyst-estimates` to the nightly `market-data-run` command and set
`sources.sleep_between_calls.analyst_estimates: 2` in `config.yaml`, matching
fundamentals, which makes the same `.info` call. The code already reads this
key and defaults it to 2. Tests read the batch file, parse its command with
the real CLI parser, and check the config key. The batch file is never run.

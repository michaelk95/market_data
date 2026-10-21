# 0002 — Re-download a symbol's OHLCV history after a split

Status: accepted (2026-10-20)

## Context

OHLCV bars are fetched split- and dividend-adjusted as of the fetch, then
appended with keep-the-stored-row dedup. A split leaves older bars on the
old basis, and `merged.parquet` shows a fake crash or spike at the split
(BKNG −96% on 2026-04-06). Research: `docs/research/split-adjustment.md`.

## Options

1. **Rescale stored rows by the split ratio.** Rejected: yfinance split dates
   can be days off (APH: listed 09-03, price halved 09-01), and a second run
   would double-adjust.
2. **Store unadjusted bars plus a splits table, adjust on read.** Rejected:
   Yahoo's unadjusted close is still split-adjusted, and the basis of
   existing rows can't be recovered (`collected_at` on all migrated rows is
   the 2026-10-17 migration time).
3. **Re-download the symbol's stored date range and replace those rows.**

## Decision

Option 3.

- `fetch.fetch_incremental_with_splits` asks yfinance for `actions=True` on
  the same request and returns the bars plus any split rows.
  `fetch_incremental` and the indices path are unchanged.
- The update step adds each symbol with a split to
  `state["split_refetch_pending"]` (symbol → split date, ratio, date first
  queued). After the update writes, every pending symbol's stored date range
  is re-downloaded with `fetch_date_range`.
- `splits.check_refetch` gates each re-download. It fails, and the stored
  rows are left alone, if:
  - the re-download covers less than `splits.min_coverage` of the stored
    dates in range; or
  - the stored/new close ratio breaks more than `splits.max_breaks` times,
    which points to a different security under the same ticker.
- Re-downloads that pass are written in one `storage.replace_rows` call. On
  a repeated key that call keeps the incoming row, and it only touches the
  re-downloaded `(symbol, date)` keys. Stored dates the re-download lacks are
  kept and logged.
- A symbol leaves the pending list:
  - once it is written;
  - once the re-download shows no break, i.e. the stored rows are already
    consistent;
  - once it fails the gate (logged as an error for the repair script);
  - after `splits.max_pending_days` (also logged as an error).
  Its split date is then kept in `state["split_refetch_done"]`, and the
  update step does not queue that split again. The update window overlaps
  the previous run by a day, so the same split row can come back the next
  night, and re-downloading it would waste a request.
- A failed download or write keeps it pending, so it is retried every night.
  So does a re-download that still drops by the split ratio within
  `splits.jump_window_days` of the split date: Yahoo sometimes adjusts the
  history a day or more late, and replacing then would change nothing.
- `scripts/repair_split_adjustment.py` shortlists split-shaped jumps already
  in the store and runs them through the same gate and replace. It is a dry
  run by default, caches downloads for `--apply`, and backs up the touched
  partitions first.

## Consequences

- `merged.parquet` is continuous across splits once a symbol is re-downloaded.
- Point in time:
  - Replaced rows keep `report_date` = bar date, but their adjusted price
    level was not knowable then.
  - That is already true of every onboarding download (a 10-year adjusted
    history), and returns, which are what the backtest uses, don't depend
    on the basis.
  - The replace overwrites the old version of each row; old values are not
    kept.
- Each split costs one extra request and a rewrite of the partitions the
  symbol spans.
- The nightly detector only sees a split whose yfinance row falls inside the
  update window. A missed night, or a split row Yahoo adds late, is caught
  only by re-running the repair script. Fixing the shared `since` window is a
  separate task.
- The dividend basis is still mixed: older bars are dividend-adjusted as of
  their download, newer bars are close to raw. That is a known limitation.

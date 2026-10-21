# Options rows with null keys (issue #74)

Issue: <https://github.com/michaelk95/market_data/issues/74>

## Question

Old `data/options/<SYMBOL>.parquet` files contain rows whose `snapshot_date`,
`symbol` and `expiry` are null. Is the cause still live, and what do those
rows do now that options live in the bitemporal `options` table (0.11.0)?

## Findings

**The constructor bug in the issue is already fixed.** Commit `77894b5`
(2026-04-06) replaced building the frame column by column with one
`pd.DataFrame({...})` call. Assigning a scalar to an empty `DataFrame` makes a
zero-row column, and later Series assignments align on their index, which is
what left `snapshot_date`/`symbol`/`expiry` null. In the dict constructor the
index is the union of the Series indexes and scalars are broadcast to it
([`pandas.DataFrame`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html),
[Intro to data structures: from dict of Series](https://pandas.pydata.org/docs/user_guide/dsintro.html#from-dict-of-series-or-dicts)).
The empty `pd.Series(dtype=bool)` fallback for a missing `inTheMoney` adds no
index labels, so it only makes `in_the_money` all-NaN, which `from_flat`
stores as null. Reproduced on pandas 3.0.6: scalars stay populated.

The issue's proposed replacement, `pd.Series([False] * len(df_raw))`, would be
worse. It has a fresh `RangeIndex`; when `df_raw`'s index differs the union
adds rows with NaN `strike` next to broadcast scalars — new null-key rows —
and it records "not in the money" when the value is unknown. A missing value
should be null ([nullable boolean](https://pandas.pydata.org/docs/user_guide/boolean.html)).

**Legacy null rows now cost data.** Reproduced against temp dirs:

- `scripts/migrate_flat_tables.py` skips any file whose `symbol` values are
  not exactly `{file stem}`. `unique()` includes the NaN, so one legacy null
  row makes the migration skip the *whole* file, good rows included. The
  first migration has run, so every real options file holding such rows is
  probably missing from the table.
- The live writer has no guard: `save_options_snapshot` stores rows with a
  null `symbol`, `expiry` or `strike` as-is.
- Rows with a null `snapshot_date` already vanish silently in
  `storage._write`: their partition year is NaN and `groupby` drops NaN keys
  by default ([`DataFrame.groupby`, `dropna=True`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html)).

A row with a null dedup key can't be identified, deduplicated or attributed
to a symbol, so it has no use in any table.

## Decision

Drop and log null-key rows at the flat → bitemporal boundary; see
[ADR 0001](../decisions/0001-drop-null-key-flat-rows.md).

- New `flat_tables.drop_null_keys(flat, table)` drops rows with a null key
  (the table's `schema.DEDUP_KEYS`, with `period_start_date` mapped back to
  the flat date column) and logs a warning with counts per symbol.
- `from_flat` calls it, covering the ohlcv, indices and options writers.
- The migration calls it before the file-name check, so a file with legacy
  null rows migrates its good rows instead of being skipped.
- `fetch_option_chain` fills a missing `inTheMoney` with nullable-boolean NA
  on `df_raw`'s index (explicit, no alignment surprises).
- No repair script: flat files are no longer read, and re-running
  `migrate_flat_tables.py --table options` after this fix recovers the rows
  the first migration skipped.

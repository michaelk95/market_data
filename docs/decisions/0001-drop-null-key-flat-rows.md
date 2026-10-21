# 0001 — Drop flat rows with null keys before writing

Status: accepted (2026-10-20)

## Context

Collector frames (ohlcv, indices, options) can carry rows whose dedup-key
columns are null: legacy options files hold such rows (issue #74), and
`pd.to_numeric(..., errors="coerce")` can turn a bad `strike` into NaN. A
null-key row can't be deduplicated or attributed to a symbol. Today the live
writers store them, and the flat-file migration skips any file containing one,
losing that file's good rows. Research: `docs/research/options-null-keys.md`.

## Options

1. **Raise in `storage._write`** on any null dedup key. Strict and covers every
   table, but options and batched ohlcv write once per run, so one bad row
   would fail every symbol in the batch — against the "per-symbol failures
   are non-fatal" rule.
2. **Drop and log in `storage._write`.** Covers every table, but silently
   changes the macro, fundamentals and analyst-estimates writers too, and the
   migration's file-name check runs before storage, so it still skips files.
3. **Drop and log at the flat boundary (`flat_tables`).** One helper used by
   `from_flat` (live writers) and by the migration before its file-name check.

## Decision

Option 3. `flat_tables.drop_null_keys` derives the key columns from
`schema.DEDUP_KEYS`, drops rows with any null key and logs a warning with
counts per symbol.

## Consequences

- Bad rows never reach the ohlcv, indices or options tables, and the rest of
  the batch is written.
- Re-running the migration recovers files it previously skipped.
- Drops are visible only in logs; a source returning many null-key rows shows
  up as warnings, not failures.
- Tables written through `storage.write_table` directly (macro, fundamentals,
  analyst estimates) are unchanged; a storage-level check can be added later
  if they need one.

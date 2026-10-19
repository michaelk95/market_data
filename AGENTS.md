# AGENTS.md

`market_data` collects OHLCV, indices, FRED macro, fundamentals and options (mostly yfinance) into a bitemporal parquet store under `data/` for the `paper_trading` backtest engine. Usage: `README.md`. Unscheduled ideas: `backlog.md`.

## Workflow

Every task, in order:

1. **Research**: read the relevant code (`schema.py` and `storage.py` are the source of truth for the store) and existing ADRs. Write any research up in `docs/research/<slug>.md`, citing sources with URLs, ending in a **Decision** section.
2. **Plan**: propose the change and the tests before writing code. A design decision no ADR covers gets a new one: `docs/decisions/NNNN-title.md` (context, options, decision, consequences).
3. **Write failing tests** and confirm they fail.
4. **Implement**.
5. **Run tests**, fix failures.
6. **Run linter**, fix findings.
7. **Open a PR** with a new top entry in `CHANGELOG.md` (`MAJOR.MINOR.PATCH`) and the matching `version` bump in `pyproject.toml`.

One task at a time; don't pick up `backlog.md` items unasked.

## Hard rules

- **Don't touch real data unasked.** `D:\market_data\data`, `state.json` and `tickers.csv` are the only copy. Don't run the pipeline, fetch CLIs, or `scripts/migrate_*`/`repair_*` against them. To experiment, point `MARKET_DATA_CONFIG` at a `config.yaml` copy with temp `paths`.
- **Never run** `run_and_sleep.bat` (hibernates the PC) or `market-data-agent` (files GitHub issues).
- **Point-in-time correctness:** `report_date` is when a value became publicly knowable, never earlier. New data types carry the bitemporal fields from `schema.py`.
- **Use the existing writers:** `fetch.save_ticker_data` / `fetch_options.save_options_snapshot` (ohlcv, indices, options; flat collector frames converted by `flat_tables.py`) or `storage.write_table` (macro, fundamentals, analyst estimates). All write the bitemporal tables (`data/<table>/year=YYYY/data.parquet`, indices a single `data.parquet`), take the data root, and are atomic and dedup; old flat `<SYMBOL>.parquet` files are no longer read; never write parquet into `data/` by hand. One-off rewrites back up first.
- **Be polite to data providers:** cache responses and reuse what's already stored; never refetch what you have. Every fetch loop goes through the configured sleeps and `resilience.py` retries.
- **Per-symbol failures are non-fatal:** log, record, continue.
- **No live network in tests:** patch yfinance, `requests`, `fredapi`.
- **Never commit** `.env`, API keys, `data/`, `logs/`, `state.json`, `tickers.csv`.

## Conventions

- **Script repeated tasks.** If a task will recur (checks, data inspection, repairs), write a script in `scripts/` or a CLI entry and run it, instead of redoing it by hand each time.
- Python >= 3.10 (CI runs 3.10 and 3.12).
- Tunables (batch sizes, sleeps, retries, thresholds, paths) go in `config.yaml`, not code.
- Log via `market_data.*` loggers; `logger.exception` in `except`; narrow exceptions, deliberate catch-alls get `# noqa: BLE001`.
- Conventional commits (`fix(fundamentals): ...`). Branches: `feature/`, `fix/`, `hotfix/`, `release/`, `chore/` + lowercase-hyphenated name.
- No AI attribution in commits or PRs.

## Commands

Each checkout and worktree has its own `.venv`; bare `python`/`pip` won't resolve.

```bash
/d/market_data/.venv/Scripts/python.exe -m venv .venv && .venv/Scripts/pip.exe install -e ".[dev]"   # new worktree setup
.venv/Scripts/python.exe -m pytest                       # tests (offline, with coverage)
.venv/Scripts/ruff.exe check src/ tests/                 # lint (--fix to autofix)
.venv/Scripts/python.exe scripts/smoke_test.py           # live source reachability
.venv/Scripts/market-data-health.exe                     # data freshness
```

CLIs are listed in `pyproject.toml` `[project.scripts]`; each takes `--help`.

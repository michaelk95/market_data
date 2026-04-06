"""
fetch_fundamentals.py
---------------------
Collect point-in-time fundamental snapshots for equity tickers via yfinance.

Each run appends one row per ticker (tagged with today's date as `as_of`) to
a per-ticker Parquet file under data/fundamentals/<SYMBOL>.parquet.  Over time
this builds a monthly time series of valuation and analyst-estimate data.

Fields captured
---------------
Valuation
  market_cap            marketCap
  enterprise_value      enterpriseValue
  trailing_pe           trailingPE
  forward_pe            forwardPE
  price_to_book         priceToBook

Earnings & revenue
  trailing_eps          trailingEps
  forward_eps           forwardEps
  total_revenue         totalRevenue
  profit_margin         profitMargins

Analyst estimates
  analyst_target_mean   targetMeanPrice
  analyst_target_low    targetLowPrice
  analyst_target_high   targetHighPrice
  analyst_recommendation  recommendationMean  (1=Strong Buy … 5=Strong Sell)
  analyst_count         numberOfAnalystOpinions

Usage
-----
    market-data-fetch-fundamentals                     # fetch all onboarded tickers
    market-data-fetch-fundamentals --symbols AAPL MSFT # specific symbols only
"""

from __future__ import annotations

import argparse
import time
from datetime import date
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FUNDAMENTALS_DIR = Path("data/fundamentals")
SLEEP_BETWEEN_CALLS = 2  # seconds — .info is lighter than a full history pull

# Mapping: our column name → yfinance info key
INFO_FIELDS: dict[str, str] = {
    # Valuation
    "market_cap":               "marketCap",
    "enterprise_value":         "enterpriseValue",
    "trailing_pe":              "trailingPE",
    "forward_pe":               "forwardPE",
    "price_to_book":            "priceToBook",
    # Earnings & revenue
    "trailing_eps":             "trailingEps",
    "forward_eps":              "forwardEps",
    "total_revenue":            "totalRevenue",
    "profit_margin":            "profitMargins",
    # Analyst estimates
    "analyst_target_mean":      "targetMeanPrice",
    "analyst_target_low":       "targetLowPrice",
    "analyst_target_high":      "targetHighPrice",
    "analyst_recommendation":   "recommendationMean",
    "analyst_count":            "numberOfAnalystOpinions",
}

SCHEMA_COLS = ["as_of", "symbol"] + list(INFO_FIELDS.keys())


# ---------------------------------------------------------------------------
# Storage helpers
# ---------------------------------------------------------------------------

def _path(symbol: str, fund_dir: Path) -> Path:
    return fund_dir / f"{symbol}.parquet"


def load_fundamentals(symbol: str, fund_dir: Path) -> pd.DataFrame | None:
    path = _path(symbol, fund_dir)
    if not path.exists():
        return None
    return pd.read_parquet(path)


def save_fundamentals(symbol: str, record: dict, fund_dir: Path) -> int:
    """
    Append one snapshot row to the per-ticker fundamentals Parquet.

    Deduplicates on (as_of, symbol) so re-running on the same day is safe.
    Returns 1 if a new row was added, 0 if the row already existed.
    """
    fund_dir.mkdir(parents=True, exist_ok=True)
    path = _path(symbol, fund_dir)

    new_row = pd.DataFrame([record])[SCHEMA_COLS]
    new_row["as_of"] = pd.to_datetime(new_row["as_of"]).dt.date

    if path.exists():
        existing = pd.read_parquet(path)
        combined = pd.concat([existing, new_row], ignore_index=True)
    else:
        combined = new_row.copy()

    before = len(combined) - len(new_row)

    combined["as_of"] = pd.to_datetime(combined["as_of"]).dt.date
    combined = (
        combined
        .drop_duplicates(subset=["as_of", "symbol"])
        .sort_values("as_of")
        .reset_index(drop=True)
    )

    tmp_path = path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)

    return len(combined) - before


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_fundamentals(symbol: str) -> dict | None:
    """
    Pull the fundamentals snapshot for `symbol` via yfinance .info.

    Returns a dict ready to pass to save_fundamentals(), or None if the ticker
    returned no usable data.
    """
    try:
        info = yf.Ticker(symbol).info
    except Exception:
        return None

    # Require at least a market cap to consider the record valid
    if not info.get("marketCap"):
        return None

    record: dict = {"as_of": date.today(), "symbol": symbol}
    for col, yf_key in INFO_FIELDS.items():
        record[col] = info.get(yf_key)  # None if key is missing

    return record


# ---------------------------------------------------------------------------
# Core run logic
# ---------------------------------------------------------------------------

def run(
    symbols: list[str],
    fund_dir: Path = FUNDAMENTALS_DIR,
) -> None:
    """
    Fetch and store a fundamentals snapshot for each symbol in `symbols`.
    """
    today = date.today()
    total = len(symbols)

    print(f"\nmarket_data fundamentals  —  {today}")
    print(f"  Tickers: {total}")
    print(f"{'='*55}")

    saved = 0
    skipped = 0
    failed = 0

    for i, symbol in enumerate(symbols, 1):
        prefix = f"  [{i:>4}/{total}] {symbol:<8}"
        try:
            record = fetch_fundamentals(symbol)
            if record is None:
                print(f"{prefix}  no data")
                skipped += 1
            else:
                added = save_fundamentals(symbol, record, fund_dir)
                if added:
                    mktcap = record.get("market_cap")
                    cap_str = f"  mktcap={mktcap:,.0f}" if mktcap else ""
                    print(f"{prefix}  saved{cap_str}")
                    saved += 1
                else:
                    print(f"{prefix}  already up to date")
                    skipped += 1
        except Exception as exc:
            print(f"{prefix}  ERROR: {exc}")
            failed += 1

        if i < total:
            time.sleep(SLEEP_BETWEEN_CALLS)

    print(f"{'='*55}")
    print(f"Done.  Saved: {saved}  |  Skipped: {skipped}  |  Failed: {failed}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch fundamental snapshots (market cap, analyst estimates, etc.) "
            "for equity tickers via yfinance."
        )
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        metavar="SYMBOL",
        help=(
            "Symbols to fetch. Defaults to all tickers in state.json (onboarded list). "
            "Example: --symbols AAPL MSFT TSLA"
        ),
    )
    args = parser.parse_args()

    if args.symbols:
        symbols = args.symbols
    else:
        # Load from state.json
        import json  # noqa: PLC0415
        state_file = Path("state.json")
        if not state_file.exists():
            print("No state.json found and no --symbols provided. Nothing to do.")
            return
        state = json.loads(state_file.read_text())
        symbols = sorted(state.get("onboarded", []))
        if not symbols:
            print("No onboarded tickers in state.json. Run market-data-run first.")
            return

    run(symbols=symbols)


if __name__ == "__main__":
    main()

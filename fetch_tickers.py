"""
fetch_tickers.py
----------------
Downloads the current Russell 2000 constituent list from the iShares IWM ETF
holdings page and saves it as tickers.csv, sorted by market value descending
(largest market cap first).

iShares publishes a daily CSV of all IWM holdings — no login required.
IWM tracks the Russell 2000 index exactly.

Usage:
    python fetch_tickers.py
    python fetch_tickers.py --out tickers.csv   # default output path
"""

import argparse
import io
import sys
from pathlib import Path

import pandas as pd
import requests

# iShares IWM holdings CSV endpoint (no auth required)
IWM_CSV_URL = (
    "https://www.ishares.com/us/products/239710/"
    "ishares-russell-2000-etf/1467271812596.ajax"
    "?fileType=csv&fileName=IWM_holdings&dataType=fund"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_iwm_holdings(url: str = IWM_CSV_URL) -> pd.DataFrame:
    """Download the IWM holdings CSV and return a cleaned DataFrame."""
    print(f"Downloading IWM holdings from iShares...")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    # The iShares CSV has a few header rows of metadata before the actual
    # column headers. We find the real header by looking for the 'Ticker' row.
    lines = resp.text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Ticker,"):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(
            "Could not locate the 'Ticker' header row in the IWM CSV. "
            "The iShares page format may have changed."
        )

    csv_body = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_body))

    return df


def clean_holdings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names, drop non-equity rows (cash, futures, etc.),
    parse market value, and sort by market value descending.
    """
    df.columns = [c.strip() for c in df.columns]

    # Keep only rows that look like real equity tickers
    # iShares marks non-equity rows with '-' or blank tickers
    df = df[df["Ticker"].notna()]
    df = df[~df["Ticker"].isin(["-", "", "CASH"])]
    df = df[df["Asset Class"] == "Equity"] if "Asset Class" in df.columns else df

    # Parse market value — comes in as "$1,234,567.89" strings
    if "Market Value" in df.columns:
        df["market_value"] = (
            df["Market Value"]
            .astype(str)
            .str.replace(r"[$,]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
        )
    else:
        df["market_value"] = float("nan")

    result = (
        df[["Ticker", "Name", "market_value"]]
        .rename(columns={"Ticker": "symbol", "Name": "name"})
        .dropna(subset=["symbol"])
        .sort_values("market_value", ascending=False)
        .reset_index(drop=True)
    )

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Russell 2000 tickers from iShares IWM.")
    parser.add_argument(
        "--out",
        default="tickers.csv",
        help="Output CSV path (default: tickers.csv)",
    )
    args = parser.parse_args()

    out_path = Path(args.out)

    try:
        raw = fetch_iwm_holdings()
    except requests.RequestException as exc:
        print(f"ERROR: Failed to download IWM holdings: {exc}", file=sys.stderr)
        sys.exit(1)

    tickers = clean_holdings(raw)

    if tickers.empty:
        print("ERROR: No tickers found after cleaning — the CSV format may have changed.", file=sys.stderr)
        sys.exit(1)

    tickers.to_csv(out_path, index=False)
    print(f"Saved {len(tickers)} tickers to {out_path}")
    print(tickers.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

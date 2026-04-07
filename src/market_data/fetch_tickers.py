"""
fetch_tickers.py
----------------
Downloads the current Russell 2000 (IWM) and S&P 500 (IVV) constituent lists
from the iShares ETF holdings pages and saves a combined tickers.csv, sorted by
market value descending (largest market cap first).

iShares publishes a daily CSV of all ETF holdings — no login required.

Usage:
    python fetch_tickers.py
    python fetch_tickers.py --out tickers.csv   # default output path
"""

import argparse
import io
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests

# iShares ETF holdings CSV endpoints (no auth required)
IWM_CSV_URL = (
    "https://www.ishares.com/us/products/239710/"
    "ishares-russell-2000-etf/1467271812596.ajax"
    "?fileType=csv&fileName=IWM_holdings&dataType=fund"
)

IVV_CSV_URL = (
    "https://www.ishares.com/us/products/239726/"
    "ishares-core-sp-500-etf/1467271812596.ajax"
    "?fileType=csv&fileName=IVV_holdings&dataType=fund"
)

# iShares strips dots from dual-class ticker symbols (e.g. BRK.B → BRKB),
# but yfinance requires hyphens (BRK-B). Map known cases explicitly.
TICKER_CORRECTIONS: dict[str, str] = {
    "BRKB": "BRK-B",
    "BFB":  "BF-B",
    "GEFB": "GEF-B",
    "CRDA": "CRD-A",
    "MOGA": "MOG-A",
}

# Exclude non-tradeable instruments that appear as ETF line items but have
# no meaningful price history on yfinance.
_SKIP_NAME_RE = re.compile(r"\b(CVR|RIGHTS?|ESCROW|WARRANT)\b", re.IGNORECASE)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_etf_holdings(url: str) -> pd.DataFrame:
    """Download an iShares ETF holdings CSV and return the raw DataFrame."""
    print(f"Downloading ETF holdings from {url[:60]}...")
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
            "Could not locate the 'Ticker' header row in the ETF CSV. "
            "The iShares page format may have changed."
        )

    csv_body = "\n".join(lines[header_idx:])
    df = pd.read_csv(io.StringIO(csv_body))

    return df


def clean_holdings(df: pd.DataFrame, index_label: str) -> pd.DataFrame:
    """
    Normalize column names, drop non-equity rows (cash, futures, etc.),
    parse market value, tag the index, and sort by market value descending.

    Returns a DataFrame with columns: symbol, name, market_value, index.
    """
    df.columns = [c.strip() for c in df.columns]

    # Keep only rows that look like real equity tickers
    # iShares marks non-equity rows with '-' or blank tickers
    df = df[df["Ticker"].notna()]
    df = df[~df["Ticker"].isin(["-", "", "CASH"])]
    df = df[df["Asset Class"] == "Equity"] if "Asset Class" in df.columns else df

    # Exclude CVRs, warrants, rights, and escrow shares — these are not
    # tradeable equities and yfinance has no price data for them.
    if "Name" in df.columns:
        df = df[~df["Name"].str.contains(_SKIP_NAME_RE, na=False)]

    # Normalize dual-class symbols from iShares compact format to yfinance format.
    df = df.copy()
    df["Ticker"] = df["Ticker"].replace(TICKER_CORRECTIONS)

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

    result["index"] = index_label

    return result


def _combine_index_labels(labels: pd.Series) -> str:
    """Aggregate helper: if both SP500 and RUT2000 are present, combine them."""
    unique = set(labels)
    if "SP500" in unique and "RUT2000" in unique:
        return "SP500,RUT2000"
    return labels.iloc[0]


def merge_holdings(iwm_df: pd.DataFrame, ivv_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge IWM (Russell 2000) and IVV (S&P 500) holdings into a single DataFrame.

    For tickers appearing in both:
      - index = "SP500,RUT2000"
      - market_value = max of the two values
      - name = from the row with the higher market_value

    Returns columns: symbol, name, market_value, index — sorted by market_value desc.
    """
    merged = (
        pd.concat([iwm_df, ivv_df], ignore_index=True)
        .sort_values("market_value", ascending=False)  # first() picks higher-cap name
        .groupby("symbol", sort=False)
        .agg(
            name=("name", "first"),
            market_value=("market_value", "max"),
            index=("index", _combine_index_labels),
        )
        .reset_index()
        .sort_values("market_value", ascending=False)
        .reset_index(drop=True)
    )
    return merged


def apply_date_added(
    new_df: pd.DataFrame,
    existing_path: Path,
    today: str,
) -> pd.DataFrame:
    """
    Assign a date_added column to new_df by merging with an existing tickers.csv.

    Rules:
    - New tickers (not in existing file) get date_added = today.
    - Existing tickers keep their original date_added.
    - Tickers that dropped out of both indices are carried forward unchanged
      (avoids survivorship bias).
    - Backward-compat backfill: if existing file has no date_added column,
      all its rows get "2000-01-01"; if no index column, they get "RUT2000".
    """
    if not existing_path.exists():
        new_df = new_df.copy()
        new_df["date_added"] = today
        return new_df

    existing_df = pd.read_csv(existing_path, dtype=str)

    # Backward-compat backfill for old schema
    if "date_added" not in existing_df.columns:
        existing_df["date_added"] = "2000-01-01"
    if "index" not in existing_df.columns:
        existing_df["index"] = "RUT2000"

    # Build lookup: symbol -> date_added from existing file
    known = dict(zip(existing_df["symbol"], existing_df["date_added"]))

    # Assign date_added: preserve existing dates, new tickers get today
    new_df = new_df.copy()
    new_df["date_added"] = new_df["symbol"].map(known).fillna(today)

    # Carry forward any tickers that dropped out of both indices
    new_symbols = set(new_df["symbol"])
    dropped = existing_df[~existing_df["symbol"].isin(new_symbols)].copy()

    if not dropped.empty:
        # Ensure dropped rows have the same columns as new_df
        for col in new_df.columns:
            if col not in dropped.columns:
                dropped[col] = None
        dropped = dropped[new_df.columns]
        # market_value may be string from read_csv; coerce
        dropped["market_value"] = pd.to_numeric(dropped["market_value"], errors="coerce")

    result = (
        pd.concat([new_df, dropped], ignore_index=True)
        .drop_duplicates(subset=["symbol"], keep="first")
        .sort_values("market_value", ascending=False, na_position="last")
        .reset_index(drop=True)
    )

    return result


def _inject_etf_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append rows for sector and broad-market ETFs that are not already present
    in `df` (the merged constituent DataFrame).

    ETF rows use index = "SECTOR_ETF" or "BROAD_ETF" and NaN market_value
    (AUM is not comparable to equity market cap and not needed for pipeline
    operation).  NaN values sort to the bottom of tickers.csv, keeping the
    constituent stocks at the top.
    """
    from market_data.etf_config import SECTOR_ETFS, BROAD_ETFS  # noqa: PLC0415

    existing_symbols: set[str] = set(df["symbol"].dropna())

    new_rows = []
    for symbol, name in SECTOR_ETFS:
        if symbol not in existing_symbols:
            new_rows.append({"symbol": symbol, "name": name,
                              "market_value": float("nan"), "index": "SECTOR_ETF"})
    for symbol, name in BROAD_ETFS:
        if symbol not in existing_symbols:
            new_rows.append({"symbol": symbol, "name": name,
                              "market_value": float("nan"), "index": "BROAD_ETF"})

    if not new_rows:
        return df

    etf_df = pd.DataFrame(new_rows, columns=["symbol", "name", "market_value", "index"])
    return pd.concat([df, etf_df], ignore_index=True)


def run(out_path: Path, today: str | None = None) -> pd.DataFrame:
    """
    Fetch IWM + IVV holdings, merge, apply date_added logic, and write tickers.csv.
    Returns the final DataFrame.
    """
    if today is None:
        today = date.today().isoformat()

    raw_iwm = fetch_etf_holdings(IWM_CSV_URL)
    raw_ivv = fetch_etf_holdings(IVV_CSV_URL)

    iwm = clean_holdings(raw_iwm, "RUT2000")
    ivv = clean_holdings(raw_ivv, "SP500")

    merged = merge_holdings(iwm, ivv)
    merged = _inject_etf_rows(merged)
    result = apply_date_added(merged, out_path, today)

    if result.empty:
        raise ValueError("No tickers after merging — CSV format may have changed.")

    result[["symbol", "name", "market_value", "index", "date_added"]].to_csv(
        out_path, index=False
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Russell 2000 (IWM) + S&P 500 (IVV) tickers from iShares."
    )
    parser.add_argument(
        "--out",
        default="tickers.csv",
        help="Output CSV path (default: tickers.csv)",
    )
    args = parser.parse_args()

    out_path = Path(args.out)

    try:
        tickers = run(out_path)
    except requests.RequestException as exc:
        print(f"ERROR: Failed to download ETF holdings: {exc}", file=sys.stderr)
        sys.exit(1)

    counts = tickers["index"].value_counts()
    print(f"\nSaved {len(tickers)} tickers to {out_path}")
    for label, count in counts.items():
        print(f"  {label}: {count}")
    print()
    print(tickers.head(10).to_string(index=False))


if __name__ == "__main__":
    main()

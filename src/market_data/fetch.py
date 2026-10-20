"""
fetch.py
--------
Core OHLCV fetch and storage logic.

Functions
---------
fetch_history(symbol, years)        Full historical pull (default 10 years).
fetch_max_history(symbol)           Full available history via period="max".
fetch_incremental(symbol, since)    Pull data from a given date onward.
fetch_date_range(symbol, start, end) Bounded historical pull for a date range.
save_ticker_data(symbol, df, root)  Merge flat OHLCV rows into the bitemporal
                                    ``ohlcv`` (or ``indices``) table under the
                                    data root, deduplicating on
                                    (symbol, period_start_date).
load_ticker_data(symbol, root)      Load one symbol's rows back in the flat
                                    OHLCV layout (or None).
TickerBatchWriter(root)             Buffer many symbols' rows and write them
                                    with one ``write_table`` call per batch, so
                                    each year partition is rewritten once per
                                    batch instead of once per symbol.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

from market_data.config import cfg as _cfg
from market_data.flat_tables import OHLCV_FLAT_COLS, from_flat, to_flat
from market_data.resilience import yf_retry
from market_data.storage import read_table, write_table, write_table_counts

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_HISTORY_YEARS: int = _cfg.get("collection.history_years", 10)
OHLCV_COLS = OHLCV_FLAT_COLS
# Symbols buffered by TickerBatchWriter before each storage write.
DEFAULT_WRITE_BATCH_SYMBOLS: int = _cfg.get("collection.write_batch_symbols", 100)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(raw: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    Convert a raw yfinance DataFrame into our standard schema:
        date (date), symbol (str), open, high, low, close, volume (float)

    yfinance returns a DatetimeIndex that may be timezone-aware; we strip
    the time component and work purely with calendar dates.
    """
    if raw.empty:
        return pd.DataFrame(columns=OHLCV_COLS)

    df = raw.copy()

    # Flatten MultiIndex columns produced by yf.download() multi-ticker calls
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Normalize column names
    df.columns = [c.lower() for c in df.columns]

    # Strip timezone and convert index to plain date
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df.index = df.index.date
    df.index.name = "date"
    df = df.reset_index()

    df["symbol"] = symbol

    # Keep only the columns we care about; tolerate missing optional ones
    available = [c for c in OHLCV_COLS if c in df.columns]
    df = df[available].dropna(subset=["close"])

    # Ensure correct types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["symbol"] = df["symbol"].astype(str)

    return df.reset_index(drop=True)


def _check_data_root(data_dir: Path, table: str) -> Path:
    """Reject a table directory passed where the data root is expected.

    Before the bitemporal migration callers passed ``data/ohlcv``; writing the
    table under it would silently create ``data/ohlcv/ohlcv/``.
    """
    data_dir = Path(data_dir)
    if data_dir.name == table:
        raise ValueError(
            f"data_dir must be the data root (containing '{table}/'), got {data_dir}"
        )
    return data_dir


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@yf_retry
def fetch_history(symbol: str, years: int = DEFAULT_HISTORY_YEARS) -> pd.DataFrame:
    """
    Download `years` of daily OHLCV history for `symbol`.

    Returns an empty DataFrame (with correct columns) if no data is available
    (e.g. delisted ticker, bad symbol).
    """
    end = date.today()
    start = end.replace(year=end.year - years)

    raw = yf.Ticker(symbol).history(
        start=str(start),
        end=str(end),
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


@yf_retry
def fetch_max_history(symbol: str) -> pd.DataFrame:
    """Download the maximum available OHLCV history for *symbol* using period="max"."""
    raw = yf.Ticker(symbol).history(
        period="max",
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


@yf_retry
def fetch_incremental(symbol: str, since: date) -> pd.DataFrame:
    """
    Download daily OHLCV data for `symbol` from `since` onward.

    We start from `since` (inclusive) rather than since+1 so we never
    accidentally skip a trading day near the boundary. Duplicate rows are
    dropped by save_ticker_data().
    """
    raw = yf.Ticker(symbol).history(
        start=str(since),
        end=str(date.today() + timedelta(days=1)),  # end is exclusive in yf
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


@yf_retry
def fetch_date_range(
    symbol: str,
    start: date,
    end: date | None = None,
) -> pd.DataFrame:
    """Download daily OHLCV history for *symbol* from *start* to *end* inclusive.

    Used by the backfill pipeline to fetch bounded membership periods for
    delisted tickers.  Falls back to today when *end* is None.
    """
    if end is None:
        end = date.today()
    raw = yf.Ticker(symbol).history(
        start=str(start),
        end=str(end + timedelta(days=1)),  # yfinance end is exclusive
        auto_adjust=True,
        actions=False,
    )
    return _normalize(raw, symbol)


def load_ticker_data(
    symbol: str, data_dir: Path, table: str = "ohlcv"
) -> pd.DataFrame | None:
    """
    Load `symbol`'s rows from *table* under the data root *data_dir*.

    Returns the flat OHLCV layout (``OHLCV_COLS``) sorted by date, or None if
    the table holds no rows for `symbol`.
    """
    data_dir = _check_data_root(data_dir, table)
    df = read_table(table, data_dir, symbols=[symbol])
    if df.empty:
        return None
    return to_flat(df.sort_values("period_start_date"), table)


def save_ticker_data(
    symbol: str, new_df: pd.DataFrame, data_dir: Path, table: str = "ohlcv"
) -> int:
    """
    Merge flat OHLCV rows into the bitemporal *table* under the data root.

    *new_df* uses the flat ``OHLCV_COLS`` layout; it is converted with
    ``flat_tables.from_flat`` (``collected_at`` = now) and written with
    ``storage.write_table``, which deduplicates on (symbol, period_start_date)
    keeping the stored row, and writes each partition atomically.

    `symbol` is used for logging context only; the rows carry their own symbol.

    Returns the number of net-new rows added.
    """
    data_dir = _check_data_root(data_dir, table)
    if new_df.empty:
        return 0
    collected_at = pd.Timestamp.now(tz="UTC").floor("us")
    return write_table(from_flat(new_df, table, collected_at), table, data_dir)


class BatchWriteError(Exception):
    """A ``TickerBatchWriter`` flush failed; no row of *symbols* was written."""

    def __init__(self, symbols: list[str], cause: Exception) -> None:
        super().__init__(f"write of {len(symbols)} symbol(s) failed: {cause}")
        self.symbols = symbols


class TickerBatchWriter:
    """
    Buffer flat OHLCV rows for many symbols and write them in one call.

    ``save_ticker_data`` rewrites every year partition a symbol touches, so
    saving symbols one by one rewrites the same partitions once per symbol.
    This writer conforms each symbol's rows on ``add`` (so a malformed frame
    fails for that symbol alone) and writes the whole buffer with a single
    ``storage.write_table_counts`` call on ``flush``.

    The caller decides when to flush (``full`` turns true at *max_symbols*)
    and must flush once more at the end.  Only mark symbols done after the
    flush that wrote them succeeds.
    """

    def __init__(
        self,
        data_dir: Path,
        table: str = "ohlcv",
        max_symbols: int | None = None,
    ) -> None:
        self.data_dir = _check_data_root(data_dir, table)
        self.table = table
        self.max_symbols = max_symbols or DEFAULT_WRITE_BATCH_SYMBOLS
        self._symbols: list[str] = []
        self._frames: list[pd.DataFrame] = []

    @property
    def symbols(self) -> list[str]:
        """Symbols buffered since the last flush, in the order added."""
        return list(self._symbols)

    @property
    def full(self) -> bool:
        """True once *max_symbols* symbols are buffered."""
        return len(self._symbols) >= self.max_symbols

    def add(self, symbol: str, new_df: pd.DataFrame) -> None:
        """Conform *new_df* (flat ``OHLCV_COLS``) and buffer it under *symbol*.

        Raises (and buffers nothing) if the rows cannot be conformed.
        """
        if not new_df.empty:
            collected_at = pd.Timestamp.now(tz="UTC").floor("us")
            self._frames.append(from_flat(new_df, self.table, collected_at))
        self._symbols.append(symbol)

    def flush(self) -> dict[str, int]:
        """Write the buffer; return net-new rows for each buffered symbol.

        The buffer is cleared whether or not the write succeeds.  A failed
        write raises ``BatchWriteError`` naming every buffered symbol.
        """
        symbols, frames = self._symbols, self._frames
        self._symbols, self._frames = [], []
        if not symbols:
            return {}
        counts: dict[str, int] = {}
        if frames:
            try:
                counts = write_table_counts(
                    pd.concat(frames, ignore_index=True), self.table, self.data_dir
                )
            except Exception as exc:
                raise BatchWriteError(symbols, exc) from exc
        return {symbol: counts.get(symbol, 0) for symbol in symbols}

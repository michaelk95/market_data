"""
health.py
---------
Health-check utilities for the market_data pipeline.

health_check() scans each data directory and verifies that its most recently
modified Parquet file is within the expected update-frequency window.  Stale
or missing data types are logged as warnings; fresh ones at INFO level.

Expected freshness windows
--------------------------
  ohlcv          2 days  (daily equity prices)
  options        14 days (options cycle covers ~500 tickers over ~10 days)
  fundamentals   35 days (monthly snapshot run)
  macro           7 days (FRED data; some series are monthly)

Usage
-----
    from market_data.health import health_check
    results = health_check()   # returns dict[str, dict]

    # Or run as a standalone CLI check:
    market-data-health
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Subdirectory names under the data root, keyed by data-type label
_SUBDIR_NAMES: dict[str, str] = {
    "ohlcv": "ohlcv",
    "options": "options",
    "fundamentals": "fundamentals",
    "macro": "macro",
}

# Maximum acceptable age (days) for the most recently modified file
FRESHNESS_WINDOWS: dict[str, int] = {
    "ohlcv": 2,
    "options": 14,
    "fundamentals": 35,
    "macro": 7,
}

_DEFAULT_DATA_DIR = Path("data")


def health_check(data_dir: Path | None = None) -> dict[str, dict]:
    """
    Check whether each data type has been updated within its freshness window.

    Uses the most recent file-modification timestamp among all *.parquet files
    in each subdirectory as a proxy for "last updated".

    Parameters
    ----------
    data_dir : Root directory containing ohlcv/, options/, fundamentals/, macro/
               subdirectories.  Defaults to ``data/`` relative to cwd.

    Returns
    -------
    dict mapping data_type → report dict with keys:
      last_updated         datetime (UTC) of the most recent parquet file,
                           or None if no files were found
      expected_within_days int freshness threshold for this data type
      is_stale             bool — True when missing or age exceeds threshold
      age_days             float age in days, or None if no files were found
    """
    root = data_dir if data_dir is not None else _DEFAULT_DATA_DIR

    now = datetime.now(timezone.utc)
    results: dict[str, dict] = {}

    for data_type, subdir_name in _SUBDIR_NAMES.items():
        threshold_days = FRESHNESS_WINDOWS[data_type]
        data_subdir = root / subdir_name

        if not data_subdir.exists():
            results[data_type] = {
                "last_updated": None,
                "expected_within_days": threshold_days,
                "is_stale": True,
                "age_days": None,
            }
            logger.warning(
                "health_check: %s — directory not found (%s)", data_type, data_subdir
            )
            continue

        parquet_files = list(data_subdir.glob("*.parquet"))
        if not parquet_files:
            results[data_type] = {
                "last_updated": None,
                "expected_within_days": threshold_days,
                "is_stale": True,
                "age_days": None,
            }
            logger.warning(
                "health_check: %s — no parquet files in %s", data_type, data_subdir
            )
            continue

        most_recent_mtime = max(p.stat().st_mtime for p in parquet_files)
        last_updated = datetime.fromtimestamp(most_recent_mtime, tz=timezone.utc)
        age_days = (now - last_updated).total_seconds() / 86400.0
        is_stale = age_days > threshold_days

        results[data_type] = {
            "last_updated": last_updated,
            "expected_within_days": threshold_days,
            "is_stale": is_stale,
            "age_days": age_days,
        }

        if is_stale:
            logger.warning(
                "health_check: %s — stale (%.1fd old, threshold %dd)",
                data_type,
                age_days,
                threshold_days,
            )
        else:
            logger.info(
                "health_check: %s — ok (%.1fd old, threshold %dd)",
                data_type,
                age_days,
                threshold_days,
            )

    stale_types = [k for k, v in results.items() if v["is_stale"]]
    if stale_types:
        logger.warning(
            "health_check summary: %d stale/missing data type(s): %s",
            len(stale_types),
            stale_types,
        )
    else:
        logger.info("health_check summary: all %d data types are fresh", len(results))

    return results


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

def main() -> None:
    from market_data.logging_config import setup_logging  # noqa: PLC0415

    setup_logging()

    parser = argparse.ArgumentParser(
        description="Check freshness of market_data pipeline outputs."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="Root data directory containing ohlcv/, options/, etc. (default: data/)",
    )
    args = parser.parse_args()

    results = health_check(data_dir=args.data_dir)

    stale = [k for k, v in results.items() if v["is_stale"]]
    if stale:
        sys.exit(1)


if __name__ == "__main__":
    main()

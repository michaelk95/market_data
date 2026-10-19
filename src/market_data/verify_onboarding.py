"""
verify_onboarding.py
--------------------
Checks that every ticker listed as onboarded in state.json actually has rows
in the bitemporal ohlcv table (data/ohlcv/year=YYYY/).  Any ticker that is
"onboarded" in state but has no stored rows is a ghost entry -- it will never be re-fetched by the
pipeline because the pipeline skips already-onboarded tickers.

Usage:
    python -m market_data.verify_onboarding            # report only
    python -m market_data.verify_onboarding --fix      # remove ghosts from state.json
    python -m market_data.verify_onboarding --state path/to/state.json --data path/to/data
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from market_data.storage import read_table

logger = logging.getLogger(__name__)

STATE_FILE = Path("state.json")
DATA_DIR   = Path("data")  # data root containing ohlcv/


def stored_symbols(data_dir: Path) -> set[str]:
    """Return the symbols with rows in the ohlcv table under the data root."""
    return set(read_table("ohlcv", data_dir, columns=["symbol"])["symbol"])


def check(state_file: Path, data_dir: Path) -> tuple[list[str], list[str]]:
    """
    Compare the onboarded list in state.json against symbols stored in the
    ohlcv table under the data root *data_dir*.

    Returns:
        ghosts  — onboarded in state but no stored rows (need re-onboarding)
        orphans — stored rows but not in state         (informational only)
    """
    if not state_file.exists():
        raise FileNotFoundError(f"State file not found: {state_file}")

    raw = json.loads(state_file.read_text())
    onboarded: set[str] = set(raw.get("onboarded", []))

    files_on_disk: set[str] = stored_symbols(data_dir)

    ghosts  = sorted(onboarded - files_on_disk)
    orphans = sorted(files_on_disk - onboarded)

    return ghosts, orphans


def fix(state_file: Path, ghosts: list[str]) -> int:
    """Remove ghost entries from state.json so the pipeline will re-onboard them."""
    raw = json.loads(state_file.read_text())
    before = set(raw.get("onboarded", []))
    after  = before - set(ghosts)
    raw["onboarded"] = sorted(after)
    state_file.write_text(json.dumps(raw, indent=2, default=str))
    return len(before) - len(after)


def main() -> None:
    from market_data.logging_config import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Verify onboarded tickers in state.json have rows in the ohlcv table."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Remove ghost entries from state.json so they get re-onboarded.",
    )
    parser.add_argument("--state", default=str(STATE_FILE), help="Path to state.json")
    parser.add_argument("--data",  default=str(DATA_DIR),   help="Data root containing the ohlcv/ table")
    args = parser.parse_args()

    state_file = Path(args.state)
    data_dir   = Path(args.data)

    ghosts, orphans = check(state_file, data_dir)

    # --- Report ---
    n_onboarded = len(json.loads(state_file.read_text()).get("onboarded", []))
    n_stored = len(stored_symbols(data_dir))
    logger.info("Onboarded in state: %d", n_onboarded)
    logger.info("Symbols in ohlcv table: %d", n_stored)

    if ghosts:
        logger.warning(
            "GHOSTS — %d ticker(s) onboarded in state but missing from the ohlcv table: %s",
            len(ghosts),
            ghosts,
        )
    else:
        logger.info("OK  No ghosts — every onboarded ticker has stored rows.")

    if orphans:
        logger.info(
            "Orphans (stored but not in state) — %d ticker(s): %s  "
            "(informational — the pipeline will not re-fetch them.)",
            len(orphans),
            orphans,
        )

    # --- Fix ---
    if args.fix:
        if not ghosts:
            logger.info("Nothing to fix.")
        else:
            removed = fix(state_file, ghosts)
            logger.info(
                "Fixed: removed %d ghost(s) from %s. Run the pipeline to re-onboard them.",
                removed,
                state_file,
            )
    elif ghosts:
        logger.info("Run with --fix to remove ghost(s) from state.json.")
        sys.exit(1)


if __name__ == "__main__":
    main()

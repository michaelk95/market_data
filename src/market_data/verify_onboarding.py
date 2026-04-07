"""
verify_onboarding.py
--------------------
Checks that every ticker listed as onboarded in state.json actually has a
corresponding data file on disk.  Any ticker that is "onboarded" in state but
missing its parquet file is a ghost entry -- it will never be re-fetched by the
pipeline because the pipeline skips already-onboarded tickers.

Usage:
    python -m market_data.verify_onboarding            # report only
    python -m market_data.verify_onboarding --fix      # remove ghosts from state.json
    python -m market_data.verify_onboarding --state path/to/state.json --data path/to/ohlcv
"""

import argparse
import json
from pathlib import Path


STATE_FILE = Path("state.json")
DATA_DIR   = Path("data/ohlcv")


def check(state_file: Path, data_dir: Path) -> tuple[list[str], list[str]]:
    """
    Compare the onboarded list in state.json against parquet files on disk.

    Returns:
        ghosts  — onboarded in state but no file on disk (need re-onboarding)
        orphans — file exists on disk but not in state   (informational only)
    """
    if not state_file.exists():
        raise FileNotFoundError(f"State file not found: {state_file}")

    raw = json.loads(state_file.read_text())
    onboarded: set[str] = set(raw.get("onboarded", []))

    files_on_disk: set[str] = {p.stem for p in data_dir.glob("*.parquet")}

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
    parser = argparse.ArgumentParser(
        description="Verify onboarded tickers in state.json have data files on disk."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Remove ghost entries from state.json so they get re-onboarded.",
    )
    parser.add_argument("--state", default=str(STATE_FILE), help="Path to state.json")
    parser.add_argument("--data",  default=str(DATA_DIR),   help="Path to ohlcv data directory")
    args = parser.parse_args()

    state_file = Path(args.state)
    data_dir   = Path(args.data)

    ghosts, orphans = check(state_file, data_dir)

    # --- Report ---
    print(f"\nOnboarded in state : {len(json.loads(state_file.read_text()).get('onboarded', []))}")
    print(f"Files on disk       : {len(list(data_dir.glob('*.parquet')))}")

    if ghosts:
        print(f"\n{'-'*50}")
        print(f"GHOSTS -- {len(ghosts)} ticker(s) onboarded in state but missing data file:")
        for sym in ghosts:
            print(f"  {sym}")
        print(f"{'-'*50}")
    else:
        print("\nOK  No ghosts -- every onboarded ticker has a data file.")

    if orphans:
        # Orphans aren't a problem -- they may be leftover from manual testing, etc.
        print(f"\nOrphans (file exists but not in state) -- {len(orphans)} ticker(s):")
        for sym in orphans:
            print(f"  {sym}")
        print("  (These are informational -- the pipeline will not re-fetch them.)")

    # --- Fix ---
    if args.fix:
        if not ghosts:
            print("\nNothing to fix.")
        else:
            removed = fix(state_file, ghosts)
            print(f"\nFixed: removed {removed} ghost(s) from {state_file}.")
            print("Run the pipeline to re-onboard them.")
    elif ghosts:
        print("\nRun with --fix to remove ghost(s) from state.json.")


if __name__ == "__main__":
    main()

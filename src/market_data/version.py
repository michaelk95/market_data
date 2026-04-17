from __future__ import annotations

import subprocess
from importlib.metadata import version as _pkg_version
from pathlib import Path


def version() -> dict:
    pkg = _pkg_version("market_data")
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).parent,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        sha = "unknown"
    return {"version": pkg, "sha": sha}

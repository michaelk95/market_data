"""
tests/test_runner_script.py
---------------------------
Checks the nightly command in run_and_sleep.bat and the config it relies on.

The batch file is only read, never executed (it hibernates the PC).
"""

from __future__ import annotations

import shlex
import sys
from pathlib import Path
from unittest.mock import patch

import yaml

from market_data import orchestrator

REPO_ROOT = Path(__file__).resolve().parents[1]


def _nightly_args() -> list[str]:
    """Return the market-data-run arguments from run_and_sleep.bat."""
    for line in (REPO_ROOT / "run_and_sleep.bat").read_text(encoding="utf-8").splitlines():
        if line.startswith("market-data-run"):
            return shlex.split(line.split(">>")[0])[1:]
    raise AssertionError("no market-data-run line in run_and_sleep.bat")


def test_nightly_command_collects_analyst_estimates():
    assert "--analyst-estimates" in _nightly_args()


def test_nightly_command_parses_and_enables_analyst_estimates():
    with patch.object(sys, "argv", ["market-data-run", *_nightly_args()]), \
            patch.object(orchestrator, "run") as run, \
            patch("market_data.logging_config.setup_logging"):
        orchestrator.main()
    assert run.call_args.kwargs["run_analyst_estimates"] is True


def test_config_sets_analyst_estimates_sleep():
    data = yaml.safe_load((REPO_ROOT / "config.yaml").read_text(encoding="utf-8"))
    assert data["sources"]["sleep_between_calls"]["analyst_estimates"] > 0

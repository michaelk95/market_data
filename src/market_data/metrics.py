"""
metrics.py
----------
Tracks and persists pipeline run metrics to logs/metrics.json.

Each pipeline run records:
  data_type           Pipeline step label (onboard, update, options, fundamentals, macro)
  start_time          ISO-8601 UTC timestamp when the run began
  end_time            ISO-8601 UTC timestamp when the run finished
  duration_seconds    Wall-clock seconds for the full run
  symbols_attempted   Total symbols processed
  symbols_succeeded   Symbols that produced data without error
  symbols_failed      List of {"symbol": ..., "reason": ...} for every failure
  rows_written        Dict of {data_type: row_count} for data written

The file keeps a rolling 90-day window of run history; older entries are
pruned automatically when finish_run() is called.

Usage (from orchestrator)
--------------------------
    from market_data import metrics

    metrics.start_run("onboard")
    ...
    metrics.record_symbol_result("AAPL", success=True, rows_written=2520)
    metrics.record_symbol_result("BADTICKER", success=False, reason="no data")
    ...
    metrics.finish_run()
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

METRICS_FILE = Path("logs/metrics.json")
RETENTION_DAYS = 90

# Module-level state for the currently active run
_current_run: dict | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_run(data_type: str) -> None:
    """Initialise metrics tracking for a new pipeline run."""
    global _current_run
    _current_run = {
        "data_type": data_type,
        "start_time": datetime.now(timezone.utc).isoformat(),
        "end_time": None,
        "duration_seconds": None,
        "symbols_attempted": 0,
        "symbols_succeeded": 0,
        "symbols_failed": [],
        "rows_written": {},
    }
    logger.debug("metrics: %s run started at %s", data_type, _current_run["start_time"])


def record_symbol_result(
    symbol: str,
    success: bool,
    rows_written: int = 0,
    reason: str | None = None,
) -> None:
    """
    Record the outcome for a single symbol.

    Parameters
    ----------
    symbol       : Ticker or series ID
    success      : True if data was fetched without error
    rows_written : Net-new rows written (meaningful only when success=True)
    reason       : Short description of why the symbol failed (when success=False)
    """
    if _current_run is None:
        return

    _current_run["symbols_attempted"] += 1

    if success:
        _current_run["symbols_succeeded"] += 1
        data_type = _current_run["data_type"]
        _current_run["rows_written"][data_type] = (
            _current_run["rows_written"].get(data_type, 0) + rows_written
        )
    else:
        failure_reason = reason or "unknown"
        _current_run["symbols_failed"].append({"symbol": symbol, "reason": failure_reason})
        logger.debug(
            "metrics: %s/%s failed — %s", _current_run["data_type"], symbol, failure_reason
        )


def finish_run() -> None:
    """
    Finalise the current run, compute duration, persist to metrics.json, and
    reset module state.
    """
    global _current_run
    if _current_run is None:
        return

    end_time = datetime.now(timezone.utc)
    start_time = datetime.fromisoformat(_current_run["start_time"])

    _current_run["end_time"] = end_time.isoformat()
    _current_run["duration_seconds"] = round(
        (end_time - start_time).total_seconds(), 2
    )

    logger.debug(
        "metrics: %s run finished — %.1fs, %d succeeded, %d failed",
        _current_run["data_type"],
        _current_run["duration_seconds"],
        _current_run["symbols_succeeded"],
        len(_current_run["symbols_failed"]),
    )

    try:
        _persist(_current_run.copy())
    except Exception:
        logger.warning("metrics: failed to persist run metrics", exc_info=True)

    _current_run = None


def load_history() -> dict:
    """Load the full metrics history from disk. Returns {"runs": []} if missing."""
    if not METRICS_FILE.exists():
        return {"runs": []}
    try:
        return json.loads(METRICS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"runs": []}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _persist(run: dict) -> None:
    """Append `run` to metrics.json and prune entries older than RETENTION_DAYS."""
    METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)

    if METRICS_FILE.exists():
        try:
            data = json.loads(METRICS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            logger.warning("metrics: could not read existing metrics.json; starting fresh")
            data = {"runs": []}
    else:
        data = {"runs": []}

    data["runs"].append(run)

    # Prune runs older than RETENTION_DAYS
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    ).isoformat()
    data["runs"] = [r for r in data["runs"] if r.get("start_time", "") >= cutoff]

    METRICS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

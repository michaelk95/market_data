"""
Every script in scripts/ must log under the 'market_data' logger.

setup_logging() only attaches handlers to 'market_data'. A script that uses
logging.getLogger(__name__) gets a '__main__' logger when run directly, so its
output never reaches logs/market_data.log or the console handler.

caplog is not enough to catch this: it listens on the root logger, which every
logger propagates to. These tests check that records reach the handlers
setup_logging() installs on 'market_data' itself.
"""

import importlib
import logging
from pathlib import Path

import pytest

from market_data import logging_config

SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
SCRIPT_NAMES = sorted(p.stem for p in SCRIPTS_DIR.glob("*.py"))


class _ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def configured_logging(tmp_path, monkeypatch):
    """Run the real setup_logging() into tmp_path, then restore the logger."""
    monkeypatch.chdir(tmp_path)
    pkg_logger = logging.getLogger("market_data")
    saved_handlers = pkg_logger.handlers[:]
    saved_level = pkg_logger.level
    pkg_logger.handlers.clear()

    logging_config.setup_logging()
    capture = _ListHandler()
    pkg_logger.addHandler(capture)
    yield capture

    for handler in pkg_logger.handlers:
        handler.close()
    pkg_logger.handlers[:] = saved_handlers
    pkg_logger.setLevel(saved_level)


def test_scripts_found():
    assert SCRIPT_NAMES, f"no scripts found in {SCRIPTS_DIR}"


@pytest.mark.parametrize("script", SCRIPT_NAMES)
def test_script_logger_is_under_market_data(script):
    mod = importlib.import_module(script)
    assert mod.logger.name == f"market_data.{script}"


@pytest.mark.parametrize("script", SCRIPT_NAMES)
def test_script_logger_reaches_configured_handlers(script, configured_logging):
    mod = importlib.import_module(script)

    mod.logger.info("hello from %s", script)

    messages = [r.getMessage() for r in configured_logging.records]
    assert f"hello from {script}" in messages
    log_file = Path(logging_config.LOG_FILE)
    for handler in logging.getLogger("market_data").handlers:
        handler.flush()
    assert f"hello from {script}" in log_file.read_text(encoding="utf-8")

"""
logging_config.py
-----------------
Configures the market_data package logger.

Call setup_logging() once at the start of each CLI entry point.  All module-level
loggers (logging.getLogger(__name__)) in this package propagate to the root
'market_data' logger configured here.

Log destinations
----------------
  logs/market_data.log  RotatingFileHandler — DEBUG and above, 10 MB max, 5 backups
  stderr                StreamHandler       — INFO and above
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "market_data.log"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5


def setup_logging(level: int = logging.DEBUG) -> None:
    """
    Configure the 'market_data' package logger.

    Idempotent: calling this function more than once has no effect.
    """
    logger = logging.getLogger("market_data")

    # Avoid adding duplicate handlers on repeated calls
    if logger.handlers:
        return

    logger.setLevel(level)

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(LOG_FORMAT)

    # Rotating file handler — captures DEBUG and above
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    # Console handler — captures INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

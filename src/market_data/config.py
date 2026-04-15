"""
config.py
---------
Centralized configuration loader for the market_data pipeline.

Loads ``config.yaml`` once at import time and exposes a ``cfg`` singleton.
All other modules read their tunable constants from ``cfg`` at startup,
falling back to the same hardcoded defaults they previously embedded directly.

Config file location
--------------------
By default the loader looks for ``config.yaml`` at the repository root
(three directory levels above this file: ``src/market_data/config.py``
→ ``src/market_data/`` → ``src/`` → repo root).

Tests can override the location by setting the environment variable
``MARKET_DATA_CONFIG`` to an absolute path before importing this module
(or by calling ``reload_config(path)`` from test fixtures).

Usage
-----
    from market_data.config import cfg

    batch_size = cfg.get("collection.batch_size", 50)
    data_path  = cfg.resolve_path("paths.ohlcv_dir", "data/ohlcv")
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repo root detection
# ---------------------------------------------------------------------------

# config.py lives at src/market_data/config.py; go up three levels to reach
# the repository root where config.yaml is stored.
_REPO_ROOT: Path = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# YAML loading (graceful degradation if PyYAML not installed)
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict:
    """Load a YAML file and return its contents as a dict.

    Returns an empty dict if the file is missing, unreadable, or if PyYAML
    is not installed — so modules that read from ``cfg`` always get their
    hardcoded fallback defaults rather than crashing.
    """
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore[import]
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


# ---------------------------------------------------------------------------
# Config class
# ---------------------------------------------------------------------------

class Config:
    """Thin wrapper around a nested dict that provides safe dot-key access
    and path resolution relative to the repository root.
    """

    def __init__(self, data: dict, root: Path) -> None:
        self._data = data
        self.root = root

    # ------------------------------------------------------------------
    # Value access
    # ------------------------------------------------------------------

    def get(self, key: str | list[str], default: Any = None) -> Any:
        """Retrieve a config value by dot-separated key string or key list.

        Parameters
        ----------
        key:
            Either a dot-separated string (``"collection.batch_size"``) or a
            list of strings (``["collection", "batch_size"]``).
        default:
            Value returned when *any* segment of the path is missing or the
            value at that path is ``None``.

        Examples
        --------
        >>> cfg.get("collection.batch_size", 50)
        50
        >>> cfg.get(["health", "freshness_days"], {})
        {'ohlcv': 2, 'options': 14, 'fundamentals': 35, 'macro': 7}
        """
        keys = key.split(".") if isinstance(key, str) else list(key)
        node: Any = self._data
        for k in keys:
            if not isinstance(node, dict) or k not in node:
                return default
            node = node[k]
        return node if node is not None else default

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def resolve_path(self, key: str, default: str = "") -> Path:
        """Return an absolute ``Path`` for a config path value.

        The raw string is resolved relative to the repository root so that
        relative paths like ``"data/ohlcv"`` work regardless of the current
        working directory.

        Parameters
        ----------
        key:
            Dot-separated key pointing to the path string in the config.
        default:
            Fallback string if the key is missing.
        """
        raw = self.get(key, default)
        return (self.root / raw).resolve()


# ---------------------------------------------------------------------------
# Singleton — loaded once at import time
# ---------------------------------------------------------------------------

def _build_config() -> Config:
    """Locate and load config.yaml, returning a ready ``Config`` instance."""
    env_path = os.environ.get("MARKET_DATA_CONFIG", "")
    if env_path:
        config_path = Path(env_path)
        # When a custom path is given in tests, use its parent as root so
        # path resolution stays consistent with where the file lives.
        root = config_path.parent
    else:
        config_path = _REPO_ROOT / "config.yaml"
        root = _REPO_ROOT

    data = _load_yaml(config_path)
    return Config(data, root)


cfg: Config = _build_config()


# ---------------------------------------------------------------------------
# Test helper
# ---------------------------------------------------------------------------

def reload_config(path: Path | None = None) -> Config:
    """Reload ``cfg`` from *path* (or the default location if None).

    Intended for use in test fixtures that need to inject a temporary
    config file.  Updates the module-level ``cfg`` singleton in place so
    that callers which have already done ``from market_data.config import cfg``
    will see the new values via attribute access on the returned object.

    Returns the new ``Config`` so callers can also rebind their local name.

    Note: Module-level constants that were already evaluated at import time
    (e.g. ``health.FRESHNESS_WINDOWS``) are **not** re-evaluated; only future
    calls to ``cfg.get()`` will see the new values.
    """
    global cfg  # noqa: PLW0603
    if path is not None:
        root = path.parent
        data = _load_yaml(path)
    else:
        env_path = os.environ.get("MARKET_DATA_CONFIG", "")
        if env_path:
            p = Path(env_path)
            root = p.parent
            data = _load_yaml(p)
        else:
            root = _REPO_ROOT
            data = _load_yaml(_REPO_ROOT / "config.yaml")
    cfg = Config(data, root)
    return cfg

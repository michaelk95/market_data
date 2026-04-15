"""
tests/test_config.py
--------------------
Tests for the centralized configuration loader (src/market_data/config.py).

All tests are self-contained: they either use the real config.yaml that ships
with the repo (round-trip checks) or write a minimal temp file to exercise
path resolution and override logic.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_config(path: Path, data: dict) -> None:
    """Write *data* as YAML to *path*."""
    path.write_text(yaml.dump(data), encoding="utf-8")


# ---------------------------------------------------------------------------
# Basic loading
# ---------------------------------------------------------------------------

class TestConfigLoads:
    def test_cfg_singleton_exists(self):
        from market_data.config import cfg
        assert cfg is not None

    def test_cfg_is_config_instance(self):
        from market_data.config import Config, cfg
        assert isinstance(cfg, Config)

    def test_loads_without_error(self):
        """Importing config must never raise even if YAML is missing."""
        import market_data.config  # noqa: F401  — just verify no ImportError


# ---------------------------------------------------------------------------
# Top-level section presence
# ---------------------------------------------------------------------------

class TestExpectedSections:
    @pytest.mark.parametrize("section", [
        "collection",
        "macro",
        "indices",
        "paths",
        "sources",
        "health",
    ])
    def test_section_present(self, section):
        from market_data.config import cfg
        assert cfg.get(section) is not None, f"Missing top-level section: {section!r}"


# ---------------------------------------------------------------------------
# Collection section
# ---------------------------------------------------------------------------

class TestCollectionConfig:
    def test_history_years_is_int(self):
        from market_data.config import cfg
        assert isinstance(cfg.get("collection.history_years"), int)

    def test_history_years_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.history_years") == 10

    def test_batch_size_is_int(self):
        from market_data.config import cfg
        assert isinstance(cfg.get("collection.batch_size"), int)

    def test_batch_size_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.batch_size") == 50

    def test_options_batch_size_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.options_batch_size") == 50

    def test_options_max_expiries_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.options_max_expiries") == 4

    def test_ticker_refresh_days_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.ticker_refresh_days") == 90

    def test_fundamentals_refresh_days_value(self):
        from market_data.config import cfg
        assert cfg.get("collection.fundamentals_refresh_days") == 30

    def test_macro_start_is_string(self):
        from market_data.config import cfg
        assert isinstance(cfg.get("collection.macro_start"), str)


# ---------------------------------------------------------------------------
# Health / freshness section
# ---------------------------------------------------------------------------

class TestHealthConfig:
    def test_freshness_days_present(self):
        from market_data.config import cfg
        assert cfg.get("health.freshness_days") is not None

    @pytest.mark.parametrize("data_type,expected", [
        ("ohlcv", 2),
        ("options", 14),
        ("fundamentals", 35),
        ("macro", 7),
    ])
    def test_freshness_value(self, data_type, expected):
        from market_data.config import cfg
        fw = cfg.get("health.freshness_days")
        assert fw[data_type] == expected, (
            f"health.freshness_days.{data_type} should be {expected}, got {fw[data_type]}"
        )


# ---------------------------------------------------------------------------
# Macro series section
# ---------------------------------------------------------------------------

class TestMacroConfig:
    def test_series_mapping_present(self):
        from market_data.config import cfg
        assert cfg.get("macro.series") is not None

    @pytest.mark.parametrize("series_id", [
        "DFF", "T10Y2Y",                          # daily
        "CPIAUCSL", "CPILFESL", "PCEPI",          # monthly CPI/PCE
        "PCEPILFE", "UNRATE", "PAYEMS",           # monthly employment
        "GDPC1", "GDP",                           # quarterly GDP
    ])
    def test_series_id_present(self, series_id):
        from market_data.config import cfg
        series = cfg.get("macro.series")
        assert series_id in series, f"FRED series {series_id!r} missing from macro.series"

    def test_series_values_are_strings(self):
        from market_data.config import cfg
        for sid, desc in cfg.get("macro.series").items():
            assert isinstance(desc, str), f"macro.series[{sid!r}] description is not a string"


# ---------------------------------------------------------------------------
# Indices section
# ---------------------------------------------------------------------------

class TestIndicesConfig:
    def test_symbols_list_present(self):
        from market_data.config import cfg
        assert cfg.get("indices.symbols") is not None

    @pytest.mark.parametrize("symbol", ["^VIX", "^TNX", "^GSPC", "ZQ=F"])
    def test_expected_symbol_present(self, symbol):
        from market_data.config import cfg
        assert symbol in cfg.get("indices.symbols"), (
            f"{symbol!r} missing from indices.symbols"
        )


# ---------------------------------------------------------------------------
# Paths section
# ---------------------------------------------------------------------------

class TestPathsConfig:
    @pytest.mark.parametrize("key", [
        "paths.data_dir",
        "paths.ohlcv_dir",
        "paths.options_dir",
        "paths.fundamentals_dir",
        "paths.macro_dir",
        "paths.indices_dir",
        "paths.logs_dir",
        "paths.state_file",
        "paths.tickers_file",
        "paths.metrics_file",
    ])
    def test_path_key_present(self, key):
        from market_data.config import cfg
        assert cfg.get(key) is not None, f"Missing config key: {key!r}"

    def test_resolve_path_returns_absolute(self):
        from market_data.config import cfg
        resolved = cfg.resolve_path("paths.ohlcv_dir", "data/ohlcv")
        assert resolved.is_absolute()

    def test_resolve_path_ends_with_relative_segment(self):
        from market_data.config import cfg
        resolved = cfg.resolve_path("paths.ohlcv_dir", "data/ohlcv")
        assert "ohlcv" in resolved.parts

    def test_resolve_path_missing_key_uses_default(self):
        from market_data.config import cfg
        resolved = cfg.resolve_path("paths.nonexistent_key", "data/fallback")
        assert resolved.is_absolute()
        assert "fallback" in resolved.parts


# ---------------------------------------------------------------------------
# Sources section
# ---------------------------------------------------------------------------

class TestSourcesConfig:
    @pytest.mark.parametrize("pipeline", ["ohlcv", "options", "fundamentals", "indices"])
    def test_sleep_between_calls_present(self, pipeline):
        from market_data.config import cfg
        val = cfg.get(f"sources.sleep_between_calls.{pipeline}")
        assert isinstance(val, (int, float)), (
            f"sources.sleep_between_calls.{pipeline} should be numeric, got {val!r}"
        )


# ---------------------------------------------------------------------------
# Safe-access (get with default)
# ---------------------------------------------------------------------------

class TestSafeAccess:
    def test_missing_key_returns_none(self):
        from market_data.config import cfg
        assert cfg.get("totally.nonexistent.key") is None

    def test_missing_key_returns_supplied_default(self):
        from market_data.config import cfg
        assert cfg.get("totally.nonexistent.key", "fallback") == "fallback"

    def test_missing_key_returns_zero_default(self):
        from market_data.config import cfg
        assert cfg.get("totally.nonexistent.key", 0) == 0

    def test_missing_nested_key_returns_default(self):
        from market_data.config import cfg
        # "collection" exists but "collection.nonexistent" does not
        assert cfg.get("collection.nonexistent_key", 999) == 999

    def test_list_key_syntax(self):
        from market_data.config import cfg
        # List-of-keys form should work identically to dot-string form
        v1 = cfg.get("collection.batch_size")
        v2 = cfg.get(["collection", "batch_size"])
        assert v1 == v2


# ---------------------------------------------------------------------------
# Module-level constant propagation
# ---------------------------------------------------------------------------

class TestModuleConstants:
    """Verify that updated modules actually read their values from config."""

    def test_health_freshness_windows(self):
        from market_data.health import FRESHNESS_WINDOWS
        assert FRESHNESS_WINDOWS["ohlcv"] == 2
        assert FRESHNESS_WINDOWS["options"] == 14
        assert FRESHNESS_WINDOWS["fundamentals"] == 35
        assert FRESHNESS_WINDOWS["macro"] == 7

    def test_fetch_history_years(self):
        from market_data.fetch import DEFAULT_HISTORY_YEARS
        assert DEFAULT_HISTORY_YEARS == 10

    def test_orchestrator_batch_size(self):
        from market_data.orchestrator import DEFAULT_BATCH_SIZE
        assert DEFAULT_BATCH_SIZE == 50

    def test_orchestrator_refresh_days(self):
        from market_data.orchestrator import (
            FUNDAMENTALS_REFRESH_DAYS,
            TICKER_REFRESH_DAYS,
        )
        assert TICKER_REFRESH_DAYS == 90
        assert FUNDAMENTALS_REFRESH_DAYS == 30

    def test_orchestrator_options_defaults(self):
        from market_data.orchestrator import DEFAULT_MAX_EXPIRIES, DEFAULT_OPTIONS_BATCH_SIZE
        assert DEFAULT_OPTIONS_BATCH_SIZE == 50
        assert DEFAULT_MAX_EXPIRIES == 4

    def test_fetch_macro_default_series(self):
        from market_data.fetch_macro import DEFAULT_SERIES
        assert "DFF" in DEFAULT_SERIES
        assert "CPIAUCSL" in DEFAULT_SERIES
        assert "GDPC1" in DEFAULT_SERIES

    def test_fetch_indices_symbols(self):
        from market_data.fetch_indices import INDEX_SYMBOLS
        assert "^VIX" in INDEX_SYMBOLS
        assert "^GSPC" in INDEX_SYMBOLS


# ---------------------------------------------------------------------------
# Custom config file override (reload_config)
# ---------------------------------------------------------------------------

class TestConfigOverride:
    def test_reload_from_custom_file(self, tmp_path):
        """reload_config() with a tmp file returns a Config with the new data."""
        custom = tmp_path / "custom.yaml"
        _write_config(custom, {"collection": {"batch_size": 99}})

        from market_data.config import reload_config
        custom_cfg = reload_config(path=custom)
        assert custom_cfg.get("collection.batch_size") == 99

    def test_reload_missing_key_returns_default(self, tmp_path):
        custom = tmp_path / "partial.yaml"
        _write_config(custom, {"collection": {}})

        from market_data.config import reload_config
        custom_cfg = reload_config(path=custom)
        assert custom_cfg.get("collection.batch_size", 50) == 50

    def test_reload_restores_original(self, tmp_path):
        """Calling reload_config(None) re-reads the default config.yaml."""
        custom = tmp_path / "override.yaml"
        _write_config(custom, {"collection": {"batch_size": 1}})

        from market_data.config import reload_config
        reload_config(path=custom)
        # Restore
        original_cfg = reload_config(path=None)
        # Should have the real value back
        assert original_cfg.get("collection.batch_size") == 50

from __future__ import annotations

from unittest.mock import patch

import market_data
from market_data.version import version


class TestVersion:
    def test_returns_dict_with_required_keys(self):
        result = version()
        assert "version" in result
        assert "sha" in result

    def test_version_string_is_str(self):
        result = version()
        assert isinstance(result["version"], str)
        assert result["version"] != ""

    def test_sha_is_str(self):
        result = version()
        assert isinstance(result["sha"], str)
        assert result["sha"] != ""

    def test_sha_fallback_on_git_unavailable(self):
        with patch("subprocess.check_output", side_effect=FileNotFoundError):
            result = version()
        assert result["sha"] == "unknown"

    def test_sha_fallback_on_subprocess_error(self):
        import subprocess
        with patch("subprocess.check_output", side_effect=subprocess.CalledProcessError(128, "git")):
            result = version()
        assert result["sha"] == "unknown"


class TestDunderVersion:
    def test_module_version_is_plain_string(self):
        assert isinstance(market_data.__version__, str)
        assert market_data.__version__ != ""

    def test_matches_version_function(self):
        assert market_data.__version__ == version()["version"]

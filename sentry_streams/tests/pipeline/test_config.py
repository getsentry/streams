import os
from pathlib import Path
from unittest import mock

import pytest

from sentry_streams.pipeline.config import ConfigEnvError, load_config, resolve_envvars

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def test_substitute_String() -> None:
    # Env var is set
    config = {"host": "${envvar:MY_HOST}"}
    with mock.patch.dict(os.environ, {"MY_HOST": "kafka.example.com"}, clear=True):
        resolve_envvars(config)
    assert config["host"] == "kafka.example.com"

    # Env var is not set
    config = {"host": "${envvar:MISSING_VAR}"}
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigEnvError) as exc_info:
            resolve_envvars(config)
    assert exc_info.value.var_name == "MISSING_VAR"
    assert "MISSING_VAR" in str(exc_info.value)
    assert "not set" in str(exc_info.value).lower()

    # String does not match pattern
    config = {"host": "localhost", "url": "http://${envvar:HOST}:8080"}
    resolve_envvars(config)
    assert config["host"] == "localhost"
    assert config["url"] == "http://${envvar:HOST}:8080"


def test_no_substitution_for_dict_recurse_only() -> None:
    """For dict values we recurse; we do not replace the whole dict with an env var."""
    config = {"nested": {"key": "${envvar:INNER}"}}
    with mock.patch.dict(os.environ, {"INNER": "value"}, clear=True):
        resolve_envvars(config)
    assert config["nested"] == {"key": "value"}


def test_substitution_in_nested_dict_and_list_elements() -> None:
    """Substitution in nested dict and inside list elements."""
    config = {
        "pipeline": {
            "segments": [
                {
                    "steps_config": {
                        "myinput": {
                            "bootstrap_servers": ["${envvar:BOOTSTRAP_SERVERS}"],
                        },
                    },
                },
            ],
        },
    }
    with mock.patch.dict(os.environ, {"BOOTSTRAP_SERVERS": "127.0.0.1:9092"}, clear=True):
        resolve_envvars(config)
    assert config["pipeline"]["segments"][0]["steps_config"]["myinput"]["bootstrap_servers"] == [
        "127.0.0.1:9092",
    ]


def test_multiple_list_entries_substituted() -> None:
    """Each list element that matches the pattern is substituted."""
    config = {"servers": ["${envvar:FIRST}", "${envvar:SECOND}"]}
    with mock.patch.dict(os.environ, {"FIRST": "a:9092", "SECOND": "b:9092"}, clear=True):
        resolve_envvars(config)
    assert config["servers"] == ["a:9092", "b:9092"]


def test_non_string_values_unchanged() -> None:
    """Integers, booleans, None are left unchanged."""
    config = {"count": 42, "enabled": True, "empty": None}
    resolve_envvars(config)
    assert config["count"] == 42
    assert config["enabled"] is True
    assert config["empty"] is None


def test_var_name_regex() -> None:
    """Variable names must match [A-Za-z_][A-Za-z0-9_]*."""
    with mock.patch.dict(os.environ, {"VALID_NAME": "ok", "_also_valid": "ok"}, clear=True):
        config1 = {"a": "${envvar:VALID_NAME}"}
        resolve_envvars(config1)
        assert config1["a"] == "ok"
        config2 = {"a": "${envvar:_also_valid}"}
        resolve_envvars(config2)
        assert config2["a"] == "ok"
    # These are not substituted (no full match — leading digit invalid)
    with mock.patch.dict(os.environ, {}, clear=True):
        config3 = {"a": "${envvar:123invalid}"}
        resolve_envvars(config3)
    assert config3["a"] == "${envvar:123invalid}"


def test_load_config_returns_pipeline_config() -> None:
    """load_config loads YAML, resolves envvars, validates, and returns PipelineConfig."""
    config_file = FIXTURES_DIR / "config_with_envvar.yaml"
    with mock.patch.dict(os.environ, {"BOOTSTRAP_SERVERS": "127.0.0.1:9092"}, clear=True):
        config = load_config(str(config_file))
    assert "pipeline" in config
    assert config["pipeline"]["segments"][0]["steps_config"]["myinput"]["bootstrap_servers"] == [
        "127.0.0.1:9092",
    ]
    assert config.get("metrics", {}).get("type") == "dummy"

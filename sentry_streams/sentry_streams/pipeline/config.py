import importlib.resources
import json
import os
import re
from typing import Any, cast

import jsonschema
import yaml

from sentry_streams.adapters.stream_adapter import PipelineConfig

# Whole-string match: ${envvar:VAR_NAME} where VAR_NAME is [A-Za-z_][A-Za-z0-9_]*
_ENVVAR_PATTERN = re.compile(r"^\$\{envvar:([A-Za-z_][A-Za-z0-9_]*)\}$")


class ConfigEnvError(Exception):
    """Raised when a referenced environment variable is not set."""

    def __init__(self, var_name: str) -> None:
        self.var_name = var_name
        super().__init__(f"Environment variable {var_name!r} is not set (referenced in config)")


def resolve_envvars(obj: Any) -> None:
    """
    Recursively resolve ${envvar:VAR_NAME} placeholders in config data in place.

    - For dicts: recurse into each value.
    - For lists: recurse into each element.
    - For strings: if the entire value matches ${envvar:NAME}, replace with
      os.environ[NAME]; if NAME is not set, raise ConfigEnvError.
    - Other types: left unchanged.
    """
    if isinstance(obj, dict):
        for key, value in list(obj.items()):
            if isinstance(value, str):
                resolved = _resolve_string(value)
                if resolved is not None:
                    obj[key] = resolved
            else:
                resolve_envvars(value)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str):
                resolved = _resolve_string(item)
                if resolved is not None:
                    obj[i] = resolved
            else:
                resolve_envvars(item)


def _resolve_string(value: str) -> str | None:
    """If value is exactly ${envvar:VAR_NAME}, return os.environ[VAR_NAME]; else None."""
    match = _ENVVAR_PATTERN.fullmatch(value)
    if match is None:
        return None
    var_name = match.group(1)
    if var_name not in os.environ:
        raise ConfigEnvError(var_name)
    return os.environ[var_name]


def load_config(config_path: str) -> PipelineConfig:
    """
    Load a pipeline config file: read YAML, resolve ${envvar:...}, validate against schema.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    resolve_envvars(config)

    schema_path = importlib.resources.files("sentry_streams") / "config.json"
    with schema_path.open("r") as file:
        schema = json.load(file)
    jsonschema.validate(config, schema)

    return cast(PipelineConfig, config)

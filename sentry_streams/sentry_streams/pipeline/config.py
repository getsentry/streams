import copy
import importlib.resources
import json
import os
import re
from typing import Any, Optional, cast

import jsonschema
import yaml

from sentry_streams.adapters.stream_adapter import PipelineConfig

# Whole-string match: ${envvar:VAR_NAME} where VAR_NAME is [A-Za-z_][A-Za-z0-9_]*
_ENVVAR_PATTERN = re.compile(r"^\$\{envvar:([A-Za-z_][A-Za-z0-9_]*)\}$")


class TypeMismatchError(TypeError):
    """Raised when attempting to merge incompatible types in deepmerge."""

    pass


class ScalarOverwriteError(ValueError):
    """Raised when attempting to overwrite a scalar value during deepmerge."""

    pass


def deepmerge(
    base: dict[str, Any],
    override: dict[str, Any],
    fail_on_scalar_overwrite: bool = False,
    _path: list[str] | None = None,
) -> dict[str, Any]:
    """
    Deep merge two dictionaries.

    Merge semantics:
    - Simple types (str, int, bool, None): override replaces base
    - Dictionaries: recursively merge
    - Lists: concatenate (append override elements to base)
    - Type mismatches (e.g., dict + list, dict + str): raises TypeMismatchError
    """
    if _path is None:
        _path = []

    result = copy.deepcopy(base)

    for key, override_value in override.items():
        current_path = _path + [key]
        path_str = ".".join(current_path)

        if key not in result:
            result[key] = copy.deepcopy(override_value)
        else:
            base_value = result[key]

            if isinstance(base_value, dict) and isinstance(override_value, dict):
                result[key] = deepmerge(
                    base_value,
                    override_value,
                    fail_on_scalar_overwrite=fail_on_scalar_overwrite,
                    _path=current_path,
                )
            elif isinstance(base_value, list) and isinstance(override_value, list):
                result[key] = base_value + copy.deepcopy(override_value)
            elif type(base_value) is not type(override_value):
                raise TypeMismatchError(
                    f"Cannot merge key '{key}': base type is {type(base_value)} but override type is {type(override_value)}"
                )
            else:
                if fail_on_scalar_overwrite and base_value != override_value:
                    raise ScalarOverwriteError(
                        f"Cannot overwrite scalar at '{path_str}': would change {base_value!r} to {override_value!r}"
                    )
                result[key] = copy.deepcopy(override_value)

    return result


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


def load_config(config_path: str, override_path: Optional[str] = None) -> PipelineConfig:
    """
    Load a pipeline config file: read YAML, resolve ${envvar:...}, validate against schema.

    If override_path is provided, load the override YAML and deep-merge it into the base
    config before schema validation.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if override_path is not None:
        with open(override_path, "r") as f:
            override = yaml.safe_load(f)
        config = deepmerge(config, override)

    resolve_envvars(config)

    schema_path = importlib.resources.files("sentry_streams") / "config.json"
    with schema_path.open("r") as file:
        schema = json.load(file)
    jsonschema.validate(config, schema)

    return cast(PipelineConfig, config)

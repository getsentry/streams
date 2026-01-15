import importlib.resources
import json
from typing import Any

import jsonschema


def validate_pipeline_config(config: dict[str, Any]) -> None:
    """
    Validates a pipeline configuration against the config schema.

    Reads the schema from sentry_streams package's config.json file.

    Args:
        config: The pipeline configuration dictionary to validate

    Raises:
        jsonschema.ValidationError: If the configuration doesn't conform to the schema
    """
    config_template = importlib.resources.files("sentry_streams") / "config.json"
    with config_template.open("r") as file:
        schema = json.load(file)

        try:
            jsonschema.validate(config, schema)
        except Exception:
            raise

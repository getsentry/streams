from typing import Any

import jsonschema

# Inline schema from sentry_streams/config.json
CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$ref": "#/definitions/Main",
    "definitions": {
        "Main": {
            "type": "object",
            "properties": {
                "env": {"$ref": "#/definitions/Env"},
                "pipeline": {"$ref": "#/definitions/Pipeline"},
                "metrics": {"$ref": "#/definitions/Metrics"},
            },
        },
        "Pipeline": {
            "type": "object",
            "title": "pipeline",
            "additionalProperties": True,
            "properties": {
                "segments": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"steps_config": {"type": "object", "title": "steps_config"}},
                    },
                }
            },
            "required": ["segments"],
        },
        "Env": {"type": "object", "additionalProperties": True},
        "Metrics": {
            "oneOf": [
                {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["datadog"]},
                        "host": {"type": "string"},
                        "port": {"type": "integer"},
                        "tags": {"type": "object", "additionalProperties": True},
                    },
                    "required": ["type", "host", "port"],
                },
                {
                    "type": "object",
                    "properties": {"type": {"type": "string", "enum": ["dummy"]}},
                    "required": ["type"],
                },
            ]
        },
    },
}


def validate_pipeline_config(config: dict[str, Any]) -> None:
    """
    Validates a pipeline configuration against the config schema.

    Args:
        config: The pipeline configuration dictionary to validate

    Raises:
        jsonschema.ValidationError: If the configuration doesn't conform to the schema
    """
    try:
        jsonschema.validate(config, CONFIG_SCHEMA)
    except Exception:
        raise

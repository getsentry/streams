from typing import Any

import pytest
from jsonschema import ValidationError

from sentry_streams_k8s.validation import validate_pipeline_config


def test_validate_pipeline_config_valid() -> None:
    """Test that a valid pipeline config passes validation."""
    valid_config = {
        "env": {},
        "pipeline": {
            "segments": [
                {
                    "steps_config": {
                        "myinput": {
                            "starts_segment": True,
                            "bootstrap_servers": ["127.0.0.1:9092"],
                        },
                        "mysink": {"bootstrap_servers": ["127.0.0.1:9092"]},
                    }
                }
            ]
        },
    }

    # Should not raise any exception
    validate_pipeline_config(valid_config)


def test_validate_pipeline_config_invalid() -> None:
    """Test that an invalid pipeline config raises ValidationError."""
    invalid_config: dict[str, Any] = {
        "env": {},
        "pipeline": {
            # Missing required 'segments' key
        },
    }

    with pytest.raises(ValidationError):
        validate_pipeline_config(invalid_config)


def test_validate_pipeline_config_invalid_segments() -> None:
    """Test that a config with invalid segments structure raises ValidationError."""
    invalid_config = {
        "env": {},
        "pipeline": {
            # segments should be an array, not a string
            "segments": "invalid"
        },
    }

    with pytest.raises(ValidationError):
        validate_pipeline_config(invalid_config)

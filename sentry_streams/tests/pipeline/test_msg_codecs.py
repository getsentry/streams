import json
from datetime import datetime
from importlib import resources
from typing import Any, Mapping, Sequence
from unittest.mock import MagicMock

import pytest

from sentry_streams.pipeline.message import Message, PyMessage
from sentry_streams.pipeline.msg_codecs import (
    _get_codec_from_msg,
    batch_msg_parser,
    msg_serializer,
)


def test_msg_serializer_default_isoformat() -> None:
    mock_msg = MagicMock()
    dt = datetime(2025, 6, 5, 14, 30, 0)
    mock_msg.payload = {"timestamp": dt}

    result_bytes = msg_serializer(mock_msg)
    result_str = result_bytes.decode("utf-8")
    assert json.loads(result_str) == {"timestamp": dt.isoformat()}


def test_msg_serializer_custom_dt_format() -> None:
    mock_msg = MagicMock()
    dt = datetime(2025, 6, 5, 14, 30, 0)
    mock_msg.payload = {"timestamp": dt}

    dt_format = "%Y/%m/%d %H:%M"
    result_bytes = msg_serializer(mock_msg, dt_format=dt_format)
    result_str = result_bytes.decode("utf-8")
    assert json.loads(result_str) == {"timestamp": dt.strftime(dt_format)}


def test_batch_msg_parser_nominal_case() -> None:
    with (
        resources.files("sentry_kafka_schemas.examples.ingest-metrics.1")
        .joinpath("base64-set.json")
        .open("r") as f
    ):
        data = json.load(f)
    expected = [data]

    payload: Sequence[bytes] = [json.dumps(data).encode("utf-8")]

    msg = PyMessage(
        payload=payload,
        schema="ingest-metrics",
        headers=[],
        timestamp=0.0,
    )

    result = batch_msg_parser(msg)
    assert result == expected


def test_msg_no_schema() -> None:
    msg: Message[Mapping[Any, Any]] = PyMessage(
        payload={},
        schema=None,
        headers=[],
        timestamp=0.0,
    )
    with pytest.raises(AssertionError):
        _get_codec_from_msg(msg)


def test_msg_no_found_schema() -> None:
    msg: Message[Mapping[Any, Any]] = PyMessage(
        payload={},
        schema="invalid-schema",
        headers=[],
        timestamp=0.0,
    )
    with pytest.raises(ValueError) as e:
        _get_codec_from_msg(msg)
    assert "Kafka topic invalid-schema has no associated schema" in str(e.value)

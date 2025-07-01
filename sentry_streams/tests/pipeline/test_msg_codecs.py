import json
from collections.abc import Iterable
from datetime import datetime
from importlib import resources
from io import BytesIO
from typing import Any, Mapping, Sequence, Union
from unittest.mock import MagicMock

import polars as pl
import pytest
from polars import Schema as PolarsSchema
from polars.testing import assert_frame_equal

from sentry_streams.pipeline.datatypes import (
    Field,
    Int64,
    List,
    String,
    Struct,
)
from sentry_streams.pipeline.message import Message, PyMessage
from sentry_streams.pipeline.msg_codecs import (
    _get_codec_from_msg,
    batch_msg_parser,
    msg_serializer,
    resolve_polars_schema,
    serialize_to_parquet,
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


def test_serialize_to_parquet_with_polars_schema() -> None:
    payload = [
        {
            "org_id": 420,
            "project_id": 420,
            "name": "s:sessions/user@none",
            "tags": {
                "sdk": "raven-node/2.6.3",
                "environment": "production",
                "release": "sentry-test@1.0.0",
            },
            "timestamp": 11111111111,
            "type": "s",
            "retention_days": 90,
            "value": [1617781333],
        },
        {
            "org_id": 420,
            "project_id": 420,
            "name": "s:sessions/user@none",
            "tags": {
                "sdk": "raven-node/2.6.3",
                "environment": "production",
                "release": "sentry-test@1.0.0",
            },
            "timestamp": 11111111111,
            "type": "s",
            "retention_days": 90,
            "value": [1617781333],
        },
    ]

    msg: Message[Iterable[Any]] = PyMessage(
        payload=payload,
        schema="example-schema",
        headers=[],
        timestamp=0.0,
    )

    schema_mapping: Mapping[str, Union[pl.DataType, pl.DataTypeClass]] = {
        "org_id": pl.Int64,
        "project_id": pl.Int64,
        "name": pl.String,
        "tags": pl.Struct(
            [
                pl.Field("sdk", pl.String),
                pl.Field("environment", pl.String),
                pl.Field("release", pl.String),
            ]
        ),
        "timestamp": pl.Int64,
        "type": pl.String,
        "retention_days": pl.Int64,
        "value": pl.List(pl.Int64),
    }
    polars_schema: PolarsSchema = pl.Schema(schema_mapping)

    result = serialize_to_parquet(msg, polars_schema, "snappy")

    assert isinstance(result, bytes)

    df = pl.read_parquet(BytesIO(result))
    assert df.shape == (2, 8)

    expected_df = pl.DataFrame(payload)
    assert_frame_equal(df, expected_df)


def test_resolve_polars_schema() -> None:
    streaming_schema = {
        "org_id": Int64(),
        "project_id": Int64(),
        "name": String(),
        "tags": Struct(
            [
                Field("sdk", String()),
                Field("environment", String()),
                Field("release", String()),
            ]
        ),
        "timestamp": Int64(),
        "type": String(),
        "retention_days": Int64(),
        "value": List(Int64()),
    }
    expected = {
        "org_id": pl.Int64,
        "project_id": pl.Int64,
        "name": pl.String,
        "tags": pl.Struct(
            [
                pl.Field("sdk", pl.String),
                pl.Field("environment", pl.String),
                pl.Field("release", pl.String),
            ]
        ),
        "timestamp": pl.Int64,
        "type": pl.String,
        "retention_days": pl.Int64,
        "value": pl.List(pl.Int64),
    }
    transformed_schema = resolve_polars_schema(streaming_schema)
    assert expected == transformed_schema

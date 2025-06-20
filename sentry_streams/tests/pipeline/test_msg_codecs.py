import json
from datetime import datetime
from importlib import resources
from typing import Any, Mapping, Sequence, Tuple
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sentry_streams.pipeline.message import Message, PyMessage
from sentry_streams.pipeline.msg_codecs import (
    _get_codec_from_msg,
    batch_msg_parser,
    msg_serializer,
    parquet_serializer,
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


def test_parquet_parser_nominal_case() -> None:
    schema_fields: Sequence[Tuple[str, Any]] = [
        ("deleted", pa.int64()),
        ("project_id", pa.int64()),
        ("event_id", pa.string()),
        ("organization_id", pa.int64()),
        ("retention_days", pa.int64()),
        ("group_id", pa.int64()),
        ("timestamp", pa.date64()),
        ("timestamp_ts", pa.int64()),
        ("platform", pa.string()),
        ("version", pa.string()),
        ("location", pa.string()),
        (
            "modules",
            pa.list_(pa.struct([("name", pa.string()), ("version", pa.string())])),
        ),
        ("user_name", pa.string()),
        ("user_id", pa.string()),
        ("user_email", pa.string()),
        ("http_method", pa.string()),
        ("http_referer", pa.string()),
        ("http_user_agent", pa.string()),
        ("message", pa.string()),
        ("primary_hash", pa.string()),
        ("culprit", pa.string()),
        ("type", pa.string()),
        ("title", pa.string()),
        ("event_size", pa.int64()),
        ("bytes_ingested_event", pa.int64()),
        ("bytes_stored_event_attachment", pa.int64()),
        ("sdk_name", pa.string()),
        ("sdk_version", pa.string()),
        ("sdk_integrations", pa.list_(pa.struct([("value", pa.string())]))),
        ("sdk_features", pa.list_(pa.struct([("value", pa.string())]))),
        (
            "sdk_packages",
            pa.list_(pa.struct([("name", pa.string()), ("version", pa.string())])),
        ),
        ("received", pa.timestamp("s")),
        ("received_ts", pa.int64()),
        ("os", pa.string()),
        ("os_name", pa.string()),
        ("client_os", pa.string()),
        ("client_os_name", pa.string()),
        ("release", pa.string()),
        ("environment", pa.string()),
        ("user", pa.string()),
        ("dist", pa.string()),
        ("transaction_name", pa.string()),
        ("trace_id", pa.string()),
        ("span_id", pa.string()),
        (
            "contexts",
            pa.list_(pa.struct([("key", pa.string()), ("value", pa.string())])),
        ),
        (
            "tags",
            pa.list_(pa.struct([("key", pa.string()), ("value", pa.string())])),
        ),
        ("has_stacktrace", pa.bool_()),
        ("vercel", pa.string()),
        (
            "exception_stacks",
            pa.list_(
                pa.struct(
                    [
                        ("type", pa.string()),
                        ("value", pa.string()),
                        ("mechanism_type", pa.string()),
                        ("mechanism_handled", pa.bool_()),
                    ]
                )
            ),
        ),
        (
            "exception_frames",
            pa.list_(
                pa.struct(
                    [
                        ("abs_path", pa.string()),
                        ("filename", pa.string()),
                        ("package", pa.string()),
                        ("module", pa.string()),
                        ("function", pa.string()),
                        ("in_app", pa.bool_()),
                        ("colno", pa.int64()),
                        ("lineno", pa.int64()),
                        ("stack_level", pa.int64()),
                    ]
                )
            ),
        ),
        ("breadcrumbs_count", pa.int64()),
        (
            "breadcrumb_counts",
            pa.list_(pa.struct([("type", pa.string()), ("count", pa.int64())])),
        ),
        ("breadcrumbs_count_console", pa.int64()),
        ("engine_mode", pa.string()),
        ("engine_version", pa.string()),
        ("is_marketplace_version", pa.string()),
        ("target_type", pa.string()),
        (
            "breadcrumb_category_counts",
            pa.list_(pa.struct([("category", pa.string()), ("count", pa.int64())])),
        ),
        ("replay_id", pa.string()),
        (
            "breadcrumb_level_counts",
            pa.list_(pa.struct([("level", pa.string()), ("count", pa.int64())])),
        ),
        ("breadcrumbs_raw_size", pa.int64()),
    ]
    data = {
        "deleted": 0,
        "project_id": 6036610,
        "event_id": "9cdc4c32-dff1-4fbb-b012-b0aa9e908126",
        "organization_id": 123,
        "retention_days": 90,
        "group_id": 124,
        "timestamp": datetime(2023, 2, 27, 15, 40, 12, 223000),
        "timestamp_ts": 223,
        "platform": "javascript",
        "received": 1677530412,
        "received_ts": 1677512412,
        "version": "7",
        "location": None,
        "event_size": 736,
        "breadcrumbs_count": 0,
        "breadcrumbs_raw_size": 0,
        "breadcrumb_counts": [],
        "breadcrumb_category_counts": [],
        "breadcrumb_level_counts": [],
        "breadcrumbs_count_console": 0,
        "user_name": None,
        "user_id": None,
        "user_email": None,
        "ip_address": "None",
        "http_method": None,
        "http_referer": None,
        "http_user_agent": None,
        "message": "hello world",
        "primary_hash": "061cf02b-2637-4d10-8694-d6643a7a2f4e",
        "culprit": "",
        "type": "error",
        "title": "",
        "bytes_ingested_event": 0,
        "bytes_stored_event_attachment": 0,
        "sdk_name": None,
        "sdk_version": None,
        "sdk_integrations": [],
        "sdk_features": [],
        "packages.version": [],
        "release": None,
        "environment": None,
        "user": "",
        "dist": "",
        "transaction_name": "",
        "os": None,
        "os_name": None,
        "client_os": None,
        "client_os_name": None,
        "has_stacktrace": False,
        "modules": [],
        "contexts": [],
        "sdk_packages": [],
        "tags": [],
        "exception_stacks": [],
        "exception_frames": [],
        "trace_id": None,
        "span_id": None,
        "vercel": None,
        "engine_mode": None,
        "engine_version": None,
        "is_marketplace_version": None,
        "target_type": None,
        "replay_id": None,
    }
    msg = PyMessage(
        payload=[data],
        schema="test-schema",
        headers=[],
        timestamp=0.0,
    )
    payload = msg.payload
    df = pd.DataFrame([i for i in payload if i is not None])
    pa_schema_fields = pa.schema(schema_fields).remove_metadata()
    table = pa.Table.from_pandas(
        df, pa_schema_fields, preserve_index=False
    ).replace_schema_metadata()

    writer = pa.BufferOutputStream()
    pq.write_table(table, writer, use_compliant_nested_type=True)
    expected = bytes(writer.getvalue())
    # code snippet ends

    result = parquet_serializer(msg=msg, schema_fields=schema_fields)

    # for testing purposes, only compare the contents of the parquet tables
    expected_table = pq.read_table(pa.BufferReader(expected))
    result_table = pq.read_table(pa.BufferReader(result))
    assert expected_table == result_table


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

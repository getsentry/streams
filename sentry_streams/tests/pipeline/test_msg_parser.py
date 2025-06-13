import json
from datetime import datetime
from typing import Any, Mapping, Sequence, cast
from unittest.mock import MagicMock

from pytest import MonkeyPatch

from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.msg_parser import batch_msg_parser, msg_serializer


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


class FakeMessage:
    def __init__(self, payload: Sequence[bytes], schema: Any) -> None:
        self.payload = payload
        self.schema = schema


def test_batch_msg_parser_nominal_case(monkeypatch: MonkeyPatch) -> None:
    input_payload = [b'{"a": 1}', b'{"b": 2}']

    expected_output = [{"a": 1}, {"b": 2}]

    class FakeCodec:
        def decode(self, payload: bytes) -> Mapping[str, int]:
            if payload == b'{"a": 1}':
                return {"a": 1}
            else:
                return {"b": 2}

    monkeypatch.setattr(
        "sentry_streams.pipeline.msg_parser._get_codec_from_msg", lambda _: FakeCodec()
    )

    msg = FakeMessage(payload=input_payload, schema="test-schema")

    result = batch_msg_parser(cast(Message[Sequence[bytes]], msg))
    assert result == expected_output

from typing import Sequence, Tuple

from arroyo.types import Message as ArroyoMessage

from sentry_streams.adapters.arroyo.rust_step import Committable
from sentry_streams.pipeline.message import Message, PipelineMessage, PyMessage


def str_transformer(msg: ArroyoMessage[Message[str]]) -> Message[str]:
    return PyMessage(
        f"transformed {msg.payload.payload}",
        msg.payload.headers,
        msg.payload.timestamp,
        msg.payload.schema,
    )


def assert_equal_batches(
    batch1: Sequence[Tuple[PipelineMessage, Committable]],
    batch2: Sequence[Tuple[PipelineMessage, Committable]],
) -> None:
    assert len(batch1) == len(batch2)
    for i, msg1 in enumerate(batch1):
        msg2 = batch2[i]
        assert (
            msg1[0].payload == msg2[0].payload
        ), f"Payload mismatch at index {i}: {msg1[0].payload!r} vs {msg2[0].payload!r}"
        assert msg1[1] == msg2[1], f"Committable mismatch at index {i}"

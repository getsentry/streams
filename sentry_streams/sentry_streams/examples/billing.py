from typing import Optional, Self

from sentry_kafka_schemas.schema_types.outcomes_v1 import Outcome

from sentry_streams.pipeline import (
    Parser,
    Reducer,
    StreamSink,
    streaming_source,
)
from sentry_streams.pipeline.function_template import KVAccumulator
from sentry_streams.pipeline.message import Message
from sentry_streams.pipeline.window import TumblingWindow


class OutcomesBuffer(KVAccumulator[Message[Outcome]]):
    """
    An accumulator which adds outcomes data to a PendingBuffer.
    Upon the closing of a window, the Buffer is flushed to a
    sample backend (the OutcomesBackend). As of now this backend
    is not a mocked DB, it is a simple hash map.
    """

    def __init__(self, outcomes_dict: Optional[dict[str, int]] = None) -> None:
        if outcomes_dict:
            self.map: dict[str, int] = outcomes_dict

        else:
            self.map = {}

    def add(self, message: Message[Outcome]) -> Self:
        value = message.payload
        outcome_type = ""

        if "reason" in value:
            outcome_type += value["reason"] or ""

        if "category" in value:
            outcome_type += str(value["category"])

        if outcome_type in self.map:
            self.map[outcome_type] += 1

        else:
            self.map[outcome_type] = 1

        return self

    def get_value(self) -> dict[str, int]:
        return self.map

    def merge(self, other: Self) -> Self:

        first = self.map
        second = other.map

        for outcome_key in second:
            if outcome_key in first:
                first[outcome_key] += second[outcome_key]

            else:
                first[outcome_key] = second[outcome_key]

        self.map = first

        return self


pipeline = (
    streaming_source(
        name="myinput",
        stream_name="events",
    )
    .apply("myparser", Parser(msg_type=Outcome))
    .apply(
        "myreduce",
        Reducer(aggregate_func=lambda: OutcomesBuffer(), window=TumblingWindow(window_size=3)),
    )
    .sink("mysink", StreamSink(stream_name="transformed-events"))
)

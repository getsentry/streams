from datetime import datetime
from typing import Any

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.types import (
    BrokerValue,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue

TEST_PARTITION = Partition(Topic("test_topic"), 0)


def make_msg(payload: Any, route: Route, offset: int) -> Message[Any]:
    """
    Makes a message containing a BrokerValue based on the offset passed.
    """
    if isinstance(payload, FilteredPayload):
        return Message(
            BrokerValue(
                payload=payload,
                partition=TEST_PARTITION,
                offset=offset,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        )
    else:
        return Message(
            BrokerValue(
                payload=RoutedValue(route=route, payload=payload),
                partition=TEST_PARTITION,
                offset=offset,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        )


def make_value_msg(
    payload: Any, route: Route, offset: int, include_timestamp: bool = True
) -> Message[Any]:
    """
    Makes a message containing a Value based on the offset passed.
    Useful if a step you're testing always transforms a Message payload into a Value,
    or if you need an emtpy comittable/timestamp for whatever reason (BrokerValue doesn't support that).
    """
    if isinstance(payload, FilteredPayload):
        return Message(
            Value(
                payload=payload,
                committable={Partition(Topic("test_topic"), 0): offset},
                timestamp=datetime(2025, 1, 1, 12, 0) if include_timestamp else None,
            )
        )
    else:
        return Message(
            Value(
                payload=RoutedValue(route=route, payload=payload),
                committable={Partition(Topic("test_topic"), 0): offset},
                timestamp=datetime(2025, 1, 1, 12, 0) if include_timestamp else None,
            )
        )


def make_kafka_msg(
    payload: str,
    topic: str,
    offset: int,
) -> Message[Any]:
    return Message(
        BrokerValue(
            payload=KafkaPayload(None, payload.encode("utf-8"), []),
            partition=Partition(Topic(topic), 0),
            offset=offset,
            timestamp=datetime.now(),
        )
    )

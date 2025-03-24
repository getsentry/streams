from datetime import datetime
from typing import Any, Mapping
from unittest import mock

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, FilteredPayload, Message, Partition, Topic

from sentry_streams.adapters.arroyo.custom_strategies import Forwarder
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue


# TODO: move make_msg into some kind of test utils folder as it's shared with other tests
def make_msg(payload: Any, route: Route, offset: int) -> Message[Any]:
    if isinstance(payload, FilteredPayload):
        return Message(
            BrokerValue(
                payload=payload,
                partition=Partition(Topic("test_topic"), 0),
                offset=offset,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        )
    else:
        return Message(
            BrokerValue(
                payload=RoutedValue(route=route, payload=payload),
                partition=Partition(Topic("test_topic"), 0),
                offset=offset,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        )


@pytest.fixture
def forwarder() -> Forwarder:
    return Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=mock.Mock(spec=Produce),
        next_step=mock.Mock(spec=ProcessingStrategy),
    )


def test_forwarder_submit(forwarder: Forwarder) -> None:
    messages: Mapping[str, Message[RoutedValue]] = {
        "correct": make_msg(
            payload="test-payload",
            route=Route(source="source", waypoints=["correct_branch"]),
            offset=0,
        ),
        "wrong": make_msg(
            payload="test-payload",
            route=Route(source="source", waypoints=["wrong_branch"]),
            offset=1,
        ),
    }
    expected_messages = {
        "correct": Message(
            value=BrokerValue(
                payload=KafkaPayload(None, "test-payload".encode("utf-8"), []),
                partition=Partition(Topic("test_topic"), 0),
                offset=0,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        ),
        "wrong": Message(
            value=BrokerValue(
                payload=RoutedValue(
                    route=Route(source="source", waypoints=["wrong_branch"]), payload="test-payload"
                ),
                partition=Partition(Topic("test_topic"), 0),
                offset=1,
                timestamp=datetime(2025, 1, 1, 12, 0),
            )
        ),
    }

    forwarder.submit(messages["correct"])
    forwarder._Forwarder__produce_step.submit.assert_called_once_with(expected_messages["correct"])  # type: ignore
    forwarder.submit(messages["wrong"])
    forwarder._Forwarder__next_step.submit.assert_called_once_with(expected_messages["wrong"]) == 1  # type: ignore

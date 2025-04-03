from datetime import datetime
from typing import Mapping
from unittest import mock

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, Message, Partition, Topic

from sentry_streams.adapters.arroyo.forwarder import Forwarder
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from tests.adapters.arroyo.message_helpers import make_msg


def test_submit() -> None:
    produce_step = mock.Mock(spec=Produce)
    next_step = mock.Mock(spec=ProcessingStrategy)
    forwarder = Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=produce_step,
        next_step=next_step,
    )

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
    produce_step.submit.assert_called_once_with(expected_messages["correct"])
    forwarder.submit(messages["wrong"])
    next_step.submit.assert_called_once_with(expected_messages["wrong"])


def test_poll() -> None:
    produce_step = mock.Mock(spec=Produce)
    next_step = mock.Mock(spec=ProcessingStrategy)
    forwarder = Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=produce_step,
        next_step=next_step,
    )
    forwarder.poll()
    produce_step.poll.assert_called_once()
    next_step.poll.assert_called_once()


def test_join() -> None:
    produce_step = mock.Mock(spec=Produce)
    next_step = mock.Mock(spec=ProcessingStrategy)
    forwarder = Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=produce_step,
        next_step=next_step,
    )
    forwarder.join()
    produce_step.join.assert_called_once()
    next_step.join.assert_called_once()


def test_close() -> None:
    produce_step = mock.Mock(spec=Produce)
    next_step = mock.Mock(spec=ProcessingStrategy)
    forwarder = Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=produce_step,
        next_step=next_step,
    )
    forwarder.close()
    produce_step.close.assert_called_once()
    next_step.close.assert_called_once()


def test_terminate() -> None:
    produce_step = mock.Mock(spec=Produce)
    next_step = mock.Mock(spec=ProcessingStrategy)
    forwarder = Forwarder(
        route=Route(source="source", waypoints=["correct_branch"]),
        produce_step=produce_step,
        next_step=next_step,
    )
    forwarder.terminate()
    produce_step.terminate.assert_called_once()
    next_step.terminate.assert_called_once()

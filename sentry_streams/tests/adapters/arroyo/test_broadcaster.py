from datetime import datetime
from typing import Any
from unittest import mock
from unittest.mock import call

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Partition, Topic, Value

from sentry_streams.adapters.arroyo.broadcaster import Broadcaster
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue


# TODO: move make_value_msg into some kind of test utils folder as it's shared with other tests
def make_value_msg(payload: Any, route: Route, offset: int) -> Message[Any]:
    """
    Makes a message containing a Value based on the offset passed.
    Useful if a step you're testing always transforms a Message payload into a Value,
    or if you need an emtpy comittable/timestamp for whatever reason (BrokerValue doesn't support that).
    """
    timestamp = datetime(2025, 1, 1, 12, 0)
    if isinstance(payload, FilteredPayload):
        return Message(
            Value(
                payload=payload,
                committable={Partition(Topic("test_topic"), 0): offset},
                timestamp=timestamp,
            )
        )
    else:
        return Message(
            Value(
                payload=RoutedValue(route=route, payload=payload),
                committable={Partition(Topic("test_topic"), 0): offset},
                timestamp=timestamp,
            )
        )


def test_submit_routedvalue() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    message = make_value_msg(
        payload="test-payload", route=Route(source="source", waypoints=[]), offset=0
    )

    expected_calls = [
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_1"]),
                offset=0,
            )
        ),
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_2"]),
                offset=0,
            )
        ),
    ]

    broadcaster.submit(message)
    next_step.assert_has_calls(expected_calls)


def test_submit_filteredpayload() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    message = make_value_msg(
        payload=FilteredPayload(), route=Route(source="source", waypoints=[]), offset=0
    )

    broadcaster.submit(message)
    next_step.submit.assert_called_once_with(message)


def test_submit_wrong_route() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    message = make_value_msg(
        payload="wrong_route", route=Route(source="source", waypoints=["wrong"]), offset=0
    )

    broadcaster.submit(message)
    next_step.submit.assert_called_once_with(message)


def test_message_rejected() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    # raise MessageRejected on submit
    next_step.submit.side_effect = MessageRejected()

    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    message = make_value_msg(
        payload="test-payload", route=Route(source="source", waypoints=[]), offset=0
    )

    expected_calls = [
        # MessageRejected calls
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_1"]),
                offset=0,
            )
        ),
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_2"]),
                offset=0,
            )
        ),
        # __flush_pending() calls
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_1"]),
                offset=0,
            )
        ),
        call.submit(
            make_value_msg(
                payload="test-payload",
                route=Route(source="source", waypoints=["branch_2"]),
                offset=0,
            )
        ),
    ]

    broadcaster.submit(message)

    # stop raising MessageRejected
    next_step.submit.side_effect = None
    broadcaster.poll()
    assert next_step.submit.call_args_list == expected_calls


def test_poll() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )
    broadcaster.poll()
    next_step.poll.assert_called_once()


def test_join() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )
    broadcaster.join()
    next_step.join.assert_called_once()


def test_close() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )
    broadcaster.close()
    next_step.close.assert_called_once()


def test_terminate() -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )
    broadcaster.terminate()
    next_step.terminate.assert_called_once()

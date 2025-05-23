import time
from unittest import mock
from unittest.mock import call

import pytest
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.adapters.arroyo.broadcaster import Broadcaster
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.pipeline.message import PyMessage
from tests.adapters.arroyo.message_helpers import make_value_msg


def test_submit_routedvalue(metric: IngestMetric) -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    msg = PyMessage(metric, [], time.time(), None)
    message = make_value_msg(payload=msg, route=Route(source="source", waypoints=[]), offset=0)

    expected_calls = [
        call.submit(
            make_value_msg(
                payload=msg,
                route=Route(source="source", waypoints=["branch_1"]),
                offset=0,
            )
        ),
        call.submit(
            make_value_msg(
                payload=msg,
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


def test_message_rejected(metric: IngestMetric) -> None:
    next_step = mock.Mock(spec=ProcessingStrategy)
    # raise MessageRejected on submit
    next_step.submit.side_effect = MessageRejected()

    broadcaster = Broadcaster(
        route=Route(source="source", waypoints=[]),
        downstream_branches=["branch_1", "branch_2"],
        next_step=next_step,
    )

    msg = PyMessage(metric, [], time.time(), None)
    message = make_value_msg(payload=msg, route=Route(source="source", waypoints=[]), offset=0)

    message_rejected_expected_call = call(
        make_value_msg(
            payload=msg,
            route=Route(source="source", waypoints=["branch_1"]),
            offset=0,
        )
    )

    with pytest.raises(MessageRejected):
        broadcaster.submit(message)
    assert next_step.submit.call_args_list == [message_rejected_expected_call]

    # stop raising MessageRejected
    next_step.submit.side_effect = None
    broadcaster.poll()
    assert next_step.submit.call_args_list == [message_rejected_expected_call] * 2


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

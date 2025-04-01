from datetime import datetime
from typing import Any, Mapping
from unittest import mock
from unittest.mock import call

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import (
    BrokerValue,
    Commit,
    FilteredPayload,
    Message,
    Partition,
    Topic,
    Value,
)

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.steps import (
    BroadcastStep,
    FilterStep,
    MapStep,
    RouterStep,
    StreamSinkStep,
)
from sentry_streams.pipeline.pipeline import (
    Branch,
    Broadcast,
    Filter,
    Map,
    Pipeline,
    Router,
)

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
    payload: Any, route: Route, committable: Mapping[Partition, int], add_timestamp: bool = True
) -> Message[Any]:
    """
    Makes a message containing a Value based on the offset passed.
    Useful if a step you're testing always transforms a Message payload into a Value,
    or if you need an emtpy comittable/timestamp for whatever reason (BrokerValue doesn't support that).
    """
    timestamp = datetime(2025, 1, 1, 12, 0) if add_timestamp else None
    if isinstance(payload, FilteredPayload):
        return Message(
            Value(
                payload=payload,
                committable=committable,
                timestamp=timestamp,
            )
        )
    else:
        return Message(
            Value(
                payload=RoutedValue(route=route, payload=payload),
                committable=committable,
                timestamp=timestamp,
            )
        )


def test_map_step() -> None:
    """
    Send messages for different routes through the Arroyo RunTask strategy
    generate by the pipeline Map step.
    """

    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])
    pipeline = Pipeline()
    pipeline_map = Map(name="mymap", ctx=pipeline, inputs=[], function=lambda x: x + "_mapped")
    arroyo_map = MapStep(mapped_route, pipeline_map)

    next_strategy = mock.Mock(spec=ProcessingStrategy)

    strategy = arroyo_map.build(next_strategy, commit=mock.Mock(spec=Commit))

    messages = [
        make_msg("test_val", mapped_route, 0),
        make_msg("test_val", other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg("test_val_mapped", mapped_route, 0),
        ),
        call.poll(),
        call.submit(
            make_msg("test_val", other_route, 1),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_filter_step() -> None:
    """
    Send messages for different routes through the Arroyo RunTask strategy
    generate by the pipeline Filter step.
    """
    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])
    pipeline = Pipeline()

    pipeline_filter = Filter(
        name="myfilter", ctx=pipeline, inputs=[], function=lambda x: x == "test_val"
    )
    arroyo_filter = FilterStep(mapped_route, pipeline_filter)

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    strategy = arroyo_filter.build(next_strategy, commit=mock.Mock(spec=Commit))

    messages = [
        make_msg("test_val", mapped_route, 0),
        make_msg("not_test_val", mapped_route, 1),
        make_msg("test_val", other_route, 2),
        make_msg(FilteredPayload(), mapped_route, 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg("test_val", mapped_route, 0),
        ),
        call.poll(),
        call.submit(make_msg(FilteredPayload(), mapped_route, 1)),
        call.poll(),
        call.submit(
            make_msg("test_val", other_route, 2),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_router() -> None:
    """
    Verifies the Router step properly updates the waypoints of a RoutedValue message.
    """
    mapped_route = Route(source="source1", waypoints=["map_branch"])
    other_route = Route(source="source1", waypoints=["other_branch"])
    pipeline = Pipeline()

    def dummy_routing_func(message: str) -> str:
        return "map" if message == "test_val" else "other"

    pipeline_router = Router(
        name="myrouter",
        ctx=pipeline,
        inputs=[],
        routing_function=dummy_routing_func,
        routing_table={
            "map": Branch(name="map_branch", ctx=pipeline),
            "other": Branch(name="other_branch", ctx=pipeline),
        },
    )
    arroyo_router = RouterStep(Route(source="source1", waypoints=[]), pipeline_router)

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    strategy = arroyo_router.build(next_strategy, commit=mock.Mock(spec=Commit))

    messages = [
        make_msg("test_val", Route(source="source1", waypoints=[]), 0),
        make_msg("not_test_val", Route(source="source1", waypoints=[]), 1),
        make_msg("test_val", Route(source="source1", waypoints=[]), 2),
        make_msg(FilteredPayload(), Route(source="source1", waypoints=[]), 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg("test_val", mapped_route, 0),
        ),
        call.poll(),
        call.submit(make_msg("not_test_val", other_route, 1)),
        call.poll(),
        call.submit(
            make_msg("test_val", mapped_route, 2),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_broadcast() -> None:
    """
    Verifies the Broadcast step properly updates the waypoints the messages it produces.
    """
    mapped_route = Route(source="source1", waypoints=["map_branch"])
    other_route = Route(source="source1", waypoints=["other_branch"])
    pipeline = Pipeline()

    pipeline_router = Broadcast(
        name="mybroadcast",
        ctx=pipeline,
        inputs=[],
        routes=[
            Branch(name="map_branch", ctx=pipeline),
            Branch(name="other_branch", ctx=pipeline),
        ],
    )
    arroyo_broadcast = BroadcastStep(Route(source="source1", waypoints=[]), pipeline_router)

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    strategy = arroyo_broadcast.build(next_strategy, commit=mock.Mock(spec=Commit))

    messages = [
        make_value_msg(
            "test_val",
            Route(source="source1", waypoints=[]),
            {TEST_PARTITION: 0},
        ),
        make_value_msg(
            "not_test_val",
            Route(source="source1", waypoints=[]),
            {TEST_PARTITION: 1},
        ),
        make_value_msg(
            FilteredPayload(),
            Route(source="source1", waypoints=[]),
            {TEST_PARTITION: 2},
        ),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(make_value_msg("test_val", mapped_route, {}, add_timestamp=False)),
        call.submit(
            make_value_msg("test_val", other_route, {TEST_PARTITION: 0}, add_timestamp=False)
        ),
        call.poll(),
        call.submit(make_value_msg("not_test_val", mapped_route, {}, add_timestamp=False)),
        call.submit(
            make_value_msg("not_test_val", other_route, {TEST_PARTITION: 1}, add_timestamp=False)
        ),
        call.poll(),
        call.submit(
            make_value_msg(
                FilteredPayload(),
                Route(source="source1", waypoints=[]),
                {TEST_PARTITION: 2},
            )
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_sink() -> None:
    """
    Sends routed messages through a Sink and verifies that only the
    messages for the specified sink are sent to the producer.
    """
    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    producer = mock.Mock(spec=Producer)
    strategy = StreamSinkStep(mapped_route, producer, "test_topic").build(
        next_strategy, commit=mock.Mock(spec=Commit)
    )

    messages = [
        make_msg("test_val", mapped_route, 0),
        make_msg("test_val", other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 2),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    producer.produce.assert_called_with(
        Topic("test_topic"), KafkaPayload(None, "test_val".encode("utf-8"), [])
    )

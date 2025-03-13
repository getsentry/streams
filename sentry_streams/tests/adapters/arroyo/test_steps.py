from datetime import datetime
from typing import Any
from unittest import mock
from unittest.mock import call

from arroyo.backends.abstract import Producer
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import BrokerValue, FilteredPayload, Message, Partition, Topic

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.steps import FilterStep, KafkaSinkStep, MapStep
from sentry_streams.pipeline.pipeline import Filter, Map, Pipeline


def make_msg(payload: Any, route: Route, offset: int) -> Message[Any]:
    if isinstance(payload, FilteredPayload):
        return Message(
            BrokerValue(
                payload=payload,
                partition=Partition(Topic("test_topic"), 0),
                offset=offset,
                timestamp=datetime.now(),
            )
        )
    else:
        return Message(
            BrokerValue(
                payload=RoutedValue(route=route, payload=payload),
                partition=Partition(Topic("test_topic"), 0),
                offset=offset,
                timestamp=datetime.now(),
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

    strategy = arroyo_map.build(next_strategy)

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
    strategy = arroyo_filter.build(next_strategy)

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


def test_sink() -> None:
    """
    Sends routed messages through a Sink and verifies that only the
    messages for the specified sink are sent to the producer.
    """
    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    producer = mock.Mock(spec=Producer)
    strategy = KafkaSinkStep(mapped_route, producer, "test_topic").build(next_strategy)

    messages = [
        make_msg("test_val", mapped_route, 0),
        make_msg("test_val", other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 2),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.produce(Topic("test_topic"), "test_val"),
    ]

    producer.assert_has_calls(expected_calls)

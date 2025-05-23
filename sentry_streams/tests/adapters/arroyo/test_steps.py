import json
import time
from datetime import datetime, timedelta
from typing import Callable
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
from sentry_kafka_schemas.schema_types.ingest_metrics_v1 import IngestMetric

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.steps import (
    BroadcastStep,
    FilterStep,
    MapStep,
    ReduceStep,
    RouterStep,
    StreamSinkStep,
)
from sentry_streams.examples.transformer import TransformerBatch
from sentry_streams.pipeline.message import PyMessage as StreamsMessage
from sentry_streams.pipeline.pipeline import (
    Aggregate,
    Branch,
    Broadcast,
    Filter,
    Map,
    Pipeline,
    Router,
)
from sentry_streams.pipeline.window import SlidingWindow
from tests.adapters.arroyo.message_helpers import make_msg, make_value_msg


def test_map_step(metric: IngestMetric) -> None:
    """
    Send messages for different routes through the Arroyo RunTask strategy
    generate by the pipeline Map step.
    """

    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])
    pipeline = Pipeline()
    pipeline_map = Map(name="mymap", ctx=pipeline, inputs=[], function=lambda msg: msg.payload)
    arroyo_map = MapStep(mapped_route, pipeline_map)

    next_strategy = mock.Mock(spec=ProcessingStrategy)

    strategy = arroyo_map.build(next_strategy, commit=mock.Mock(spec=Commit))

    test_msg = StreamsMessage(metric, [], time.time(), None)

    messages = [
        make_msg(test_msg, mapped_route, 0),
        make_msg(test_msg, other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg(test_msg, mapped_route, 0),
        ),
        call.poll(),
        call.submit(
            make_msg(test_msg, other_route, 1),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_filter_step(metric: IngestMetric, transformed_metric: IngestMetric) -> None:
    """
    Send messages for different routes through the Arroyo RunTask strategy
    generate by the pipeline Filter step.
    """
    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])
    pipeline = Pipeline()

    pipeline_filter = Filter(
        name="myfilter",
        ctx=pipeline,
        inputs=[],
        function=lambda msg: msg.payload["name"] != "new_metric",
    )
    arroyo_filter = FilterStep(mapped_route, pipeline_filter)

    next_strategy = mock.Mock(spec=ProcessingStrategy)
    strategy = arroyo_filter.build(next_strategy, commit=mock.Mock(spec=Commit))

    msg = StreamsMessage(metric, [], time.time(), None)
    filtered_msg = StreamsMessage(transformed_metric, [], time.time(), None)
    messages = [
        make_msg(msg, mapped_route, 0),
        make_msg(filtered_msg, mapped_route, 1),
        make_msg(msg, other_route, 2),
        make_msg(FilteredPayload(), mapped_route, 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg(msg, mapped_route, 0),
        ),
        call.poll(),
        call.submit(make_msg(FilteredPayload(), mapped_route, 1)),
        call.poll(),
        call.submit(
            make_msg(msg, other_route, 2),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_router(metric: IngestMetric, transformed_metric: IngestMetric) -> None:
    """
    Verifies the Router step properly updates the waypoints of a RoutedValue message.
    """
    mapped_route = Route(source="source1", waypoints=["map_branch"])
    other_route = Route(source="source1", waypoints=["other_branch"])
    pipeline = Pipeline()

    def dummy_routing_func(message: StreamsMessage[IngestMetric]) -> str:
        return "map" if message.payload["name"] != "new_metric" else "other"

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

    msg = StreamsMessage(metric, [], time.time(), None)
    filtered_msg = StreamsMessage(transformed_metric, [], time.time(), None)

    messages = [
        make_msg(msg, Route(source="source1", waypoints=[]), 0),
        make_msg(filtered_msg, Route(source="source1", waypoints=[]), 1),
        make_msg(msg, Route(source="source1", waypoints=[]), 2),
        make_msg(FilteredPayload(), Route(source="source1", waypoints=[]), 3),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(
            make_msg(msg, mapped_route, 0),
        ),
        call.poll(),
        call.submit(make_msg(filtered_msg, other_route, 1)),
        call.poll(),
        call.submit(
            make_msg(msg, mapped_route, 2),
        ),
        call.poll(),
        call.submit(
            make_msg(FilteredPayload(), mapped_route, 3),
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_broadcast(metric: IngestMetric, transformed_metric: IngestMetric) -> None:
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

    msg = StreamsMessage(metric, [], time.time(), None)
    filtered_msg = StreamsMessage(transformed_metric, [], time.time(), None)

    messages = [
        make_value_msg(
            msg,
            Route(source="source1", waypoints=[]),
            0,
        ),
        make_value_msg(
            filtered_msg,
            Route(source="source1", waypoints=[]),
            1,
        ),
        make_value_msg(
            FilteredPayload(),
            Route(source="source1", waypoints=[]),
            2,
        ),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    expected_calls = [
        call.submit(make_value_msg(msg, mapped_route, 0)),
        call.submit(make_value_msg(msg, other_route, 0)),
        call.poll(),
        call.submit(make_value_msg(filtered_msg, mapped_route, 1)),
        call.submit(make_value_msg(filtered_msg, other_route, 1)),
        call.poll(),
        call.submit(
            make_value_msg(
                FilteredPayload(),
                Route(source="source1", waypoints=[]),
                2,
            )
        ),
        call.poll(),
    ]

    next_strategy.assert_has_calls(expected_calls)


def test_sink(metric: IngestMetric) -> None:
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

    # assume this is a serialized msg being produced to Kafka
    msg = StreamsMessage(json.dumps(metric).encode("utf-8"), [], time.time(), None)

    messages = [
        make_msg(msg, mapped_route, 0),
        make_msg(msg, other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 2),
    ]

    for message in messages:
        strategy.submit(message)
        strategy.poll()

    producer.produce.assert_called_with(Topic("test_topic"), KafkaPayload(None, msg.payload, []))


def test_reduce_step(transformer: Callable[[], TransformerBatch], metric: IngestMetric) -> None:
    """
    Send messages for different routes through the Arroyo RunTask strategy
    generate by the pipeline Reduce step.
    """

    mapped_route = Route(source="source1", waypoints=["branch1"])
    other_route = Route(source="source1", waypoints=["branch2"])
    pipeline = Pipeline()

    reduce_window = SlidingWindow(
        window_size=timedelta(seconds=6), window_slide=timedelta(seconds=2)
    )

    pipeline_reduce = Aggregate(
        name="myreduce",
        ctx=pipeline,
        inputs=[],
        window=reduce_window,
        aggregate_func=transformer,
    )
    arroyo_reduce = ReduceStep(mapped_route, pipeline_reduce)
    next_strategy = mock.Mock(spec=ProcessingStrategy)

    start_time = datetime(2025, 1, 1, 12, 0).timestamp()

    with mock.patch("time.time", return_value=start_time):
        strategy = arroyo_reduce.build(next_strategy, commit=mock.Mock(spec=Commit))

    msg = StreamsMessage(metric, [], datetime(2025, 1, 1, 12, 0).timestamp(), None)
    messages = [
        make_msg(msg, mapped_route, 0),
        make_msg(msg, other_route, 1),
        make_msg(FilteredPayload(), mapped_route, 3),  # to be filtered out
    ]

    with mock.patch("time.time", return_value=start_time + 7.0):
        for message in messages:
            strategy.submit(message)
            strategy.poll()
    with mock.patch("time.time", return_value=start_time + 12.0):
        strategy.poll()

    new_msg = StreamsMessage(
        [metric], [], datetime(2025, 1, 1, 12, 0).timestamp() + 12, None
    )  # since Reduce produces a timestamp based on when the aggregate result is produced, we mock the timestamp

    other_route_msg = Message(
        BrokerValue(
            payload=RoutedValue(route=other_route, payload=msg),
            partition=Partition(Topic("test_topic"), 0),
            offset=1,
            timestamp=datetime(2025, 1, 1, 12, 0),
        ),
    )

    mapped_msg = Message(
        Value(
            payload=RoutedValue(route=mapped_route, payload=new_msg),
            committable={Partition(Topic("test_topic"), 0): 1},
            timestamp=None,
        )
    )

    filtered_msg = Message(
        BrokerValue(
            payload=FilteredPayload(),
            partition=Partition(Topic("test_topic"), 0),
            offset=3,
            timestamp=datetime(2025, 1, 1, 12, 0),
        ),
    )

    expected_calls = [
        call.poll(),
        call.submit(other_route_msg),
        call.poll(),
        call.submit(filtered_msg),
        call.poll(),
        call.submit(mapped_msg),
        call.poll(),
    ]

    actual_calls = next_strategy.method_calls
    for index, exp_call in enumerate(expected_calls):
        assert exp_call == actual_calls[index], f"Call mismatch on index {index}"

    next_strategy.assert_has_calls(expected_calls)

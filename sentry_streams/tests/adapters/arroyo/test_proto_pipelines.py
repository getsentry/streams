from typing import cast
from unittest import mock
from unittest.mock import call

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import Commit, Partition, Topic
from sentry_kafka_schemas import get_codec
from sentry_protos.sentry.v1.taskworker_pb2 import TaskActivation

from sentry_streams.adapters.arroyo.consumer import (
    ArroyoConsumer,
    ArroyoStreamingFactory,
)
from sentry_streams.adapters.arroyo.routes import Route
from sentry_streams.adapters.arroyo.steps import MapStep, StreamSinkStep
from sentry_streams.pipeline.message import Message, MessageSchema
from sentry_streams.pipeline.msg_parser import msg_serializer
from sentry_streams.pipeline.pipeline import Map, Pipeline
from tests.adapters.arroyo.message_helpers import make_kafka_msg

PROTO_SCHEMA = get_codec("taskworker-ingest")
ACTIVATION_MSG = TaskActivation()


def test_msg_serializer() -> None:
    activation_msg = ACTIVATION_MSG
    msg = Message(payload=activation_msg, headers=[], timestamp=123.0, schema=PROTO_SCHEMA)
    serialized = msg_serializer(msg, MessageSchema.PROTOBUF)

    assert serialized == activation_msg.SerializeToString()


def test_simple_pipeline(
    broker: LocalBroker[KafkaPayload],
    basic_proto_pipeline: Pipeline,
) -> None:
    """
    Test the creation of an Arroyo Consumer from a number of
    pipeline steps.
    """
    route = Route(source="source1", waypoints=[])

    consumer = ArroyoConsumer(
        source="source1", stream_name="taskworker-ingest", schema=PROTO_SCHEMA
    )
    consumer.add_step(
        MapStep(
            route=route,
            pipeline_step=cast(Map, basic_proto_pipeline.steps["parser"]),
        )
    )
    consumer.add_step(
        MapStep(
            route=route,
            pipeline_step=cast(Map, basic_proto_pipeline.steps["serializer"]),
        )
    )
    consumer.add_step(
        StreamSinkStep(
            route=route,
            producer=broker.get_producer(),
            topic_name="taskworker-output",
        )
    )

    factory = ArroyoStreamingFactory(consumer)
    commit = mock.Mock(spec=Commit)
    strategy = factory.create_with_partitions(commit, {Partition(Topic("taskworker-ingest"), 0): 0})

    strategy.submit(make_kafka_msg(ACTIVATION_MSG.SerializeToString(), "taskworker-ingest", 0))
    strategy.poll()
    strategy.submit(make_kafka_msg(ACTIVATION_MSG.SerializeToString(), "taskworker-ingest", 1))
    strategy.poll()

    topic = Topic("taskworker-output")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == ACTIVATION_MSG.SerializeToString()
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == ACTIVATION_MSG.SerializeToString()
    assert broker.consume(Partition(topic, 0), 2) is None

    commit.assert_has_calls(
        [
            call({}),
            call({Partition(Topic("taskworker-ingest"), 0): 1}),
            call({}),
            call({}),
            call({Partition(Topic("taskworker-ingest"), 0): 2}),
            call({}),
        ],
    )

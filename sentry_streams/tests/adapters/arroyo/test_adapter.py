from unittest import mock

import pytest
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.types import Partition, Topic

from sentry_streams.adapters.arroyo.adapter import (
    ArroyoAdapter,
    KafkaConsumerConfig,
    KafkaSources,
)
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.pipeline import (
    KafkaSource,
    Pipeline,
)
from sentry_streams.runner import iterate_edges


def test_kafka_sources() -> None:
    sources_config = {
        "source1": KafkaConsumerConfig(
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_group="test_group",
            additional_settings={},
        ),
    }
    consumers = {
        "source2": mock.Mock(),
    }
    sources = KafkaSources(sources_config, consumers)

    with pytest.raises(KeyError):
        sources.get_consumer("source1")

    assert sources.get_consumer("source2") == consumers["source2"]
    with pytest.raises(KeyError):
        sources.get_topic("source2")

    pipeline = Pipeline()
    sources.add_source(KafkaSource("source1", pipeline, "test_topic"))

    assert sources.get_topic("source1") == Topic("test_topic")
    assert sources.get_consumer("source1") is not None


def test_adapter(broker: LocalBroker[KafkaPayload], pipeline: Pipeline) -> None:

    adapter = ArroyoAdapter.build(
        {
            "sources_config": {},
            "sinks_config": {},
            "sources_override": {
                "myinput": broker.get_consumer("logical-events"),
            },
            "sinks_override": {
                "kafkasink": broker.get_producer(),
            },
        }
    )
    iterate_edges(pipeline, RuntimeTranslator(adapter))

    adapter.create_processors()
    processor = adapter.get_processor("myinput")

    broker.produce(
        Partition(Topic("logical-events"), 0), KafkaPayload(None, "go_ahead".encode("utf-8"), [])
    )
    broker.produce(
        Partition(Topic("logical-events"), 0),
        KafkaPayload(None, "do_not_go_ahead".encode("utf-8"), []),
    )
    broker.produce(
        Partition(Topic("logical-events"), 0), KafkaPayload(None, "go_ahead".encode("utf-8"), [])
    )

    processor._run_once()
    processor._run_once()
    processor._run_once()

    topic = Topic("transformed-events")
    msg1 = broker.consume(Partition(topic, 0), 0)
    assert msg1 is not None and msg1.payload.value == "go_ahead_mapped".encode("utf-8")
    msg2 = broker.consume(Partition(topic, 0), 1)
    assert msg2 is not None and msg2.payload.value == "go_ahead_mapped".encode("utf-8")
    assert broker.consume(Partition(topic, 0), 2) is None

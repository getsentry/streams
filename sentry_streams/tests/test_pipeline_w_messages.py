import json
import logging
import signal
from typing import Any

import pytest
import yaml
from arroyo.backends.kafka.configuration import (
    build_kafka_configuration,
    build_kafka_consumer_configuration,
)
from arroyo.backends.kafka.consumer import KafkaConsumer, KafkaPayload, KafkaProducer
from arroyo.types import Topic

from sentry_streams.adapters.loader import load_adapter
from sentry_streams.adapters.stream_adapter import (
    RuntimeTranslator,
)
from sentry_streams.examples.simple_map_filter import pipeline as example_pipeline
from sentry_streams.pipeline.pipeline import (
    Pipeline,
)
from sentry_streams.runner import iterate_edges

logger = logging.getLogger(__name__)


message = {
    "org_id": 420,
    "project_id": 420,
    "name": "c:sessions/session@none",
    "tags": {},
    "timestamp": 1111111111111111,
    "retention_days": 90,
    "type": "c",
    "value": 1,
}


def run_pipeline(pipeline: Pipeline) -> None:
    with open("./sentry_streams/deployment_config/simple_map_filter.yaml", "r") as f:
        environment_config = yaml.safe_load(f)
    print(":HERE", environment_config)
    runtime: Any = load_adapter("arroyo", environment_config, 0)
    translator = RuntimeTranslator(runtime)

    iterate_edges(pipeline, translator)

    def signal_handler(sig: int, frame: Any) -> None:
        logger.info("Signal received, terminating the runner...")
        runtime.shutdown()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    runtime.run()


@pytest.mark.skip(reason="This test is not working")
def test_simple_map_filter() -> None:
    # Produce a message to Kafka
    producer = KafkaProducer(
        build_kafka_configuration(
            default_config={},
            bootstrap_servers="localhost:9092",
        )
    )
    producer.produce(
        Topic("ingest-metrics"), KafkaPayload(None, json.dumps(message).encode("utf-8"), [])
    )
    # producer.flush()
    producer.close()

    # Run the pipeline
    run_pipeline(example_pipeline)

    # Consume a message from the trannsformed-events topic
    consumer = KafkaConsumer(
        build_kafka_consumer_configuration(
            default_config={},
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            group_id="test-pipeline-simple-map-filter",
        )
    )
    consumer.subscribe([Topic("transformed-events")])
    msg = consumer.poll(timeout=10)
    assert msg is not None
    assert json.loads(msg.payload.value.decode("utf-8")) == message

    consumer.close()

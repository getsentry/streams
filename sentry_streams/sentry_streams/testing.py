"""
Internal testing utilities for sentry_streams pipelines.
This module provides helper functions for testing pipelines with LocalBroker.
This is not part of the public API and may change without notice.
"""

from typing import Any, Dict, List, Optional, Tuple, cast

from arroyo.backends.kafka import KafkaConsumer, KafkaProducer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.backends.local.backend import LocalBroker
from arroyo.backends.local.storages.memory import MemoryMessageStorage
from arroyo.processing.processor import StreamProcessor
from arroyo.types import Partition, Topic
from arroyo.utils.clock import MockedClock

from sentry_streams.adapters.arroyo.adapter import ArroyoAdapter
from sentry_streams.adapters.stream_adapter import RuntimeTranslator
from sentry_streams.pipeline.pipeline import Pipeline, StreamSink, StreamSource
from sentry_streams.runner import iterate_edges


class PipelineTestHarness:
    """Test harness for running pipelines with LocalBroker."""

    def __init__(self, topics: List[str]):
        """Initialize test harness with specified topics."""
        self.clock = MockedClock()
        self.broker = self._create_local_broker(topics)
        self.adapter: Optional[ArroyoAdapter] = None
        self.processor: Optional[StreamProcessor[KafkaPayload]] = None

    def _create_local_broker(self, topics: List[str]) -> LocalBroker[KafkaPayload]:
        """Create a LocalBroker with the specified topics."""
        storage = MemoryMessageStorage[KafkaPayload]()
        broker = LocalBroker(storage, self.clock)
        for topic in topics:
            broker.create_topic(Topic(topic), 1)
        return broker

    def setup_pipeline(self, pipeline: Pipeline[bytes]) -> None:
        """Setup a pipeline for testing by extracting sources/sinks and configuring adapter."""
        # Extract source and sink names from the pipeline
        source_names: List[Tuple[str, str]] = []
        sink_names: List[Tuple[str, str]] = []

        def extract_steps(pipe: Any) -> None:
            """Recursively extract source and sink names from pipeline."""
            if hasattr(pipe, "source") and isinstance(pipe.source, StreamSource):
                source_names.append((pipe.source.name, pipe.source.stream_name))
            if hasattr(pipe, "steps"):
                for step in pipe.steps:
                    if isinstance(step, StreamSink):
                        sink_names.append((step.name, step.stream_name))
            # Check for chains and sub-pipelines
            if hasattr(pipe, "chain"):
                extract_steps(pipe.chain)

        extract_steps(pipeline)

        # If extraction failed, use defaults
        if not source_names:
            # Fallback to common patterns
            source_names = [("myinput", "ingest-metrics")]
        if not sink_names:
            # Try common sink names
            sink_names = [
                ("mysink", "transformed-events"),
                ("kafkasink", "transformed-events"),
                ("kafkasink2", "transformed-events"),
            ]

        # Setup ArroyoAdapter with LocalBroker
        consumers: Dict[str, KafkaConsumer] = {}
        producers: Dict[str, KafkaProducer] = {}
        steps_config: Dict[str, Dict[str, Dict[str, Any]]] = {}

        for source_name, topic_name in source_names:
            consumers[source_name] = cast(KafkaConsumer, self.broker.get_consumer(topic_name))
            steps_config[source_name] = {source_name: {}}

        for sink_name, _ in sink_names:
            producers[sink_name] = cast(KafkaProducer, self.broker.get_producer())
            steps_config[sink_name] = {sink_name: {}}

        self.adapter = ArroyoAdapter.build(
            {
                "env": {},
                "steps_config": steps_config,
            },
            consumers,
            producers,
        )

        # Configure and create pipeline processors
        iterate_edges(pipeline, RuntimeTranslator(self.adapter))
        self.adapter.create_processors()
        self.processor = self.adapter.get_processor(source_names[0][0])

    def send_messages(self, topic: str, messages: List[bytes]) -> None:
        """Send messages to a topic."""
        for msg in messages:
            self.broker.produce(
                Partition(Topic(topic), 0),
                KafkaPayload(None, msg, []),
            )

    def process_messages(self, count: int, advance_time_seconds: float = 0) -> None:
        """
        Process the specified number of messages through the pipeline.

        Args:
            count: Number of messages to process
            advance_time_seconds: Optional time to advance clock after processing (for windowing)
        """
        if self.processor is None:
            raise RuntimeError("Pipeline not setup. Call setup_pipeline first.")

        for _ in range(count):
            self.processor._run_once()

        if advance_time_seconds > 0:
            self.advance_time(advance_time_seconds)

    def advance_time(self, seconds: float) -> None:
        """Advance the mock clock by the specified number of seconds."""
        for _ in range(int(seconds)):
            self.clock.sleep(1.0)
            # Process any pending window closes
            if self.processor is not None:
                self.processor._run_once()

    def get_messages_from_topic(self, topic: str) -> List[bytes]:
        """Get all messages from a topic."""
        messages = []
        offset = 0

        while True:
            msg = self.broker.consume(Partition(Topic(topic), 0), offset)
            if msg is None:
                break
            messages.append(msg.payload.value)
            offset += 1

        return messages

    def count_messages_in_topic(self, topic: str) -> int:
        """Count messages in a topic."""
        return len(self.get_messages_from_topic(topic))


def run_pipeline_test(
    pipeline: Pipeline[bytes],
    source_topic: str,
    sink_topics: List[str],
    input_messages: List[bytes],
    expected_messages: Dict[str, int],
    advance_time_after_processing: float = 0,
) -> None:
    """
    Run a pipeline test with LocalBroker.

    Args:
        pipeline: The pipeline to test
        source_topic: Topic to send input messages to
        sink_topics: List of sink topics to check
        input_messages: Messages to send to the source topic
        expected_messages: Expected message counts per sink topic
        advance_time_after_processing: Time in seconds to advance after processing (for windowing)
    """
    all_topics = [source_topic] + sink_topics
    harness = PipelineTestHarness(all_topics)

    harness.setup_pipeline(pipeline)
    harness.send_messages(source_topic, input_messages)
    harness.process_messages(len(input_messages))

    # For windowed operations, advance time to trigger window closes
    if advance_time_after_processing > 0:
        harness.advance_time(advance_time_after_processing)

    # Verify messages in sink topics
    for sink_topic in sink_topics:
        actual = harness.count_messages_in_topic(sink_topic)
        expected = expected_messages.get(sink_topic, 0)
        assert actual == expected, f"Expected {expected} messages on {sink_topic}, got {actual}"

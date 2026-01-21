"""
Dead Letter Queue (DLQ) implementation for the Rust Arroyo adapter.

This module provides DLQ functionality that bypasses Arroyo's built-in DLQ system
to avoid memory buffering of raw messages. Instead, it produces only metadata
(topic, partition, offset, error info) to a separate Kafka topic.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from confluent_kafka import Producer

from sentry_streams.config_types import DlqPipelineConfig, DlqStepConfig
from sentry_streams.pipeline.exception import DlqHandledError
from sentry_streams.pipeline.message import Message

logger = logging.getLogger(__name__)


@dataclass
class DlqMetadata:
    """Metadata about a failed message that gets sent to the DLQ topic."""

    original_topic: str
    original_partition: int | None
    original_offset: int | None
    original_key: str | None
    step_name: str
    consumer_group: str
    error: str
    error_type: str
    timestamp: float

    def to_json(self) -> bytes:
        """Serialize the metadata to JSON bytes."""
        return json.dumps(
            {
                "original_topic": self.original_topic,
                "original_partition": self.original_partition,
                "original_offset": self.original_offset,
                "original_key": self.original_key,
                "step_name": self.step_name,
                "consumer_group": self.consumer_group,
                "error": self.error,
                "error_type": self.error_type,
                "timestamp": self.timestamp,
            }
        ).encode("utf-8")


class DlqProducer:
    """
    A wrapper around a Kafka producer specifically for DLQ messages.

    This producer is used to send metadata about failed messages to a DLQ topic.
    It does NOT buffer or store the original message content, only metadata.
    """

    def __init__(
        self,
        bootstrap_servers: Sequence[str],
        default_topic: str,
    ) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": ",".join(bootstrap_servers),
                "enable.idempotence": "false",  # DLQ doesn't need exactly-once
            }
        )
        self._default_topic = default_topic

    def produce(self, metadata: DlqMetadata, topic: str | None = None) -> None:
        """
        Produce a DLQ metadata message to the specified topic.

        Args:
            metadata: The metadata about the failed message
            topic: Optional topic override; uses default if not specified
        """
        target_topic = topic or self._default_topic
        try:
            self._producer.produce(
                topic=target_topic,
                value=metadata.to_json(),
            )
            # Trigger delivery reports without blocking
            self._producer.poll(0)
        except Exception as e:
            # Log but don't fail - DLQ should not block processing
            logger.error(f"Failed to produce to DLQ topic {target_topic}: {e}")

    def flush(self, timeout: float = 10.0) -> int:
        """
        Flush pending DLQ messages.

        Returns the number of messages still in queue (0 if all flushed).
        """
        return self._producer.flush(timeout)


class DlqStepWrapper:
    """
    Wraps a step function with DLQ error handling.

    When an exception occurs during step execution:
    1. If DLQ is enabled for this step, produce metadata to the DLQ topic
    2. Raise DlqHandledError to signal the Rust layer to skip the message
    3. Processing continues normally with the next message

    If DLQ is not enabled, exceptions propagate normally (crash/retry behavior).
    """

    def __init__(
        self,
        step_name: str,
        func: Callable[[Message[Any]], Any],
        dlq_config: DlqStepConfig | None,
        dlq_producer: DlqProducer | None,
        consumer_group: str,
        source_topic: str,
    ) -> None:
        self._step_name = step_name
        self._func = func
        self._dlq_config = dlq_config
        self._dlq_producer = dlq_producer
        self._consumer_group = consumer_group
        self._source_topic = source_topic

        # Check if DLQ is actually enabled
        self._dlq_enabled = (
            dlq_config is not None and dlq_config.get("enabled", False) and dlq_producer is not None
        )

    def __call__(self, msg: Message[Any]) -> Any:
        """
        Execute the wrapped function with DLQ error handling.
        """
        if not self._dlq_enabled:
            # No DLQ configured - let exceptions propagate normally
            return self._func(msg)

        try:
            return self._func(msg)
        except Exception as e:
            # Log the original error with full traceback before sending to DLQ
            logger.exception(
                f"Error in step {self._step_name}, sending to DLQ: "
                f"partition={msg.partition}, offset={msg.offset}"
            )
            self._produce_to_dlq(msg, e)
            raise DlqHandledError() from e

    def _produce_to_dlq(self, msg: Message[Any], error: Exception) -> None:
        """
        Produce metadata about the failed message to the DLQ topic.
        """
        assert self._dlq_producer is not None

        # Extract key from headers if available
        original_key: str | None = None
        for header_name, header_value in msg.headers:
            if header_name.lower() == "key":
                try:
                    original_key = header_value.decode("utf-8")
                except (UnicodeDecodeError, AttributeError):
                    original_key = str(header_value)
                break

        metadata = DlqMetadata(
            original_topic=self._source_topic,
            original_partition=msg.partition,
            original_offset=msg.offset,
            original_key=original_key,
            step_name=self._step_name,
            consumer_group=self._consumer_group,
            error=str(error),
            error_type=type(error).__name__,
            timestamp=time.time(),
        )

        # Use step-specific topic override if configured
        topic_override = None
        if self._dlq_config is not None:
            topic_override = self._dlq_config.get("topic")

        self._dlq_producer.produce(metadata, topic_override)


def create_dlq_producer(
    dlq_config: DlqPipelineConfig | None,
    fallback_bootstrap_servers: Sequence[str] | None = None,
) -> DlqProducer | None:
    """
    Create a DLQ producer from pipeline configuration.

    Returns None if DLQ is not configured at the pipeline level.
    """
    if dlq_config is None:
        return None

    topic = dlq_config.get("topic")
    if not topic:
        logger.warning("DLQ config provided but no topic specified")
        return None

    # Use DLQ-specific bootstrap servers, or fall back to source config
    bootstrap_servers = dlq_config.get("bootstrap_servers")
    if not bootstrap_servers:
        if fallback_bootstrap_servers:
            bootstrap_servers = fallback_bootstrap_servers
        else:
            logger.warning(
                "DLQ config provided but no bootstrap_servers specified and no fallback available"
            )
            return None

    return DlqProducer(
        bootstrap_servers=bootstrap_servers,
        default_topic=topic,
    )


def wrap_step_with_dlq(
    step_name: str,
    func: Callable[[Message[Any]], Any],
    step_config: Mapping[str, Any] | None,
    dlq_producer: DlqProducer | None,
    consumer_group: str,
    source_topic: str,
) -> Callable[[Message[Any]], Any]:
    """
    Wrap a step function with DLQ error handling if configured.

    Args:
        step_name: Name of the step (for logging/metadata)
        func: The step function to wrap
        step_config: The step's configuration dict (may contain 'dlq' key)
        dlq_producer: The DLQ producer (or None if not configured)
        consumer_group: The Kafka consumer group ID
        source_topic: The source Kafka topic name

    Returns:
        The wrapped function if DLQ is enabled, otherwise the original function.
    """
    dlq_step_config: DlqStepConfig | None = None
    if step_config is not None:
        dlq_step_config = step_config.get("dlq")  # type: ignore[assignment]

    # If no DLQ config or not enabled, return the original function
    if dlq_step_config is None or not dlq_step_config.get("enabled", False):
        return func

    if dlq_producer is None:
        logger.warning(
            f"Step {step_name} has DLQ enabled but no DLQ producer is configured at pipeline level"
        )
        return func

    return DlqStepWrapper(
        step_name=step_name,
        func=func,
        dlq_config=dlq_step_config,
        dlq_producer=dlq_producer,
        consumer_group=consumer_group,
        source_topic=source_topic,
    )

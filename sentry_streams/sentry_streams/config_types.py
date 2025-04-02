from typing import Any, Mapping, Optional, TypedDict


class StepConfig(TypedDict):
    """
    A generic Step
    """

    starts_segment: Optional[bool]


class KafkaConsumerConfig(TypedDict, StepConfig):
    bootstrap_servers: str
    auto_offset_reset: str
    consumer_group: str
    additional_settings: Mapping[str, Any]


class KafkaProducerConfig(TypedDict, StepConfig):
    bootstrap_servers: str
    additional_settings: Mapping[str, Any]


class SegmentConfig(TypedDict):
    parallelism: int
    steps_config: Mapping[str, StepConfig]

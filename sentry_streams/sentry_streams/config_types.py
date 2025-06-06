from typing import Any, Mapping, Optional, Sequence, TypedDict


class StepConfig(TypedDict):
    """
    A generic Step
    """

    starts_segment: Optional[bool]


class KafkaConsumerConfig(TypedDict, StepConfig):
    bootstrap_servers: Sequence[str]
    auto_offset_reset: str
    consumer_group: str
    additional_settings: Mapping[str, Any]


class KafkaProducerConfig(TypedDict, StepConfig):
    bootstrap_servers: Sequence[str]
    additional_settings: Mapping[str, Any]


class SegmentConfig(TypedDict):
    parallelism: int
    steps_config: Mapping[str, StepConfig]


class MultiProcessConfig(TypedDict):
    processes: int
    batch_size: int
    batch_time: float
    input_block_size: int | None
    output_block_size: int | None
    max_input_block_size: int | None
    max_output_block_size: int | None

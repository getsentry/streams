from typing import Mapping, NotRequired, Optional, Sequence, TypedDict


class StepConfig(TypedDict):
    """
    A generic Step
    """

    starts_segment: Optional[bool]


class KafkaConsumerConfig(TypedDict, StepConfig):
    bootstrap_servers: Sequence[str]
    auto_offset_reset: str
    consumer_group: NotRequired[str]
    override_params: NotRequired[Mapping[str, str]]


class KafkaProducerConfig(TypedDict, StepConfig):
    bootstrap_servers: Sequence[str]
    override_params: NotRequired[Mapping[str, str]]


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

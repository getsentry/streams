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


class DlqStepConfig(TypedDict, total=False):
    """
    Per-step DLQ configuration. Steps must explicitly opt-in
    with enabled=True to have their errors sent to the DLQ.
    """

    enabled: bool  # Required to be True for DLQ to be active
    topic: str  # Optional: override the pipeline default DLQ topic


class DlqPipelineConfig(TypedDict, total=False):
    """
    Pipeline-level DLQ configuration. Provides defaults for all steps.
    """

    topic: str  # Default DLQ topic for all opted-in steps
    bootstrap_servers: Sequence[str]  # Optional: defaults to source config

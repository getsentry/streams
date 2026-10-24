from __future__ import annotations

from typing import Any, Mapping, NotRequired, TypedDict

from sentry_streams_k8s.consumer_builder import (
    ConsumerBuilder,
    ConsumerSpec,
    RenderedDeployments,
    RenderedPods,
)


class StreamingPipelineSpec(TypedDict):
    service_name: str
    pipeline_name: str
    pipeline_module: str
    image_name: str
    cpu_per_process: int
    memory_per_process: int
    deployment_template: dict[str, Any]
    container_template: dict[str, Any]
    pipeline_config: dict[str, Any]
    replicas: NotRequired[int]
    with_canary: NotRequired[bool]
    segment_id: NotRequired[int]
    log_level: NotRequired[str]
    enable_liveness_probe: NotRequired[bool]
    container_name: NotRequired[str]
    emergency_patch: NotRequired[dict[str, Any]]


REQUIRED_FIELDS = (
    "service_name",
    "pipeline_name",
    "pipeline_module",
    "image_name",
    "cpu_per_process",
    "memory_per_process",
    "deployment_template",
    "container_template",
    "pipeline_config",
)


def from_crd_spec(crd_spec: Mapping[str, Any], *, name: str | None = None) -> StreamingPipelineSpec:
    spec: dict[str, Any] = dict(crd_spec)
    if "pipeline_name" not in spec and name is not None:
        spec["pipeline_name"] = name
    return spec  # type: ignore[return-value]


def validate(spec: StreamingPipelineSpec) -> None:
    missing = [f for f in REQUIRED_FIELDS if f not in spec]
    if missing:
        raise ValueError(
            f"StreamingPipeline is missing required field(s): {', '.join(sorted(missing))}."
        )


def to_consumer_spec(spec: StreamingPipelineSpec) -> ConsumerSpec:
    return ConsumerSpec(
        service_name=spec["service_name"],
        pipeline_name=spec["pipeline_name"],
        pipeline_module=spec["pipeline_module"],
        image=spec["image_name"],
        cpu_per_process=spec["cpu_per_process"],
        memory_per_process=spec["memory_per_process"],
        segment_id=spec.get("segment_id", 0),
        replicas=spec.get("replicas", 1),
        log_level=spec.get("log_level", "INFO"),
        enable_liveness_probe=spec.get("enable_liveness_probe", True),
        with_canary=spec.get("with_canary", False),
        container_name=spec.get("container_name", "pipeline-consumer"),
        emergency_patch=spec.get("emergency_patch", {}),
    )


def render_deployments(spec: StreamingPipelineSpec) -> RenderedDeployments:
    builder = ConsumerBuilder(spec["deployment_template"], spec["container_template"])
    consumer = to_consumer_spec(spec)
    builder.validate(consumer, spec["pipeline_config"])
    return builder.build_deployments(consumer, spec["pipeline_config"])


def render_pods(spec: StreamingPipelineSpec) -> RenderedPods:
    builder = ConsumerBuilder(spec["deployment_template"], spec["container_template"])
    consumer = to_consumer_spec(spec)
    builder.validate(consumer, spec["pipeline_config"])
    return builder.build_pods(consumer, spec["pipeline_config"])

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from kubernetes.client import (
    V1ContainerState,
    V1ContainerStateTerminated,
    V1ContainerStateWaiting,
    V1ContainerStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodStatus,
)


def make_condition(type_: str, status: str) -> V1PodCondition:
    return V1PodCondition(type=type_, status=status)


def make_waiting_container_status(reason: str, *, name: str = "c") -> V1ContainerStatus:
    return V1ContainerStatus(
        name=name,
        ready=False,
        restart_count=0,
        image="test-image",
        image_id="",
        state=V1ContainerState(waiting=V1ContainerStateWaiting(reason=reason)),
    )


def make_terminated_container_status(
    exit_code: int, reason: str | None = None, *, name: str = "c"
) -> V1ContainerStatus:
    return V1ContainerStatus(
        name=name,
        ready=False,
        restart_count=0,
        image="test-image",
        image_id="",
        state=V1ContainerState(
            terminated=V1ContainerStateTerminated(exit_code=exit_code, reason=reason)
        ),
    )


def make_pod(
    *,
    name: str = "consumer-0-0",
    labels: Mapping[str, str] | None = None,
    annotations: Mapping[str, str] | None = None,
    creation_timestamp: datetime | None = None,
    deletion_timestamp: datetime | None = None,
    phase: str | None = None,
    conditions: list[V1PodCondition] | None = None,
    container_statuses: list[V1ContainerStatus] | None = None,
    init_container_statuses: list[V1ContainerStatus] | None = None,
    start_time: datetime | None = None,
    reason: str | None = None,
) -> V1Pod:
    return V1Pod(
        metadata=V1ObjectMeta(
            name=name,
            labels=dict(labels) if labels is not None else None,
            annotations=dict(annotations) if annotations is not None else None,
            creation_timestamp=creation_timestamp,
            deletion_timestamp=deletion_timestamp,
        ),
        status=V1PodStatus(
            phase=phase,
            conditions=conditions,
            container_statuses=container_statuses,
            init_container_statuses=init_container_statuses,
            start_time=start_time,
            reason=reason,
        ),
    )

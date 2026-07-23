from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from kubernetes.client import V1ContainerStatus, V1Pod

from sentry_streams_k8s.operator.constants import (
    PERMANENT_WAITING_REASONS,
    POD_TERMINATING_GRACE_SECONDS,
    POD_WAITING_GRACE_SECONDS,
    SPEC_HASH_ANNOTATION,
    UNHEALTHY_WAITING_REASONS,
)
from sentry_streams_k8s.operator.pod_types import PodManifest


def is_deleting(pod: V1Pod) -> bool:
    metadata = pod.metadata
    return metadata is not None and metadata.deletion_timestamp is not None


def age_seconds(timestamp: datetime | None, now: datetime) -> float | None:
    if timestamp is None:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return (now - timestamp).total_seconds()


def pod_is_ready(pod: V1Pod) -> bool:
    status = pod.status
    if status is None:
        return False
    conditions = status.conditions or []
    return status.phase == "Running" and any(
        condition.type == "Ready" and condition.status == "True" for condition in conditions
    )


def pod_spec_changed(current: V1Pod, desired: PodManifest) -> bool:
    metadata = current.metadata
    current_hash = ((metadata.annotations or {}) if metadata else {}).get(SPEC_HASH_ANNOTATION)
    desired_annotations = desired["metadata"].get("annotations") or {}
    desired_hash = desired_annotations.get(SPEC_HASH_ANNOTATION)
    return desired_hash is not None and current_hash != desired_hash


def _container_waiting_reason(status: V1ContainerStatus) -> str | None:
    waiting = status.state.waiting if status.state else None
    return waiting.reason if waiting else None


def _first_waiting_reason(
    statuses: list[V1ContainerStatus] | None, reasons: frozenset[str]
) -> str | None:
    for status in statuses or []:
        reason = _container_waiting_reason(status)
        if reason in reasons:
            return reason
    return None


def _first_unhealthy_waiting(statuses: list[V1ContainerStatus] | None) -> str | None:
    return _first_waiting_reason(statuses, UNHEALTHY_WAITING_REASONS)


def _first_permanent_waiting(statuses: list[V1ContainerStatus] | None) -> str | None:
    return _first_waiting_reason(statuses, PERMANENT_WAITING_REASONS)


def _container_failed_terminated_reason(status: V1ContainerStatus) -> str | None:
    terminated = status.state.terminated if status.state else None
    if terminated is None:
        return None

    exit_code = terminated.exit_code
    reason = terminated.reason

    if exit_code == 0 or reason == "Completed":
        return None

    if isinstance(reason, str) and reason:
        return reason

    if isinstance(exit_code, int):
        return f"ExitCode{exit_code}"

    return "Terminated"


def _first_failed_terminated_reason(statuses: list[V1ContainerStatus] | None) -> str | None:
    for status in statuses or []:
        reason = _container_failed_terminated_reason(status)
        if reason is not None:
            return reason
    return None


def _waiting_grace_elapsed(pod: V1Pod, now: datetime) -> bool:
    metadata = pod.metadata
    status = pod.status
    if metadata is None or status is None:
        return False
    age = age_seconds(status.start_time, now)
    if age is None:
        age = age_seconds(metadata.creation_timestamp, now)
    return age is not None and age >= POD_WAITING_GRACE_SECONDS


@dataclass(frozen=True)
class PodHealth:
    name: str
    ready: bool = False
    reason: str | None = None
    unhealthy: bool = False
    delete: bool = False
    force: bool = False
    permanent: bool = False


def _verdict(
    pod_name: str,
    *,
    ready: bool = False,
    reason: str | None = None,
    unhealthy: bool = False,
    delete: bool = False,
    force: bool = False,
    permanent: bool = False,
) -> PodHealth:
    return PodHealth(
        name=pod_name,
        ready=ready,
        reason=reason,
        unhealthy=unhealthy,
        delete=delete,
        force=force,
        permanent=permanent,
    )


def _container_statuses_verdict(
    pod_name: str,
    statuses: list[V1ContainerStatus] | None,
    pod: V1Pod,
    now: datetime,
    *,
    reason_prefix: str = "",
) -> PodHealth | None:
    permanent_waiting = _first_permanent_waiting(statuses)
    if permanent_waiting is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{permanent_waiting}",
            unhealthy=True,
            permanent=True,
        )

    terminated_reason = _first_failed_terminated_reason(statuses)
    if terminated_reason is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{terminated_reason}",
            unhealthy=True,
            delete=True,
        )

    unhealthy_waiting = _first_unhealthy_waiting(statuses)
    if unhealthy_waiting is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{unhealthy_waiting}",
            unhealthy=True,
            delete=_waiting_grace_elapsed(pod, now),
        )

    return None


def pod_health(pod: V1Pod, now: datetime) -> PodHealth:
    """Classify a Pod into a PodHealth verdict."""

    metadata = pod.metadata
    pod_name = metadata.name if metadata and metadata.name else ""

    # If the pod is terminating, let it finish terminating by itself.
    # Only force-delete it if it has been stuck terminating for too long.

    terminating_age = age_seconds(metadata.deletion_timestamp if metadata else None, now)

    if terminating_age is not None:
        stuck = terminating_age >= POD_TERMINATING_GRACE_SECONDS
        reason = "StuckTerminating" if stuck else "Terminating"
        return _verdict(
            pod_name,
            reason=reason,
            unhealthy=stuck,
            delete=stuck,
            force=stuck,
        )

    status = pod.status
    if status is None:
        return _verdict(pod_name)

    init_verdict = _container_statuses_verdict(
        pod_name,
        status.init_container_statuses,
        pod,
        now,
        reason_prefix="InitContainer",
    )
    if init_verdict is not None:
        return init_verdict

    phase = status.phase

    if phase == "Succeeded":
        return _verdict(pod_name, reason="Succeeded", delete=True)

    app_verdict = _container_statuses_verdict(
        pod_name,
        status.container_statuses,
        pod,
        now,
    )
    if app_verdict is not None:
        return app_verdict

    if phase == "Failed":
        reason = status.reason or phase or "Terminated"
        return _verdict(pod_name, reason=reason, unhealthy=True, delete=True)

    return _verdict(pod_name, ready=pod_is_ready(pod))

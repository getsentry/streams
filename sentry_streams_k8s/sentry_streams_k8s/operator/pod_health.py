from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from sentry_streams_k8s.operator.constants import (
    ORDINAL_LABEL,
    PERMANENT_WAITING_REASONS,
    POD_TERMINATING_GRACE_SECONDS,
    POD_WAITING_GRACE_SECONDS,
    SPEC_HASH_ANNOTATION,
    UNHEALTHY_WAITING_REASONS,
)


def pod_metadata(resource: Mapping[str, Any]) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], resource.get("metadata", {}))


def pod_labels(resource: Mapping[str, Any]) -> Mapping[str, str]:
    return cast(Mapping[str, str], pod_metadata(resource).get("labels", {}) or {})


def pod_annotations(resource: Mapping[str, Any]) -> Mapping[str, str]:
    return cast(Mapping[str, str], pod_metadata(resource).get("annotations", {}) or {})


def pod_status(resource: Mapping[str, Any]) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], resource.get("status", {}) or {})


def is_deleting(resource: Mapping[str, Any]) -> bool:
    return pod_metadata(resource).get("deletionTimestamp") is not None


def parse_k8s_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def age_seconds(value: object, now: datetime) -> float | None:
    timestamp = parse_k8s_timestamp(value)
    if timestamp is None:
        return None
    return (now - timestamp).total_seconds()


def pod_is_ready(pod: Mapping[str, Any]) -> bool:
    status = pod_status(pod)
    conditions = cast(list[Mapping[str, Any]], status.get("conditions", []) or [])
    return status.get("phase") == "Running" and any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in conditions
    )


def pod_spec_changed(current: Mapping[str, Any], desired: Mapping[str, Any]) -> bool:
    current_hash = pod_annotations(current).get(SPEC_HASH_ANNOTATION)
    desired_hash = pod_annotations(desired).get(SPEC_HASH_ANNOTATION)
    return desired_hash is not None and current_hash != desired_hash


def _container_waiting_reason(status: Mapping[str, Any]) -> str | None:
    state = cast(Mapping[str, Any], status.get("state", {}) or {})
    waiting = cast(Mapping[str, Any], state.get("waiting", {}) or {})
    reason = waiting.get("reason")
    return reason if isinstance(reason, str) else None


def _first_waiting_reason(statuses: object, reasons: frozenset[str]) -> str | None:
    for status in cast(list[Mapping[str, Any]], statuses or []):
        reason = _container_waiting_reason(status)
        if reason in reasons:
            return reason
    return None


def _first_unhealthy_waiting(statuses: object) -> str | None:
    return _first_waiting_reason(statuses, UNHEALTHY_WAITING_REASONS)


def _first_permanent_waiting(statuses: object) -> str | None:
    return _first_waiting_reason(statuses, PERMANENT_WAITING_REASONS)


def _container_failed_terminated_reason(status: Mapping[str, Any]) -> str | None:
    state = cast(Mapping[str, Any], status.get("state", {}) or {})
    terminated = cast(Mapping[str, Any], state.get("terminated", {}) or {})
    if not terminated:
        return None

    exit_code = terminated.get("exitCode")
    reason = terminated.get("reason")

    if exit_code == 0 or reason == "Completed":
        return None

    if isinstance(reason, str) and reason:
        return reason

    if isinstance(exit_code, int):
        return f"ExitCode{exit_code}"

    return "Terminated"


def _first_failed_terminated_reason(statuses: object) -> str | None:
    for status in cast(list[Mapping[str, Any]], statuses or []):
        reason = _container_failed_terminated_reason(status)
        if reason is not None:
            return reason
    return None


def _waiting_grace_elapsed(pod: Mapping[str, Any], now: datetime) -> bool:
    status = pod_status(pod)
    age = age_seconds(status.get("startTime"), now)
    if age is None:
        age = age_seconds(pod_metadata(pod).get("creationTimestamp"), now)
    return age is not None and age >= POD_WAITING_GRACE_SECONDS


@dataclass(frozen=True)
class PodHealth:
    name: str
    ready: bool = False
    reason: str | None = None
    delete: bool = False
    force: bool = False
    permanent: bool = False


def _verdict(
    pod_name: str,
    *,
    ready: bool = False,
    reason: str | None = None,
    delete: bool = False,
    force: bool = False,
    permanent: bool = False,
) -> PodHealth:
    return PodHealth(
        name=pod_name,
        ready=ready,
        reason=reason,
        delete=delete,
        force=force,
        permanent=permanent,
    )


def _container_statuses_verdict(
    pod_name: str,
    statuses: object,
    pod: Mapping[str, Any],
    now: datetime,
    *,
    reason_prefix: str = "",
) -> PodHealth | None:
    permanent_waiting = _first_permanent_waiting(statuses)
    if permanent_waiting is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{permanent_waiting}",
            permanent=True,
        )

    terminated_reason = _first_failed_terminated_reason(statuses)
    if terminated_reason is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{terminated_reason}",
            delete=True,
        )

    unhealthy_waiting = _first_unhealthy_waiting(statuses)
    if unhealthy_waiting is not None:
        return _verdict(
            pod_name,
            reason=f"{reason_prefix}{unhealthy_waiting}",
            delete=_waiting_grace_elapsed(pod, now),
        )

    return None


def pod_health(pod: Mapping[str, Any], now: datetime) -> PodHealth:
    """Classify a Pod into a PodHealth verdict."""

    metadata = pod_metadata(pod)
    status = pod_status(pod)

    pod_name = cast(str, metadata.get("name", ""))

    # If the pod is terminating, let it finish terminating by itself.
    # Only force-delete it if it has been stuck terminating for too long.

    terminating_age = age_seconds(metadata.get("deletionTimestamp"), now)

    if terminating_age is not None:
        stuck = terminating_age >= POD_TERMINATING_GRACE_SECONDS
        reason = "StuckTerminating" if stuck else "Terminating"
        return _verdict(pod_name, reason=reason, delete=stuck, force=stuck)

    init_verdict = _container_statuses_verdict(
        pod_name,
        status.get("initContainerStatuses"),
        pod,
        now,
        reason_prefix="InitContainer",
    )
    if init_verdict is not None:
        return init_verdict

    phase = status.get("phase")

    if phase == "Succeeded":
        return _verdict(pod_name, reason="Succeeded", delete=True)

    app_verdict = _container_statuses_verdict(
        pod_name,
        status.get("containerStatuses"),
        pod,
        now,
    )
    if app_verdict is not None:
        return app_verdict

    if phase == "Failed":
        status_reason = status.get("reason")
        status_reason_str = status_reason if isinstance(status_reason, str) else None
        phase_str = phase if isinstance(phase, str) else None
        reason = status_reason_str or phase_str or "Terminated"
        return _verdict(pod_name, reason=reason, delete=True)

    return _verdict(pod_name, ready=pod_is_ready(pod))


def pod_status_entry(pod: Mapping[str, Any], health: PodHealth) -> dict[str, Any]:
    labels = pod_labels(pod)
    entry: dict[str, Any] = {
        "name": health.name,
        "ready": health.ready,
        "phase": pod_status(pod).get("phase", "Unknown"),
    }
    if labels.get(ORDINAL_LABEL) is not None:
        entry["ordinal"] = labels[ORDINAL_LABEL]
    if health.reason is not None:
        entry["reason"] = health.reason
    if health.permanent:
        entry["permanent"] = True
    return entry

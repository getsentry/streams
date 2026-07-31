from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from kubernetes.client import V1ObjectMeta, V1Pod, V1PodCondition

from sentry_streams_k8s.k8s_types import V1PodDict
from sentry_streams_k8s.operator.constants import ORDINAL_LABEL, SPEC_HASH_ANNOTATION
from sentry_streams_k8s.operator.pod_health import (
    pod_health,
    pod_spec_changed,
)
from tests.k8s_fixtures import (
    make_condition,
    make_pod,
    make_terminated_container_status,
    make_waiting_container_status,
)


def _image_pull_pod(
    *,
    start_time: datetime | None,
    creation_time: datetime,
    reason: str = "ImagePullBackOff",
) -> V1Pod:
    return make_pod(
        name="consumer-0-0",
        creation_timestamp=creation_time,
        phase="Pending",
        container_statuses=[make_waiting_container_status(reason)],
        start_time=start_time,
    )


def test_image_pull_wait_uses_pod_start_time_for_grace() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    start_time = now - timedelta(seconds=299)
    creation_time = now - timedelta(hours=1)

    pod = _image_pull_pod(start_time=start_time, creation_time=creation_time)

    assert pod_health(pod, now).reason == "ImagePullBackOff"
    assert pod_health(pod, now).delete is False


def test_image_pull_wait_is_deleted_after_start_time_grace() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    start_time = now - timedelta(minutes=5)
    creation_time = now - timedelta(hours=1)

    pod = _image_pull_pod(start_time=start_time, creation_time=creation_time)

    assert pod_health(pod, now).delete is True


def test_image_pull_wait_uses_creation_time_when_not_started() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    creation_time = now - timedelta(minutes=5)

    pod = _image_pull_pod(start_time=None, creation_time=creation_time)

    assert pod_health(pod, now).delete is True


def test_invalid_image_name_is_a_non_replacing_permanent_error() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)
    pod = _image_pull_pod(
        start_time=now - timedelta(hours=1),
        creation_time=now - timedelta(hours=1),
        reason="InvalidImageName",
    )

    health = pod_health(pod, now)

    assert health.reason == "InvalidImageName"
    assert health.unhealthy is True
    assert health.delete is False
    assert health.permanent is True


def test_crash_loop_backoff_is_not_a_waiting_replacement_reason() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)
    pod = _image_pull_pod(
        start_time=now - timedelta(hours=1),
        creation_time=now - timedelta(hours=1),
        reason="CrashLoopBackOff",
    )

    health = pod_health(pod, now)

    assert health.reason == "CrashLoopBackOff"
    assert health.unhealthy is False
    assert health.delete is False
    assert health.permanent is False


@pytest.mark.parametrize(
    ("phase", "conditions", "expected"),
    [
        ("Running", [make_condition("Ready", "True")], True),
        ("Running", [make_condition("Ready", "False")], False),
        ("Pending", [make_condition("Ready", "True")], False),
    ],
)
def test_pod_health_only_marks_running_ready_pods_ready(
    phase: str, conditions: list[V1PodCondition], expected: bool
) -> None:
    pod = make_pod(phase=phase, conditions=conditions)

    health = pod_health(pod, datetime(2026, 7, 15, tzinfo=timezone.utc))

    assert health.ready is expected
    assert health.delete is False


@pytest.mark.parametrize(
    ("pod_kwargs", "reason"),
    [
        ({"phase": "Succeeded"}, "Succeeded"),
        ({"phase": "Failed", "reason": "Evicted"}, "Evicted"),
        (
            {
                "phase": "Running",
                "container_statuses": [make_terminated_container_status(137, "OOMKilled")],
            },
            "OOMKilled",
        ),
        (
            {
                "phase": "Running",
                "container_statuses": [make_terminated_container_status(9)],
            },
            "ExitCode9",
        ),
        (
            {
                "phase": "Pending",
                "init_container_statuses": [make_terminated_container_status(1, "Error")],
            },
            "InitContainerError",
        ),
    ],
)
def test_pod_health_replaces_terminal_failures(pod_kwargs: dict[str, Any], reason: str) -> None:
    health = pod_health(
        make_pod(**pod_kwargs),
        datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    assert health.reason == reason
    assert health.delete is True
    assert health.force is False


@pytest.mark.parametrize("statuses_key", ["container_statuses", "init_container_statuses"])
def test_completed_container_does_not_fail_pod(statuses_key: str) -> None:
    health = pod_health(
        make_pod(
            phase="Running",
            **{statuses_key: [make_terminated_container_status(0, "Completed")]},
        ),
        datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    assert health.reason is None
    assert health.delete is False


@pytest.mark.parametrize(
    ("statuses_key", "reason", "expected_reason", "permanent"),
    [
        ("init_container_statuses", "ImagePullBackOff", "InitContainerImagePullBackOff", False),
        ("init_container_statuses", "InvalidImageName", "InitContainerInvalidImageName", True),
    ],
)
def test_pod_health_classifies_init_and_permanent_waiting_reasons(
    statuses_key: str, reason: str, expected_reason: str, permanent: bool
) -> None:
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    health = pod_health(
        make_pod(
            creation_timestamp=now,
            phase="Pending",
            **{statuses_key: [make_waiting_container_status(reason)]},
        ),
        now,
    )

    assert health.reason == expected_reason
    assert health.permanent is permanent


@pytest.mark.parametrize(
    ("age", "delete", "force", "reason"),
    [
        (599, False, False, "Terminating"),
        (600, True, True, "StuckTerminating"),
    ],
)
def test_pod_health_force_deletes_only_stuck_terminating_pods(
    age: int, delete: bool, force: bool, reason: str
) -> None:
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    health = pod_health(
        make_pod(deletion_timestamp=now - timedelta(seconds=age)),
        now,
    )

    assert health.reason == reason
    assert health.unhealthy is delete
    assert health.delete is delete
    assert health.force is force


def test_pod_health_handles_pod_without_status() -> None:
    health = pod_health(
        V1Pod(metadata=V1ObjectMeta(name="consumer-0-0")),
        datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    assert health.ready is False
    assert health.delete is False


def test_pod_spec_changed() -> None:
    current = make_pod(
        labels={ORDINAL_LABEL: "0"},
        annotations={SPEC_HASH_ANNOTATION: "old"},
        phase="Pending",
    )
    desired: V1PodDict = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "consumer-0-0",
            "annotations": {SPEC_HASH_ANNOTATION: "new"},
        },
        "spec": {},
    }

    assert pod_spec_changed(current, desired) is True

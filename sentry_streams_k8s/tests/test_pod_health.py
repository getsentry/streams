from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sentry_streams_k8s.operator.constants import ORDINAL_LABEL, SPEC_HASH_ANNOTATION
from sentry_streams_k8s.operator.pod_health import (
    pod_health,
    pod_spec_changed,
)


def _image_pull_pod(
    *,
    now: datetime,
    start_time: datetime | None,
    creation_time: datetime,
    reason: str = "ImagePullBackOff",
) -> dict[str, Any]:
    status: dict[str, Any] = {
        "phase": "Pending",
        "containerStatuses": [
            {
                "state": {"waiting": {"reason": reason}},
                "lastState": {"terminated": {"finishedAt": (now - timedelta(hours=1)).isoformat()}},
            }
        ],
    }
    if start_time is not None:
        status["startTime"] = start_time.isoformat()
    return {
        "metadata": {
            "name": "consumer-0-0",
            "creationTimestamp": creation_time.isoformat(),
        },
        "status": status,
    }


def test_image_pull_wait_uses_pod_start_time_for_grace() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    start_time = now - timedelta(seconds=299)
    creation_time = now - timedelta(hours=1)

    pod = _image_pull_pod(now=now, start_time=start_time, creation_time=creation_time)

    assert pod_health(pod, now).reason == "ImagePullBackOff"
    assert pod_health(pod, now).delete is False


def test_image_pull_wait_is_deleted_after_start_time_grace() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    start_time = now - timedelta(minutes=5)
    creation_time = now - timedelta(hours=1)

    pod = _image_pull_pod(now=now, start_time=start_time, creation_time=creation_time)

    assert pod_health(pod, now).delete is True


def test_image_pull_wait_uses_creation_time_when_not_started() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)

    creation_time = now - timedelta(minutes=5)

    pod = _image_pull_pod(now=now, start_time=None, creation_time=creation_time)

    assert pod_health(pod, now).delete is True


def test_invalid_image_name_is_a_non_replacing_permanent_error() -> None:
    now = datetime(2026, 7, 15, 20, 0, tzinfo=timezone.utc)
    pod = _image_pull_pod(
        now=now,
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
        now=now,
        start_time=now - timedelta(hours=1),
        creation_time=now - timedelta(hours=1),
        reason="CrashLoopBackOff",
    )

    health = pod_health(pod, now)

    assert health.reason is None
    assert health.delete is False


@pytest.mark.parametrize(
    ("phase", "conditions", "expected"),
    [
        ("Running", [{"type": "Ready", "status": "True"}], True),
        ("Running", [{"type": "Ready", "status": "False"}], False),
        ("Pending", [{"type": "Ready", "status": "True"}], False),
    ],
)
def test_pod_health_only_marks_running_ready_pods_ready(
    phase: str, conditions: list[dict[str, str]], expected: bool
) -> None:
    pod = {
        "metadata": {"name": "consumer-0-0"},
        "status": {"phase": phase, "conditions": conditions},
    }

    health = pod_health(pod, datetime(2026, 7, 15, tzinfo=timezone.utc))

    assert health.ready is expected
    assert health.delete is False


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ({"phase": "Succeeded"}, "Succeeded"),
        ({"phase": "Failed", "reason": "Evicted"}, "Evicted"),
        (
            {
                "phase": "Running",
                "containerStatuses": [
                    {"state": {"terminated": {"exitCode": 137, "reason": "OOMKilled"}}}
                ],
            },
            "OOMKilled",
        ),
        (
            {"phase": "Running", "containerStatuses": [{"state": {"terminated": {"exitCode": 9}}}]},
            "ExitCode9",
        ),
        (
            {
                "phase": "Pending",
                "initContainerStatuses": [
                    {"state": {"terminated": {"exitCode": 1, "reason": "Error"}}}
                ],
            },
            "InitContainerError",
        ),
    ],
)
def test_pod_health_replaces_terminal_failures(status: dict[str, Any], reason: str) -> None:
    health = pod_health(
        {"metadata": {"name": "consumer-0-0"}, "status": status},
        datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    assert health.reason == reason
    assert health.delete is True
    assert health.force is False


@pytest.mark.parametrize("statuses_key", ["containerStatuses", "initContainerStatuses"])
def test_completed_container_does_not_fail_pod(statuses_key: str) -> None:
    health = pod_health(
        {
            "metadata": {"name": "consumer-0-0"},
            "status": {
                "phase": "Running",
                statuses_key: [{"state": {"terminated": {"exitCode": 0, "reason": "Completed"}}}],
            },
        },
        datetime(2026, 7, 15, tzinfo=timezone.utc),
    )

    assert health.reason is None
    assert health.delete is False


@pytest.mark.parametrize(
    ("statuses_key", "reason", "expected_reason", "permanent"),
    [
        ("initContainerStatuses", "ImagePullBackOff", "InitContainerImagePullBackOff", False),
        ("initContainerStatuses", "InvalidImageName", "InitContainerInvalidImageName", True),
        ("containerStatuses", "InvalidImageName", "InvalidImageName", True),
    ],
)
def test_pod_health_classifies_init_and_permanent_waiting_reasons(
    statuses_key: str, reason: str, expected_reason: str, permanent: bool
) -> None:
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    health = pod_health(
        {
            "metadata": {"name": "consumer-0-0", "creationTimestamp": now.isoformat()},
            "status": {
                "phase": "Pending",
                statuses_key: [{"state": {"waiting": {"reason": reason}}}],
            },
        },
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
        {
            "metadata": {
                "name": "consumer-0-0",
                "deletionTimestamp": (now - timedelta(seconds=age)).isoformat(),
            }
        },
        now,
    )

    assert health.reason == reason
    assert health.unhealthy is delete
    assert health.delete is delete
    assert health.force is force


def test_pod_spec_changed() -> None:
    current = {
        "metadata": {
            "name": "consumer-0-0",
            "labels": {ORDINAL_LABEL: "0"},
            "annotations": {SPEC_HASH_ANNOTATION: "old"},
        },
        "status": {"phase": "Pending"},
    }
    desired = {
        "metadata": {"annotations": {SPEC_HASH_ANNOTATION: "new"}},
    }

    assert pod_spec_changed(current, desired) is True

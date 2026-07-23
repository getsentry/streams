from __future__ import annotations

from datetime import datetime, timezone

from sentry_streams_k8s.operator.constants import ORDINAL_LABEL, SPEC_HASH_ANNOTATION
from sentry_streams_k8s.operator.pod_health import (
    PodHealth,
    pod_health,
    pod_spec_changed,
)
from sentry_streams_k8s.operator.pod_status import reported_pod_status


def test_reported_pod_status() -> None:
    pod = {
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
    health = PodHealth(name="consumer-0-0", ready=False, reason="ImagePullBackOff", unhealthy=True)

    assert pod_spec_changed(pod, desired) is True
    assert reported_pod_status(pod, health).to_status_dict() == {
        "name": "consumer-0-0",
        "ready": False,
        "phase": "Pending",
        "ordinal": "0",
        "reason": "ImagePullBackOff",
    }


def test_terminating_pod_is_not_reported_as_unhealthy() -> None:
    now = datetime(2026, 7, 15, tzinfo=timezone.utc)
    pod = {
        "metadata": {
            "name": "consumer-0-0",
            "deletionTimestamp": now.isoformat(),
            "labels": {ORDINAL_LABEL: "0"},
        },
        "status": {"phase": "Running"},
    }

    status = reported_pod_status(pod, pod_health(pod, now))

    assert status.reason == "Terminating"
    assert status.is_unhealthy is False

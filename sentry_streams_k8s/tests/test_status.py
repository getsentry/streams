from __future__ import annotations

from datetime import datetime, timezone

from kubernetes.client import V1ObjectMeta, V1Pod

from sentry_streams_k8s.operator.constants import ORDINAL_LABEL, SPEC_HASH_ANNOTATION
from sentry_streams_k8s.operator.pod_health import (
    PodHealth,
    pod_health,
    pod_spec_changed,
)
from sentry_streams_k8s.operator.pod_status import reported_pod_status
from sentry_streams_k8s.operator.pod_types import PodManifest
from tests.k8s_fixtures import make_pod


def test_reported_pod_status() -> None:
    pod = make_pod(
        labels={ORDINAL_LABEL: "0"},
        annotations={SPEC_HASH_ANNOTATION: "old"},
        phase="Pending",
    )
    desired: PodManifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "consumer-0-0",
            "annotations": {SPEC_HASH_ANNOTATION: "new"},
        },
        "spec": {},
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
    pod = make_pod(
        deletion_timestamp=now,
        labels={ORDINAL_LABEL: "0"},
        phase="Running",
    )

    status = reported_pod_status(pod, pod_health(pod, now))

    assert status.reason == "Terminating"
    assert status.is_unhealthy is False


def test_reported_pod_status_handles_pod_without_status() -> None:
    pod = V1Pod(metadata=V1ObjectMeta(name="consumer-0-0"))

    status = reported_pod_status(pod, PodHealth(name="consumer-0-0"))

    assert status.phase == "Unknown"

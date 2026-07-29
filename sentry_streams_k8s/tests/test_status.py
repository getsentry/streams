from __future__ import annotations

from kubernetes.client import V1ObjectMeta, V1Pod

from sentry_streams_k8s.operator.constants import ORDINAL_LABEL
from sentry_streams_k8s.operator.pod_health import PodHealth
from sentry_streams_k8s.operator.pod_status import reported_pod_status
from tests.k8s_fixtures import make_pod


def test_reported_pod_status() -> None:
    pod = make_pod(
        labels={ORDINAL_LABEL: "0"},
        phase="Pending",
    )
    health = PodHealth(name="consumer-0-0", ready=False, reason="ImagePullBackOff", unhealthy=True)

    assert reported_pod_status(pod, health).to_status_dict() == {
        "name": "consumer-0-0",
        "ready": False,
        "phase": "Pending",
        "ordinal": "0",
        "reason": "ImagePullBackOff",
    }


def test_reported_pod_status_handles_pod_without_status() -> None:
    pod = V1Pod(metadata=V1ObjectMeta(name="consumer-0-0"))

    status = reported_pod_status(pod, PodHealth(name="consumer-0-0"))

    assert status.phase == "Unknown"

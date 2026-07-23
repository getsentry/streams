"""
pod_health classifies a Pod into a PodHealth verdict that controls reconcile
actions such as delete, force-delete, and replacement. This module serializes that
verdict with Pod metadata fields into the StreamingPipeline CR's status.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sentry_streams_k8s.operator.constants import ORDINAL_LABEL
from sentry_streams_k8s.operator.pod_health import PodHealth, pod_labels, pod_status


@dataclass(frozen=True)
class ReportedPodStatus:
    name: str
    ready: bool
    phase: str
    unhealthy: bool = False
    ordinal: str | None = None
    reason: str | None = None
    permanent: bool = False

    @property
    def is_unhealthy(self) -> bool:
        return self.unhealthy

    def to_status_dict(self) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "name": self.name,
            "ready": self.ready,
            "phase": self.phase,
        }
        if self.ordinal is not None:
            entry["ordinal"] = self.ordinal
        if self.reason is not None:
            entry["reason"] = self.reason
        if self.permanent:
            entry["permanent"] = True
        return entry


def reported_pod_status(pod: Mapping[str, Any], health: PodHealth) -> ReportedPodStatus:
    labels = pod_labels(pod)
    phase = pod_status(pod).get("phase", "Unknown")
    return ReportedPodStatus(
        name=health.name,
        ready=health.ready,
        phase=phase if isinstance(phase, str) else "Unknown",
        unhealthy=health.unhealthy,
        ordinal=labels.get(ORDINAL_LABEL),
        reason=health.reason,
        permanent=health.permanent,
    )

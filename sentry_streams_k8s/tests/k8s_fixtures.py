from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from kubernetes.client import (
    V1ConfigMap,
    V1ConfigMapList,
    V1ContainerState,
    V1ContainerStateTerminated,
    V1ContainerStateWaiting,
    V1ContainerStatus,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodList,
    V1PodStatus,
)

from sentry_streams_k8s.k8s_types import V1ConfigMapDict, V1PodDict
from sentry_streams_k8s.operator.control_client import ControlError, RuntimeState


def _matches_selector(labels: Mapping[str, str] | None, selector: str | None) -> bool:
    if not selector:
        return True
    labels = labels or {}
    return all(
        labels.get(key) == value
        for requirement in selector.split(",")
        for key, value in [requirement.split("=", maxsplit=1)]
    )


@dataclass
class FakeCoreV1Api:
    """In-memory CoreV1Api subset used by operator tests."""

    pods: list[V1Pod] = field(default_factory=list)
    configmaps: list[V1ConfigMap] = field(default_factory=list)

    applied_pods: list[V1PodDict] = field(default_factory=list, init=False)
    deleted_pods: list[tuple[str, bool]] = field(default_factory=list, init=False)

    applied_configmaps: list[V1ConfigMapDict] = field(default_factory=list, init=False)
    deleted_configmaps: list[str] = field(default_factory=list, init=False)

    pod_list_calls: list[tuple[str, str | None]] = field(default_factory=list, init=False)
    configmap_list_calls: list[tuple[str, str | None]] = field(default_factory=list, init=False)

    pod_patch_calls: list[dict[str, Any]] = field(default_factory=list, init=False)
    configmap_patch_calls: list[dict[str, Any]] = field(default_factory=list, init=False)

    operations: list[str] = field(default_factory=list, init=False)

    def list_namespaced_pod(
        self,
        *,
        namespace: str,
        label_selector: str | None = None,
    ) -> V1PodList:
        self.pod_list_calls.append((namespace, label_selector))
        items = [
            copy.deepcopy(pod)
            for pod in self.pods
            if _matches_selector(pod.metadata.labels if pod.metadata else None, label_selector)
        ]
        return V1PodList(items=items)

    def patch_namespaced_pod(
        self,
        *,
        name: str,
        namespace: str,
        body: object,
        field_manager: str,
        force: bool,
        _content_type: str,
    ) -> None:
        self.applied_pods.append(copy.deepcopy(cast(V1PodDict, body)))
        self.pod_patch_calls.append(
            {
                "name": name,
                "namespace": namespace,
                "body": copy.deepcopy(body),
                "field_manager": field_manager,
                "force": force,
                "_content_type": _content_type,
            }
        )
        self.operations.append(f"apply:{name}")

    def delete_namespaced_pod(
        self,
        *,
        name: str,
        namespace: str,
        body: object | None = None,
    ) -> None:
        del namespace
        force = getattr(body, "grace_period_seconds", None) == 0
        self.deleted_pods.append((name, force))
        self.operations.append(f"delete:{name}")

    def list_namespaced_config_map(
        self,
        *,
        namespace: str,
        label_selector: str | None = None,
    ) -> V1ConfigMapList:
        self.configmap_list_calls.append((namespace, label_selector))
        items = [
            copy.deepcopy(configmap)
            for configmap in self.configmaps
            if _matches_selector(
                configmap.metadata.labels if configmap.metadata else None,
                label_selector,
            )
        ]
        return V1ConfigMapList(items=items)

    def patch_namespaced_config_map(
        self,
        *,
        name: str,
        namespace: str,
        body: object,
        field_manager: str,
        force: bool,
        _content_type: str,
    ) -> None:
        self.applied_configmaps.append(copy.deepcopy(cast(V1ConfigMapDict, body)))
        self.configmap_patch_calls.append(
            {
                "name": name,
                "namespace": namespace,
                "body": copy.deepcopy(body),
                "field_manager": field_manager,
                "force": force,
                "_content_type": _content_type,
            }
        )
        self.operations.append(f"apply-configmap:{name}")

    def delete_namespaced_config_map(self, *, name: str, namespace: str) -> None:
        del namespace
        self.deleted_configmaps.append(name)
        self.operations.append(f"delete-configmap:{name}")


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
    pod_ip: str | None = None,
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
            pod_ip=pod_ip,
        ),
    )


@dataclass
class FakeControlClient:
    """In-memory control client used by operator tests."""

    states: dict[str, RuntimeState] = field(default_factory=dict)
    unreachable: set[str] = field(default_factory=set)

    started: list[tuple[str, str]] = field(default_factory=list, init=False)
    stopped: list[str] = field(default_factory=list, init=False)

    def status(self, ip: str) -> RuntimeState | None:
        if ip in self.unreachable:
            return None
        return self.states.get(ip)

    def readyz(self, ip: str) -> bool:
        return ip not in self.unreachable

    def start(self, ip: str, group_instance_id: str) -> None:
        if ip in self.unreachable:
            raise ControlError(f"cannot reach {ip}")
        self.started.append((ip, group_instance_id))
        self.states[ip] = RuntimeState.CONSUMING

    def stop(self, ip: str) -> bool:
        if ip in self.unreachable:
            return False
        self.stopped.append(ip)
        self.states[ip] = RuntimeState.STOPPED
        return True

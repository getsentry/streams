from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from typing import Any, cast

from kubernetes import client
from kubernetes.client import V1Pod, V1PodList

from sentry_streams_k8s.operator.constants import (
    FIELD_MANAGER,
    GENERATION_LABEL,
    MANAGED_BY_LABEL,
    ORDINAL_LABEL,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    SPEC_HASH_ANNOTATION,
    WORKLOAD_SET_LABEL,
    Logger,
)
from sentry_streams_k8s.operator.pod_health import PodHealth, is_deleting
from sentry_streams_k8s.operator.pod_types import PodManifest


def list_owned_pods(
    core: client.CoreV1Api,
    namespace: str,
    owner_uid: str,
    workload_set: str | None = None,
) -> list[V1Pod]:
    selector = f"{OWNER_UID_LABEL}={owner_uid}"
    if workload_set is not None:
        selector = f"{selector},{WORKLOAD_SET_LABEL}={workload_set}"
    listing = cast(
        V1PodList, core.list_namespaced_pod(namespace=namespace, label_selector=selector)
    )
    return cast(list[V1Pod], listing.items)


def apply_pod(core: client.CoreV1Api, pod: PodManifest, namespace: str) -> None:
    core.patch_namespaced_pod(
        name=pod["metadata"]["name"],
        namespace=namespace,
        body=dict(pod),
        field_manager=FIELD_MANAGER,
        force=True,
        _content_type="application/apply-patch+yaml",
    )


def delete_pod(core: client.CoreV1Api, name: str, namespace: str, force: bool = False) -> None:
    if force:
        # Only use this for a Pod that has been stuck terminating:
        core.delete_namespaced_pod(
            name=name,
            namespace=namespace,
            body=client.V1DeleteOptions(grace_period_seconds=0),
        )
    else:
        core.delete_namespaced_pod(name=name, namespace=namespace)


def pod_name(pod: V1Pod) -> str:
    metadata = pod.metadata
    return metadata.name if metadata and metadata.name else ""


def consumer_pod_name(base_name: str, ordinal: int, generation: int) -> str:
    return f"{base_name}-{ordinal}-{generation}"


def pod_ordinal(pod: V1Pod) -> int | None:
    metadata = pod.metadata
    label = (metadata.labels or {}).get(ORDINAL_LABEL) if metadata else None
    if label is None:
        return None
    try:
        return int(label)
    except ValueError:
        return None


def pod_generation(pod: V1Pod) -> int:
    metadata = pod.metadata
    label = (metadata.labels or {}).get(GENERATION_LABEL) if metadata else None
    if label is None:
        return 0
    try:
        return int(label)
    except ValueError:
        return 0


def pod_workload_set(pod: V1Pod) -> str | None:
    metadata = pod.metadata
    return (metadata.labels or {}).get(WORKLOAD_SET_LABEL) if metadata else None


def pod_keep_key(pod: V1Pod, health: PodHealth) -> tuple[bool, int, str]:
    return (health.ready, pod_generation(pod), pod_name(pod))


def pod_template_from_deployment(
    deployment: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    template = deployment["spec"]["template"]
    return template.get("metadata", {}) or {}, template["spec"]


def _pod_spec_hash(pod: PodManifest) -> str:
    metadata = pod["metadata"]
    labels = {
        key: value
        for key, value in (metadata.get("labels", {}) or {}).items()
        if key != GENERATION_LABEL
    }
    annotations = {
        key: value
        for key, value in (metadata.get("annotations", {}) or {}).items()
        if key != SPEC_HASH_ANNOTATION
    }
    desired = {
        "metadata": {"labels": labels, "annotations": annotations},
        "spec": pod["spec"],
    }
    encoded = json.dumps(desired, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def build_pipeline_pod(
    *,
    base_name: str,
    template_metadata: Mapping[str, Any],
    template_spec: Mapping[str, Any],
    ordinal: int,
    generation: int,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    workload_set: str,
) -> PodManifest:
    pod_spec = copy.deepcopy(dict(template_spec))

    # The operator replaces unhealthy Pods rather than letting k8s restart them:

    pod_spec["restartPolicy"] = "Never"

    # Pods can live in a different namespace from their StreamingPipeline, so they
    # cannot use owner references. The operator finds them by owner label instead.

    labels = {
        **(template_metadata.get("labels", {}) or {}),
        MANAGED_BY_LABEL: FIELD_MANAGER,
        OWNER_UID_LABEL: owner_uid,
        WORKLOAD_SET_LABEL: workload_set,
        ORDINAL_LABEL: str(ordinal),
        GENERATION_LABEL: str(generation),
    }

    annotations = {
        **(template_metadata.get("annotations", {}) or {}),
        OWNER_NAME_ANNOTATION: owner_name,
        OWNER_NAMESPACE_ANNOTATION: owner_namespace,
    }

    pod: PodManifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": consumer_pod_name(base_name, ordinal, generation),
            "labels": labels,
            "annotations": annotations,
        },
        "spec": pod_spec,
    }

    annotations[SPEC_HASH_ANNOTATION] = _pod_spec_hash(pod)

    return pod


def delete_owned_pods(
    core: client.CoreV1Api, namespace: str, owner_uid: str, logger: Logger
) -> None:
    for pod in list_owned_pods(core, namespace, owner_uid):
        name = pod_name(pod)
        if is_deleting(pod):
            continue
        delete_pod(core, name, namespace)
        logger.info("deleted pipeline Pod %s/%s reason=OwnerDeleted", namespace, name)

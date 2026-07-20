from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping
from typing import Any, cast

from kubernetes import client
from kubernetes.dynamic import DynamicClient

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
from sentry_streams_k8s.operator.pod_health import (
    is_deleting,
    pod_labels,
    pod_metadata,
)


def _pod_resource(dyn: DynamicClient) -> Any:
    return dyn.resources.get(api_version="v1", kind="Pod")


def _to_dict(obj: Any) -> dict[str, Any]:
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        return cast(dict[str, Any], to_dict())
    return cast(dict[str, Any], client.ApiClient().sanitize_for_serialization(obj))


def list_owned_pods(
    dyn: DynamicClient,
    namespace: str,
    owner_uid: str,
    workload_set: str | None = None,
) -> list[dict[str, Any]]:
    selector = f"{OWNER_UID_LABEL}={owner_uid}"
    if workload_set is not None:
        selector = f"{selector},{WORKLOAD_SET_LABEL}={workload_set}"
    listing = _pod_resource(dyn).get(namespace=namespace, label_selector=selector)
    return [_to_dict(item) for item in listing.items]


def apply_pod(dyn: DynamicClient, pod: Mapping[str, Any], namespace: str) -> None:
    _pod_resource(dyn).server_side_apply(
        body=pod,
        name=pod_metadata(pod)["name"],
        namespace=namespace,
        field_manager=FIELD_MANAGER,
        force_conflicts=True,
    )


def delete_pod(dyn: DynamicClient, name: str, namespace: str, force: bool = False) -> None:
    if force:
        # Only use this for a Pod that has been stuck terminating:
        _pod_resource(dyn).delete(
            name=name,
            namespace=namespace,
            body={"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 0},
        )
    else:
        _pod_resource(dyn).delete(name=name, namespace=namespace)


def pod_name(pod: Mapping[str, Any]) -> str:
    return cast(str, pod_metadata(pod).get("name", ""))


def consumer_pod_name(base_name: str, ordinal: int, generation: int) -> str:
    return f"{base_name}-{ordinal}-{generation}"


def pod_ordinal(pod: Mapping[str, Any]) -> int | None:
    label = pod_labels(pod).get(ORDINAL_LABEL)
    if label is None:
        return None
    try:
        return int(label)
    except ValueError:
        return None


def pod_generation(pod: Mapping[str, Any]) -> int:
    label = pod_labels(pod).get(GENERATION_LABEL)
    if label is None:
        return 0
    try:
        return int(label)
    except ValueError:
        return 0


def pod_workload_set(pod: Mapping[str, Any]) -> str | None:
    return pod_labels(pod).get(WORKLOAD_SET_LABEL)


def pod_keep_key(pod: Mapping[str, Any], health: Mapping[str, Any]) -> tuple[bool, int, str]:
    return (cast(bool, health["ready"]), pod_generation(pod), pod_name(pod))


def pod_template_from_deployment(
    deployment: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    template = cast(Mapping[str, Any], deployment["spec"]["template"])
    return cast(Mapping[str, Any], template.get("metadata", {}) or {}), cast(
        Mapping[str, Any], template["spec"]
    )


def _pod_spec_hash(pod: Mapping[str, Any]) -> str:
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
) -> dict[str, Any]:
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

    pod: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": consumer_pod_name(base_name, ordinal, generation),
            "labels": labels,
            "annotations": annotations,
        },
        "spec": pod_spec,
    }

    pod["metadata"]["annotations"][SPEC_HASH_ANNOTATION] = _pod_spec_hash(pod)

    return pod


def delete_owned_pods(dyn: DynamicClient, namespace: str, owner_uid: str, logger: Logger) -> None:
    for pod in list_owned_pods(dyn, namespace, owner_uid):
        name = pod_name(pod)
        if is_deleting(pod):
            continue
        delete_pod(dyn, name, namespace)
        logger.info("deleted pipeline Pod %s/%s reason=OwnerDeleted", namespace, name)

from __future__ import annotations

import logging
from typing import Any

import kopf
from kubernetes import client

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator.constants import (
    APPLY_PATCH_CONTENT_TYPE,
    FIELD_MANAGER,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render,
    validate,
)

logger = logging.getLogger(__name__)


def _prepare_manifest(
    manifest: dict[str, Any],
    *,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
) -> None:
    metadata = manifest.setdefault("metadata", {})
    metadata["namespace"] = workload_namespace
    metadata["labels"] = {
        **metadata.get("labels", {}),
        OWNER_UID_LABEL: owner_uid,
    }
    metadata["annotations"] = {
        **metadata.get("annotations", {}),
        OWNER_NAME_ANNOTATION: owner_name,
        OWNER_NAMESPACE_ANNOTATION: owner_namespace,
    }


def _apply_configmap(
    core: client.CoreV1Api,
    manifest: dict[str, Any],
    *,
    workload_namespace: str,
) -> None:
    core.patch_namespaced_config_map(
        name=manifest["metadata"]["name"],
        namespace=workload_namespace,
        body=manifest,
        field_manager=FIELD_MANAGER,
        force=True,
        _content_type=APPLY_PATCH_CONTENT_TYPE,
    )


def _apply_deployment(
    apps: client.AppsV1Api,
    manifest: dict[str, Any],
    *,
    workload_namespace: str,
) -> None:
    apps.patch_namespaced_deployment(
        name=manifest["metadata"]["name"],
        namespace=workload_namespace,
        body=manifest,
        field_manager=FIELD_MANAGER,
        force=True,
        _content_type=APPLY_PATCH_CONTENT_TYPE,
    )


def _prune_stale_resources(
    *,
    workload_namespace: str,
    owner_uid: str,
    desired_deployments: set[str],
    desired_configmaps: set[str],
) -> None:
    selector = f"{OWNER_UID_LABEL}={owner_uid}"

    apps = client.AppsV1Api()
    deployments = apps.list_namespaced_deployment(
        namespace=workload_namespace,
        label_selector=selector,
    )
    for deployment in deployments.items:
        if deployment.metadata.name not in desired_deployments:
            logger.info(
                "Pruning stale deployment %s/%s",
                workload_namespace,
                deployment.metadata.name,
            )
            apps.delete_namespaced_deployment(
                name=deployment.metadata.name,
                namespace=workload_namespace,
            )

    core = client.CoreV1Api()
    configmaps = core.list_namespaced_config_map(
        namespace=workload_namespace,
        label_selector=selector,
    )
    for configmap in configmaps.items:
        if configmap.metadata.name not in desired_configmaps:
            logger.info(
                "Pruning stale configmap %s/%s",
                workload_namespace,
                configmap.metadata.name,
            )
            core.delete_namespaced_config_map(
                name=configmap.metadata.name,
                namespace=workload_namespace,
            )


def _condition(type_: str, status: bool, reason: str, message: str = "") -> dict[str, Any]:
    return {
        "type": type_,
        "status": "True" if status else "False",
        "reason": reason,
        "message": message,
    }


def reconcile_pipeline(
    *,
    spec: Any,
    name: str,
    namespace: str,
    uid: str,
    workload_namespace: str,
    patch: kopf.Patch,
) -> None:
    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render(consumer)
    except Exception as e:
        patch.status["conditions"] = [_condition("Rendered", False, type(e).__name__, str(e))]
        raise kopf.PermanentError(f"StreamingPipeline {namespace}/{name} failed to render: {e}")

    manifests = [result["configmap"], result["deployment"]]
    if "canary_deployment" in result:
        manifests.append(result["canary_deployment"])

    core = client.CoreV1Api()
    apps = client.AppsV1Api()

    for manifest in manifests:
        _prepare_manifest(
            manifest,
            workload_namespace=workload_namespace,
            owner_uid=uid,
            owner_name=name,
            owner_namespace=namespace,
        )
        kind = manifest["kind"]
        if kind == "ConfigMap":
            _apply_configmap(core, manifest, workload_namespace=workload_namespace)
        elif kind == "Deployment":
            _apply_deployment(apps, manifest, workload_namespace=workload_namespace)
        else:
            raise kopf.PermanentError(f"Cannot apply unsupported manifest kind {kind}.")

    _prune_stale_resources(
        workload_namespace=workload_namespace,
        owner_uid=uid,
        desired_deployments={
            manifest["metadata"]["name"]
            for manifest in manifests
            if manifest["kind"] == "Deployment"
        },
        desired_configmaps={
            manifest["metadata"]["name"]
            for manifest in manifests
            if manifest["kind"] == "ConfigMap"
        },
    )

    replicas = consumer.get("replicas", 1)
    canary = 1 if "canary_deployment" in result else 0
    patch.status["conditions"] = [
        _condition("Rendered", True, "Rendered"),
        _condition("Applied", True, "Applied"),
    ]
    patch.status["config_version"] = compute_config_version(consumer["pipeline_config"])
    patch.status["replicas"] = {"primary": replicas - canary, "canary": canary}
    patch.status["workload_namespace"] = workload_namespace

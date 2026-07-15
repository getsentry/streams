from __future__ import annotations

import logging
import os
from typing import Any

import kopf
from kubernetes import client, dynamic
from kubernetes.client.exceptions import ApiException

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render,
    validate,
)

logger = logging.getLogger(__name__)

GROUP = "streams.sentry.io"
VERSION = "v1alpha1"
PLURAL = "streamingpipelines"
FIELD_MANAGER = "streaming-operator"
WORKLOAD_NAMESPACE_ENV = "WORKLOAD_NAMESPACE"
OWNER_UID_LABEL = "streams.sentry.io/owner-uid"
OWNER_NAME_ANNOTATION = "streams.sentry.io/owner-name"
OWNER_NAMESPACE_ANNOTATION = "streams.sentry.io/owner-namespace"


def _workload_namespace() -> str:
    namespace = os.environ.get(WORKLOAD_NAMESPACE_ENV, "").strip()
    if not namespace:
        raise RuntimeError(f"{WORKLOAD_NAMESPACE_ENV} must be set.")
    return namespace


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


def _apply(
    dyn: dynamic.DynamicClient,
    manifest: dict[str, Any],
    *,
    workload_namespace: str,
    owner_uid: str,
) -> None:
    resource = dyn.resources.get(api_version=manifest["apiVersion"], kind=manifest["kind"])
    name = manifest["metadata"]["name"]
    try:
        existing = resource.get(name=name, namespace=workload_namespace)
    except ApiException as e:
        if e.status != 404:
            raise
    else:
        labels = existing.metadata.labels or {}
        existing_owner_uid = labels.get(OWNER_UID_LABEL)
        if existing_owner_uid != owner_uid:
            raise kopf.PermanentError(
                f"{manifest['kind']} {workload_namespace}/{name} is already present and is not "
                "managed by this StreamingPipeline."
            )

    dyn.server_side_apply(
        resource,
        body=manifest,
        namespace=workload_namespace,
        field_manager=f"{FIELD_MANAGER}-{owner_uid}",
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


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(
    spec: kopf.Spec,
    name: str,
    namespace: str | None,
    uid: str,
    patch: kopf.Patch,
    **_: Any,
) -> None:
    assert namespace is not None
    workload_namespace = _workload_namespace()

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

    dyn = dynamic.DynamicClient(client.ApiClient())
    for manifest in manifests:
        _prepare_manifest(
            manifest,
            workload_namespace=workload_namespace,
            owner_uid=uid,
            owner_name=name,
            owner_namespace=namespace,
        )
        _apply(
            dyn,
            manifest,
            workload_namespace=workload_namespace,
            owner_uid=uid,
        )

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


@kopf.on.delete(GROUP, VERSION, PLURAL)
def cleanup(uid: str, **_: Any) -> None:
    _prune_stale_resources(
        workload_namespace=_workload_namespace(),
        owner_uid=uid,
        desired_deployments=set(),
        desired_configmaps=set(),
    )


def main() -> None:
    _workload_namespace()
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

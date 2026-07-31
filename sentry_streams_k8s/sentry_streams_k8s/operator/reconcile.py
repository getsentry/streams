from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypedDict, cast

import kopf
from kubernetes import client
from kubernetes.client import V1Condition

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.k8s_types import (
    V1ConditionDict,
    V1ConfigMapDict,
    V1DeploymentDict,
)
from sentry_streams_k8s.operator.constants import (
    APPLY_PATCH_CONTENT_TYPE,
    FIELD_MANAGER,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    Logger,
)
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render,
    validate,
)


class PipelineStatusPatch(TypedDict, total=False):
    conditions: list[V1ConditionDict]
    config_version: str
    replicas: dict[str, int]
    workload_namespace: str


def _prepare_manifest(
    manifest: V1ConfigMapDict | V1DeploymentDict,
    *,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
) -> None:
    metadata = manifest["metadata"]
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
    manifest: V1ConfigMapDict,
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
    manifest: V1DeploymentDict,
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
    logger: Logger,
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


def _merge_conditions(
    previous: list[V1ConditionDict] | None,
    conditions: list[V1ConditionDict],
) -> list[V1ConditionDict]:
    """
    Carries lastTransitionTime over from the conditions already published.
    """

    published = {condition["type"]: condition for condition in previous or []}

    merged: list[V1ConditionDict] = []
    for condition in conditions:
        existing = published.get(condition["type"])
        if existing is not None and existing.get("status") == condition["status"]:
            condition = {**condition, "lastTransitionTime": existing["lastTransitionTime"]}
        merged.append(condition)
    return merged


def reconcile_pipeline(
    *,
    spec: Any,
    name: str,
    namespace: str,
    uid: str,
    workload_namespace: str,
    logger: Logger,
    patch: kopf.Patch | None = None,
    status: PipelineStatusPatch | None = None,
    previous_conditions: list[V1ConditionDict] | None = None,
) -> None:
    status_patch = (
        status
        if status is not None
        else cast(PipelineStatusPatch, patch.status) if patch is not None else None
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)

    core = client.CoreV1Api()
    apps = client.AppsV1Api()

    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render(consumer)
    except Exception as e:
        if status_patch is not None:
            failed = [
                V1Condition(
                    type="Rendered",
                    status="False",
                    reason=type(e).__name__,
                    message=str(e),
                    last_transition_time=now,
                )
            ]
            status_patch["conditions"] = _merge_conditions(
                previous_conditions,
                cast(
                    list[V1ConditionDict],
                    core.api_client.sanitize_for_serialization(failed),
                ),
            )
        raise kopf.PermanentError(f"StreamingPipeline {namespace}/{name} failed to render: {e}")

    manifests: list[V1ConfigMapDict | V1DeploymentDict] = [
        result["configmap"],
        result["deployment"],
    ]
    if "canary_deployment" in result:
        manifests.append(result["canary_deployment"])

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
            _apply_configmap(
                core,
                cast(V1ConfigMapDict, manifest),
                workload_namespace=workload_namespace,
            )
        elif kind == "Deployment":
            _apply_deployment(
                apps,
                cast(V1DeploymentDict, manifest),
                workload_namespace=workload_namespace,
            )
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
        logger=logger,
    )

    if status_patch is not None:
        replicas = consumer.get("replicas", 1)
        canary = 1 if "canary_deployment" in result else 0
        applied = [
            V1Condition(
                type="Rendered",
                status="True",
                reason="Rendered",
                message="",
                last_transition_time=now,
            ),
            V1Condition(
                type="Applied",
                status="True",
                reason="Applied",
                message="",
                last_transition_time=now,
            ),
        ]
        status_patch["conditions"] = _merge_conditions(
            previous_conditions,
            cast(
                list[V1ConditionDict],
                core.api_client.sanitize_for_serialization(applied),
            ),
        )
        status_patch["config_version"] = compute_config_version(consumer["pipeline_config"])
        status_patch["replicas"] = {"primary": replicas - canary, "canary": canary}
        status_patch["workload_namespace"] = workload_namespace

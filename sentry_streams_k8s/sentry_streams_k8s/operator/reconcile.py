from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, cast

import kopf
from kubernetes import client, dynamic
from kubernetes.client.exceptions import ApiException

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator.constants import (
    CANARY_WORKLOAD_SET,
    FIELD_MANAGER,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    PRIMARY_WORKLOAD_SET,
    Logger,
)
from sentry_streams_k8s.operator.generations import (
    ledger_configmap_name,
    load_generations,
    save_generations,
)
from sentry_streams_k8s.operator.pod_health import (
    is_deleting,
    pod_health,
    pod_spec_changed,
    pod_status_entry,
)
from sentry_streams_k8s.operator.pod_resources import (
    apply_pod,
    build_pipeline_pod,
    delete_pod,
    list_owned_pods,
    pod_generation,
    pod_keep_key,
    pod_name,
    pod_ordinal,
    pod_template_from_deployment,
    pod_workload_set,
)
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render,
    validate,
)


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
        # Do not take over a resource with the same name from another pipeline:
        if existing_owner_uid != owner_uid:
            raise kopf.PermanentError(
                f"{manifest['kind']} {workload_namespace}/{name} is already present and is not "
                "managed by this StreamingPipeline."
            )

    dyn.server_side_apply(
        resource,
        body=manifest,
        namespace=workload_namespace,
        field_manager=FIELD_MANAGER,
        force_conflicts=True,
    )


def prune_stale_configmaps(
    *,
    workload_namespace: str,
    owner_uid: str,
    desired_configmaps: set[str],
    logger: Logger,
) -> None:
    selector = f"{OWNER_UID_LABEL}={owner_uid}"
    core = client.CoreV1Api()
    configmaps = cast(
        Any,
        core.list_namespaced_config_map(
            namespace=workload_namespace,
            label_selector=selector,
        ),
    )
    for configmap in configmaps.items:
        if configmap.metadata.name not in desired_configmaps:
            logger.info(
                "Pruning stale configmap %s/%s", workload_namespace, configmap.metadata.name
            )
            core.delete_namespaced_config_map(
                name=configmap.metadata.name, namespace=workload_namespace
            )


def _condition(type_: str, status: bool, reason: str, message: str = "") -> dict[str, Any]:
    return {
        "type": type_,
        "status": "True" if status else "False",
        "reason": reason,
        "message": message,
    }


def _delete_current_pod(
    dyn: dynamic.DynamicClient,
    pod: Mapping[str, Any],
    namespace: str,
    logger: Logger,
    health: Mapping[str, Any],
    reason: str,
) -> None:
    name = pod_name(pod)
    if is_deleting(pod) and not cast(bool, health["delete"]):
        logger.info("pipeline Pod %s/%s is already deleting reason=%s", namespace, name, reason)
        return
    delete_pod(dyn, name, namespace, force=cast(bool, health["force"]))
    logger.info("deleted pipeline Pod %s/%s reason=%s", namespace, name, reason)


def delete_obsolete_pod_sets(
    dyn: dynamic.DynamicClient,
    namespace: str,
    owner_uid: str,
    desired_sets: set[str],
    logger: Logger,
) -> None:
    now = datetime.now(timezone.utc)
    for pod in list_owned_pods(dyn, namespace, owner_uid):
        workload_set = pod_workload_set(pod)
        if workload_set in desired_sets:
            continue
        health = pod_health(pod, now)
        _delete_current_pod(
            dyn,
            pod,
            namespace,
            logger,
            health,
            f"StaleWorkloadSet:{workload_set or 'missing'}",
        )


def _allocate_generation(
    generations: dict[int, int], ordinal: int, pods: list[dict[str, Any]]
) -> int:
    live_max = max((pod_generation(pod) for pod in pods), default=-1)
    generation = max(generations.get(ordinal, -1), live_max) + 1
    generations[ordinal] = generation
    return generation


def reconcile_pipeline_pods(
    *,
    dyn: dynamic.DynamicClient,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    base_name: str,
    template_metadata: Mapping[str, Any],
    template_spec: Mapping[str, Any],
    replicas: int,
    generations: dict[int, int],
    logger: Logger,
    workload_set: str,
) -> dict[str, Any]:
    desired_ordinals = set(range(max(replicas, 0)))
    current = list_owned_pods(
        dyn,
        workload_namespace,
        owner_uid,
        workload_set=workload_set,
    )
    now = datetime.now(timezone.utc)

    pods_by_ordinal: dict[int, list[dict[str, Any]]] = {}
    health_by_name: dict[str, dict[str, Any]] = {}
    pod_statuses: list[dict[str, Any]] = []
    active_pod_names: list[str] = []

    def _build(ordinal: int, generation: int) -> dict[str, Any]:
        return build_pipeline_pod(
            base_name=base_name,
            template_metadata=template_metadata,
            template_spec=template_spec,
            ordinal=ordinal,
            generation=generation,
            owner_uid=owner_uid,
            owner_name=owner_name,
            owner_namespace=owner_namespace,
            workload_set=workload_set,
        )

    for pod in current:
        name = pod_name(pod)
        health = pod_health(pod, now)
        health_by_name[name] = health
        ordinal = pod_ordinal(pod)
        if ordinal is None or ordinal not in desired_ordinals:
            _delete_current_pod(dyn, pod, workload_namespace, logger, health, "Stale")
            continue
        pods_by_ordinal.setdefault(ordinal, []).append(pod)
        pod_statuses.append(pod_status_entry(pod, health))

    for ordinal in sorted(desired_ordinals):
        pods = pods_by_ordinal.get(ordinal, [])
        desired_template = _build(ordinal, 0)
        candidates = [
            pod
            for pod in pods
            if (
                not is_deleting(pod)
                and not pod_spec_changed(pod, desired_template)
                and not cast(bool, health_by_name[pod_name(pod)]["delete"])
            )
        ]

        if candidates:
            # Prefer a ready Pod, otherwise keep the newest generation:
            keep = max(candidates, key=lambda pod: pod_keep_key(pod, health_by_name[pod_name(pod)]))
            active_pod_names.append(pod_name(keep))
            generations[ordinal] = max(generations.get(ordinal, -1), pod_generation(keep))
        else:
            # Create the replacement before deleting the old Pod:
            generation = _allocate_generation(generations, ordinal, pods)
            keep = _build(ordinal, generation)
            apply_pod(dyn, keep, workload_namespace)
            active_pod_names.append(pod_name(keep))
            logger.info(
                "applied replacement pipeline Pod %s/%s",
                workload_namespace,
                pod_name(keep),
            )

        for pod in pods:
            if pod is keep:
                continue
            health = health_by_name[pod_name(pod)]
            if pod_spec_changed(pod, desired_template):
                _delete_current_pod(dyn, pod, workload_namespace, logger, health, "Outdated")
            elif cast(bool, health["delete"]):
                _delete_current_pod(
                    dyn, pod, workload_namespace, logger, health, cast(str, health["reason"])
                )
            elif pod in candidates:
                _delete_current_pod(dyn, pod, workload_namespace, logger, health, "Duplicate")

    ready_ordinals = {
        ordinal
        for ordinal, pods in pods_by_ordinal.items()
        if any(
            not is_deleting(pod)
            and pod_name(pod) in active_pod_names
            and cast(bool, health_by_name[pod_name(pod)]["ready"])
            for pod in pods
        )
    }
    unhealthy_pods = [entry for entry in pod_statuses if entry.get("reason") is not None]
    permanent_errors = [entry for entry in unhealthy_pods if entry.get("permanent")]
    return {
        "childPods": sorted(active_pod_names),
        "desiredReplicas": len(desired_ordinals),
        "readyReplicas": len(ready_ordinals),
        "unhealthyPods": unhealthy_pods,
        "permanentErrors": permanent_errors,
    }


def _reconcile_pod_set(
    *,
    dyn: dynamic.DynamicClient,
    deployment: dict[str, Any],
    workload_set: str,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    logger: Logger,
) -> tuple[dict[str, Any], str]:
    base_name = deployment["metadata"]["name"]
    template_metadata, template_spec = pod_template_from_deployment(deployment)
    replicas = deployment["spec"].get("replicas", 0)
    if type(replicas) is not int or replicas < 0:
        raise kopf.PermanentError(
            f"Rendered {workload_set} replica count must be a non-negative integer."
        )

    ledger_name = ledger_configmap_name(base_name)
    generations = load_generations(dyn, workload_namespace, ledger_name)
    generations_before = dict(generations)
    pod_result = reconcile_pipeline_pods(
        dyn=dyn,
        workload_namespace=workload_namespace,
        owner_uid=owner_uid,
        owner_name=owner_name,
        owner_namespace=owner_namespace,
        base_name=base_name,
        template_metadata=template_metadata,
        template_spec=template_spec,
        replicas=replicas,
        generations=generations,
        logger=logger,
        workload_set=workload_set,
    )
    if generations != generations_before:
        save_generations(
            dyn,
            workload_namespace,
            ledger_name,
            owner_uid=owner_uid,
            owner_name=owner_name,
            owner_namespace=owner_namespace,
            generations=generations,
        )
    return pod_result, ledger_name


def _combine_pod_results(results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    child_pods: list[str] = []
    unhealthy_pods: list[dict[str, Any]] = []
    permanent_errors: list[dict[str, Any]] = []
    for workload_set, result in results.items():
        child_pods.extend(result["childPods"])
        unhealthy_pods.extend(
            {**entry, "workloadSet": workload_set} for entry in result["unhealthyPods"]
        )
        permanent_errors.extend(
            {**entry, "workloadSet": workload_set} for entry in result["permanentErrors"]
        )
    return {
        "childPods": sorted(child_pods),
        "desiredReplicas": sum(result["desiredReplicas"] for result in results.values()),
        "readyReplicas": sum(result["readyReplicas"] for result in results.values()),
        "unhealthyPods": unhealthy_pods,
        "permanentErrors": permanent_errors,
        "sets": results,
    }


def reconcile_pipeline(
    *,
    spec: Any,
    name: str,
    namespace: str,
    uid: str,
    workload_namespace: str,
    logger: Logger,
    patch: kopf.Patch | None = None,
    status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status_patch = status if status is not None else patch.status if patch is not None else None
    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render(consumer)
    except Exception as e:
        if status_patch is not None:
            status_patch["conditions"] = [
                _condition("Rendered", False, type(e).__name__, str(e))
            ]
        raise kopf.PermanentError(f"StreamingPipeline {namespace}/{name} failed to render: {e}")

    dyn = dynamic.DynamicClient(client.ApiClient())
    configmap = result["configmap"]

    _prepare_manifest(
        configmap,
        workload_namespace=workload_namespace,
        owner_uid=uid,
        owner_name=name,
        owner_namespace=namespace,
    )
    _apply(dyn, configmap, workload_namespace=workload_namespace, owner_uid=uid)

    rendered_sets = {PRIMARY_WORKLOAD_SET: result["deployment"]}
    if "canary_deployment" in result:
        rendered_sets[CANARY_WORKLOAD_SET] = result["canary_deployment"]

    pod_set_results: dict[str, dict[str, Any]] = {}
    ledger_names: set[str] = set()
    for workload_set, deployment in rendered_sets.items():
        set_result, ledger_name = _reconcile_pod_set(
            dyn=dyn,
            deployment=deployment,
            workload_set=workload_set,
            workload_namespace=workload_namespace,
            owner_uid=uid,
            owner_name=name,
            owner_namespace=namespace,
            logger=logger,
        )
        pod_set_results[workload_set] = set_result
        ledger_names.add(ledger_name)

    # Remove Pods and generation ledgers left behind by a removed canary set:
    delete_obsolete_pod_sets(
        dyn,
        workload_namespace,
        uid,
        set(rendered_sets),
        logger,
    )
    pod_result = _combine_pod_results(pod_set_results)
    prune_stale_configmaps(
        workload_namespace=workload_namespace,
        owner_uid=uid,
        desired_configmaps={configmap["metadata"]["name"], *ledger_names},
        logger=logger,
    )

    if status_patch is not None:
        conditions = [
            _condition("Rendered", True, "Rendered"),
            _condition("Applied", True, "Applied"),
        ]
        permanent_errors = pod_result["permanentErrors"]
        if permanent_errors:
            error = permanent_errors[0]
            conditions[-1] = _condition(
                "Applied",
                False,
                "PermanentPodFailure",
                f"Pod {error['name']} is permanently unhealthy: {error['reason']}. "
                "Update the StreamingPipeline spec.",
            )
        status_patch["conditions"] = conditions
        status_patch["config_version"] = compute_config_version(consumer["pipeline_config"])
        status_patch["pods"] = pod_result
        status_patch["workload_namespace"] = workload_namespace

    return pod_result

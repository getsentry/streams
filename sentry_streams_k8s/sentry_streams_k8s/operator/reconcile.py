from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, TypedDict, cast

import kopf
from kubernetes import client
from kubernetes.client import V1Condition, V1Pod

from sentry_streams_k8s.constants import ALL_WORKLOAD_SETS
from sentry_streams_k8s.consumer_builder import WorkloadSet, compute_config_version
from sentry_streams_k8s.k8s_types import (
    V1ConditionDict,
    V1ConfigMapDict,
    V1PodDict,
)
from sentry_streams_k8s.operator.constants import (
    APPLY_PATCH_CONTENT_TYPE,
    FIELD_MANAGER,
    MAX_BASE_NAME_LENGTH,
    MAX_GENERATION,
    MAX_REPLICAS,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    Logger,
)
from sentry_streams_k8s.operator.pod_health import (
    PodHealth,
    is_deleting,
    pod_health,
    pod_spec_changed,
)
from sentry_streams_k8s.operator.pod_resources import (
    apply_pod,
    build_pipeline_pod,
    consumer_pod_name,
    delete_pod,
    list_owned_pods,
    pod_generation,
    pod_keep_key,
    pod_name,
    pod_ordinal,
    pod_workload_set,
)
from sentry_streams_k8s.operator.pod_status import (
    PodStatusEntry,
    ReportedPodStatus,
    reported_pod_status,
)
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render_pods,
    validate,
)


class PodSetResult(TypedDict):
    childPods: list[str]
    desiredReplicas: int
    readyReplicas: int
    unhealthyPods: list[PodStatusEntry]
    permanentErrors: list[PodStatusEntry]


class CombinedPodResult(PodSetResult):
    sets: dict[str, PodSetResult | None]


class PipelineStatusPatch(TypedDict, total=False):
    conditions: list[V1ConditionDict]
    config_version: str
    pods: CombinedPodResult
    workload_namespace: str
    generations: dict[str, dict[str, int] | None]


def _prepare_manifest(
    manifest: V1ConfigMapDict,
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


def prune_stale_configmaps(
    *,
    core: client.CoreV1Api,
    workload_namespace: str,
    owner_uid: str,
    desired_configmaps: set[str],
    logger: Logger,
) -> None:
    selector = f"{OWNER_UID_LABEL}={owner_uid}"

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


def _parse_generations(data: object) -> dict[int, int]:
    """
    Returns a dict mapping ordinal -> highest generation.
    Empty when there is no ledger yet (e.g. first reconcile).
    """

    if not isinstance(data, Mapping):
        return {}
    return {int(ordinal): generation for ordinal, generation in data.items()}


def _serialize_generations(generations: dict[int, int]) -> dict[str, int]:
    """
    Pods for a replica are named {name}-{ordinal}-{generation}. The generation
    increments every time the operator replaces a Pod, so a newly created Pod
    never shares a name with the old one that is still terminating. This is what
    lets us recreate instantly instead of waiting for the old Pod to be deleted.
    We save the highest generation per ordinal in the CR's status.generations.
    """

    return {str(ordinal): generation for ordinal, generation in sorted(generations.items())}


def _delete_current_pod(
    core: client.CoreV1Api,
    pod: V1Pod,
    namespace: str,
    logger: Logger,
    health: PodHealth,
    reason: str,
) -> None:
    name = pod_name(pod)
    if is_deleting(pod) and not health.delete:
        logger.info("pipeline Pod %s/%s is already deleting reason=%s", namespace, name, reason)
        return
    delete_pod(core, name, namespace, force=health.force)
    logger.info("deleted pipeline Pod %s/%s reason=%s", namespace, name, reason)


def delete_obsolete_pod_sets(
    core: client.CoreV1Api,
    namespace: str,
    owner_uid: str,
    desired_sets: set[str],
    logger: Logger,
) -> None:
    now = datetime.now(timezone.utc)
    for pod in list_owned_pods(core, namespace, owner_uid):
        workload_set = pod_workload_set(pod)
        if workload_set in desired_sets:
            continue
        health = pod_health(pod, now)
        _delete_current_pod(
            core,
            pod,
            namespace,
            logger,
            health,
            f"StaleWorkloadSet:{workload_set or 'missing'}",
        )


def _allocate_generation(generations: dict[int, int], ordinal: int, pods: list[V1Pod]) -> int:
    live_max = max((pod_generation(pod) for pod in pods), default=-1)
    generation = max(generations.get(ordinal, -1), live_max) + 1

    if generation > MAX_GENERATION:
        # TODO: We should see if it is safe to wrap back to generation 0 at this point.
        raise kopf.PermanentError(f"Replica {ordinal} exceeds {MAX_GENERATION} generations.")

    generations[ordinal] = generation
    return generation


def reconcile_pipeline_pods(
    *,
    core: client.CoreV1Api,
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
) -> PodSetResult:
    desired_ordinals = set(range(max(replicas, 0)))
    current = list_owned_pods(
        core,
        workload_namespace,
        owner_uid,
        workload_set=workload_set,
    )
    now = datetime.now(timezone.utc)

    pods_by_ordinal: dict[int, list[V1Pod]] = {}
    health_by_name: dict[str, PodHealth] = {}
    reported_statuses: list[ReportedPodStatus] = []
    active_pod_names: list[str] = []

    def _build(ordinal: int, generation: int) -> V1PodDict:
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
            _delete_current_pod(core, pod, workload_namespace, logger, health, "Stale")
            continue
        pods_by_ordinal.setdefault(ordinal, []).append(pod)
        reported_statuses.append(reported_pod_status(pod, health))

    for ordinal in sorted(desired_ordinals):
        pods = pods_by_ordinal.get(ordinal, [])
        desired_template = _build(ordinal, 0)
        candidates = [
            pod
            for pod in pods
            if (
                not is_deleting(pod)
                and not pod_spec_changed(pod, desired_template)
                and not health_by_name[pod_name(pod)].delete
            )
        ]

        keep: V1Pod | None
        if candidates:
            # Prefer a ready Pod, otherwise keep the newest generation:
            keep = max(candidates, key=lambda pod: pod_keep_key(pod, health_by_name[pod_name(pod)]))
            active_pod_names.append(pod_name(keep))
            generations[ordinal] = max(generations.get(ordinal, -1), pod_generation(keep))
        else:
            # Create the replacement before deleting the old Pod:
            generation = _allocate_generation(generations, ordinal, pods)
            keep_manifest = _build(ordinal, generation)
            apply_pod(core, keep_manifest, workload_namespace)
            keep = None
            keep_name = consumer_pod_name(base_name, ordinal, generation)
            active_pod_names.append(keep_name)
            logger.info(
                "applied replacement pipeline Pod %s/%s",
                workload_namespace,
                keep_name,
            )

        for pod in pods:
            if pod is keep:
                continue
            health = health_by_name[pod_name(pod)]
            if pod_spec_changed(pod, desired_template):
                _delete_current_pod(core, pod, workload_namespace, logger, health, "Outdated")
            elif health.delete:
                _delete_current_pod(
                    core, pod, workload_namespace, logger, health, health.reason or "Unhealthy"
                )
            elif pod in candidates:
                _delete_current_pod(core, pod, workload_namespace, logger, health, "Duplicate")

    active = set(active_pod_names)
    ready_ordinals = {
        ordinal
        for ordinal, pods in pods_by_ordinal.items()
        if any(
            not is_deleting(pod) and pod_name(pod) in active and health_by_name[pod_name(pod)].ready
            for pod in pods
        )
    }
    unhealthy_pods = [status.to_status_dict() for status in reported_statuses if status.unhealthy]
    permanent_errors = [
        status.to_status_dict()
        for status in reported_statuses
        if status.unhealthy and status.permanent
    ]
    return {
        "childPods": sorted(active_pod_names),
        "desiredReplicas": len(desired_ordinals),
        "readyReplicas": len(ready_ordinals),
        "unhealthyPods": unhealthy_pods,
        "permanentErrors": permanent_errors,
    }


def _reconcile_pod_set(
    *,
    core: client.CoreV1Api,
    workload: WorkloadSet,
    workload_set: str,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    logger: Logger,
    previous_generations: dict[int, int],
) -> tuple[PodSetResult, dict[int, int]]:
    base_name = workload.name

    if len(base_name) > MAX_BASE_NAME_LENGTH:
        raise kopf.PermanentError(
            f"{workload_set} name cannot exceed {MAX_BASE_NAME_LENGTH} characters."
        )

    replicas = workload.replicas

    if type(replicas) is not int or replicas < 0:
        raise kopf.PermanentError(f"{workload_set} replica count must be a non-negative integer.")

    if replicas > MAX_REPLICAS:
        raise kopf.PermanentError(f"{workload_set} replica count cannot exceed {MAX_REPLICAS}.")

    generations = dict(previous_generations)
    pod_result = reconcile_pipeline_pods(
        core=core,
        workload_namespace=workload_namespace,
        owner_uid=owner_uid,
        owner_name=owner_name,
        owner_namespace=owner_namespace,
        base_name=base_name,
        template_metadata=workload.pod_template["metadata"],
        template_spec=workload.pod_template["spec"],
        replicas=replicas,
        generations=generations,
        logger=logger,
        workload_set=workload_set,
    )
    return pod_result, generations


def _with_workload_set(entry: PodStatusEntry, workload_set: str) -> PodStatusEntry:
    return {**entry, "workloadSet": workload_set}


def _combine_pod_results(results: dict[str, PodSetResult]) -> CombinedPodResult:
    child_pods: list[str] = []
    unhealthy_pods: list[PodStatusEntry] = []
    permanent_errors: list[PodStatusEntry] = []
    for workload_set, result in results.items():
        child_pods.extend(result["childPods"])
        unhealthy_pods.extend(
            _with_workload_set(entry, workload_set) for entry in result["unhealthyPods"]
        )
        permanent_errors.extend(
            _with_workload_set(entry, workload_set) for entry in result["permanentErrors"]
        )
    return {
        "childPods": sorted(child_pods),
        "desiredReplicas": sum(result["desiredReplicas"] for result in results.values()),
        "readyReplicas": sum(result["readyReplicas"] for result in results.values()),
        "unhealthyPods": unhealthy_pods,
        "permanentErrors": permanent_errors,
        # Status is updated as a JSON merge patch, so a set that is not included keeps its old
        # value. Explicitly include all sets and null out the removed ones to clear them:
        "sets": {workload_set: results.get(workload_set) for workload_set in ALL_WORKLOAD_SETS},
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
    status: PipelineStatusPatch | None = None,
    previous_conditions: list[V1ConditionDict] | None = None,
    previous_generations: object = None,
) -> CombinedPodResult:
    status_patch = (
        status
        if status is not None
        else cast(PipelineStatusPatch, patch.status) if patch is not None else None
    )

    now = datetime.now(timezone.utc).replace(microsecond=0)

    core = client.CoreV1Api()
    ledger = previous_generations if isinstance(previous_generations, Mapping) else {}

    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render_pods(consumer)
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

    configmap = result["configmap"]
    _prepare_manifest(
        configmap,
        workload_namespace=workload_namespace,
        owner_uid=uid,
        owner_name=name,
        owner_namespace=namespace,
    )
    _apply_configmap(core, configmap, workload_namespace=workload_namespace)

    pod_set_results: dict[str, PodSetResult] = {}
    generations_by_set: dict[str, dict[int, int]] = {}
    for workload_set, workload in result["sets"].items():
        set_result, generations = _reconcile_pod_set(
            core=core,
            workload=workload,
            workload_set=workload_set,
            workload_namespace=workload_namespace,
            owner_uid=uid,
            owner_name=name,
            owner_namespace=namespace,
            logger=logger,
            previous_generations=_parse_generations(ledger.get(workload_set)),
        )
        pod_set_results[workload_set] = set_result
        generations_by_set[workload_set] = generations

    # Remove Pods left behind by a workload set that is no longer rendered:
    delete_obsolete_pod_sets(core, workload_namespace, uid, set(result["sets"]), logger)

    pod_result = _combine_pod_results(pod_set_results)
    prune_stale_configmaps(
        core=core,
        workload_namespace=workload_namespace,
        owner_uid=uid,
        desired_configmaps={configmap["metadata"]["name"]},
        logger=logger,
    )

    if status_patch is not None:
        applied = [
            V1Condition(
                type="Rendered",
                status="True",
                reason="Rendered",
                message="",
                last_transition_time=now,
            ),
        ]
        permanent_errors = pod_result["permanentErrors"]
        if permanent_errors:
            error = permanent_errors[0]
            applied.append(
                V1Condition(
                    type="Applied",
                    status="False",
                    reason="PermanentPodFailure",
                    message=(
                        f"Pod {error['name']} is permanently unhealthy: "
                        f"{error.get('reason', 'Unknown')}. Update the StreamingPipeline spec."
                    ),
                    last_transition_time=now,
                )
            )
        else:
            applied.append(
                V1Condition(
                    type="Applied",
                    status="True",
                    reason="Applied",
                    message="",
                    last_transition_time=now,
                )
            )
        status_patch["conditions"] = _merge_conditions(
            previous_conditions,
            cast(
                list[V1ConditionDict],
                core.api_client.sanitize_for_serialization(applied),
            ),
        )
        status_patch["config_version"] = compute_config_version(consumer["pipeline_config"])
        status_patch["pods"] = pod_result
        status_patch["workload_namespace"] = workload_namespace
        status_patch["generations"] = {
            workload_set: (
                _serialize_generations(generations_by_set[workload_set])
                if workload_set in generations_by_set
                else None
            )
            for workload_set in ALL_WORKLOAD_SETS
        }

    return pod_result

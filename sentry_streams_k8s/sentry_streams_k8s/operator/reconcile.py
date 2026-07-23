from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, TypedDict, cast

import kopf
from kubernetes import client
from kubernetes.client import V1Pod
from kubernetes.client.exceptions import ApiException

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator.constants import (
    ALL_WORKLOAD_SETS,
    CANARY_WORKLOAD_SET,
    FIELD_MANAGER,
    MAX_BASE_NAME_LENGTH,
    MAX_GENERATION,
    MAX_REPLICAS,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    PRIMARY_WORKLOAD_SET,
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
    pod_template_from_deployment,
    pod_workload_set,
)
from sentry_streams_k8s.operator.pod_status import (
    PodStatusEntry,
    ReportedPodStatus,
    reported_pod_status,
)
from sentry_streams_k8s.operator.pod_types import PodManifest
from sentry_streams_k8s.operator.streaming_pipeline import (
    from_crd_spec,
    render,
    validate,
)


class Condition(TypedDict):
    type: str
    status: str
    reason: str
    message: str


class PodSetResult(TypedDict):
    childPods: list[str]
    desiredReplicas: int
    readyReplicas: int
    unhealthyPods: list[PodStatusEntry]
    permanentErrors: list[PodStatusEntry]


class CombinedPodResult(PodSetResult):
    sets: dict[str, PodSetResult | None]


class PipelineStatusPatch(TypedDict, total=False):
    conditions: list[Condition]
    config_version: str
    pods: CombinedPodResult
    workload_namespace: str
    generations: dict[str, dict[str, int] | None]


def _parse_generations(data: object) -> dict[int, int]:
    """
    Returns a dict mapping ordinal -> highest generation.
    Empty when there is no ledger yet (e.g. first reconcile).
    """

    if not isinstance(data, Mapping):
        return {}

    generations: dict[int, int] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, int):
            continue
        try:
            generations[int(key)] = value
        except ValueError:
            continue
    return generations


def _serialize_generations(generations: dict[int, int]) -> dict[str, int]:
    """
    Pods for a replica are named {name}-{ordinal}-{generation}. The generation
    increments every time the operator replaces a Pod, so a newly created Pod
    never shares a name with the old one that is still terminating. This is what
    lets us recreate instantly instead of waiting for the old Pod to be deleted.
    We save the highest generation per ordinal in the CR's status.generations.
    """

    return {str(ordinal): generation for ordinal, generation in sorted(generations.items())}


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
    owner_uid: str,
) -> None:
    name = manifest["metadata"]["name"]
    try:
        existing = core.read_namespaced_config_map(name=name, namespace=workload_namespace)
    except ApiException as e:
        if e.status != 404:
            raise
    else:
        labels = existing.metadata.labels if existing.metadata and existing.metadata.labels else {}
        existing_owner_uid = labels.get(OWNER_UID_LABEL)
        # Do not take over a resource with the same name from another pipeline:
        if existing_owner_uid != owner_uid:
            raise kopf.PermanentError(
                f"ConfigMap {workload_namespace}/{name} is already present and is not "
                "managed by this StreamingPipeline."
            )

    core.patch_namespaced_config_map(
        name=name,
        namespace=workload_namespace,
        body=manifest,
        field_manager=FIELD_MANAGER,
        force=True,
        _content_type="application/apply-patch+yaml",
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
    configmaps = core.list_namespaced_config_map(
        namespace=workload_namespace,
        label_selector=selector,
    )
    for configmap in configmaps.items:
        metadata = configmap.metadata
        name = metadata.name if metadata else None
        if name is None:
            continue
        if name not in desired_configmaps:
            logger.info("Pruning stale configmap %s/%s", workload_namespace, name)
            core.delete_namespaced_config_map(name=name, namespace=workload_namespace)


def _condition(type_: str, status: bool, reason: str, message: str = "") -> Condition:
    return {
        "type": type_,
        "status": "True" if status else "False",
        "reason": reason,
        "message": message,
    }


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

    def _build(ordinal: int, generation: int) -> PodManifest:
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

    ready_ordinals = {
        ordinal
        for ordinal, pods in pods_by_ordinal.items()
        if any(
            not is_deleting(pod)
            and pod_name(pod) in active_pod_names
            and health_by_name[pod_name(pod)].ready
            for pod in pods
        )
    }
    unhealthy_pods = [
        status.to_status_dict() for status in reported_statuses if status.is_unhealthy
    ]
    permanent_errors = [
        status.to_status_dict()
        for status in reported_statuses
        if status.is_unhealthy and status.permanent
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
    deployment: dict[str, Any],
    workload_set: str,
    workload_namespace: str,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    logger: Logger,
    previous_generations: dict[int, int],
) -> tuple[PodSetResult, dict[int, int]]:
    base_name = deployment["metadata"]["name"]

    if len(base_name) > MAX_BASE_NAME_LENGTH:
        raise kopf.PermanentError(
            f"{workload_set} name cannot exceed {MAX_BASE_NAME_LENGTH} characters."
        )

    template_metadata, template_spec = pod_template_from_deployment(deployment)
    replicas = deployment["spec"].get("replicas", 0)

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
        template_metadata=template_metadata,
        template_spec=template_spec,
        replicas=replicas,
        generations=generations,
        logger=logger,
        workload_set=workload_set,
    )
    return pod_result, generations


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
        # value. Explicity include all sets and null out the removed ones to clear them:
        "sets": {workload_set: results.get(workload_set) for workload_set in ALL_WORKLOAD_SETS},
    }


def _with_workload_set(entry: PodStatusEntry, workload_set: str) -> PodStatusEntry:
    result: PodStatusEntry = {
        "name": entry["name"],
        "ready": entry["ready"],
        "phase": entry["phase"],
        "workloadSet": workload_set,
    }
    if "ordinal" in entry:
        result["ordinal"] = entry["ordinal"]
    if "reason" in entry:
        result["reason"] = entry["reason"]
    if "permanent" in entry:
        result["permanent"] = entry["permanent"]
    return result


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
    previous_generations: object = None,
) -> CombinedPodResult:
    status_patch = (
        status
        if status is not None
        else cast(PipelineStatusPatch, patch.status) if patch is not None else None
    )
    previous_generations = previous_generations if isinstance(previous_generations, Mapping) else {}
    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render(consumer)
    except Exception as e:
        if status_patch is not None:
            status_patch["conditions"] = [_condition("Rendered", False, type(e).__name__, str(e))]
        raise kopf.PermanentError(f"StreamingPipeline {namespace}/{name} failed to render: {e}")

    core = client.CoreV1Api()
    configmap = result["configmap"]

    _prepare_manifest(
        configmap,
        workload_namespace=workload_namespace,
        owner_uid=uid,
        owner_name=name,
        owner_namespace=namespace,
    )
    _apply_configmap(core, configmap, workload_namespace=workload_namespace, owner_uid=uid)

    rendered_sets = {PRIMARY_WORKLOAD_SET: result["deployment"]}
    if "canary_deployment" in result:
        rendered_sets[CANARY_WORKLOAD_SET] = result["canary_deployment"]

    pod_set_results: dict[str, PodSetResult] = {}
    generations_by_set: dict[str, dict[int, int]] = {}
    for workload_set, deployment in rendered_sets.items():
        set_result, generations = _reconcile_pod_set(
            core=core,
            deployment=deployment,
            workload_set=workload_set,
            workload_namespace=workload_namespace,
            owner_uid=uid,
            owner_name=name,
            owner_namespace=namespace,
            logger=logger,
            previous_generations=_parse_generations(previous_generations.get(workload_set)),
        )
        pod_set_results[workload_set] = set_result
        generations_by_set[workload_set] = generations

    # Remove Pods left behind by a removed canary set:
    delete_obsolete_pod_sets(
        core,
        workload_namespace,
        uid,
        set(rendered_sets),
        logger,
    )
    pod_result = _combine_pod_results(pod_set_results)
    prune_stale_configmaps(
        workload_namespace=workload_namespace,
        owner_uid=uid,
        desired_configmaps={configmap["metadata"]["name"]},
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
                f"Pod {error['name']} is permanently unhealthy: {error.get('reason', 'Unknown')}. "
                "Update the StreamingPipeline spec.",
            )
        status_patch["conditions"] = conditions
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

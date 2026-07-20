from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import kopf
from kubernetes import client, dynamic
from kubernetes.client.exceptions import ApiException

from sentry_streams_k8s.operator.constants import (
    FIELD_MANAGER,
    GROUP,
    HEALTH_SCAN_INTERVAL_SECONDS,
    MANAGED_BY_LABEL,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    PLURAL,
    VERSION,
    WORKLOAD_NAMESPACE_ENV,
    Logger,
)
from sentry_streams_k8s.operator.pod_health import pod_health
from sentry_streams_k8s.operator.pod_resources import delete_owned_pods
from sentry_streams_k8s.operator.reconcile import (
    prune_stale_configmaps,
    reconcile_pipeline,
)


def _workload_namespace() -> str:
    namespace = os.environ.get(WORKLOAD_NAMESPACE_ENV, "").strip()
    if not namespace:
        raise RuntimeError(f"{WORKLOAD_NAMESPACE_ENV} must be set.")
    return namespace


def _fetch_owner(name: str, namespace: str) -> dict[str, Any] | None:
    api = client.CustomObjectsApi()
    try:
        obj = api.get_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=namespace,
            plural=PLURAL,
            name=name,
        )
    except ApiException as e:
        if e.status == 404:
            return None
        raise
    return dict(obj)


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(
    spec: kopf.Spec,
    name: str,
    namespace: str | None,
    uid: str,
    patch: kopf.Patch,
    logger: Logger,
    **_: Any,
) -> dict[str, Any]:
    if namespace is None:
        raise kopf.PermanentError("Missing namespace!")
    result = reconcile_pipeline(
        spec=spec,
        name=name,
        namespace=namespace,
        uid=uid,
        workload_namespace=_workload_namespace(),
        logger=logger,
        patch=patch,
    )
    logger.info("reconciled pipeline Pods %s/%s", namespace, name)
    return result


@kopf.timer(GROUP, VERSION, PLURAL, interval=HEALTH_SCAN_INTERVAL_SECONDS)
def health_scan(
    spec: kopf.Spec,
    name: str,
    namespace: str | None,
    uid: str,
    patch: kopf.Patch,
    logger: Logger,
    **_: Any,
) -> dict[str, Any]:
    if namespace is None:
        raise kopf.PermanentError("Missing namespace!")
    return reconcile_pipeline(
        spec=spec,
        name=name,
        namespace=namespace,
        uid=uid,
        workload_namespace=_workload_namespace(),
        logger=logger,
        patch=patch,
    )


@kopf.on.event("", "v1", "pods", labels={MANAGED_BY_LABEL: FIELD_MANAGER})
def handle_pipeline_pod_event(
    type: str | None,
    body: kopf.Body,
    meta: kopf.Meta,
    annotations: kopf.Annotations,
    name: str | None,
    namespace: str | None,
    logger: Logger,
    **_: Any,
) -> None:
    if type not in {"DELETED", "MODIFIED"}:
        return

    if type == "MODIFIED" and meta.get("deletionTimestamp") is None:
        health = pod_health(dict(body), datetime.now(timezone.utc))
        if not health["delete"]:
            return

    owner_name = annotations.get(OWNER_NAME_ANNOTATION)
    owner_namespace = annotations.get(OWNER_NAMESPACE_ANNOTATION)
    if not owner_name or not owner_namespace:
        logger.warning(
            "managed Pod %s/%s is missing owner annotations; cannot recover",
            namespace,
            name,
        )
        return

    owner = _fetch_owner(owner_name, owner_namespace)
    if owner is None:
        logger.info(
            "owning StreamingPipeline %s/%s is gone; not recreating Pod %s",
            owner_namespace,
            owner_name,
            name,
        )
        return
    if (owner.get("metadata", {}) or {}).get("deletionTimestamp") is not None:
        logger.info(
            "owning StreamingPipeline %s/%s is deleting; not recreating Pod %s",
            owner_namespace,
            owner_name,
            name,
        )
        return

    reconcile_pipeline(
        spec=owner.get("spec", {}) or {},
        name=owner_name,
        namespace=owner_namespace,
        uid=owner["metadata"]["uid"],
        workload_namespace=_workload_namespace(),
        logger=logger,
    )
    logger.info(
        "recovered pipeline Pods for %s/%s after Pod %s event=%s",
        owner_namespace,
        owner_name,
        name,
        type,
    )


@kopf.on.delete(GROUP, VERSION, PLURAL)
def cleanup(uid: str, logger: Logger, **_: Any) -> None:
    workload_namespace = _workload_namespace()
    dyn = dynamic.DynamicClient(client.ApiClient())
    delete_owned_pods(dyn, workload_namespace, uid, logger)
    prune_stale_configmaps(
        workload_namespace=workload_namespace,
        owner_uid=uid,
        desired_configmaps=set(),
        logger=logger,
    )


def main() -> None:
    _workload_namespace()
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

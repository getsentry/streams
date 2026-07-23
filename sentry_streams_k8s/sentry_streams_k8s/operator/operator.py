from __future__ import annotations

import asyncio
import copy
import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

import kopf
from kubernetes import client
from kubernetes.client import V1Pod
from kubernetes.client.exceptions import ApiException

from sentry_streams_k8s.operator.constants import (
    FIELD_MANAGER,
    GROUP,
    HEALTH_SCAN_INTERVAL_SECONDS,
    MANAGED_BY_LABEL,
    MAX_CONCURRENT_RECONCILES,
    OWNER_UID_LABEL,
    PLURAL,
    VERSION,
    WORKLOAD_NAMESPACE_ENV,
    Logger,
)
from sentry_streams_k8s.operator.pod_health import pod_health
from sentry_streams_k8s.operator.pod_resources import delete_owned_pods
from sentry_streams_k8s.operator.reconcile import (
    PipelineStatusPatch,
    prune_stale_configmaps,
    reconcile_pipeline,
)


class ReconcileScheduler:
    def __init__(self, max_concurrent: int = MAX_CONCURRENT_RECONCILES) -> None:
        self._events: dict[str, asyncio.Event] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.limit = asyncio.Semaphore(max_concurrent)

    def register(self, uid: str) -> asyncio.Event:
        event = self._events.setdefault(uid, asyncio.Event())
        self._locks.setdefault(uid, asyncio.Lock())
        return event

    def unregister(self, uid: str, event: asyncio.Event) -> None:
        if self._events.get(uid) is event:
            self._events.pop(uid, None)

    def notify(self, uid: str) -> bool:
        event = self._events.get(uid)
        if event is None:
            return False
        event.set()
        return True

    def lock(self, uid: str) -> asyncio.Lock:
        return self._locks.setdefault(uid, asyncio.Lock())

    def discard(self, uid: str) -> None:
        self._events.pop(uid, None)
        self._locks.pop(uid, None)


def _scheduler(memo: kopf.Memo) -> ReconcileScheduler:
    scheduler = getattr(memo, "reconcile_scheduler", None)
    if not isinstance(scheduler, ReconcileScheduler):
        raise RuntimeError("Reconcile scheduler is not initialized.")
    return scheduler


@kopf.on.startup()
async def configure_reconciliation(memo: kopf.Memo, **_: Any) -> None:
    memo.reconcile_scheduler = ReconcileScheduler()


def _workload_namespace() -> str:
    namespace = os.environ.get(WORKLOAD_NAMESPACE_ENV, "").strip()
    if not namespace:
        raise RuntimeError(f"{WORKLOAD_NAMESPACE_ENV} must be set.")
    return namespace


def _deserialize_pod(body: kopf.Body) -> V1Pod:
    # kopf provides the event's raw JSON body so we need to deserialize.
    # Use a simple namespace since ApiClient expects a RESTResponse:
    json_response = SimpleNamespace(data=json.dumps(dict(body)))
    return cast(V1Pod, client.ApiClient().deserialize(json_response, "V1Pod"))


def _patch_pipeline_status(name: str, namespace: str, status: PipelineStatusPatch) -> None:
    api = client.CustomObjectsApi()
    api.patch_namespaced_custom_object_status(
        group=GROUP,
        version=VERSION,
        namespace=namespace,
        plural=PLURAL,
        name=name,
        body={"status": status},
    )


def _get_pipeline_status(name: str, namespace: str) -> Mapping[str, object]:
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
            return {}
        raise
    obj = cast(Mapping[str, object], obj)
    status = obj.get("status")
    return cast(Mapping[str, object], status) if isinstance(status, Mapping) else {}


@kopf.on.update(GROUP, VERSION, PLURAL)
async def request_pipeline_reconcile(
    uid: str,
    memo: kopf.Memo,
    logger: Logger,
    **_: Any,
) -> None:
    if _scheduler(memo).notify(uid):
        logger.debug("requested pipeline reconciliation uid=%s", uid)


async def _wait_for_reconcile(
    event: asyncio.Event,
    stopped: kopf.DaemonStopped,
    timeout: float | None,
) -> None:
    event_waiter: asyncio.Future[Any] = asyncio.ensure_future(event.wait())
    stop_waiter: asyncio.Future[Any] = asyncio.ensure_future(stopped.wait(timeout))
    _, pending = await asyncio.wait(
        {event_waiter, stop_waiter},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(
            *cast(tuple[asyncio.Future[Any], ...], tuple(pending)),
            return_exceptions=True,
        )


async def _reconcile_once(
    *,
    spec: kopf.Spec,
    name: str,
    namespace: str,
    uid: str,
    logger: Logger,
    scheduler: ReconcileScheduler,
    stopped: kopf.DaemonStopped,
) -> float | None:
    status: PipelineStatusPatch = {}
    try:
        async with scheduler.limit:
            async with scheduler.lock(uid):
                if stopped:
                    return None
                try:
                    previous_status = await asyncio.to_thread(_get_pipeline_status, name, namespace)
                    await asyncio.to_thread(
                        reconcile_pipeline,
                        spec=copy.deepcopy(dict(spec)),
                        name=name,
                        namespace=namespace,
                        uid=uid,
                        workload_namespace=_workload_namespace(),
                        logger=logger,
                        status=status,
                        previous_generations=previous_status.get("generations"),
                    )
                except kopf.PermanentError as e:
                    if status:
                        await asyncio.to_thread(_patch_pipeline_status, name, namespace, status)
                    logger.error("%s", e)
                    return None
                if status:
                    await asyncio.to_thread(_patch_pipeline_status, name, namespace, status)
    except Exception:
        logger.exception("pipeline reconciliation failed; retrying")
        return 5.0

    logger.info("reconciled pipeline Pods %s/%s", namespace, name)
    return float(HEALTH_SCAN_INTERVAL_SECONDS)


@kopf.daemon(GROUP, VERSION, PLURAL)
async def reconcile_pipeline_daemon(
    stopped: kopf.DaemonStopped,
    spec: kopf.Spec,
    name: str,
    namespace: str | None,
    uid: str,
    memo: kopf.Memo,
    logger: Logger,
    **_: Any,
) -> None:
    if namespace is None:
        raise kopf.PermanentError("Missing namespace!")

    scheduler = _scheduler(memo)
    event = scheduler.register(uid)
    try:
        while not stopped:
            event.clear()
            timeout = await _reconcile_once(
                spec=spec,
                name=name,
                namespace=namespace,
                uid=uid,
                logger=logger,
                scheduler=scheduler,
                stopped=stopped,
            )
            if stopped:
                break
            await _wait_for_reconcile(event, stopped, timeout)
    finally:
        scheduler.unregister(uid, event)


@kopf.on.event("", "v1", "pods", labels={MANAGED_BY_LABEL: FIELD_MANAGER})
async def handle_pipeline_pod_event(
    type: str | None,
    body: kopf.Body,
    meta: kopf.Meta,
    labels: kopf.Labels,
    name: str | None,
    namespace: str | None,
    memo: kopf.Memo,
    logger: Logger,
    **_: Any,
) -> None:
    if type not in {"DELETED", "MODIFIED"}:
        return

    if type == "MODIFIED" and meta.deletion_timestamp is None:
        health = pod_health(_deserialize_pod(body), datetime.now(timezone.utc))
        if not health.delete:
            return

    owner_uid = labels.get(OWNER_UID_LABEL)
    if not owner_uid:
        logger.warning(
            "managed Pod %s/%s is missing its owner UID label; cannot reconcile",
            namespace,
            name,
        )
        return

    if _scheduler(memo).notify(owner_uid):
        logger.info("requested reconciliation after Pod %s event=%s", name, type)


@kopf.on.delete(GROUP, VERSION, PLURAL)
async def cleanup(uid: str, memo: kopf.Memo, logger: Logger, **_: Any) -> None:
    scheduler = _scheduler(memo)
    async with scheduler.limit:
        async with scheduler.lock(uid):
            workload_namespace = _workload_namespace()
            core = client.CoreV1Api()
            await asyncio.to_thread(delete_owned_pods, core, workload_namespace, uid, logger)
            await asyncio.to_thread(
                prune_stale_configmaps,
                workload_namespace=workload_namespace,
                owner_uid=uid,
                desired_configmaps=set(),
                logger=logger,
            )
    scheduler.discard(uid)


def main() -> None:
    _workload_namespace()
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

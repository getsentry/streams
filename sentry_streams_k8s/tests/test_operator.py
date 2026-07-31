from __future__ import annotations

import asyncio
import json
from datetime import datetime
from functools import lru_cache
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import ANY, MagicMock, patch

import kopf
import pytest
from kubernetes import client

from sentry_streams_k8s.operator import operator as operator_module
from sentry_streams_k8s.operator.constants import (
    HEALTH_SCAN_INTERVAL_SECONDS,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.operator import (
    ReconcileScheduler,
    _patch_pipeline_status,
    _reconcile_once,
    _wait_for_reconcile,
    cleanup,
    reconcile_pipeline_daemon,
    request_pipeline_reconcile,
)
from sentry_streams_k8s.operator.reconcile import (
    APPLY_PATCH_CONTENT_TYPE,
    PipelineStatusPatch,
    _apply_configmap,
    _apply_deployment,
    _prepare_manifest,
    _prune_stale_resources,
    reconcile_pipeline,
)

WORKLOAD_NAMESPACE = "test-streaming-pipelines"


def test_prepare_manifest_routes_workload_and_records_source_cr() -> None:
    manifest = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "pipeline",
            "labels": {"service": "test"},
            "annotations": {"existing": "annotation"},
            "namespace": "source",
        },
    }

    _prepare_manifest(
        manifest,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
        owner_name="pipeline-cr",
        owner_namespace="default",
    )

    assert manifest["metadata"] == {
        "name": "pipeline",
        "namespace": WORKLOAD_NAMESPACE,
        "labels": {
            "service": "test",
            OWNER_UID_LABEL: "owner-uid",
        },
        "annotations": {
            "existing": "annotation",
            OWNER_NAME_ANNOTATION: "pipeline-cr",
            OWNER_NAMESPACE_ANNOTATION: "default",
        },
    }


def _configmap_manifest() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }


def _deployment_manifest() -> dict[str, Any]:
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }


def test_apply_configmap() -> None:
    core = MagicMock()
    manifest = _configmap_manifest()

    _apply_configmap(core, manifest, workload_namespace=WORKLOAD_NAMESPACE)

    core.patch_namespaced_config_map.assert_called_once_with(
        name="pipeline",
        namespace=WORKLOAD_NAMESPACE,
        body=manifest,
        field_manager="streaming-operator",
        force=True,
        _content_type=APPLY_PATCH_CONTENT_TYPE,
    )


def test_apply_deployment() -> None:
    apps = MagicMock()
    manifest = _deployment_manifest()

    _apply_deployment(apps, manifest, workload_namespace=WORKLOAD_NAMESPACE)

    apps.patch_namespaced_deployment.assert_called_once_with(
        name="pipeline",
        namespace=WORKLOAD_NAMESPACE,
        body=manifest,
        field_manager="streaming-operator",
        force=True,
        _content_type=APPLY_PATCH_CONTENT_TYPE,
    )


@patch("sentry_streams_k8s.operator.reconcile.client.CoreV1Api")
@patch("sentry_streams_k8s.operator.reconcile.client.AppsV1Api")
def test_prune_removes_only_stale_resources(
    apps_api: MagicMock,
    core_api: MagicMock,
) -> None:
    apps = apps_api.return_value
    apps.list_namespaced_deployment.return_value.items = [
        SimpleNamespace(metadata=SimpleNamespace(name="desired-deployment")),
        SimpleNamespace(metadata=SimpleNamespace(name="stale-deployment")),
    ]
    core = core_api.return_value
    core.list_namespaced_config_map.return_value.items = [
        SimpleNamespace(metadata=SimpleNamespace(name="desired-configmap")),
        SimpleNamespace(metadata=SimpleNamespace(name="stale-configmap")),
    ]

    _prune_stale_resources(
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
        desired_deployments={"desired-deployment"},
        desired_configmaps={"desired-configmap"},
        logger=MagicMock(),
    )

    apps.delete_namespaced_deployment.assert_called_once_with(
        name="stale-deployment",
        namespace=WORKLOAD_NAMESPACE,
    )
    core.delete_namespaced_config_map.assert_called_once_with(
        name="stale-configmap",
        namespace=WORKLOAD_NAMESPACE,
    )


class FakeStopped:
    def __init__(self) -> None:
        self._event = asyncio.Event()

    def __bool__(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    async def wait(self, timeout: float | None = None) -> bool:
        try:
            await asyncio.wait_for(self._event.wait(), timeout)
        except asyncio.TimeoutError:
            return False
        return True


def _stopped(flag: FakeStopped) -> kopf.DaemonStopped:
    return cast(kopf.DaemonStopped, flag)


@lru_cache(maxsize=1)
def _api_client() -> client.ApiClient:
    return client.ApiClient()


def _undated(conditions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    undated = []
    for condition in conditions:
        remainder = dict(condition)
        assert datetime.fromisoformat(remainder.pop("lastTransitionTime")).tzinfo is not None
        undated.append(remainder)
    return undated


def _pipeline_spec() -> dict[str, Any]:
    return {"pipeline_config": {"steps": []}, "replicas": 2, "with_canary": True}


def _stub_render(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline-config"},
    }
    primary = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "consumer"},
        "spec": {"replicas": 1},
    }
    canary = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": "consumer-canary"},
        "spec": {"replicas": 1},
    }
    core = MagicMock()
    core.list_namespaced_config_map.return_value.items = []
    core.api_client = _api_client()
    apps = MagicMock()
    apps.list_namespaced_deployment.return_value.items = []

    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.from_crd_spec", lambda spec, name: spec
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.validate", lambda _consumer: None)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render",
        lambda _consumer: {
            "configmap": configmap,
            "deployment": primary,
            "canary_deployment": canary,
        },
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.compute_config_version", lambda _config: "version"
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.client.CoreV1Api", lambda: core)
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.client.AppsV1Api", lambda: apps)
    return core, apps


def test_reconcile_applies_deployments_and_reports_status_through_a_plain_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core, apps = _stub_render(monkeypatch)
    status: PipelineStatusPatch = {}

    reconcile_pipeline(
        spec=_pipeline_spec(),
        name="pipeline",
        namespace="source",
        uid="owner-uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        status=status,
    )

    assert core.patch_namespaced_config_map.call_count == 1
    assert [call.kwargs["name"] for call in apps.patch_namespaced_deployment.call_args_list] == [
        "consumer",
        "consumer-canary",
    ]
    conditions = cast(list[dict[str, Any]], status.pop("conditions"))
    assert _undated(conditions) == [
        {"type": "Rendered", "status": "True", "reason": "Rendered", "message": ""},
        {"type": "Applied", "status": "True", "reason": "Applied", "message": ""},
    ]
    assert status == {
        "config_version": "version",
        "replicas": {"primary": 1, "canary": 1},
        "workload_namespace": WORKLOAD_NAMESPACE,
    }
    # Both write paths hand the status to json.dumps, so no model objects
    # may survive into the payload:
    json.dumps(conditions)


def test_reconcile_records_render_failure_in_the_status_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_render(monkeypatch)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render",
        MagicMock(side_effect=ValueError("bad spec")),
    )
    status: PipelineStatusPatch = {}

    with pytest.raises(kopf.PermanentError):
        reconcile_pipeline(
            spec=_pipeline_spec(),
            name="pipeline",
            namespace="source",
            uid="owner-uid",
            workload_namespace=WORKLOAD_NAMESPACE,
            logger=MagicMock(),
            status=status,
        )

    assert _undated(cast(list[dict[str, Any]], status["conditions"])) == [
        {"type": "Rendered", "status": "False", "reason": "ValueError", "message": "bad spec"}
    ]


def test_unchanged_conditions_keep_their_transition_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_render(monkeypatch)
    published = [
        {
            "type": "Rendered",
            "status": "True",
            "reason": "Rendered",
            "message": "",
            "lastTransitionTime": "2020-01-01T00:00:00+00:00",
        }
    ]
    status: PipelineStatusPatch = {}

    reconcile_pipeline(
        spec=_pipeline_spec(),
        name="pipeline",
        namespace="source",
        uid="owner-uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        status=status,
        previous_conditions=published,
    )

    conditions = cast(list[dict[str, Any]], status["conditions"])
    # Rendered was already True, so its timestamp is carried over untouched;
    # Applied was not published before, so it is stamped now:
    assert conditions[0]["lastTransitionTime"] == "2020-01-01T00:00:00+00:00"
    assert conditions[1]["type"] == "Applied"
    assert conditions[1]["lastTransitionTime"] != "2020-01-01T00:00:00+00:00"


def test_a_flipped_condition_is_restamped(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_render(monkeypatch)
    published = [
        {
            "type": "Rendered",
            "status": "False",
            "reason": "ValueError",
            "message": "bad spec",
            "lastTransitionTime": "2020-01-01T00:00:00+00:00",
        }
    ]
    status: PipelineStatusPatch = {}

    reconcile_pipeline(
        spec=_pipeline_spec(),
        name="pipeline",
        namespace="source",
        uid="owner-uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        status=status,
        previous_conditions=published,
    )

    conditions = cast(list[dict[str, Any]], status["conditions"])
    assert conditions[0] == {
        "type": "Rendered",
        "status": "True",
        "reason": "Rendered",
        "message": "",
        "lastTransitionTime": ANY,
    }
    assert conditions[0]["lastTransitionTime"] != "2020-01-01T00:00:00+00:00"


@patch("sentry_streams_k8s.operator.operator.client.CustomObjectsApi")
def test_patch_pipeline_status_targets_the_status_subresource(api: MagicMock) -> None:
    status: PipelineStatusPatch = {"workload_namespace": WORKLOAD_NAMESPACE}

    _patch_pipeline_status("pipeline", "source", status)

    api.return_value.patch_namespaced_custom_object_status.assert_called_once_with(
        group="streams.sentry.io",
        version="v1alpha1",
        namespace="source",
        plural="streamingpipelines",
        name="pipeline",
        body={"status": status},
    )


def _run_reconcile_once(
    scheduler: ReconcileScheduler,
    *,
    uid: str = "owner-uid",
    stopped: FakeStopped | None = None,
) -> float | None:
    return asyncio.run(
        _reconcile_once(
            spec=cast(kopf.Spec, _pipeline_spec()),
            name="pipeline",
            namespace="source",
            uid=uid,
            logger=MagicMock(),
            scheduler=scheduler,
            stopped=_stopped(stopped if stopped is not None else FakeStopped()),
        )
    )


def test_reconcile_once_publishes_status_and_schedules_the_health_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    patch_status = MagicMock()
    monkeypatch.setattr(operator_module, "_patch_pipeline_status", patch_status)

    def render_status(*, status: PipelineStatusPatch, **_: Any) -> None:
        status["workload_namespace"] = WORKLOAD_NAMESPACE

    monkeypatch.setattr(operator_module, "reconcile_pipeline", render_status)

    timeout = _run_reconcile_once(ReconcileScheduler())

    assert timeout == float(HEALTH_SCAN_INTERVAL_SECONDS)
    patch_status.assert_called_once_with(
        "pipeline", "source", {"workload_namespace": WORKLOAD_NAMESPACE}
    )


def test_reconcile_once_reports_a_permanent_error_without_scheduling_a_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    patch_status = MagicMock()
    monkeypatch.setattr(operator_module, "_patch_pipeline_status", patch_status)

    def fail(*, status: PipelineStatusPatch, **_: Any) -> None:
        status["conditions"] = [
            {"type": "Rendered", "status": "False", "reason": "ValueError", "message": "bad spec"}
        ]
        raise kopf.PermanentError("failed to render")

    monkeypatch.setattr(operator_module, "reconcile_pipeline", fail)
    assert _run_reconcile_once(ReconcileScheduler()) is None
    assert patch_status.call_count == 1


def test_reconcile_once_retries_soon_after_an_unexpected_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    monkeypatch.setattr(operator_module, "_patch_pipeline_status", MagicMock())
    monkeypatch.setattr(
        operator_module, "reconcile_pipeline", MagicMock(side_effect=RuntimeError("boom"))
    )

    assert _run_reconcile_once(ReconcileScheduler()) == 5.0


def test_reconcile_once_skips_the_pass_when_already_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    reconcile = MagicMock()
    monkeypatch.setattr(operator_module, "reconcile_pipeline", reconcile)
    stopped = FakeStopped()
    stopped.set()

    assert _run_reconcile_once(ReconcileScheduler(), stopped=stopped) is None
    reconcile.assert_not_called()


def test_request_pipeline_reconcile_wakes_a_registered_daemon() -> None:
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    async def scenario() -> tuple[bool, bool]:
        event = scheduler.register("uid")
        await request_pipeline_reconcile(uid="unknown-uid", memo=memo, logger=MagicMock())
        unknown_woke = event.is_set()
        await request_pipeline_reconcile(uid="uid", memo=memo, logger=MagicMock())
        return unknown_woke, event.is_set()

    assert asyncio.run(scenario()) == (False, True)


def test_wait_for_reconcile_leaves_no_pending_waiter_behind() -> None:
    async def scenario() -> int:
        event = asyncio.Event()
        event.set()
        # The stop waiter loses the race and must be cancelled and collected,
        # otherwise every loop iteration would leak a task:
        await _wait_for_reconcile(event, _stopped(FakeStopped()), None)
        return len(asyncio.all_tasks())

    assert asyncio.run(scenario()) == 1


def _run_daemon(
    monkeypatch: pytest.MonkeyPatch,
    reconcile_once: Any,
    memo: SimpleNamespace,
    stopped: FakeStopped,
) -> None:
    monkeypatch.setattr(operator_module, "_reconcile_once", reconcile_once)
    asyncio.run(
        asyncio.wait_for(
            reconcile_pipeline_daemon(
                stopped=_stopped(stopped),
                spec=cast(kopf.Spec, _pipeline_spec()),
                body=kopf.Body({}),
                name="pipeline",
                namespace="source",
                uid="uid",
                memo=memo,
                logger=MagicMock(),
            ),
            timeout=10,
        )
    )


def test_daemon_coalesces_wake_up_requests_into_one_extra_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)
    stopped = FakeStopped()
    passes = []

    async def reconcile_once(**_: Any) -> float | None:
        passes.append(len(passes))
        if len(passes) == 1:
            # Three updates land while the first pass is still running:
            for _unused in range(3):
                await request_pipeline_reconcile(uid="uid", memo=memo, logger=MagicMock())
        else:
            stopped.set()
        # None: only an explicit wake-up can drive the next pass.
        return None

    _run_daemon(monkeypatch, reconcile_once, memo, stopped)

    assert len(passes) == 2


def test_daemon_exits_without_waiting_out_its_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)
    stopped = FakeStopped()
    passes = []

    async def reconcile_once(**_: Any) -> float | None:
        passes.append(len(passes))
        stopped.set()
        # A 60s timeout the daemon must not sit through, having been stopped:
        return float(HEALTH_SCAN_INTERVAL_SECONDS)

    _run_daemon(monkeypatch, reconcile_once, memo, stopped)

    assert len(passes) == 1
    # The daemon retracts its wake-up event on the way out:
    assert not scheduler.notify("uid")


def test_cleanup_prunes_every_resource_and_forgets_the_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    prune = MagicMock()
    monkeypatch.setattr(operator_module, "_prune_stale_resources", prune)
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    async def scenario() -> None:
        scheduler.register("uid")
        await cleanup(uid="uid", memo=memo, logger=MagicMock())

    asyncio.run(scenario())

    prune.assert_called_once_with(
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="uid",
        desired_deployments=set(),
        desired_configmaps=set(),
        logger=ANY,
    )
    assert not scheduler.notify("uid")

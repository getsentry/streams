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

from sentry_streams_k8s.constants import CANARY_WORKLOAD_SET, PRIMARY_WORKLOAD_SET
from sentry_streams_k8s.consumer_builder import WorkloadSet
from sentry_streams_k8s.k8s_types import V1ConditionDict
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
    handle_pipeline_pod_event,
    reconcile_pipeline_daemon,
    request_pipeline_reconcile,
)
from sentry_streams_k8s.operator.reconcile import (
    APPLY_PATCH_CONTENT_TYPE,
    PipelineStatusPatch,
    _apply_configmap,
    _merge_conditions,
    _prepare_manifest,
    prune_stale_configmaps,
    reconcile_pipeline,
)
from tests.k8s_fixtures import FakeCoreV1Api

WORKLOAD_NAMESPACE = "test-streaming-pipelines"


def test_prepare_manifest_routes_workload_and_records_source_cr() -> None:
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
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


def test_apply_configmap() -> None:
    core = FakeCoreV1Api()
    manifest = _configmap_manifest()

    _apply_configmap(core, manifest, workload_namespace=WORKLOAD_NAMESPACE)

    assert core.configmap_patch_calls == [
        {
            "name": "pipeline",
            "namespace": WORKLOAD_NAMESPACE,
            "body": manifest,
            "field_manager": "streaming-operator",
            "force": True,
            "_content_type": APPLY_PATCH_CONTENT_TYPE,
        }
    ]


def test_prune_removes_only_stale_configmaps() -> None:
    core = FakeCoreV1Api(
        configmaps=[
            client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name="desired-configmap",
                    labels={OWNER_UID_LABEL: "owner-uid"},
                )
            ),
            client.V1ConfigMap(
                metadata=client.V1ObjectMeta(
                    name="stale-configmap",
                    labels={OWNER_UID_LABEL: "owner-uid"},
                )
            ),
        ]
    )

    prune_stale_configmaps(
        core=core,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
        desired_configmaps={"desired-configmap"},
        logger=MagicMock(),
    )

    assert core.deleted_configmaps == ["stale-configmap"]


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


def _workload(name: str, replicas: int) -> WorkloadSet:
    return WorkloadSet(
        name=name,
        replicas=replicas,
        labels={"app": "consumer"},
        pod_template={
            "metadata": {"labels": {"app": "consumer"}},
            "spec": {"containers": [{"name": "consumer", "image": "example/consumer:v1"}]},
        },
    )


def _stub_render(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline-config"},
    }
    core = MagicMock()
    core.list_namespaced_config_map.return_value.items = []
    core.list_namespaced_pod.return_value.items = []
    core.api_client = _api_client()

    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.from_crd_spec", lambda spec, name: spec
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.validate", lambda _consumer: None)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render_pods",
        lambda _consumer: {
            "configmap": configmap,
            "sets": {
                PRIMARY_WORKLOAD_SET: _workload("consumer", 1),
                CANARY_WORKLOAD_SET: _workload("consumer-canary", 1),
            },
        },
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.compute_config_version", lambda _config: "version"
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.client.CoreV1Api", lambda: core)
    return core


def test_reconcile_applies_pods_and_reports_status_through_a_plain_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _stub_render(monkeypatch)
    status: PipelineStatusPatch = {}

    result = reconcile_pipeline(
        spec=_pipeline_spec(),
        name="pipeline",
        namespace="source",
        uid="owner-uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        status=status,
    )

    assert core.patch_namespaced_config_map.call_count == 1
    assert [call.kwargs["name"] for call in core.patch_namespaced_pod.call_args_list] == [
        "consumer-0-0",
        "consumer-canary-0-0",
    ]
    assert result["childPods"] == ["consumer-0-0", "consumer-canary-0-0"]
    assert result["desiredReplicas"] == 2
    assert result["readyReplicas"] == 0

    conditions = cast(list[dict[str, Any]], status.pop("conditions"))
    assert _undated(conditions) == [
        {"type": "Rendered", "status": "True", "reason": "Rendered", "message": ""},
        {"type": "Applied", "status": "True", "reason": "Applied", "message": ""},
    ]
    assert status["config_version"] == "version"
    assert status["workload_namespace"] == WORKLOAD_NAMESPACE
    assert status["pods"] == result
    assert status["generations"] == {
        PRIMARY_WORKLOAD_SET: {"0": 0},
        CANARY_WORKLOAD_SET: {"0": 0},
    }
    # Both write paths hand the status to json.dumps, so no model objects
    # may survive into the payload:
    json.dumps(status)


def test_reconcile_nulls_out_a_workload_set_that_is_no_longer_rendered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = _stub_render(monkeypatch)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render_pods",
        lambda _consumer: {
            "configmap": {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "pipeline-config"},
            },
            "sets": {PRIMARY_WORKLOAD_SET: _workload("consumer", 1)},
        },
    )
    status: PipelineStatusPatch = {}

    result = reconcile_pipeline(
        spec=_pipeline_spec(),
        name="pipeline",
        namespace="source",
        uid="owner-uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        status=status,
        previous_generations={PRIMARY_WORKLOAD_SET: {"0": 4}, CANARY_WORKLOAD_SET: {"0": 2}},
    )

    assert result["sets"][CANARY_WORKLOAD_SET] is None
    assert status["generations"] == {PRIMARY_WORKLOAD_SET: {"0": 5}, CANARY_WORKLOAD_SET: None}
    assert [call.kwargs["name"] for call in core.patch_namespaced_pod.call_args_list] == [
        "consumer-0-5"
    ]


def test_reconcile_records_render_failure_in_the_status_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_render(monkeypatch)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render_pods",
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


@pytest.mark.parametrize(
    ("previous_status", "expected_timestamp"),
    [
        ("True", "2020-01-01T00:00:00+00:00"),
        ("False", "2026-01-01T00:00:00+00:00"),
    ],
)
def test_merge_conditions_only_preserves_unchanged_transition_timestamp(
    previous_status: str,
    expected_timestamp: str,
) -> None:
    previous = cast(
        list[V1ConditionDict],
        [
            {
                "type": "Rendered",
                "status": previous_status,
                "reason": "Previous",
                "message": "",
                "lastTransitionTime": "2020-01-01T00:00:00+00:00",
            }
        ],
    )
    current = cast(
        list[V1ConditionDict],
        [
            {
                "type": "Rendered",
                "status": "True",
                "reason": "Rendered",
                "message": "",
                "lastTransitionTime": "2026-01-01T00:00:00+00:00",
            }
        ],
    )

    merged = _merge_conditions(previous, current)

    assert merged == [
        {
            "type": "Rendered",
            "status": "True",
            "reason": "Rendered",
            "message": "",
            "lastTransitionTime": expected_timestamp,
        }
    ]


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
    monkeypatch: pytest.MonkeyPatch,
    scheduler: ReconcileScheduler,
    *,
    uid: str = "owner-uid",
    stopped: FakeStopped | None = None,
    published: dict[str, Any] | None = None,
) -> float | None:
    monkeypatch.setattr(
        operator_module,
        "_get_pipeline_status",
        lambda _name, _namespace: published if published is not None else {},
    )
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

    timeout = _run_reconcile_once(monkeypatch, ReconcileScheduler())

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
    assert _run_reconcile_once(monkeypatch, ReconcileScheduler()) is None
    assert patch_status.call_count == 1


def test_reconcile_once_retries_soon_after_an_unexpected_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    monkeypatch.setattr(operator_module, "_patch_pipeline_status", MagicMock())
    monkeypatch.setattr(
        operator_module, "reconcile_pipeline", MagicMock(side_effect=RuntimeError("boom"))
    )

    assert _run_reconcile_once(monkeypatch, ReconcileScheduler()) == 5.0


def test_reconcile_once_skips_the_pass_when_already_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    reconcile = MagicMock()
    monkeypatch.setattr(operator_module, "reconcile_pipeline", reconcile)
    stopped = FakeStopped()
    stopped.set()

    assert _run_reconcile_once(monkeypatch, ReconcileScheduler(), stopped=stopped) is None
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


def test_cleanup_deletes_owned_pods_and_prunes_configmaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    core = MagicMock()
    delete_pods = MagicMock()
    prune = MagicMock()
    monkeypatch.setattr(operator_module.client, "CoreV1Api", lambda: core)
    monkeypatch.setattr(operator_module, "delete_owned_pods", delete_pods)
    monkeypatch.setattr(operator_module, "prune_stale_configmaps", prune)
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    async def scenario() -> None:
        scheduler.register("uid")
        await cleanup(uid="uid", memo=memo, logger=MagicMock())

    asyncio.run(scenario())

    delete_pods.assert_called_once_with(core, WORKLOAD_NAMESPACE, "uid", ANY)
    prune.assert_called_once_with(
        core=core,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="uid",
        desired_configmaps=set(),
        logger=ANY,
    )
    assert not scheduler.notify("uid")


def test_pod_event_wakes_for_unhealthy_updates_and_ignores_healthy_updates() -> None:
    scheduler = ReconcileScheduler()
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    async def scenario() -> tuple[bool, bool]:
        event = scheduler.register("uid")
        await handle_pipeline_pod_event(
            type="MODIFIED",
            body=kopf.Body({"metadata": {"name": "consumer-0-0"}, "status": {"phase": "Running"}}),
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "uid"},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=MagicMock(),
        )
        healthy_woke = event.is_set()

        await handle_pipeline_pod_event(
            type="MODIFIED",
            body=kopf.Body({"metadata": {"name": "consumer-0-0"}, "status": {"phase": "Failed"}}),
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "uid"},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=MagicMock(),
        )
        return healthy_woke, event.is_set()

    assert asyncio.run(scenario()) == (False, True)

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, MagicMock, patch

import kopf
import pytest

from sentry_streams_k8s.operator.constants import (
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.operator import (
    ReconcileScheduler,
    cleanup,
    handle_pipeline_pod_event,
)
from sentry_streams_k8s.operator.reconcile import (
    _apply_configmap,
    _prepare_manifest,
    prune_stale_configmaps,
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


def test_apply_configmap_rejects_resource_owned_by_another_cr() -> None:
    core = MagicMock()
    core.read_namespaced_config_map.return_value = SimpleNamespace(
        metadata=SimpleNamespace(labels={OWNER_UID_LABEL: "another-owner"})
    )
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }

    with pytest.raises(kopf.PermanentError, match="not managed by this StreamingPipeline"):
        _apply_configmap(
            core,
            manifest,
            workload_namespace=WORKLOAD_NAMESPACE,
            owner_uid="owner-uid",
        )

    core.patch_namespaced_config_map.assert_not_called()


def test_apply_configmap_uses_workload_namespace_and_stable_field_manager() -> None:
    core = MagicMock()
    core.read_namespaced_config_map.return_value = SimpleNamespace(
        metadata=SimpleNamespace(labels={OWNER_UID_LABEL: "owner-uid"})
    )
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }

    _apply_configmap(
        core,
        manifest,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
    )

    core.read_namespaced_config_map.assert_called_once_with(
        name="pipeline", namespace=WORKLOAD_NAMESPACE
    )
    core.patch_namespaced_config_map.assert_called_once_with(
        name="pipeline",
        namespace=WORKLOAD_NAMESPACE,
        body=manifest,
        field_manager="streaming-operator",
        force=True,
        _content_type="application/apply-patch+yaml",
    )


@patch("sentry_streams_k8s.operator.reconcile.client.CoreV1Api")
def test_prune_removes_only_stale_configmaps(core_api: MagicMock) -> None:
    core = core_api.return_value
    core.list_namespaced_config_map.return_value.items = [
        SimpleNamespace(metadata=SimpleNamespace(name="desired-configmap")),
        SimpleNamespace(metadata=SimpleNamespace(name="stale-configmap")),
    ]

    prune_stale_configmaps(
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
        desired_configmaps={"desired-configmap"},
        logger=MagicMock(),
    )

    core.delete_namespaced_config_map.assert_called_once_with(
        name="stale-configmap",
        namespace=WORKLOAD_NAMESPACE,
    )


def test_do_reconcile_applies_config_reconciles_pods_and_reports_generations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = MagicMock()
    configmap = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "pipeline-config"}}
    primary_deployment = {
        "metadata": {"name": "consumer"},
        "spec": {
            "replicas": 2,
            "template": {"metadata": {"labels": {"env": "primary"}}, "spec": {"containers": []}},
        },
    }
    canary_deployment = {
        "metadata": {"name": "consumer-canary"},
        "spec": {
            "replicas": 1,
            "template": {"metadata": {"labels": {"env": "canary"}}, "spec": {"containers": []}},
        },
    }
    primary_result = {
        "childPods": ["consumer-0-2"],
        "desiredReplicas": 2,
        "readyReplicas": 1,
        "unhealthyPods": [],
        "permanentErrors": [],
    }
    canary_result = {
        "childPods": ["consumer-canary-0-5"],
        "desiredReplicas": 1,
        "readyReplicas": 0,
        "unhealthyPods": [],
        "permanentErrors": [],
    }
    apply = MagicMock()
    reconcile_pods = MagicMock()
    delete_obsolete = MagicMock()
    prune = MagicMock()
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.from_crd_spec", lambda spec, name: spec
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.validate", lambda _consumer: None)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render",
        lambda _consumer: {
            "configmap": configmap,
            "deployment": primary_deployment,
            "canary_deployment": canary_deployment,
        },
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.client.CoreV1Api", lambda: core)
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile._apply_configmap", apply)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.reconcile_pipeline_pods", reconcile_pods
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_obsolete_pod_sets",
        delete_obsolete,
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.prune_stale_configmaps", prune)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.compute_config_version", lambda _: "version"
    )

    def allocate_generation(
        *, generations: dict[int, int], workload_set: str, **_: object
    ) -> dict[str, object]:
        if workload_set == "primary":
            generations[0] = 2
            return primary_result
        generations[0] = 5
        return canary_result

    reconcile_pods.side_effect = allocate_generation
    status: dict[str, Any] = {}
    logger = MagicMock()
    result = reconcile_pipeline(
        spec={"pipeline_config": {}, "replicas": 1, "with_canary": True},
        name="pipeline",
        namespace="source",
        uid="uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=logger,
        status=status,
        previous_generations={"primary": {"0": 1}, "canary": {"0": 4}},
    )

    assert result == {
        "childPods": ["consumer-0-2", "consumer-canary-0-5"],
        "desiredReplicas": 3,
        "readyReplicas": 1,
        "unhealthyPods": [],
        "permanentErrors": [],
        "sets": {
            "primary": primary_result,
            "canary": canary_result,
        },
    }
    apply.assert_called_once_with(
        core,
        configmap,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="uid",
    )
    assert [call.kwargs["workload_set"] for call in reconcile_pods.call_args_list] == [
        "primary",
        "canary",
    ]
    assert [call.kwargs["replicas"] for call in reconcile_pods.call_args_list] == [2, 1]
    assert [call.kwargs["base_name"] for call in reconcile_pods.call_args_list] == [
        "consumer",
        "consumer-canary",
    ]
    delete_obsolete.assert_called_once_with(
        core,
        WORKLOAD_NAMESPACE,
        "uid",
        {"primary", "canary"},
        ANY,
    )
    prune.assert_called_once_with(
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="uid",
        desired_configmaps={"pipeline-config"},
        logger=logger,
    )
    assert status["conditions"][-1] == {
        "type": "Applied",
        "status": "True",
        "reason": "Applied",
        "message": "",
    }
    assert status["pods"] == result
    assert status["generations"] == {"primary": {"0": 2}, "canary": {"0": 5}}


def test_do_reconcile_preserves_unchanged_ledger_and_reports_permanent_pod_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = MagicMock()
    result = {
        "childPods": ["consumer-0-1"],
        "desiredReplicas": 1,
        "readyReplicas": 0,
        "unhealthyPods": [],
        "permanentErrors": [
            {
                "name": "consumer-0-1",
                "ready": False,
                "phase": "Pending",
                "reason": "InvalidImageName",
                "permanent": True,
            }
        ],
    }
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.from_crd_spec", lambda spec, name: spec
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.validate", lambda _consumer: None)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.render",
        lambda _: {
            "configmap": {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "config"}},
            "deployment": {
                "metadata": {"name": "consumer"},
                "spec": {
                    "replicas": 1,
                    "template": {"metadata": {}, "spec": {"containers": []}},
                },
            },
        },
    )
    monkeypatch.setattr("sentry_streams_k8s.operator.reconcile.client.CoreV1Api", lambda: core)
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile._apply_configmap", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.reconcile_pipeline_pods", lambda **_: result
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_obsolete_pod_sets",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.prune_stale_configmaps", lambda **_: None
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.compute_config_version", lambda _: "version"
    )
    patch = SimpleNamespace(status={})

    reconcile_pipeline(
        spec={"pipeline_config": {}},
        name="pipeline",
        namespace="source",
        uid="uid",
        workload_namespace=WORKLOAD_NAMESPACE,
        logger=MagicMock(),
        patch=patch,
        previous_generations={"primary": {"0": 1}},
    )

    assert patch.status["conditions"][-1]["status"] == "False"
    assert patch.status["conditions"][-1]["reason"] == "PermanentPodFailure"
    assert patch.status["generations"] == {"primary": {"0": 1}, "canary": None}


def test_pod_event_coalesces_reconcile_requests_and_ignores_healthy_updates() -> None:
    scheduler = ReconcileScheduler()
    event = scheduler.register("uid")
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    asyncio.run(
        handle_pipeline_pod_event(
            type="MODIFIED",
            body={"metadata": {"name": "consumer-0-0"}, "status": {"phase": "Running"}},
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "uid"},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=MagicMock(),
        )
    )
    assert not event.is_set()

    asyncio.run(
        handle_pipeline_pod_event(
            type="DELETED",
            body={},
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "uid"},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=MagicMock(),
        )
    )
    asyncio.run(
        handle_pipeline_pod_event(
            type="DELETED",
            body={},
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "uid"},
            name="consumer-1-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=MagicMock(),
        )
    )
    assert event.is_set()


def test_pod_event_ignores_missing_or_inactive_owner() -> None:
    scheduler = ReconcileScheduler()
    event = scheduler.register("active-uid")
    memo = SimpleNamespace(reconcile_scheduler=scheduler)
    logger = MagicMock()

    asyncio.run(
        handle_pipeline_pod_event(
            type="DELETED",
            body={},
            meta=kopf.Meta({}),
            labels={},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=logger,
        )
    )
    asyncio.run(
        handle_pipeline_pod_event(
            type="DELETED",
            body={},
            meta=kopf.Meta({}),
            labels={OWNER_UID_LABEL: "deleted-uid"},
            name="consumer-0-0",
            namespace=WORKLOAD_NAMESPACE,
            memo=memo,
            logger=logger,
        )
    )
    assert not event.is_set()


def test_cleanup_deletes_owned_pods_and_prunes_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    core = MagicMock()
    delete_pods = MagicMock()
    prune = MagicMock()
    monkeypatch.setenv("WORKLOAD_NAMESPACE", WORKLOAD_NAMESPACE)
    monkeypatch.setattr("sentry_streams_k8s.operator.operator.client.CoreV1Api", lambda: core)
    monkeypatch.setattr("sentry_streams_k8s.operator.operator.delete_owned_pods", delete_pods)
    monkeypatch.setattr("sentry_streams_k8s.operator.operator.prune_stale_configmaps", prune)
    scheduler = ReconcileScheduler()
    scheduler.register("uid")
    memo = SimpleNamespace(reconcile_scheduler=scheduler)

    asyncio.run(cleanup(uid="uid", memo=memo, logger=MagicMock()))

    delete_pods.assert_called_once_with(core, WORKLOAD_NAMESPACE, "uid", ANY)
    prune.assert_called_once_with(
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="uid",
        desired_configmaps=set(),
        logger=ANY,
    )

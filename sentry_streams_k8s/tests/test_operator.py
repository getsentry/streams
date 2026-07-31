from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

from sentry_streams_k8s.operator.constants import (
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.reconcile import (
    APPLY_PATCH_CONTENT_TYPE,
    _apply_configmap,
    _apply_deployment,
    _prepare_manifest,
    _prune_stale_resources,
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
    )

    apps.delete_namespaced_deployment.assert_called_once_with(
        name="stale-deployment",
        namespace=WORKLOAD_NAMESPACE,
    )
    core.delete_namespaced_config_map.assert_called_once_with(
        name="stale-configmap",
        namespace=WORKLOAD_NAMESPACE,
    )

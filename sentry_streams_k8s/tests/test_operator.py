from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import kopf
import pytest

from sentry_streams_k8s.operator.constants import (
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.reconcile import (
    _apply,
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


def test_apply_rejects_resource_owned_by_another_cr() -> None:
    resource = MagicMock()
    resource.get.return_value = SimpleNamespace(
        metadata=SimpleNamespace(labels={OWNER_UID_LABEL: "another-owner"})
    )
    dyn = MagicMock()
    dyn.resources.get.return_value = resource
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }

    with pytest.raises(kopf.PermanentError, match="not managed by this StreamingPipeline"):
        _apply(
            dyn,
            manifest,
            workload_namespace=WORKLOAD_NAMESPACE,
            owner_uid="owner-uid",
        )

    dyn.server_side_apply.assert_not_called()


def test_apply_uses_workload_namespace_and_stable_field_manager() -> None:
    resource = MagicMock()
    resource.get.return_value = SimpleNamespace(
        metadata=SimpleNamespace(labels={OWNER_UID_LABEL: "owner-uid"})
    )
    dyn = MagicMock()
    dyn.resources.get.return_value = resource
    manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "pipeline", "namespace": WORKLOAD_NAMESPACE},
    }

    _apply(
        dyn,
        manifest,
        workload_namespace=WORKLOAD_NAMESPACE,
        owner_uid="owner-uid",
    )

    resource.get.assert_called_once_with(name="pipeline", namespace=WORKLOAD_NAMESPACE)
    dyn.server_side_apply.assert_called_once_with(
        resource,
        body=manifest,
        namespace=WORKLOAD_NAMESPACE,
        field_manager="streaming-operator",
        force_conflicts=True,
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

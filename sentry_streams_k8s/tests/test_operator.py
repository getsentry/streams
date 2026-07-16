from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call

import kopf
import pytest

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator import operator


def consumer_spec(**overrides: Any) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "my-pipeline",
        "pipeline_module": "pipelines/my_pipeline.py",
        "image_name": "registry.example.com/image:abc123",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "replicas": 4,
        "deployment_template": {
            "metadata": {"labels": {"component": "my-component"}},
            "spec": {
                "selector": {"matchLabels": {"component": "my-component"}},
                "template": {
                    "metadata": {"labels": {"component": "my-component"}},
                    "spec": {"serviceAccountName": "some-service-account"},
                },
            },
        },
        "container_template": {},
        "pipeline_config": {
            "env": {},
            "pipeline": {
                "segments": [
                    {
                        "steps_config": {
                            "myinput": {
                                "starts_segment": True,
                                "bootstrap_servers": ["127.0.0.1:9092"],
                            }
                        }
                    }
                ]
            },
        },
    }
    spec.update(overrides)
    return spec


def mock_k8s(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock, MagicMock]:
    api_client = MagicMock(name="api_client")
    dynamic_client = MagicMock(name="dynamic_client")
    apps_api = MagicMock(name="apps_api")
    apps_api.list_namespaced_deployment.return_value = SimpleNamespace(items=[])

    monkeypatch.setattr(operator.client, "ApiClient", MagicMock(return_value=api_client))
    monkeypatch.setattr(
        operator.dynamic, "DynamicClient", MagicMock(return_value=dynamic_client)
    )
    monkeypatch.setattr(operator.client, "AppsV1Api", MagicMock(return_value=apps_api))
    adopt = MagicMock()
    monkeypatch.setattr(operator.kopf, "adopt", adopt)
    return dynamic_client, apps_api, adopt


def test_reconcile_applies_rendered_resources_and_updates_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dynamic_client, apps_api, adopt = mock_k8s(monkeypatch)
    spec = consumer_spec(with_canary=True)
    patch = SimpleNamespace(status={})

    operator.reconcile(
        spec=spec,
        name="my-pipeline",
        namespace="streams",
        uid="pipeline-uid",
        patch=patch,
    )

    manifests = [args[1]["body"] for args in dynamic_client.server_side_apply.call_args_list]
    assert [(manifest["kind"], manifest["metadata"]["name"]) for manifest in manifests] == [
        ("ConfigMap", "my-service-pipeline-my-pipeline"),
        ("Deployment", "my-service-pipeline-my-pipeline-0"),
        ("Deployment", "my-service-pipeline-my-pipeline-0-canary"),
    ]
    assert adopt.call_args_list == [call(manifest) for manifest in manifests]
    for applied in dynamic_client.server_side_apply.call_args_list:
        assert applied.kwargs["namespace"] == "streams"
        assert applied.kwargs["field_manager"] == operator.FIELD_MANAGER
        assert applied.kwargs["force_conflicts"] is True

    apps_api.list_namespaced_deployment.assert_called_once_with(
        namespace="streams",
        label_selector="service=my-service,pipeline=my-pipeline",
    )
    assert patch.status == {
        "conditions": [
            {"type": "Rendered", "status": "True", "reason": "Rendered", "message": ""},
            {"type": "Applied", "status": "True", "reason": "Applied", "message": ""},
        ],
        "config_version": compute_config_version(spec["pipeline_config"]),
        "replicas": {"primary": 3, "canary": 1},
    }


def test_reconcile_prunes_only_owned_stale_deployments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, apps_api, _ = mock_k8s(monkeypatch)

    def deployment(name: str, owner_uid: str) -> SimpleNamespace:
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                owner_references=[SimpleNamespace(uid=owner_uid)],
            )
        )

    apps_api.list_namespaced_deployment.return_value = SimpleNamespace(
        items=[
            deployment("my-service-pipeline-my-pipeline-0", "pipeline-uid"),
            deployment("stale-owned", "pipeline-uid"),
            deployment("stale-owned-by-another-resource", "other-uid"),
        ]
    )

    operator.reconcile(
        spec=consumer_spec(),
        name="my-pipeline",
        namespace="streams",
        uid="pipeline-uid",
        patch=SimpleNamespace(status={}),
    )

    apps_api.delete_namespaced_deployment.assert_called_once_with(
        name="stale-owned",
        namespace="streams",
    )


def test_reconcile_records_render_failure_without_calling_k8s(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api_client = MagicMock()
    monkeypatch.setattr(operator.client, "ApiClient", api_client)
    patch = SimpleNamespace(status={})

    with pytest.raises(kopf.PermanentError, match="failed to render"):
        operator.reconcile(
            spec={"service_name": "my-service"},
            name="my-pipeline",
            namespace="streams",
            uid="pipeline-uid",
            patch=patch,
        )

    api_client.assert_not_called()
    assert patch.status["conditions"] == [
        {
            "type": "Rendered",
            "status": "False",
            "reason": "ValueError",
            "message": (
                "StreamingPipeline is missing required field(s): container_template, "
                "cpu_per_process, deployment_template, image_name, memory_per_process, "
                "pipeline_config, pipeline_module."
            ),
        }
    ]

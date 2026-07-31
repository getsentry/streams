from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest
import yaml

from sentry_streams_k8s.consumer_builder import compute_config_version
from sentry_streams_k8s.operator.streaming_pipeline import (
    REQUIRED_FIELDS,
    StreamingPipelineSpec,
    from_crd_spec,
    render,
    validate,
)

CRD_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "chart"
    / "streaming-operator"
    / "crds"
    / "crd.yaml"
)


def pipeline_config() -> dict[str, Any]:
    return {
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
    }


def deployment_template() -> dict[str, Any]:
    """A materialized deployment template, as authored onto the CR — per-consumer
    values (labels, selector) are already filled in."""
    return {
        "metadata": {
            "labels": {
                "app_feature": "my_feature",
                "component": "my-component",
            }
        },
        "spec": {
            "minReadySeconds": 60,
            "selector": {"matchLabels": {"component": "my-component"}},
            "template": {
                "metadata": {
                    "labels": {
                        "app_feature": "my_feature",
                        "component": "my-component",
                    }
                },
                "spec": {
                    "serviceAccountName": "some-service-account",
                    "nodeSelector": {"node-class": "general"},
                },
            },
        },
    }


def container_template() -> dict[str, Any]:
    return {
        "env": [
            {"name": "MY_ENVIRONMENT", "value": "some-region"},
            {
                "name": "SOME_SECRET",
                "valueFrom": {"secretKeyRef": {"name": "some-secret-name", "key": "SOME_SECRET"}},
            },
        ],
        "securityContext": {"runAsUser": 1000},
    }


def consumer_spec(**overrides: Any) -> StreamingPipelineSpec:
    spec: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "my-pipeline",
        "pipeline_module": "pipelines/my_pipeline.py",
        "image_name": "registry.example.com/image:abc123",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "replicas": 4,
        "deployment_template": deployment_template(),
        "container_template": container_template(),
        "pipeline_config": pipeline_config(),
    }
    spec.update(overrides)
    return spec  # type: ignore[return-value]


def test_from_crd_spec_defaults_pipeline_name_to_metadata_name() -> None:
    spec = from_crd_spec({"service_name": "svc"}, name="from-metadata")
    assert spec["pipeline_name"] == "from-metadata"

    spec = from_crd_spec({"pipeline_name": "explicit"}, name="from-metadata")
    assert spec["pipeline_name"] == "explicit"


def test_validate_lists_missing_fields() -> None:
    with pytest.raises(ValueError) as exc_info:
        validate({"service_name": "svc"})  # type: ignore[typeddict-item]
    for field in REQUIRED_FIELDS:
        if field != "service_name":
            assert field in str(exc_info.value)


def test_render_produces_manifests_from_inputs() -> None:
    result = render(consumer_spec())

    deployment = result["deployment"]
    configmap = result["configmap"]
    assert "canary_deployment" not in result

    # The template content came through verbatim.
    assert deployment["metadata"]["labels"]["app_feature"] == "my_feature"
    assert deployment["metadata"]["labels"]["component"] == "my-component"
    assert deployment["spec"]["selector"]["matchLabels"]["component"] == "my-component"
    assert deployment["spec"]["template"]["spec"]["serviceAccountName"] == "some-service-account"
    assert deployment["spec"]["template"]["spec"]["nodeSelector"] == {"node-class": "general"}
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert {"name": "MY_ENVIRONMENT", "value": "some-region"} in container["env"]

    # The pipeline-managed layer was added on top.
    assert deployment["metadata"]["name"] == "my-service-pipeline-my-pipeline-0"
    assert deployment["spec"]["replicas"] == 4
    assert container["image"] == "registry.example.com/image:abc123"

    # The pipeline config is input, serialized canonically (sorted keys).
    data = configmap["data"]["pipeline_config.yaml"]
    assert data == json.dumps(pipeline_config(), sort_keys=True)
    assert deployment["spec"]["template"]["metadata"]["annotations"]["configVersion"] == (
        compute_config_version(pipeline_config())
    )


def test_render_passes_envvar_tokens_through_config() -> None:
    config = pipeline_config()
    config["metrics"] = {"type": "datadog", "host": "${envvar:HOST_IP}", "port": 8128}
    result = render(consumer_spec(pipeline_config=config))
    assert "${envvar:HOST_IP}" in result["configmap"]["data"]["pipeline_config.yaml"]


def test_render_canary_split() -> None:
    result = render(consumer_spec(with_canary=True, replicas=4))
    assert result["deployment"]["spec"]["replicas"] == 3
    assert result["canary_deployment"]["spec"]["replicas"] == 1
    assert result["deployment"]["spec"]["selector"]["matchLabels"]["env"] == "primary"
    assert result["canary_deployment"]["spec"]["selector"]["matchLabels"]["env"] == "canary"


def test_render_rejects_liveness_probe_conflict() -> None:
    template = container_template()
    template["livenessProbe"] = {"exec": {"command": ["true"]}}
    with pytest.raises(ValueError, match="livenessProbe"):
        render(consumer_spec(container_template=template))


def test_render_matches_macro_adapter() -> None:
    """Operator and macro are two adapters over the same builder: given the same
    inputs they must produce byte-identical manifests."""
    from sentry_streams_k8s.pipeline_step import PipelineStep

    spec = consumer_spec()
    macro_result = PipelineStep().run(
        {
            "service_name": spec["service_name"],
            "pipeline_name": spec["pipeline_name"],
            "deployment_template": deployment_template(),
            "container_template": container_template(),
            "pipeline_config": spec["pipeline_config"],
            "pipeline_module": spec["pipeline_module"],
            "image_name": spec["image_name"],
            "cpu_per_process": spec["cpu_per_process"],
            "memory_per_process": spec["memory_per_process"],
            "segment_id": 0,
            "replicas": spec["replicas"],
        }
    )
    operator_result = render(spec)
    assert operator_result == macro_result


def test_crd_required_fields_match_operator_validation() -> None:
    crd = yaml.safe_load(CRD_PATH.read_text())
    version = crd["spec"]["versions"][0]
    crd_required = version["schema"]["openAPIV3Schema"]["properties"]["spec"]["required"]
    assert set(crd_required) == set(REQUIRED_FIELDS)

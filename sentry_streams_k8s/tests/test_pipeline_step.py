from typing import Any

import pytest
import yaml
from jsonschema import ValidationError

from sentry_streams_k8s.pipeline_step import (
    PipelineStep,
    build_container,
    build_labels,
    make_k8s_name,
)


def test_make_k8s_name() -> None:
    """Test that make_k8s_name generates valid Kubernetes names."""
    assert make_k8s_name("sbc.profiles") == "sbc-profiles"

    assert make_k8s_name("my_module.sub_module") == "my-module-sub-module"

    # Test with mixed case (should be lowercase)
    assert make_k8s_name("MyModule.SubModule") == "mymodule-submodule"

    # Test with special characters (should be removed)
    assert make_k8s_name("my@module.sub#module") == "mymodule-submodule"


def test_build_labels() -> None:
    """Test that build_labels generates correct Kubernetes labels."""
    template_labels = {"existing-label": "value"}
    labels = build_labels(template_labels, "profiles", "sbc.profiles")

    assert labels == {
        "existing-label": "value",
        "pipeline-app": "sbc-profiles",
        "pipeline": "profiles",
    }


def test_build_container() -> None:
    """Test that build_container creates a complete container spec."""
    container_template = {
        "name": "streaming-consumer",
        "image": "my-image:latest",
    }

    container = build_container(
        container_template=container_template,
        pipeline_name="profiles",
        pipeline_module="sbc.profiles",
        pipeline_config={},
        image_name="my-image:latest",
        cpu_per_process=1000,
        memory_per_process=512,
        segment_id=0,
    )

    assert container == {
        "name": "streaming-consumer",
        "image": "my-image:latest",
        "command": ["python", "-m", "sentry_streams.runner"],
        "args": [
            "-n",
            "profiles",
            "--adapter",
            "rust_arroyo",
            "--segment-id",
            "0",
            "--config",
            "/etc/pipeline-config/pipeline_config.yaml",
            "sbc.profiles",
        ],
        "resources": {
            "requests": {
                "cpu": "1000m",
                "memory": "512Mi",
            },
            "limits": {
                "memory": "512Mi",
            },
        },
        "volumeMounts": [
            {
                "name": "pipeline-config",
                "mountPath": "/etc/pipeline-config",
                "readOnly": True,
            }
        ],
    }


def test_build_container_preserves_existing_fields() -> None:
    """Test that build_container preserves existing container fields."""
    container_template = {
        "name": "streaming-consumer",
        "image": "my-image:latest",
        "env": [{"name": "MY_VAR", "value": "my_value"}],
        "volumeMounts": [{"name": "existing-volume", "mountPath": "/existing"}],
    }

    container = build_container(
        container_template=container_template,
        pipeline_name="profiles",
        pipeline_module="sbc.profiles",
        pipeline_config={},
        image_name="my-image:latest",
        cpu_per_process=1000,
        memory_per_process=512,
        segment_id=0,
    )

    # Check that existing fields are preserved
    assert container["env"] == [{"name": "MY_VAR", "value": "my_value"}]
    assert container["image"] == "my-image:latest"
    assert len(container["volumeMounts"]) == 2
    assert container["volumeMounts"][0] == {"name": "existing-volume", "mountPath": "/existing"}


def test_validate_context_valid() -> None:
    """Test that validate_context accepts valid context."""
    valid_context = {
        "service_name": "my-service",
        "pipeline_name": "my-pipeline",
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container"},
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
        "pipeline_module": "my_module",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "segment_id": 0,
    }

    # Should not raise any exception
    PipelineStep.validate_context(valid_context)


def test_validate_context_missing_fields() -> None:
    """Test that validate_context raises for missing fields."""
    base_context = {
        "service_name": "my-service",
        "pipeline_name": "my-pipeline",
        "segment_id": 0,
        "pipeline_module": "my_module",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    # Missing deployment_template
    with pytest.raises(AssertionError, match="deployment_template"):
        PipelineStep.validate_context(
            {
                **base_context,
                "container_template": {},
                "pipeline_config": {"env": {}, "pipeline": {"segments": []}},
            }
        )

    # Missing container_template
    with pytest.raises(AssertionError, match="container_template"):
        PipelineStep.validate_context(
            {
                **base_context,
                "deployment_template": {},
                "pipeline_config": {"env": {}, "pipeline": {"segments": []}},
            }
        )

    # Missing pipeline_config
    with pytest.raises(AssertionError, match="pipeline_config"):
        PipelineStep.validate_context(
            {
                **base_context,
                "deployment_template": {},
                "container_template": {},
            }
        )


def test_validate_context_invalid_pipeline_config() -> None:
    """Test that validate_context validates pipeline_config schema."""
    invalid_context = {
        "service_name": "my-service",
        "pipeline_name": "my-pipeline",
        "deployment_template": {},
        "container_template": {},
        "pipeline_config": {
            "env": {},
            "pipeline": {
                # Missing required 'segments' key
            },
        },
        "pipeline_module": "my_module",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "segment_id": 0,
    }

    with pytest.raises(ValidationError):
        PipelineStep.validate_context(invalid_context)


def test_run_generates_complete_manifests() -> None:
    """Test that run() generates complete deployment and configmap manifests."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container"},
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
        "pipeline_module": "sbc.profiles",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "segment_id": 0,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    # Validate return structure
    assert "deployment" in result
    assert "configmap" in result
    assert isinstance(result["deployment"], dict)
    assert isinstance(result["configmap"], dict)

    deployment = result["deployment"]
    configmap = result["configmap"]

    # Validate deployment name
    assert deployment["metadata"]["name"] == "my-service-pipeline-profiles-0"

    # Validate deployment has container
    containers = deployment["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    assert containers[0]["name"] == "container"
    assert containers[0]["resources"]["requests"]["cpu"] == "1000m"
    assert containers[0]["resources"]["requests"]["memory"] == "512Mi"

    # Validate deployment has configmap volume
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    assert len(volumes) == 1
    assert volumes[0]["name"] == "pipeline-config"
    assert volumes[0]["configMap"]["name"] == "my-service-pipeline-profiles"

    # Validate configmap structure
    assert configmap["apiVersion"] == "v1"
    assert configmap["kind"] == "ConfigMap"
    assert configmap["metadata"]["name"] == "my-service-pipeline-profiles"
    assert configmap["metadata"]["labels"]["pipeline-app"] == "sbc-profiles"
    assert configmap["metadata"]["labels"]["pipeline"] == "profiles"

    # Validate configmap data
    assert "pipeline_config.yaml" in configmap["data"]
    config_yaml = configmap["data"]["pipeline_config.yaml"]
    parsed_config = yaml.safe_load(config_yaml)
    assert parsed_config == context["pipeline_config"]


def test_run_preserves_deployment_template() -> None:
    """Test that run() preserves existing deployment_template fields."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "kind": "Deployment",
            "apiVersion": "apps/v1",
            "metadata": {
                "namespace": "my-namespace",
                "annotations": {"my-annotation": "my-value"},
            },
            "spec": {
                "replicas": 3,
                "selector": {"matchLabels": {"app": "my-app"}},
            },
        },
        "container_template": {"name": "container"},
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
        "pipeline_module": "sbc.profiles",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "segment_id": 0,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    deployment = result["deployment"]

    # Check that existing fields are preserved
    assert deployment["kind"] == "Deployment"
    assert deployment["apiVersion"] == "apps/v1"
    assert deployment["metadata"]["namespace"] == "my-namespace"
    assert deployment["metadata"]["annotations"]["my-annotation"] == "my-value"
    assert deployment["spec"]["replicas"] == 3
    assert deployment["spec"]["selector"]["matchLabels"]["app"] == "my-app"

    # Check that configmap also has namespace
    configmap = result["configmap"]
    assert configmap["metadata"]["namespace"] == "my-namespace"

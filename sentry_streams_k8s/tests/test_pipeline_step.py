from typing import Any

import pytest
import yaml
from jsonschema import ValidationError

from sentry_streams_k8s.pipeline_step import (
    PipelineStep,
    build_container,
    build_labels,
    build_name,
)


def test_build_name() -> None:
    """Test that build_name generates valid Kubernetes names."""
    # Test basic conversion
    assert build_name("sbc.profiles", {}) == "sbc-profiles"

    # Test with underscores
    assert build_name("my_module.sub_module", {}) == "my-module-sub-module"

    # Test with mixed case (should be lowercase)
    assert build_name("MyModule.SubModule", {}) == "mymodule-submodule"

    # Test with special characters (should be removed)
    assert build_name("my@module.sub#module", {}) == "mymodule-submodule"


def test_build_labels() -> None:
    """Test that build_labels generates correct Kubernetes labels."""
    labels = build_labels("sbc.profiles", {})

    assert labels["app"] == "sbc-profiles"
    assert labels["component"] == "streaming-platform"
    assert labels["pipeline-module"] == "sbc.profiles"


def test_build_container() -> None:
    """Test that build_container creates a complete container spec."""
    container_template = {
        "name": "streaming-consumer",
        "image": "my-image:latest",
    }

    container = build_container(
        container_template=container_template,
        pipeline_module="sbc.profiles",
        pipeline_config={},
        cpu_per_process=1000,
        memory_per_process=512,
    )

    # Check that template fields are preserved
    assert container["name"] == "streaming-consumer"
    assert container["image"] == "my-image:latest"

    # Check command and args
    assert container["command"] == ["python", "-m", "sentry_streams.runner"]
    assert "--adapter" in container["args"]
    assert "rust_arroyo" in container["args"]
    assert "--config" in container["args"]
    assert "/etc/pipeline-config/pipeline_config.yaml" in container["args"]
    assert "sbc.profiles" in container["args"]

    # Check resources
    assert container["resources"]["requests"]["cpu"] == "1000m"
    assert container["resources"]["requests"]["memory"] == "512Mi"

    # Check volume mount
    assert len(container["volumeMounts"]) == 1
    assert container["volumeMounts"][0]["name"] == "pipeline-config"
    assert container["volumeMounts"][0]["mountPath"] == "/etc/pipeline-config"
    assert container["volumeMounts"][0]["readOnly"] is True


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
        pipeline_module="sbc.profiles",
        pipeline_config={},
        cpu_per_process=1000,
        memory_per_process=512,
    )

    # Check that existing fields are preserved
    assert container["env"] == [{"name": "MY_VAR", "value": "my_value"}]
    assert len(container["volumeMounts"]) == 2
    assert container["volumeMounts"][0] == {"name": "existing-volume", "mountPath": "/existing"}


def test_validate_context_valid() -> None:
    """Test that validate_context accepts valid context."""
    valid_context = {
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    # Should not raise any exception
    PipelineStep.validate_context(valid_context)


def test_validate_context_missing_fields() -> None:
    """Test that validate_context raises for missing fields."""
    # Missing deployment_template
    with pytest.raises(AssertionError, match="deployment_template"):
        PipelineStep.validate_context(
            {
                "container_template": {},
                "pipeline_config": {"env": {}, "pipeline": {"segments": []}},
                "pipeline_module": "my_module",
                "cpu_per_process": 1000,
                "memory_per_process": 512,
            }
        )

    # Missing container_template
    with pytest.raises(AssertionError, match="container_template"):
        PipelineStep.validate_context(
            {
                "deployment_template": {},
                "pipeline_config": {"env": {}, "pipeline": {"segments": []}},
                "pipeline_module": "my_module",
                "cpu_per_process": 1000,
                "memory_per_process": 512,
            }
        )

    # Missing pipeline_config
    with pytest.raises(AssertionError, match="pipeline_config"):
        PipelineStep.validate_context(
            {
                "deployment_template": {},
                "container_template": {},
                "pipeline_module": "my_module",
                "cpu_per_process": 1000,
                "memory_per_process": 512,
            }
        )


def test_validate_context_invalid_pipeline_config() -> None:
    """Test that validate_context validates pipeline_config schema."""
    invalid_context = {
        "deployment_template": {},
        "container_template": {},
        "pipeline_config": {
            "env": {},
            "pipeline": {
                # Missing required 'segments' key
            },
        },
        "pipeline_module": "my_module",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    with pytest.raises(ValidationError):
        PipelineStep.validate_context(invalid_context)


def test_run_returns_both_resources() -> None:
    """Test that run() returns both deployment and configmap."""
    context: dict[str, Any] = {
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container", "image": "my-image:latest"},
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    assert "deployment" in result
    assert "configmap" in result
    assert isinstance(result["deployment"], dict)
    assert isinstance(result["configmap"], dict)


def test_run_deployment_has_container() -> None:
    """Test that run() adds container to deployment."""
    context: dict[str, Any] = {
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container", "image": "my-image:latest"},
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    deployment = result["deployment"]
    containers = deployment["spec"]["template"]["spec"]["containers"]

    assert len(containers) == 1
    assert containers[0]["name"] == "container"
    assert containers[0]["resources"]["requests"]["cpu"] == "1000m"
    assert containers[0]["resources"]["requests"]["memory"] == "512Mi"


def test_run_deployment_has_volume() -> None:
    """Test that run() adds configmap volume to deployment."""
    context: dict[str, Any] = {
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container", "image": "my-image:latest"},
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    deployment = result["deployment"]
    volumes = deployment["spec"]["template"]["spec"]["volumes"]

    assert len(volumes) == 1
    assert volumes[0]["name"] == "pipeline-config"
    assert volumes[0]["configMap"]["name"] == "sbc-profiles-config"


def test_run_configmap_structure() -> None:
    """Test that run() creates configmap with correct structure."""
    context: dict[str, Any] = {
        "deployment_template": {"kind": "Deployment"},
        "container_template": {"name": "container", "image": "my-image:latest"},
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)  # type: ignore

    configmap = result["configmap"]

    assert configmap["apiVersion"] == "v1"
    assert configmap["kind"] == "ConfigMap"
    assert configmap["metadata"]["name"] == "sbc-profiles-config"
    assert configmap["metadata"]["labels"]["app"] == "sbc-profiles"
    assert configmap["metadata"]["labels"]["component"] == "streaming-platform"

    # Check that pipeline_config is serialized as YAML
    assert "pipeline_config.yaml" in configmap["data"]
    config_yaml = configmap["data"]["pipeline_config.yaml"]
    parsed_config = yaml.safe_load(config_yaml)
    assert parsed_config == context["pipeline_config"]


def test_run_preserves_deployment_template() -> None:
    """Test that run() preserves existing deployment_template fields."""
    context: dict[str, Any] = {
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
        "container_template": {"name": "container", "image": "my-image:latest"},
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
        "cpu_per_process": 1000,
        "memory_per_process": 512,
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

from typing import Any

import pytest
import yaml
from jsonschema import ValidationError

from sentry_streams_k8s.pipeline_step import (
    PipelineStep,
    build_container,
    load_base_template,
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


def test_build_container() -> None:
    """Test that build_container creates a complete container spec."""
    container_template = {
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "runAsGroup": 1000,
            "runAsNonRoot": True,
            "runAsUser": 1000,
        },
    }

    container = build_container(
        container_template=container_template,
        pipeline_name="profiles",
        pipeline_module="sbc.profiles",
        image_name="my-image:latest",
        cpu_per_process=1000,
        memory_per_process=512,
        segment_id=0,
    )

    assert container == {
        "name": "pipeline-consumer",
        "image": "my-image:latest",
        "imagePullPolicy": "IfNotPresent",  # From base template
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
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "runAsGroup": 1000,
            "runAsNonRoot": True,
            "runAsUser": 1000,
        },
    }


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
        "deployment_template": {
            "template": {
                "metadata": {
                    "annotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
                        "sidecar.istio.io/inject": "false",
                    },
                },
            },
        },
        "container_template": {
            "env": [{"name": "MY_VAR", "value": "my_value"}],
        },
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
    result = pipeline_step.run(context)

    # Validate return structure
    assert "deployment" in result
    assert "configmap" in result
    assert isinstance(result["deployment"], dict)
    assert isinstance(result["configmap"], dict)

    deployment = result["deployment"]
    configmap = result["configmap"]

    # Validate deployment name
    assert deployment["metadata"]["name"] == "my-service-pipeline-profiles-0"
    assert deployment["metadata"]["labels"]["pipeline-app"] == "sbc-profiles"
    assert deployment["template"]["metadata"]["annotations"]["sidecar.istio.io/inject"] == "false"

    # Validate deployment has container
    containers = deployment["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    assert containers[0]["name"] == "pipeline-consumer"
    assert containers[0]["resources"]["requests"]["cpu"] == "1000m"
    assert containers[0]["resources"]["requests"]["memory"] == "512Mi"
    assert containers[0]["env"] == [{"name": "MY_VAR", "value": "my_value"}]

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


def test_load_base_templates() -> None:
    """Test that load_base_template loads the template files correctly."""
    # Test deployment template
    deployment = load_base_template("deployment")
    assert deployment["apiVersion"] == "apps/v1"
    assert deployment["kind"] == "Deployment"
    assert "metadata" in deployment
    assert "spec" in deployment
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["template"]["spec"]["restartPolicy"] == "Always"
    assert deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 30
    assert "containers" in deployment["spec"]["template"]["spec"]
    assert "volumes" in deployment["spec"]["template"]["spec"]

    # Test container template
    container = load_base_template("container")
    assert container["name"] == "streaming-consumer"
    assert container["imagePullPolicy"] == "IfNotPresent"
    assert "resources" in container
    assert "volumeMounts" in container


def test_run_with_base_templates() -> None:
    """Test that run() merges user deployment template with base template."""
    # User provides minimal deployment template
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "metadata": {
                "namespace": "my-namespace",
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
        "pipeline_module": "sbc.profiles",
        "image_name": "my-image:latest",
        "cpu_per_process": 1000,
        "memory_per_process": 512,
        "segment_id": 0,
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)

    deployment = result["deployment"]

    # Check that base template fields are present
    assert deployment["apiVersion"] == "apps/v1"
    assert deployment["kind"] == "Deployment"
    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["template"]["spec"]["restartPolicy"] == "Always"
    assert deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 30

    # Check that user template fields are preserved
    assert deployment["metadata"]["namespace"] == "my-namespace"

    # Check that pipeline additions are present
    assert deployment["metadata"]["name"] == "my-service-pipeline-profiles-0"
    assert "pipeline" in deployment["metadata"]["labels"]
    assert len(deployment["spec"]["template"]["spec"]["containers"]) == 1
    assert len(deployment["spec"]["template"]["spec"]["volumes"]) == 1


def test_user_template_overrides_base() -> None:
    """Test that user template values take precedence over base template values."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            # Override base replicas
            "spec": {
                "replicas": 5,
                "template": {
                    "spec": {
                        # Override base terminationGracePeriodSeconds
                        "terminationGracePeriodSeconds": 60,
                    }
                },
            },
        },
        "container_template": {
            # Override base imagePullPolicy
            "imagePullPolicy": "Always",
        },
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
    result = pipeline_step.run(context)

    deployment = result["deployment"]

    # Check that user overrides took effect
    assert deployment["spec"]["replicas"] == 5  # User override, not base 1
    assert (
        deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 60
    )  # User override, not base 30

    # Check container overrides
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["imagePullPolicy"] == "Always"  # User override, not base "IfNotPresent"


def test_user_volumes_and_containers_preserved() -> None:
    """Test that user-provided volumes and containers are preserved via list concatenation."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "sidecar", "image": "sidecar:latest"},
                        ],
                        "volumes": [
                            {"name": "user-volume", "emptyDir": {}},
                        ],
                    }
                },
            },
        },
        "container_template": {
            "volumeMounts": [
                {"name": "user-volume", "mountPath": "/user"},
            ],
        },
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
    result = pipeline_step.run(context)

    deployment = result["deployment"]

    # Check that both user sidecar and pipeline container are present
    containers = deployment["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 2
    container_names = [c["name"] for c in containers]
    assert "sidecar" in container_names
    assert "pipeline-consumer" in container_names

    # Check that both user volume and pipeline volume are present
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    assert len(volumes) == 2
    volume_names = [v["name"] for v in volumes]
    assert "user-volume" in volume_names
    assert "pipeline-config" in volume_names

    # Check that pipeline container has both user and pipeline volume mounts
    pipeline_container = next(c for c in containers if c["name"] == "pipeline-consumer")
    volume_mount_names = [vm["name"] for vm in pipeline_container["volumeMounts"]]
    assert "user-volume" in volume_mount_names
    assert "pipeline-config" in volume_mount_names

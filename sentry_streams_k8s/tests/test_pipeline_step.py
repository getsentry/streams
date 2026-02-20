from typing import Any

import pytest
import yaml
from jsonschema import ValidationError

from sentry_streams_k8s.merge import ScalarOverwriteError
from sentry_streams_k8s.pipeline_step import (
    PipelineStep,
    build_container,
    load_base_template,
    make_k8s_name,
    parse_context,
)


def test_make_k8s_name() -> None:
    """Test that make_k8s_name generates valid Kubernetes names."""
    assert make_k8s_name("sbc.profiles") == "sbc-profiles"

    assert make_k8s_name("my_module.sub_module") == "my-module-sub-module"

    # Test with mixed case (should be lowercase)
    assert make_k8s_name("MyModule.SubModule") == "mymodule-submodule"

    # Test with special characters (should be removed)
    assert make_k8s_name("my@module.sub#module") == "mymodule-submodule"


def test_parse_context() -> None:

    context = {
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
        "replicas": 2,
    }

    parsed_context = parse_context(context)
    assert parsed_context["service_name"] == "my-service"
    assert parsed_context["pipeline_name"] == "my-pipeline"
    assert parsed_context["deployment_template"] == {"kind": "Deployment"}
    assert parsed_context["container_template"] == {"name": "container"}
    assert parsed_context["pipeline_config"] == {
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

    assert parsed_context["pipeline_module"] == "my_module"
    assert parsed_context["image_name"] == "my-image:latest"
    assert parsed_context["cpu_per_process"] == 1000
    assert parsed_context["memory_per_process"] == 512
    assert parsed_context["segment_id"] == 0
    assert parsed_context["replicas"] == 2

    context["deployment_template"] = yaml.dump(context["deployment_template"])
    context["container_template"] = yaml.dump(context["container_template"])
    context["pipeline_config"] = yaml.dump(context["pipeline_config"])

    parsed_context = parse_context(context)
    assert parsed_context["deployment_template"] == {"kind": "Deployment"}
    assert parsed_context["container_template"] == {"name": "container"}
    assert parsed_context["pipeline_config"] == {
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
    assert parsed_context["replicas"] == 2


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
            },
            {
                "name": "liveness-health",
                "mountPath": "/tmp",
            },
        ],
        "livenessProbe": {
            "exec": {
                "command": ["rm", "/tmp/health.txt"],
            },
            "failureThreshold": 31,
            "periodSeconds": 10,
        },
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
        "replicas": 1,
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
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
                            "sidecar.istio.io/inject": "false",
                        },
                    },
                },
            }
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
        "replicas": 1,
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
    assert (
        deployment["spec"]["template"]["metadata"]["annotations"]["sidecar.istio.io/inject"]
        == "false"
    )

    selector = deployment["spec"]["selector"]
    assert selector["matchLabels"]["pipeline-app"] == "sbc-profiles"
    assert selector["matchLabels"]["pipeline"] == "profiles"

    # Validate deployment has container
    containers = deployment["spec"]["template"]["spec"]["containers"]
    assert len(containers) == 1
    assert containers[0]["name"] == "pipeline-consumer"
    assert containers[0]["resources"]["requests"]["cpu"] == "1000m"
    assert containers[0]["resources"]["requests"]["memory"] == "512Mi"
    assert containers[0]["env"] == [{"name": "MY_VAR", "value": "my_value"}]

    # Validate deployment has configmap volume and liveness-health volume
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    assert len(volumes) == 2
    volume_names = {v["name"] for v in volumes}
    assert "pipeline-config" in volume_names
    assert "liveness-health" in volume_names
    pipeline_config_vol = next(v for v in volumes if v["name"] == "pipeline-config")
    assert pipeline_config_vol["configMap"]["name"] == "my-service-pipeline-profiles"

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


def test_run_includes_liveness_probe_when_enabled() -> None:
    """Test that run() includes liveness probe and liveness-health volume when enabled, and omits them when disabled."""
    base_context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {},
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
        "replicas": 1,
    }

    pipeline_step = PipelineStep()

    # Default / enabled: deployment has livenessProbe and liveness-health volume/mount
    result = pipeline_step.run(base_context)
    deployment = result["deployment"]
    containers = deployment["spec"]["template"]["spec"]["containers"]
    volumes = deployment["spec"]["template"]["spec"]["volumes"]

    assert len(containers) == 1
    container = containers[0]
    assert "livenessProbe" in container
    assert container["livenessProbe"]["exec"]["command"] == ["rm", "/tmp/health.txt"]
    assert container["livenessProbe"]["failureThreshold"] == 31
    assert container["livenessProbe"]["periodSeconds"] == 10

    liveness_health_volumes = [v for v in volumes if v["name"] == "liveness-health"]
    assert len(liveness_health_volumes) == 1
    assert liveness_health_volumes[0]["emptyDir"] == {}

    liveness_mounts = [m for m in container["volumeMounts"] if m["name"] == "liveness-health"]
    assert len(liveness_mounts) == 1
    assert liveness_mounts[0]["mountPath"] == "/tmp"

    # Disabled: no livenessProbe, no liveness-health volume or volumeMount
    result_disabled = pipeline_step.run(
        {**base_context, "enable_liveness_probe": False}
    )
    deployment_disabled = result_disabled["deployment"]
    containers_disabled = deployment_disabled["spec"]["template"]["spec"]["containers"]
    volumes_disabled = deployment_disabled["spec"]["template"]["spec"]["volumes"]

    assert len(containers_disabled) == 1
    assert "livenessProbe" not in containers_disabled[0]
    assert not any(v["name"] == "liveness-health" for v in volumes_disabled)
    assert not any(
        m["name"] == "liveness-health" for m in containers_disabled[0]["volumeMounts"]
    )


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
        "replicas": 1,
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
    assert deployment["spec"]["selector"]["matchLabels"]["pipeline-app"] == "sbc-profiles"
    assert deployment["spec"]["selector"]["matchLabels"]["pipeline"] == "profiles"

    # Check that user template fields are preserved
    assert deployment["metadata"]["namespace"] == "my-namespace"

    # Check that pipeline additions are present
    assert deployment["metadata"]["name"] == "my-service-pipeline-profiles-0"
    assert "pipeline" in deployment["metadata"]["labels"]
    assert len(deployment["spec"]["template"]["spec"]["containers"]) == 1
    assert len(deployment["spec"]["template"]["spec"]["volumes"]) == 2  # pipeline-config + liveness-health


def test_user_template_overrides_base() -> None:
    """Test that user template values can override base template for non-controlled fields."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "spec": {
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
        "replicas": 1,  # Use base template value to avoid conflict
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)

    deployment = result["deployment"]

    # Check that replicas parameter took effect
    assert deployment["spec"]["replicas"] == 1
    # Check that user template overrides for non-controlled fields worked
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

    # Check that user volume, pipeline volume, and liveness-health volume are present
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    assert len(volumes) == 3
    volume_names = [v["name"] for v in volumes]
    assert "user-volume" in volume_names
    assert "pipeline-config" in volume_names
    assert "liveness-health" in volume_names

    # Check that pipeline container has user, pipeline, and liveness volume mounts
    pipeline_container = next(c for c in containers if c["name"] == "pipeline-consumer")
    volume_mount_names = [vm["name"] for vm in pipeline_container["volumeMounts"]]
    assert "user-volume" in volume_mount_names
    assert "pipeline-config" in volume_mount_names
    assert "liveness-health" in volume_mount_names


def test_get_multiprocess_config_detects_processes() -> None:
    """Test that get_multiprocess_config() correctly extracts process count."""
    from sentry_streams_k8s.pipeline_step import get_multiprocess_config

    pipeline_config = {
        "env": {},
        "pipeline": {
            "segments": [
                {
                    "steps_config": {
                        "myinput": {
                            "starts_segment": True,
                            "bootstrap_servers": ["127.0.0.1:9092"],
                            "parallelism": 1,
                        },
                        "parser": {
                            "starts_segment": True,
                            "parallelism": {
                                "multi_process": {
                                    "processes": 4,
                                    "batch_size": 1000,
                                    "batch_time": 0.2,
                                }
                            },
                        },
                        "mysink": {
                            "starts_segment": True,
                            "bootstrap_servers": ["127.0.0.1:9092"],
                        },
                    }
                }
            ]
        },
    }

    process_count, segments = get_multiprocess_config(pipeline_config)
    assert process_count == 4
    assert segments == [0]


def test_build_container_with_multiprocessing() -> None:
    """Test that build_container() correctly handles multiprocessing configuration."""
    container_template: dict[str, Any] = {}

    container = build_container(
        container_template=container_template,
        pipeline_name="profiles",
        pipeline_module="sbc.profiles",
        image_name="my-image:latest",
        cpu_per_process=1000,
        memory_per_process=512,
        segment_id=0,
        process_count=4,
    )

    # Check resources are multiplied by process count
    assert container["resources"]["requests"]["cpu"] == "4000m"
    assert container["resources"]["requests"]["memory"] == "2048Mi"
    assert container["resources"]["limits"]["memory"] == "2048Mi"

    # Check shared memory volume is added
    volume_mount_names = [vm["name"] for vm in container["volumeMounts"]]
    assert "pipeline-config" in volume_mount_names
    assert "dshm" in volume_mount_names

    dshm_mount = next(vm for vm in container["volumeMounts"] if vm["name"] == "dshm")
    assert dshm_mount["mountPath"] == "/dev/shm"


def test_build_container_without_multiprocessing() -> None:
    """Test that build_container() works normally when multiprocessing is not configured."""
    container_template: dict[str, Any] = {}

    container = build_container(
        container_template=container_template,
        pipeline_name="profiles",
        pipeline_module="sbc.profiles",
        image_name="my-image:latest",
        cpu_per_process=1000,
        memory_per_process=512,
        segment_id=0,
        process_count=None,
    )

    # Check resources are NOT multiplied
    assert container["resources"]["requests"]["cpu"] == "1000m"
    assert container["resources"]["requests"]["memory"] == "512Mi"
    assert container["resources"]["limits"]["memory"] == "512Mi"

    volume_mount_names = [vm["name"] for vm in container["volumeMounts"]]
    assert "pipeline-config" in volume_mount_names
    assert "dshm" not in volume_mount_names


def test_run_with_multiprocessing() -> None:
    """Test complete deployment generation with multiprocessing configuration."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {},
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
                            },
                            "parser": {
                                "starts_segment": True,
                                "parallelism": {
                                    "multi_process": {
                                        "processes": 4,
                                        "batch_size": 1000,
                                        "batch_time": 0.2,
                                    }
                                },
                            },
                            "mysink": {
                                "starts_segment": True,
                                "bootstrap_servers": ["127.0.0.1:9092"],
                            },
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

    # Check resources are multiplied
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"]["requests"]["cpu"] == "4000m"
    assert container["resources"]["requests"]["memory"] == "2048Mi"

    # Check shared memory and liveness-health volumes are in deployment
    volumes = deployment["spec"]["template"]["spec"]["volumes"]
    volume_names = [v["name"] for v in volumes]
    assert "pipeline-config" in volume_names
    assert "dshm" in volume_names
    assert "liveness-health" in volume_names

    dshm_volume = next(v for v in volumes if v["name"] == "dshm")
    assert dshm_volume["emptyDir"]["medium"] == "Memory"

    volume_mount_names = [vm["name"] for vm in container["volumeMounts"]]
    assert "dshm" in volume_mount_names
    assert "liveness-health" in volume_mount_names


def test_run_rejects_multiple_segments_with_parallelism() -> None:
    """Test that run() rejects configuration with parallelism in multiple segments."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {},
        "container_template": {},
        "pipeline_config": {
            "env": {},
            "pipeline": {
                "segments": [
                    {
                        "steps_config": {
                            "step1": {
                                "starts_segment": True,
                                "parallelism": {
                                    "multi_process": {
                                        "processes": 4,
                                        "batch_size": 1000,
                                        "batch_time": 0.2,
                                    }
                                },
                            }
                        }
                    },
                    {
                        "steps_config": {
                            "step2": {
                                "starts_segment": True,
                                "parallelism": {
                                    "multi_process": {
                                        "processes": 2,
                                        "batch_size": 500,
                                        "batch_time": 0.1,
                                    }
                                },
                            }
                        }
                    },
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

    with pytest.raises(
        ValueError,
        match=r"Multi-processing configuration can only be specified in one segment",
    ):
        pipeline_step.run(context)


def test_template_conflict_scalar_overwrite() -> None:
    """Test that PipelineStep detects and prevents scalar field conflicts in templates."""
    # Test conflict with replicas field
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "spec": {
                "replicas": 5,  # User tries to set replicas - conflicts with parameter
            }
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
        "replicas": 3,  # Different from template value
    }

    pipeline_step = PipelineStep()
    with pytest.raises(ScalarOverwriteError, match="spec.replicas"):
        pipeline_step.run(context)


def test_emergency_patch_overrides_final_deployment() -> None:
    """Test that emergency_patch overrides all other layers including pipeline additions."""
    context: dict[str, Any] = {
        "service_name": "my-service",
        "pipeline_name": "profiles",
        "deployment_template": {
            "spec": {
                "replicas": 1,  # Base template default
            }
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
        # Emergency patch to override replicas and add security context
        "emergency_patch": {
            "spec": {
                "replicas": 3,  # Override base template
                "template": {
                    "spec": {
                        "securityContext": {
                            "runAsNonRoot": True,
                            "fsGroup": 2000,
                        }
                    }
                },
            }
        },
    }

    pipeline_step = PipelineStep()
    result = pipeline_step.run(context)
    deployment = result["deployment"]

    # Verify emergency patch overrides took effect
    assert deployment["spec"]["replicas"] == 3  # Emergency patch value, not base 1

    # Verify deeply nested emergency patch values are present
    assert deployment["spec"]["template"]["spec"]["securityContext"]["runAsNonRoot"] is True
    assert deployment["spec"]["template"]["spec"]["securityContext"]["fsGroup"] == 2000

    # Verify pipeline additions are still present (not removed by emergency patch)
    assert deployment["metadata"]["name"] == "my-service-pipeline-profiles-0"
    assert deployment["metadata"]["labels"]["pipeline-app"] == "sbc-profiles"
    assert len(deployment["spec"]["template"]["spec"]["containers"]) == 1
    assert deployment["spec"]["template"]["spec"]["containers"][0]["name"] == "pipeline-consumer"

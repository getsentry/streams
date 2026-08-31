from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from importlib.resources import files
from typing import Any, Mapping, NotRequired, TypedDict, cast

import yaml

from sentry_streams_k8s.constants import CANARY_WORKLOAD_SET, PRIMARY_WORKLOAD_SET
from sentry_streams_k8s.k8s_types import V1ConfigMapDict, V1DeploymentDict
from sentry_streams_k8s.merge import ScalarOverwriteError, deepmerge
from sentry_streams_k8s.validation import validate_pipeline_config

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


class PodTemplate(TypedDict):
    spec: dict[str, Any]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class WorkloadSet:
    """Group of identical replicas (primary or canary set)."""

    name: str
    replicas: int
    labels: dict[str, str]

    pod_template: PodTemplate


class RenderedDeployments(TypedDict):
    """The pipeline rendered as Deployments."""

    deployment: V1DeploymentDict
    configmap: V1ConfigMapDict

    canary_deployment: NotRequired[V1DeploymentDict]


class RenderedPods(TypedDict):
    """The pipeline rendered as Pods."""

    sets: dict[str, WorkloadSet]
    configmap: V1ConfigMapDict


def load_base_template(file_name: str) -> dict[str, Any]:
    """
    Load base Kubernetes Deployment and Container templates from the
    packaged templates directory.
    """
    template_content = files("sentry_streams_k8s").joinpath(f"templates/{file_name}.yaml")
    return cast(dict[str, Any], yaml.safe_load(template_content.read_text()))


def make_k8s_name(name: str, limit: int | None = None) -> str:
    """
    Generate a valid Kubernetes name from a string.

    Converts the string to a valid RFC 1123 compliant name
    by replacing dots and underscores with dashes and converting to lowercase.

    Examples:
        >>> build_name("sbc.profiles")
        'sbc-profiles'
        >>> build_name("my_module.sub_module")
        'my-module-sub-module'
    """
    name = name.replace(".", "-").replace("_", "-").lower()
    name = re.sub(r"[^a-z0-9-]", "", name)
    name = name.strip("-")
    if limit is not None:
        # Truncation can land on a hyphen (e.g. a dotted module path). Kubernetes
        # label values must start and end with an alphanumeric character.
        return name[:limit].rstrip("-")
    return name


def get_multiprocess_config(pipeline_config: dict[str, Any]) -> tuple[int | None, list[int]]:
    """
    Extract multiprocessing configuration from pipeline config.

    Iterates through all segments in the pipeline configuration and looks for
    parallelism.multi_process.processes configuration in any step.

    Examples:
        >>> config = {"pipeline": {"segments": [{"steps_config": {"step1": {"parallelism": {"multi_process": {"processes": 4}}}}}]}}
        >>> get_multiprocess_config(config)
        (4, [0])
    """
    segments_with_parallelism: list[int] = []
    process_count: int | None = None

    segments = pipeline_config["pipeline"]["segments"]

    for segment_idx, segment in enumerate(segments):
        steps_config = segment.get("steps_config", {})

        for step_config in steps_config.values():
            parallelism = step_config.get("parallelism")
            if not parallelism or not isinstance(parallelism, dict):
                continue

            multi_process = parallelism.get("multi_process")
            if not multi_process:
                continue

            processes = multi_process.get("processes")
            if processes is not None:
                segments_with_parallelism.append(segment_idx)
                if process_count is None:
                    process_count = processes
                break

    return process_count, segments_with_parallelism


def serialize_pipeline_config(pipeline_config: dict[str, Any]) -> str:
    return json.dumps(pipeline_config, sort_keys=True)


def compute_config_version(pipeline_config: dict[str, Any]) -> str:
    """MD5 hash of pipeline_config serialized as JSON (matches ConfigMap encoding)."""
    return hashlib.md5(serialize_pipeline_config(pipeline_config).encode()).hexdigest()


def build_container(
    container_template: dict[str, Any],
    pipeline_name: str,
    pipeline_module: str,
    image_name: str,
    cpu_per_process: int,
    memory_per_process: int,
    segment_id: int,
    log_level: str = "INFO",
    process_count: int | None = None,
    enable_liveness_probe: bool = True,
    multiprocess_enabled: bool | None = None,
    container_name: str = "pipeline-consumer",
) -> dict[str, Any]:
    """
    Build a complete container specification for the pipeline step.

    The result is produced by:
    1. taking the base container template from container.yaml
    2. merging the user provided template. This is generally used to define
       some standard parameters like securityContext
    3. building the streaming pipeline specific parameters and merging them
       onto the result of step 2.

    """
    base_container = load_base_template("container")
    container = deepmerge(base_container, container_template)

    # CPU and memory are provided per process, so we need to multiply them
    # by the number of processes to get the total resources.
    cpu_total = cpu_per_process * (process_count or 1)
    memory_total = memory_per_process * (process_count or 1)

    volume_mounts: list[dict[str, Any]] = [
        {
            "name": "pipeline-config",
            "mountPath": "/etc/pipeline-config",
            "readOnly": True,
        }
    ]

    if multiprocess_enabled is None:
        multiprocess_enabled = process_count is not None and process_count > 1

    # Shared memory volume is needed to allow the communication between processes.
    if multiprocess_enabled:
        volume_mounts.append(
            {
                "name": "dshm",
                "mountPath": "/dev/shm",
            }
        )

    # /tmp volume is needed for both liveness probe (health.txt) and multiprocess support.
    if enable_liveness_probe or multiprocess_enabled:
        volume_mounts.append(
            {
                "name": "liveness-health",
                "mountPath": "/tmp",
            }
        )

    pipeline_additions: dict[str, Any] = {
        "name": container_name,
        "image": image_name,
        "command": ["python", "-m", "sentry_streams.runner"],
        "args": [
            "-n",
            pipeline_name,
            "--log-level",
            log_level,
            "--adapter",
            "rust_arroyo",
            "--segment-id",
            str(segment_id),
            "--config",
            "/etc/pipeline-config/pipeline_config.yaml",
            pipeline_module,
        ],
        "resources": {
            "requests": {
                "cpu": f"{cpu_total}m",
                "memory": f"{memory_total}Mi",
            },
            "limits": {
                "memory": f"{memory_total}Mi",
            },
        },
        "volumeMounts": volume_mounts,
    }

    if enable_liveness_probe:
        pipeline_additions["livenessProbe"] = {
            "exec": {
                "command": ["rm", "/tmp/health.txt"],
            },
            "failureThreshold": 31,
            "periodSeconds": 10,
        }

    return deepmerge(container, pipeline_additions)


def _build_merged_pipeline_deployment(
    *,
    base_deployment: dict[str, Any],
    deployment_template: dict[str, Any],
    emergency_patch: dict[str, Any],
    deployment_name: str,
    replica_count: int,
    step_labels: dict[str, Any],
    container: dict[str, Any],
    volumes: list[dict[str, Any]],
    config_version: str,
) -> V1DeploymentDict:
    """
    Assembles a k8s deployment by layering these structures on top of the base deployment
    manifest:
    1. deployment_template: provided by the user
    2. the streaming platform specific additions (including the container)
    3. emergency_patch: if provided, it overrides all other layers
    """

    pipeline_additions: dict[str, Any] = {
        "metadata": {
            "name": deployment_name,
            "labels": step_labels,
        },
        "spec": {
            "replicas": replica_count,
            "selector": {
                "matchLabels": step_labels,
            },
            "template": {
                "metadata": {
                    "labels": step_labels,
                    "annotations": {
                        "configVersion": config_version,
                    },
                },
                "spec": {
                    "containers": [container],
                    "volumes": volumes,
                },
            },
        },
    }
    try:
        deepmerge(deployment_template, pipeline_additions, fail_on_scalar_overwrite=True)
    except ScalarOverwriteError as e:
        raise ScalarOverwriteError(
            f"{e}\n\n"
            f"This field is automatically set by the streaming consumer builder and conflicts "
            f"with your deployment_template. "
            f"Note: Lists and dicts can be provided (they get merged), but scalar values cannot "
            f"be overridden."
        ) from e

    deployment = deepmerge(base_deployment, deployment_template)
    deployment = deepmerge(deployment, pipeline_additions)
    if emergency_patch:
        deployment = deepmerge(deployment, emergency_patch)
    return cast(V1DeploymentDict, deployment)


def _pipeline_labels(spec: ConsumerSpec) -> dict[str, str]:
    return {
        "pipeline-app": make_k8s_name(spec.pipeline_module, limit=63),
        "pipeline": make_k8s_name(spec.pipeline_name),
        "service": make_k8s_name(spec.service_name),
    }


def _configmap_name(spec: ConsumerSpec) -> str:
    return make_k8s_name(f"{spec.service_name}-pipeline-{spec.pipeline_name}")


def _to_workload_set(deployment: V1DeploymentDict) -> WorkloadSet:
    """Converts a Deployment to a WorkloadSet."""

    return WorkloadSet(
        name=deployment["metadata"]["name"],
        replicas=deployment["spec"]["replicas"],
        labels=dict(deployment["metadata"]["labels"]),
        pod_template=deployment["spec"]["template"],
    )


@dataclass(frozen=True)
class ConsumerSpec:
    service_name: str
    pipeline_name: str
    pipeline_module: str
    image: str
    cpu_per_process: int
    memory_per_process: int
    segment_id: int = 0
    replicas: int = 1
    log_level: str = "INFO"
    enable_liveness_probe: bool = True
    with_canary: bool = False
    container_name: str = "pipeline-consumer"
    emergency_patch: Mapping[str, Any] = field(default_factory=dict)


class ConsumerBuilder:
    def __init__(
        self,
        deployment_template: Mapping[str, Any],
        container_template: Mapping[str, Any],
    ) -> None:
        self._deployment_template = dict(deployment_template)
        self._container_template = dict(container_template)

    def validate(self, spec: ConsumerSpec, pipeline_config: Mapping[str, Any]) -> None:
        validate_pipeline_config(dict(pipeline_config))
        if spec.log_level not in LOG_LEVELS:
            raise ValueError(
                f"Invalid log_level {spec.log_level!r}; expected one of {', '.join(LOG_LEVELS)}"
            )

        # When the builder manages the liveness probe, the template must not define one.
        if spec.enable_liveness_probe and self._container_template.get("livenessProbe"):
            raise ValueError(
                "enable_liveness_probe is True but container_template already defines "
                "livenessProbe. When the liveness probe is managed for you, the template must "
                "not define one. Either set enable_liveness_probe to False or remove "
                "livenessProbe from container_template."
            )

    def _workload_deployments(
        self, spec: ConsumerSpec, config: dict[str, Any]
    ) -> dict[str, V1DeploymentDict]:
        """
        Generate the Kubernetes Deployments for the pipeline.
        Result is keyed by workload set name (primary or canary).
        """
        process_count, segments_with_parallelism = get_multiprocess_config(config)
        if len(segments_with_parallelism) > 1:
            raise ValueError(
                f"Multi-processing configuration can only be specified in one segment. "
                f"Found parallelism configuration in {len(segments_with_parallelism)} segments "
                f"(segment indices: {segments_with_parallelism})."
            )

        multiprocess_enabled = process_count is not None and process_count > 1

        container = build_container(
            self._container_template,
            spec.pipeline_name,
            spec.pipeline_module,
            spec.image,
            spec.cpu_per_process,
            spec.memory_per_process,
            spec.segment_id,
            spec.log_level,
            process_count,
            spec.enable_liveness_probe,
            multiprocess_enabled,
            spec.container_name,
        )

        base_deployment = load_base_template("deployment")

        labels = _pipeline_labels(spec)

        volumes: list[dict[str, Any]] = [
            {
                "name": "pipeline-config",
                "configMap": {
                    "name": _configmap_name(spec),
                },
            }
        ]

        # Shared memory volume is needed to allow the communication between processes.
        if multiprocess_enabled:
            volumes.append(
                {
                    "name": "dshm",
                    "emptyDir": {"medium": "Memory"},
                }
            )

        # /tmp volume is needed for both liveness probe (health.txt) and multiprocess support.
        if spec.enable_liveness_probe or multiprocess_enabled:
            volumes.append(
                {
                    "name": "liveness-health",
                    "emptyDir": {},
                }
            )

        config_version = compute_config_version(config)

        add_canary = spec.with_canary and spec.replicas > 1
        main_deployment_name = make_k8s_name(
            f"{spec.service_name}-pipeline-{spec.pipeline_name}-{spec.segment_id}"
        )
        canary_deployment_name = make_k8s_name(
            f"{spec.service_name}-pipeline-{spec.pipeline_name}-{spec.segment_id}-canary"
        )

        emergency_patch = dict(spec.emergency_patch)

        deployments = {
            PRIMARY_WORKLOAD_SET: _build_merged_pipeline_deployment(
                base_deployment=base_deployment,
                deployment_template=self._deployment_template,
                emergency_patch=emergency_patch,
                deployment_name=main_deployment_name,
                replica_count=spec.replicas - 1 if add_canary else spec.replicas,
                step_labels={**labels, "env": PRIMARY_WORKLOAD_SET},
                container=container,
                volumes=volumes,
                config_version=config_version,
            )
        }

        if add_canary:
            deployments[CANARY_WORKLOAD_SET] = _build_merged_pipeline_deployment(
                base_deployment=base_deployment,
                deployment_template=self._deployment_template,
                emergency_patch=emergency_patch,
                deployment_name=canary_deployment_name,
                replica_count=1,
                step_labels={**labels, "env": CANARY_WORKLOAD_SET},
                container=container,
                volumes=volumes,
                config_version=config_version,
            )

        return deployments

    def _build_configmap(
        self,
        spec: ConsumerSpec,
        config: dict[str, Any],
        primary_deployment: V1DeploymentDict,
    ) -> V1ConfigMapDict:
        configmap: V1ConfigMapDict = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": _configmap_name(spec),
                "labels": _pipeline_labels(spec),
            },
            "data": {
                "pipeline_config.yaml": serialize_pipeline_config(config),
            },
        }

        if "namespace" in primary_deployment.get("metadata", {}):
            configmap["metadata"]["namespace"] = primary_deployment["metadata"]["namespace"]

        return configmap

    def build_deployments(
        self, spec: ConsumerSpec, pipeline_config: Mapping[str, Any]
    ) -> RenderedDeployments:
        """Render the pipeline as Deployments and a ConfigMap."""

        config = dict(pipeline_config)
        deployments = self._workload_deployments(spec, config)
        primary = deployments[PRIMARY_WORKLOAD_SET]

        result: RenderedDeployments = {
            "deployment": primary,
            "configmap": self._build_configmap(spec, config, primary),
        }

        if CANARY_WORKLOAD_SET in deployments:
            result["canary_deployment"] = deployments[CANARY_WORKLOAD_SET]

        return result

    def build_pods(self, spec: ConsumerSpec, pipeline_config: Mapping[str, Any]) -> RenderedPods:
        """Render the pipeline as Pods and a ConfigMap."""

        config = dict(pipeline_config)
        deployments = self._workload_deployments(spec, config)

        return {
            "sets": {
                workload_set: _to_workload_set(deployment)
                for workload_set, deployment in deployments.items()
            },
            "configmap": self._build_configmap(spec, config, deployments[PRIMARY_WORKLOAD_SET]),
        }

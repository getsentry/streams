from __future__ import annotations

import json
import re
from importlib.resources import files
from typing import Any, NotRequired, TypedDict, cast

import yaml
from libsentrykube.ext import ExternalMacro

from sentry_streams_k8s.merge import ScalarOverwriteError, deepmerge
from sentry_streams_k8s.validation import validate_pipeline_config


def load_base_template(file_name: str) -> dict[str, Any]:
    """
    Load base Kubernetes Deployment and Container templates from the
    packaged templates directory.
    """
    template_content = files("sentry_streams_k8s").joinpath(f"templates/{file_name}.yaml")
    return cast(dict[str, Any], yaml.safe_load(template_content.read_text()))


def make_k8s_name(name: str) -> str:
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
    # Ensure it's RFC 1123 compliant (lowercase alphanumeric + hyphens)
    name = re.sub(r"[^a-z0-9-]", "", name)
    name = name.strip("-")
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
                break  # Found parallelism in this segment, move to next segment

    return process_count, segments_with_parallelism


def build_container(
    container_template: dict[str, Any],
    pipeline_name: str,
    pipeline_module: str,
    image_name: str,
    cpu_per_process: int,
    memory_per_process: int,
    segment_id: int,
    process_count: int | None = None,
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

    # Shared memory volume is needed to allow the communication between processes.
    # via shared memory. Only needed when in multiprocess mode.
    if process_count is not None and process_count > 1:
        volume_mounts.append(
            {
                "name": "dshm",
                "mountPath": "/dev/shm",
            }
        )

    pipeline_additions = {
        "name": "pipeline-consumer",
        "image": image_name,
        "command": ["python", "-m", "sentry_streams.runner"],
        "args": [
            "-n",
            pipeline_name,
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

    return deepmerge(container, pipeline_additions)


def parse_context(context: dict[str, Any]) -> PipelineStepContext:
    """
    Parse the context dictionary into a PipelineStepContext.
    """

    if isinstance(context["deployment_template"], str):
        deployment_template_parsed: dict[str, Any] | None = yaml.safe_load(
            context["deployment_template"]
        )
    else:
        deployment_template_parsed = context["deployment_template"]

    if isinstance(context["container_template"], str):
        container_template_parsed: dict[str, Any] | None = yaml.safe_load(
            context["container_template"]
        )
    else:
        container_template_parsed = context["container_template"]

    if isinstance(context["pipeline_config"], str):
        pipeline_config_parsed: dict[str, Any] | None = yaml.safe_load(context["pipeline_config"])
    else:
        pipeline_config_parsed = context["pipeline_config"]

    emergency_patch_parsed: dict[str, Any] = {}
    if "emergency_patch" in context:
        if isinstance(context["emergency_patch"], str):
            emergency_patch_parsed = yaml.safe_load(context["emergency_patch"]) or {}
        else:
            emergency_patch_parsed = context["emergency_patch"] or {}

    return {
        "service_name": context["service_name"],
        "pipeline_name": context["pipeline_name"],
        "deployment_template": deployment_template_parsed or {},
        "container_template": container_template_parsed or {},
        "pipeline_config": pipeline_config_parsed or {},
        "pipeline_module": context["pipeline_module"],
        "image_name": context["image_name"],
        "cpu_per_process": context["cpu_per_process"],
        "memory_per_process": context["memory_per_process"],
        "segment_id": context["segment_id"],
        "replicas": context.get("replicas", 1),
        "emergency_patch": emergency_patch_parsed,
    }


class PipelineStepContext(TypedDict):
    """Context dictionary for PipelineStep macro."""

    service_name: str
    pipeline_name: str
    deployment_template: dict[str, Any]
    container_template: dict[str, Any]
    pipeline_config: dict[str, Any]
    pipeline_module: str
    image_name: str
    cpu_per_process: int
    memory_per_process: int
    segment_id: int
    replicas: int
    emergency_patch: NotRequired[dict[str, Any]]


class PipelineStep(ExternalMacro):
    """
    A sentry-kube macro that creates the Kubernetes manifest for a pipeline step
    that runs the streaming platform.

    This can be imported in a sentry-kube template.
    The user can provide the basic structure of the deployment template with the
    basic infrastructure. This can include COGS labeling, nodepool config,
    some sidecars, etc.

    This macro fills it in with the streaming platform content: containers, volumes,
    configmap, naming conventions, etc.
    A similar pattern is followed by the Flink python operator, the user can
    provide a deployment template in the CRD, the flink operator fills it in
    with Flink.

    The goal of this is to standardize the deployment of streaming platform
    pipeline steps while still sticking to the Sentry Kubernetes infrastructure based
    on client side rendering of templates and sentry-kube macros.

    This would be used like this in a jinja template:

    ```
    {% import '_deployment_template.j2' as deployment -%}
    {% set deployment = deployment.deployment() %}
    {% import '_container_template.j2' as container -%}
    {% set container = container.container() %}
    {{ render_external(
            "sentry_streams_k8s.pipeline_step.PipelineStep",
            {
                "service_name": "my-service",
                "pipeline_name": "profiles",
                "deployment_template": deployment,
                "container_template": container,
                "pipeline_config": pipeline_config_dict,
                "pipeline_module": "sbc.profiles",
                "image_name": "us-central1-docker.pkg.dev/my-project/my-image:latest",
                "segment_id": 0,
                "cpu_per_process": 1000,
                "memory_per_process": 512,
                "replicas": 3,
            }
        )
    }}
    ```
    """

    @staticmethod
    def validate_context(context: dict[str, Any]) -> None:
        """
        Validates that the context contains all required fields and that
        the pipeline_config conforms to the expected schema.

        Raises:
            AssertionError: If required fields are missing
            jsonschema.ValidationError: If pipeline_config is invalid
        """
        assert "deployment_template" in context, "Missing deployment_template"
        assert "container_template" in context, "Missing container_template"
        assert "pipeline_config" in context, "Missing pipeline_config"
        assert "pipeline_module" in context, "Missing pipeline_module"
        assert "image_name" in context, "Missing image_name"
        assert "cpu_per_process" in context, "Missing cpu_per_process"
        assert "memory_per_process" in context, "Missing memory_per_process"
        assert "segment_id" in context, "Missing segment_id"
        assert "pipeline_name" in context, "Missing pipeline_name"
        assert "service_name" in context, "Missing service_name"

        validate_pipeline_config(parse_context(context)["pipeline_config"])

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Generates Kubernetes deployment and configmap manifests.

        Uses a two-stage merge approach:
        1. Merge user deployment_template onto base deployment template
        2. Merge pipeline-specific configuration onto the result

        Returns:
            Dictionary with 'deployment' and 'configmap' keys
        """

        ctx = parse_context(context)
        deployment_template = ctx["deployment_template"]
        container_template = ctx["container_template"]
        pipeline_config = ctx["pipeline_config"]
        pipeline_module = ctx["pipeline_module"]
        image_name = ctx["image_name"]
        cpu_per_process = ctx["cpu_per_process"]
        memory_per_process = ctx["memory_per_process"]
        pipeline_name = ctx["pipeline_name"]
        segment_id = ctx["segment_id"]
        service_name = ctx["service_name"]
        replicas = ctx["replicas"]
        emergency_patch = ctx.get("emergency_patch", {})

        process_count, segments_with_parallelism = get_multiprocess_config(pipeline_config)
        if len(segments_with_parallelism) > 1:
            raise ValueError(
                f"Multi-processing configuration can only be specified in one segment. "
                f"Found parallelism configuration in {len(segments_with_parallelism)} segments "
                f"(segment indices: {segments_with_parallelism})."
            )

        container = build_container(
            container_template,
            pipeline_name,
            pipeline_module,
            image_name,
            cpu_per_process,
            memory_per_process,
            segment_id,
            process_count,
        )

        base_deployment = load_base_template("deployment")

        labels = {
            "pipeline-app": make_k8s_name(pipeline_module),
            "pipeline": make_k8s_name(pipeline_name),
        }
        configmap_name = make_k8s_name(f"{service_name}-pipeline-{pipeline_name}")

        volumes: list[dict[str, Any]] = [
            {
                "name": "pipeline-config",
                "configMap": {
                    "name": configmap_name,
                },
            }
        ]

        # Shared memory volume is needed to allow the communication between processes.
        # via shared memory. Only needed when in multiprocess mode.
        if process_count is not None and process_count > 1:
            volumes.append(
                {
                    "name": "dshm",
                    "emptyDir": {"medium": "Memory"},
                }
            )

        pipeline_additions = {
            "metadata": {
                "name": make_k8s_name(f"{service_name}-pipeline-{pipeline_name}-{segment_id}"),
                "labels": labels,
            },
            "spec": {
                "replicas": replicas,
                "selector": {
                    "matchLabels": labels,
                },
                "template": {
                    "metadata": {
                        "labels": labels,
                    },
                    "spec": {
                        "containers": [container],
                        "volumes": volumes,
                    },
                },
            },
        }

        # Check for scalar conflicts between user template and pipeline additions
        # This ensures pipeline additions don't override user-provided values
        # while still allowing both to override base template defaults
        try:
            # Perform a test merge to detect conflicts
            deepmerge(deployment_template, pipeline_additions, fail_on_scalar_overwrite=True)
        except ScalarOverwriteError as e:
            raise ScalarOverwriteError(
                f"{e}\n\n"
                f"This field is automatically set by PipelineStep and conflicts with your deployment_template. "
                f"Note: Lists and dicts can be provided (they get merged), but scalar values cannot be overridden."
            ) from e

        # No conflicts found, proceed with merging
        # Both user template and pipeline additions can override base template
        deployment = deepmerge(base_deployment, deployment_template)
        deployment = deepmerge(deployment, pipeline_additions)

        if emergency_patch:
            deployment = deepmerge(deployment, emergency_patch)

        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": configmap_name,
                "labels": labels,
            },
            "data": {
                "pipeline_config.yaml": json.dumps(pipeline_config),
            },
        }

        if "namespace" in deployment.get("metadata", {}):
            metadata = cast(dict[str, Any], configmap["metadata"])
            metadata["namespace"] = deployment["metadata"]["namespace"]

        return {
            "deployment": deployment,
            "configmap": configmap,
        }

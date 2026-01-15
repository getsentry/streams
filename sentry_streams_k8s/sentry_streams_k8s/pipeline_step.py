import copy
import re
from typing import Any, TypedDict, cast

import yaml
from libsentrykube.ext import ExternalMacro

from sentry_streams_k8s.validation import validate_pipeline_config


def make_k8s_name(name: str) -> str:
    """
    Generate a valid Kubernetes name from a string.

    CConverts the string to a valid RFC 1123 compliant name
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


def build_labels(
    template_labels: dict[str, str],
    pipeline_name: str,
    pipeline_module: str,
) -> dict[str, str]:
    """
    Generate standard Kubernetes labels for the pipeline step.
    The goal is to allow us to have standard labeling for streaming
    resources.

    """
    return {
        **template_labels,
        "pipeline-app": make_k8s_name(pipeline_module),
        "pipeline": make_k8s_name(pipeline_name),
    }


def build_container(
    container_template: dict[str, Any],
    pipeline_name: str,
    pipeline_module: str,
    pipeline_config: dict[str, Any],
    image_name: str,
    cpu_per_process: int,
    memory_per_process: int,
    segment_id: int,
) -> dict[str, Any]:
    """
    Build a complete container specification for the pipeline step.

    Args:
        container_template: Base container structure to build upon
        pipeline_name: Name of the pipeline
        pipeline_module: The fully qualified Python module name
        pipeline_config: The pipeline configuration (unused but kept for consistency)
        image_name: Docker image name to use for the container
        cpu_per_process: CPU millicores to request
        memory_per_process: Memory in MiB to request
        segment_id: Segment ID for the pipeline

    Returns:
        Complete container specification with command, resources, and volume mounts
    """
    container = copy.deepcopy(container_template)

    container["image"] = image_name

    container["command"] = ["python", "-m", "sentry_streams.runner"]

    container["args"] = [
        "-n",
        pipeline_name,
        "--adapter",
        "rust_arroyo",
        "--segment-id",
        str(segment_id),
        "--config",
        "/etc/pipeline-config/pipeline_config.yaml",
        pipeline_module,
    ]

    # Add resource requests
    if "resources" not in container:
        container["resources"] = {}
    if "requests" not in container["resources"]:
        container["resources"]["requests"] = {}
    if "limits" not in container["resources"]:
        container["resources"]["limits"] = {}

    container["resources"]["requests"]["cpu"] = f"{cpu_per_process}m"
    container["resources"]["requests"]["memory"] = f"{memory_per_process}Mi"
    container["resources"]["limits"]["memory"] = f"{memory_per_process}Mi"

    # Add volume mount for configmap
    if "volumeMounts" not in container:
        container["volumeMounts"] = []

    container["volumeMounts"].append(
        {
            "name": "pipeline-config",
            "mountPath": "/etc/pipeline-config",
            "readOnly": True,
        }
    )

    return container


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

        Args:
            context: The context dictionary to validate

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

        # Validate pipeline_config structure using the same schema as runner.py
        validate_pipeline_config(context["pipeline_config"])

    def run(self, context: PipelineStepContext) -> dict[str, Any]:
        """
        Generates Kubernetes deployment and configmap manifests.

        Args:
            context: The validated context containing all required configuration

        Returns:
            Dictionary with 'deployment' and 'configmap' keys
        """
        # Extract context
        deployment_template = context["deployment_template"]
        container_template = context["container_template"]
        pipeline_config = context["pipeline_config"]
        pipeline_module = context["pipeline_module"]
        image_name = context["image_name"]
        cpu_per_process = context["cpu_per_process"]
        memory_per_process = context["memory_per_process"]
        pipeline_name = context["pipeline_name"]
        segment_id = context["segment_id"]
        service_name = context["service_name"]

        # Generate name and labels
        deployment_name = make_k8s_name(f"{service_name}-pipeline-{pipeline_name}-{segment_id}")

        # Build container
        container = build_container(
            container_template,
            pipeline_name,
            pipeline_module,
            pipeline_config,
            image_name,
            cpu_per_process,
            memory_per_process,
            segment_id,
        )

        # Deep copy deployment template to avoid mutations
        deployment = copy.deepcopy(deployment_template)

        # Update deployment metadata
        if "metadata" not in deployment:
            deployment["metadata"] = {}
        deployment["metadata"]["name"] = deployment_name
        if "labels" not in deployment["metadata"]:
            deployment["metadata"]["labels"] = {}
        labels = build_labels(deployment["metadata"]["labels"], pipeline_name, pipeline_module)
        deployment["metadata"]["labels"].update(labels)

        # Ensure spec.template.spec.containers exists
        if "spec" not in deployment:
            deployment["spec"] = {}
        if "template" not in deployment["spec"]:
            deployment["spec"]["template"] = {}
        if "metadata" not in deployment["spec"]["template"]:
            deployment["spec"]["template"]["metadata"] = {}
        if "labels" not in deployment["spec"]["template"]["metadata"]:
            deployment["spec"]["template"]["metadata"]["labels"] = {}
        deployment["spec"]["template"]["metadata"]["labels"].update(labels)

        if "spec" not in deployment["spec"]["template"]:
            deployment["spec"]["template"]["spec"] = {}
        if "containers" not in deployment["spec"]["template"]["spec"]:
            deployment["spec"]["template"]["spec"]["containers"] = []

        # Add container to deployment
        deployment["spec"]["template"]["spec"]["containers"].append(container)

        # Add configmap volume to deployment
        if "volumes" not in deployment["spec"]["template"]["spec"]:
            deployment["spec"]["template"]["spec"]["volumes"] = []

        configmap_name = make_k8s_name(f"{service_name}-pipeline-{pipeline_name}")
        deployment["spec"]["template"]["spec"]["volumes"].append(
            {
                "name": "pipeline-config",
                "configMap": {
                    "name": configmap_name,
                },
            }
        )

        # Create configmap
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": configmap_name,
                "labels": labels,
            },
            "data": {
                "pipeline_config.yaml": yaml.safe_dump(pipeline_config),
            },
        }

        # Add namespace if present in deployment template
        if "namespace" in deployment.get("metadata", {}):
            metadata = cast(dict[str, Any], configmap["metadata"])
            metadata["namespace"] = deployment["metadata"]["namespace"]

        return {
            "deployment": deployment,
            "configmap": configmap,
        }

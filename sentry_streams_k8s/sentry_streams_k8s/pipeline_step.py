import copy
import re
from typing import Any, TypedDict

from libsentrykube.ext import ExternalMacro
from sentry_streams.validation import validate_pipeline_config


def build_name(pipeline_module: str, pipeline_config: dict[str, Any]) -> str:
    """
    Generate a valid Kubernetes name from the pipeline module.

    Converts the pipeline module name to a valid RFC 1123 compliant name
    by replacing dots and underscores with dashes and converting to lowercase.

    Args:
        pipeline_module: The fully qualified Python module name
        pipeline_config: The pipeline configuration (unused but kept for consistency)

    Returns:
        A valid Kubernetes name derived from the pipeline module

    Examples:
        >>> build_name("sbc.profiles", {})
        'sbc-profiles'
        >>> build_name("my_module.sub_module", {})
        'my-module-sub-module'
    """
    # Replace dots and underscores with dashes, convert to lowercase
    name = pipeline_module.replace(".", "-").replace("_", "-").lower()
    # Ensure it's RFC 1123 compliant (lowercase alphanumeric + hyphens)
    name = re.sub(r"[^a-z0-9-]", "", name)
    # Remove leading/trailing hyphens
    name = name.strip("-")
    return name


def build_labels(pipeline_module: str, pipeline_config: dict[str, Any]) -> dict[str, str]:
    """
    Generate standard Kubernetes labels for the pipeline step.

    Args:
        pipeline_module: The fully qualified Python module name
        pipeline_config: The pipeline configuration (unused but kept for consistency)

    Returns:
        Dictionary of Kubernetes labels
    """
    app_name = build_name(pipeline_module, pipeline_config)
    return {
        "app": app_name,
        "component": "streaming-platform",
        "pipeline-module": pipeline_module,
    }


def build_container(
    container_template: dict[str, Any],
    pipeline_module: str,
    pipeline_config: dict[str, Any],
    cpu_per_process: int,
    memory_per_process: int,
) -> dict[str, Any]:
    """
    Build a complete container specification for the pipeline step.

    Args:
        container_template: Base container structure to build upon
        pipeline_module: The fully qualified Python module name
        pipeline_config: The pipeline configuration (unused but kept for consistency)
        cpu_per_process: CPU millicores to request
        memory_per_process: Memory in MiB to request

    Returns:
        Complete container specification with command, resources, and volume mounts
    """
    container = copy.deepcopy(container_template)

    # Add command/args to run the pipeline module
    # The streaming platform runner expects a Python file path
    # For now, we'll use a generic command that can be overridden
    if "command" not in container:
        container["command"] = ["python", "-m", "sentry_streams.runner"]

    if "args" not in container:
        container["args"] = [
            "--adapter",
            "rust_arroyo",
            "--config",
            "/etc/pipeline-config/pipeline_config.yaml",
            pipeline_module,
        ]

    # Add resource requests
    if "resources" not in container:
        container["resources"] = {}
    if "requests" not in container["resources"]:
        container["resources"]["requests"] = {}

    container["resources"]["requests"]["cpu"] = f"{cpu_per_process}m"
    container["resources"]["requests"]["memory"] = f"{memory_per_process}Mi"

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

    deployment_template: dict[str, Any]
    container_template: dict[str, Any]
    pipeline_config: dict[str, Any]
    pipeline_module: str
    cpu_per_process: int
    memory_per_process: int


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
                "deployment_template": deployment,
                "container_template": container,
                "pipeline_config": pipeline_config_dict,
                "pipeline_module": "sbc.profiles",
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
        assert "cpu_per_process" in context, "Missing cpu_per_process"
        assert "memory_per_process" in context, "Missing memory_per_process"

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
        # TODO: Fill me with content
        return {}

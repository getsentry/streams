from typing import Any, TypedDict

from libsentrykube.ext import ExternalMacro
from sentry_streams.validation import validate_pipeline_config


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

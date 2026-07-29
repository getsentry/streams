from __future__ import annotations

from typing import Any, NotRequired, TypedDict

import yaml
from libsentrykube.ext import ExternalMacro

from sentry_streams_k8s.consumer_builder import (  # noqa: F401
    LOG_LEVELS,
    ConsumerBuilder,
    ConsumerSpec,
    build_container,
    compute_config_version,
    get_multiprocess_config,
    load_base_template,
    make_k8s_name,
)


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
        "log_level": context.get("log_level", "INFO"),
        "replicas": context.get("replicas", 1),
        "emergency_patch": emergency_patch_parsed,
        "enable_liveness_probe": context.get("enable_liveness_probe", True),
        "container_name": context.get("container_name", "pipeline-consumer"),
        "with_canary": bool(context.get("with_canary", False)),
    }


def _to_consumer_spec(ctx: PipelineStepContext) -> ConsumerSpec:
    return ConsumerSpec(
        service_name=ctx["service_name"],
        pipeline_name=ctx["pipeline_name"],
        pipeline_module=ctx["pipeline_module"],
        image=ctx["image_name"],
        cpu_per_process=ctx["cpu_per_process"],
        memory_per_process=ctx["memory_per_process"],
        segment_id=ctx["segment_id"],
        replicas=ctx["replicas"],
        log_level=ctx.get("log_level", "INFO"),
        enable_liveness_probe=ctx.get("enable_liveness_probe", True),
        with_canary=ctx.get("with_canary", False),
        container_name=ctx.get("container_name", "pipeline-consumer"),
        emergency_patch=ctx.get("emergency_patch", {}),
    )


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
    log_level: NotRequired[str]
    replicas: int
    emergency_patch: NotRequired[dict[str, Any]]
    enable_liveness_probe: NotRequired[bool]
    container_name: NotRequired[str]
    with_canary: NotRequired[bool]


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
                "log_level": "INFO",
                "cpu_per_process": 1000,
                "memory_per_process": 512,
                "replicas": 3,
                "with_canary": True,
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

        ctx = parse_context(context)
        builder = ConsumerBuilder(ctx["deployment_template"], ctx["container_template"])
        builder.validate(_to_consumer_spec(ctx), ctx["pipeline_config"])

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Generates Kubernetes deployment and configmap manifests.

        Returns:
            Dictionary with 'deployment' and 'configmap' keys. When canary
            splitting is active (``with_canary`` and ``replicas`` > 1), also
            includes ``canary_deployment``.
        """
        ctx = parse_context(context)
        builder = ConsumerBuilder(ctx["deployment_template"], ctx["container_template"])
        return dict(builder.build_deployments(_to_consumer_spec(ctx), ctx["pipeline_config"]))

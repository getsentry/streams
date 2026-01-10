from typing import Any

from libsentrykube.ext import ExternalMacro


class Consumer(ExternalMacro):
    """
    A sentry-kube macro that creates the Kubernetes manifest for a consumer that
    runs the streaming platform.

    This can be imported in a sentry-kube template.
    The user can provide the basic structure of the deployment template with the
    basic infrastructure. This can include COGS labeling, nodepool config,
    some sidecars, etc.

    This macros fills it in with the streaming platform content: containers, volumes,
    configmap, naming conventions, etc.
    A similar pattern is followed by the Flink python operator, the user can
    provide a deployment template in the CRD, the flink operator fills it in
    with Flink.

    The goal of this is to standardize the deployment of streaming platform
    consumer while still sticking to the Sentry Kubernetes infrastructure based
    on client side rendering of templates and sentry-kube macros.

    This would be used like this in a jinja template:

    ```
    {% import '_deployment_template.j2' as deployment -%}
    {% set deployment = deployment.deployment() %}
    {{ render_external(
            "sentry_streams_k8s.consumer.Consumer",
            {
                "deployment_template": deployment,
                "pipeline_module": "sbc.profiles",
                "pipeline_config_file": "sbc_profiles.yaml",
            }
        )
    }}
    ```
    """

    @staticmethod
    def validate_context(context: dict[str, Any]) -> None:
        assert "deployment_template" in context
        assert "pipeline_module" in context
        assert "pipeline_config_file" in context

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        # TODO: Fill me with content
        return {}

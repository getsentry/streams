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

    This macros fills it in with the straming platform content: containers, volumes,
    configmap, naming conventions, etc.
    A similar pattern is followed by the Flink python operator, the user can
    provide a deployment template in the CRD, the flink operator fills it in
    with Flink.

    The goal of this is to standardie the the deployment of streaming platform
    consumer while still sticking to the Sentry Kubernetes infrastructure based
    on client side rendering of templates and sentry-kube macros.
    """

    @staticmethod
    def validate_context(context: dict[str, Any]) -> None:
        assert "deployment_template" in context
        assert "pipeline_module" in context
        assert "pipeline_config_file" in context

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        return {}

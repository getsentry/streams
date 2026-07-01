"""Kopf operator that reconciles StreamingConsumer CRs into the ConfigMap +
Deployment(s) the PipelineStep macro produces. Children are adopted by the CR, so
deleting the CR garbage-collects them (no delete handler needed)."""

from __future__ import annotations

import os
from typing import Any

import kopf
import yaml
from kubernetes import client, dynamic

from sentry_streams_k8s.operator.streaming_consumer import (
    from_crd_spec,
    render,
    validate,
)

GROUP = "streams.sentry.io"
VERSION = "v1"
PLURAL = "streamingconsumers"
DEFAULTS_PATH_ENV = "STREAMS_K8S_OPERATOR_DEFAULTS"

_operator_defaults: dict[str, Any] | None = None


def load_operator_defaults() -> dict[str, Any]:
    """Service/region defaults merged under every CR .spec, read once from the
    YAML file named by STREAMS_K8S_OPERATOR_DEFAULTS (empty when unset)."""
    global _operator_defaults
    if _operator_defaults is None:
        path = os.environ.get(DEFAULTS_PATH_ENV)
        if path:
            with open(path) as f:
                _operator_defaults = yaml.safe_load(f) or {}
        else:
            _operator_defaults = {}
    return _operator_defaults


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(spec: kopf.Spec, name: str, namespace: str | None, **_: Any) -> None:
    assert namespace is not None
    consumer = from_crd_spec(dict(spec), name=name, defaults=load_operator_defaults())
    validate(consumer)
    result = render(consumer)
    manifests = [result["configmap"], result["deployment"]]
    if "canary_deployment" in result:
        manifests.append(result["canary_deployment"])

    dyn = dynamic.DynamicClient(client.ApiClient())
    for manifest in manifests:
        kopf.adopt(manifest)
        resource = dyn.resources.get(api_version=manifest["apiVersion"], kind=manifest["kind"])
        dyn.server_side_apply(
            resource,
            body=manifest,
            namespace=namespace,
            field_manager="streams-k8s-operator",
            force_conflicts=True,
        )


def main() -> None:
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
from typing import Any

import kopf
from kubernetes import client, dynamic

from sentry_streams_k8s.consumer_builder import compute_config_version, make_k8s_name
from sentry_streams_k8s.operator.streaming_consumer import (
    from_crd_spec,
    render,
    validate,
)

logger = logging.getLogger(__name__)

GROUP = "streams.sentry.io"
VERSION = "v1alpha1"
PLURAL = "streamingconsumers"
FIELD_MANAGER = "streaming-operator"


def _apply(dyn: dynamic.DynamicClient, manifest: dict[str, Any], namespace: str) -> None:
    resource = dyn.resources.get(api_version=manifest["apiVersion"], kind=manifest["kind"])
    dyn.server_side_apply(
        resource,
        body=manifest,
        namespace=namespace,
        field_manager=FIELD_MANAGER,
        force_conflicts=True,
    )


def _prune_stale_deployments(
    *,
    namespace: str,
    owner_uid: str,
    service_name: str,
    pipeline_name: str,
    desired_names: set[str],
) -> None:
    apps = client.AppsV1Api()
    selector = f"service={make_k8s_name(service_name)},pipeline={make_k8s_name(pipeline_name)}"
    existing = apps.list_namespaced_deployment(namespace=namespace, label_selector=selector)
    for item in existing.items:
        owner_refs = item.metadata.owner_references or []
        if not any(ref.uid == owner_uid for ref in owner_refs):
            continue
        if item.metadata.name not in desired_names:
            logger.info("Pruning stale deployment %s/%s", namespace, item.metadata.name)
            apps.delete_namespaced_deployment(name=item.metadata.name, namespace=namespace)


def _condition(type_: str, status: bool, reason: str, message: str = "") -> dict[str, Any]:
    return {
        "type": type_,
        "status": "True" if status else "False",
        "reason": reason,
        "message": message,
    }


@kopf.on.create(GROUP, VERSION, PLURAL)
@kopf.on.update(GROUP, VERSION, PLURAL)
@kopf.on.resume(GROUP, VERSION, PLURAL)
def reconcile(
    spec: kopf.Spec,
    name: str,
    namespace: str | None,
    uid: str,
    patch: kopf.Patch,
    **_: Any,
) -> None:
    assert namespace is not None

    consumer = from_crd_spec(dict(spec), name=name)
    try:
        validate(consumer)
        result = render(consumer)
    except Exception as e:
        patch.status["conditions"] = [_condition("Rendered", False, type(e).__name__, str(e))]
        raise kopf.PermanentError(f"StreamingConsumer {namespace}/{name} failed to render: {e}")

    manifests = [result["configmap"], result["deployment"]]
    if "canary_deployment" in result:
        manifests.append(result["canary_deployment"])

    dyn = dynamic.DynamicClient(client.ApiClient())
    for manifest in manifests:
        kopf.adopt(manifest)
        _apply(dyn, manifest, namespace)

    _prune_stale_deployments(
        namespace=namespace,
        owner_uid=uid,
        service_name=consumer["service_name"],
        pipeline_name=consumer["pipeline_name"],
        desired_names={m["metadata"]["name"] for m in manifests if m["kind"] == "Deployment"},
    )

    replicas = consumer.get("replicas", 1)
    canary = 1 if "canary_deployment" in result else 0
    patch.status["conditions"] = [
        _condition("Rendered", True, "Rendered"),
        _condition("Applied", True, "Applied"),
    ]
    patch.status["config_version"] = compute_config_version(consumer["pipeline_config"])
    patch.status["replicas"] = {"primary": replicas - canary, "canary": canary}


def main() -> None:
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

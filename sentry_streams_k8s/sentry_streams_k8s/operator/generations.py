"""
Pods for a replica are named {name}-{ordinal}-{generation}. The generation
increments every time the operator replaces a Pod, so a newly created Pod
never shares a name with the old one that is still terminating. This is what
lets us recreate instantly instead of waiting for the old Pod to be deleted.
We save the highest generation per ordinal in a ConfigMap (one per CR).
"""

from __future__ import annotations

import json
import logging
from typing import Any, cast

from kubernetes.dynamic import DynamicClient
from kubernetes.dynamic.exceptions import NotFoundError

from sentry_streams_k8s.operator.constants import (
    FIELD_MANAGER,
    MANAGED_BY_LABEL,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)

logger = logging.getLogger(__name__)

LEDGER_RESOURCE_LABEL = "streams.sentry.io/resource"
LEDGER_RESOURCE_VALUE = "generation-ledger"

LEDGER_DATA_KEY = "generations"


def ledger_configmap_name(base_name: str) -> str:
    return f"{base_name}-generations"


def _configmap_resource(dyn: DynamicClient) -> Any:
    return dyn.resources.get(api_version="v1", kind="ConfigMap")


def _configmap_warn(namespace: str, name: str, message: str, *args: Any) -> None:
    logger.warning("generation ledger %s/%s: " + message, namespace, name, *args)


def _configmap_fail(namespace: str, name: str, message: str, *args: Any) -> dict[int, int]:
    _configmap_warn(namespace, name, message, *args)
    return {}


def load_generations(dyn: DynamicClient, namespace: str, name: str) -> dict[int, int]:
    """
    Read the persisted per-replica generation ledger from a ConfigMap.
    The generations data key has a JSON object mapping ordinal strings to
    integer counters. Returns an empty dict when there is an error reading.
    """

    try:
        configmap = _configmap_resource(dyn).get(name=name, namespace=namespace)
    except NotFoundError:
        return _configmap_fail(namespace, name, "ConfigMap not found")

    data = cast(dict[str, Any], configmap.to_dict().get("data"))

    if not data:
        return _configmap_fail(namespace, name, "missing or empty data key")

    raw_json = data.get(LEDGER_DATA_KEY)

    if not raw_json:
        return _configmap_fail(namespace, name, "missing or empty %r data key", LEDGER_DATA_KEY)

    try:
        parsed = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError) as e:
        return _configmap_fail(namespace, name, "invalid JSON in %r: %s", LEDGER_DATA_KEY, e)

    if not isinstance(parsed, dict):
        return _configmap_fail(namespace, name, "invalid JSON object: %s", type(parsed).__name__)

    try:
        return {int(key): int(value) for key, value in parsed.items()}
    except (TypeError, ValueError) as e:
        return _configmap_fail(namespace, name, "invalid generation entry: %s", e)


def save_generations(
    dyn: DynamicClient,
    namespace: str,
    name: str,
    *,
    owner_uid: str,
    owner_name: str,
    owner_namespace: str,
    generations: dict[int, int],
) -> None:
    body = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "labels": {
                OWNER_UID_LABEL: owner_uid,
                MANAGED_BY_LABEL: FIELD_MANAGER,
                LEDGER_RESOURCE_LABEL: LEDGER_RESOURCE_VALUE,
            },
            "annotations": {
                OWNER_NAME_ANNOTATION: owner_name,
                OWNER_NAMESPACE_ANNOTATION: owner_namespace,
            },
        },
        "data": {
            LEDGER_DATA_KEY: json.dumps(
                {str(ordinal): generation for ordinal, generation in sorted(generations.items())},
                sort_keys=True,
            ),
        },
    }

    _configmap_resource(dyn).server_side_apply(
        body=body,
        name=name,
        namespace=namespace,
        field_manager=FIELD_MANAGER,
        force_conflicts=True,
    )

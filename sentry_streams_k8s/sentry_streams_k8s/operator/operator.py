from __future__ import annotations

import os
from typing import Any

import kopf

from sentry_streams_k8s.operator.constants import (
    GROUP,
    PLURAL,
    VERSION,
    WORKLOAD_NAMESPACE_ENV,
)
from sentry_streams_k8s.operator.reconcile import (
    _prune_stale_resources,
    reconcile_pipeline,
)


def _workload_namespace() -> str:
    namespace = os.environ.get(WORKLOAD_NAMESPACE_ENV, "").strip()
    if not namespace:
        raise RuntimeError(f"{WORKLOAD_NAMESPACE_ENV} must be set.")
    return namespace


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
    reconcile_pipeline(
        spec=spec,
        name=name,
        namespace=namespace,
        uid=uid,
        workload_namespace=_workload_namespace(),
        patch=patch,
    )


@kopf.on.delete(GROUP, VERSION, PLURAL)
def cleanup(uid: str, **_: Any) -> None:
    _prune_stale_resources(
        workload_namespace=_workload_namespace(),
        owner_uid=uid,
        desired_deployments=set(),
        desired_configmaps=set(),
    )


def main() -> None:
    _workload_namespace()
    kopf.run(standalone=True, clusterwide=True)


if __name__ == "__main__":
    main()

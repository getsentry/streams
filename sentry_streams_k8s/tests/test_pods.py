from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import kopf
import pytest
from kubernetes.client import V1Pod

from sentry_streams_k8s.operator.constants import (
    CANARY_WORKLOAD_SET,
    GENERATION_LABEL,
    GROUP_INSTANCE_ID_ENV,
    MAX_BASE_NAME_LENGTH,
    MAX_GENERATION,
    MAX_REPLICAS,
    ORDINAL_LABEL,
    PRIMARY_WORKLOAD_SET,
    SPEC_HASH_ANNOTATION,
    WORKLOAD_SET_LABEL,
)
from sentry_streams_k8s.operator.pod_resources import (
    build_pipeline_pod,
    delete_owned_pods,
    group_instance_id,
    list_owned_pods,
    pod_workload_set,
)
from sentry_streams_k8s.operator.pod_types import PodManifest
from sentry_streams_k8s.operator.reconcile import (
    PodSetResult,
    _combine_pod_results,
    _parse_generations,
    _reconcile_pod_set,
    _serialize_generations,
    delete_obsolete_pod_sets,
    reconcile_pipeline_pods,
)
from tests.k8s_fixtures import make_condition, make_pod


def _container_env(pod: dict[str, Any], container_index: int = 0) -> dict[str, str]:
    env = pod["spec"]["containers"][container_index].get("env", [])
    return {item["name"]: item["value"] for item in env}


NAMESPACE = "workloads"
OWNER_UID = "owner-uid"


def _template() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        {"labels": {"app": "consumer"}, "annotations": {"configVersion": "one"}},
        {"containers": [{"name": "consumer", "image": "example/consumer:v1"}]},
    )


def _pod(
    ordinal: int,
    generation: int,
    *,
    ready: bool = False,
    phase: str = "Pending",
    annotations: dict[str, str] | None = None,
    deletion_timestamp: datetime | None = None,
) -> V1Pod:
    labels = {
        ORDINAL_LABEL: str(ordinal),
        GENERATION_LABEL: str(generation),
        WORKLOAD_SET_LABEL: PRIMARY_WORKLOAD_SET,
    }
    conditions = [make_condition("Ready", "True")] if ready else []
    return make_pod(
        name=f"consumer-{ordinal}-{generation}",
        labels=labels,
        annotations=annotations or {},
        deletion_timestamp=deletion_timestamp,
        phase=phase,
        conditions=conditions,
    )


def _existing_pod_from_manifest(
    manifest: PodManifest, *, phase: str = "Pending", ready: bool = False
) -> V1Pod:
    """Turns a desired-manifest dict (as returned by build_pipeline_pod) into an
    existing/observed V1Pod fixture with the same identity, for tests that need a
    "live" Pod matching a specific desired spec."""
    metadata = manifest["metadata"]
    conditions = [make_condition("Ready", "True")] if ready else []
    return make_pod(
        name=metadata["name"],
        labels=metadata.get("labels"),
        annotations=metadata.get("annotations"),
        phase=phase,
        conditions=conditions,
    )


def _reconcile(
    monkeypatch: pytest.MonkeyPatch,
    pods: list[V1Pod],
    *,
    replicas: int = 1,
    generations: dict[int, int] | None = None,
    template_metadata: dict[str, Any] | None = None,
    template_spec: dict[str, Any] | None = None,
    workload_set: str = PRIMARY_WORKLOAD_SET,
) -> tuple[list[PodManifest], list[tuple[str, bool]], PodSetResult, dict[int, int]]:
    applied: list[PodManifest] = []
    deleted: list[tuple[str, bool]] = []

    def list_pods(
        _core: object,
        _namespace: str,
        _owner_uid: str,
        workload_set: str | None = None,
    ) -> list[V1Pod]:
        current = copy.deepcopy(pods)
        if workload_set is None:
            return current
        return [pod for pod in current if pod_workload_set(pod) == workload_set]

    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.list_owned_pods",
        list_pods,
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.apply_pod",
        lambda _core, pod, _namespace: applied.append(pod),
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_pod",
        lambda _core, name, _namespace, force=False: deleted.append((name, force)),
    )
    metadata, spec = _template()
    ledger = generations if generations is not None else {}
    result = reconcile_pipeline_pods(
        core=MagicMock(),
        workload_namespace=NAMESPACE,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        base_name="consumer",
        template_metadata=template_metadata or metadata,
        template_spec=template_spec or spec,
        replicas=replicas,
        generations=ledger,
        logger=MagicMock(),
        workload_set=workload_set,
    )
    return applied, deleted, result, ledger


def test_build_pipeline_pod_stamps_identity_without_mutating_template() -> None:
    metadata, spec = _template()

    pod = build_pipeline_pod(
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        ordinal=2,
        generation=4,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=PRIMARY_WORKLOAD_SET,
    )

    assert pod["metadata"]["name"] == "consumer-2-4"
    assert pod["metadata"]["labels"][ORDINAL_LABEL] == "2"
    assert pod["metadata"]["labels"][GENERATION_LABEL] == "4"
    assert pod["metadata"]["labels"][WORKLOAD_SET_LABEL] == PRIMARY_WORKLOAD_SET
    assert pod["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
    assert pod["spec"]["restartPolicy"] == "Never"
    assert "restartPolicy" not in spec


def test_group_instance_id_is_stable_across_generations() -> None:
    assert group_instance_id("consumer", 2) == "consumer-2"
    assert group_instance_id("consumer-canary", 2) == "consumer-canary-2"


def test_build_pipeline_pod_injects_generation_stable_instance_id() -> None:
    metadata, spec = _template()

    def _build(generation: int) -> dict[str, Any]:
        return build_pipeline_pod(
            base_name="consumer",
            template_metadata=metadata,
            template_spec=spec,
            ordinal=2,
            generation=generation,
            owner_uid=OWNER_UID,
            owner_name="pipeline",
            owner_namespace="source",
            workload_set=PRIMARY_WORKLOAD_SET,
        )

    gen4 = _build(4)
    gen7 = _build(7)

    assert _container_env(gen4)[GROUP_INSTANCE_ID_ENV] == "consumer-2"
    assert _container_env(gen7)[GROUP_INSTANCE_ID_ENV] == "consumer-2"

    assert (
        gen4["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
        == gen7["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
    )


def test_build_pipeline_pod_respects_explicit_instance_id_env() -> None:
    metadata, _ = _template()
    spec = {
        "containers": [
            {
                "name": "consumer",
                "image": "example/consumer:v1",
                "env": [{"name": GROUP_INSTANCE_ID_ENV, "value": "explicit"}],
            }
        ]
    }
    pod = build_pipeline_pod(
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        ordinal=2,
        generation=0,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=PRIMARY_WORKLOAD_SET,
    )
    assert _container_env(pod)[GROUP_INSTANCE_ID_ENV] == "explicit"


def test_list_owned_pods_selects_owner_and_workload_set() -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = []

    assert list_owned_pods(core, NAMESPACE, OWNER_UID, CANARY_WORKLOAD_SET) == []

    core.list_namespaced_pod.assert_called_once_with(
        namespace=NAMESPACE,
        label_selector=(
            f"streams.sentry.io/owner-uid={OWNER_UID},"
            f"streams.sentry.io/workload-set={CANARY_WORKLOAD_SET}"
        ),
    )


def test_reconcile_creates_all_missing_ordinals_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    applied, deleted, result, ledger = _reconcile(monkeypatch, [], replicas=2)

    assert [pod["metadata"]["name"] for pod in applied] == ["consumer-0-0", "consumer-1-0"]
    assert deleted == []
    assert ledger == {0: 0, 1: 0}
    assert result["childPods"] == ["consumer-0-0", "consumer-1-0"]
    assert result["desiredReplicas"] == 2
    assert result["readyReplicas"] == 0

    metadata, spec = _template()
    current = [
        _existing_pod_from_manifest(
            build_pipeline_pod(
                base_name="consumer",
                template_metadata=metadata,
                template_spec=spec,
                ordinal=ordinal,
                generation=0,
                owner_uid=OWNER_UID,
                owner_name="pipeline",
                owner_namespace="source",
                workload_set=PRIMARY_WORKLOAD_SET,
            ),
            phase="Running",
            ready=True,
        )
        for ordinal in range(2)
    ]
    applied, deleted, result, ledger = _reconcile(monkeypatch, current, replicas=2)

    assert applied == []
    assert deleted == []
    assert ledger == {0: 0, 1: 0}
    assert result["readyReplicas"] == 2


def test_reconcile_replaces_failed_pod_with_next_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _pod(0, 3, phase="Failed")

    applied, deleted, result, ledger = _reconcile(monkeypatch, [current], generations={0: 7})

    assert [pod["metadata"]["name"] for pod in applied] == ["consumer-0-8"]
    assert deleted == [("consumer-0-3", False)]
    assert ledger == {0: 8}
    assert result["unhealthyPods"] == [
        {
            "name": "consumer-0-3",
            "ready": False,
            "phase": "Failed",
            "ordinal": "0",
            "reason": "Failed",
        }
    ]


def test_reconcile_raises_when_generation_exceeds_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = _pod(0, MAX_GENERATION, phase="Failed")
    with pytest.raises(kopf.PermanentError, match="exceeds"):
        _reconcile(monkeypatch, [current], generations={0: MAX_GENERATION})


def _set_result(*child_pods: str) -> PodSetResult:
    return {
        "childPods": list(child_pods),
        "desiredReplicas": len(child_pods),
        "readyReplicas": len(child_pods),
        "unhealthyPods": [],
        "permanentErrors": [],
    }


def test_combine_pod_results_nulls_out_absent_sets() -> None:
    combined = _combine_pod_results({PRIMARY_WORKLOAD_SET: _set_result("consumer-0-0")})
    assert combined["sets"] == {
        PRIMARY_WORKLOAD_SET: _set_result("consumer-0-0"),
        CANARY_WORKLOAD_SET: None,
    }


def _deployment(name: str, replicas: int) -> dict[str, Any]:
    metadata, spec = _template()
    return {
        "metadata": {"name": name},
        "spec": {
            "replicas": replicas,
            "template": {"metadata": metadata, "spec": spec},
        },
    }


def test_reconcile_pod_set_rejects_replicas_over_cap() -> None:
    deployment = _deployment("consumer", MAX_REPLICAS + 1)
    with pytest.raises(kopf.PermanentError, match="replica count cannot exceed"):
        _reconcile_pod_set(
            core=MagicMock(),
            deployment=deployment,
            workload_set=PRIMARY_WORKLOAD_SET,
            workload_namespace=NAMESPACE,
            owner_uid=OWNER_UID,
            owner_name="pipeline",
            owner_namespace="source",
            logger=MagicMock(),
            previous_generations={},
        )


def test_reconcile_pod_set_rejects_base_name_over_limit() -> None:
    deployment = _deployment("c" * (MAX_BASE_NAME_LENGTH + 1), 1)
    with pytest.raises(kopf.PermanentError, match="name cannot exceed"):
        _reconcile_pod_set(
            core=MagicMock(),
            deployment=deployment,
            workload_set=PRIMARY_WORKLOAD_SET,
            workload_namespace=NAMESPACE,
            owner_uid=OWNER_UID,
            owner_name="pipeline",
            owner_namespace="source",
            logger=MagicMock(),
            previous_generations={},
        )


def test_reconcile_replaces_outdated_pod_and_prunes_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outdated = _pod(0, 1, ready=True, phase="Running", annotations={SPEC_HASH_ANNOTATION: "old"})
    metadata, spec = _template()
    desired_manifest = build_pipeline_pod(
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        ordinal=0,
        generation=0,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=PRIMARY_WORKLOAD_SET,
    )
    duplicate_manifest = copy.deepcopy(desired_manifest)
    duplicate_manifest["metadata"]["name"] = "consumer-0-2"
    duplicate_manifest["metadata"]["labels"][GENERATION_LABEL] = "2"
    duplicate = _existing_pod_from_manifest(duplicate_manifest, phase="Running", ready=True)

    applied, deleted, result, ledger = _reconcile(monkeypatch, [outdated, duplicate])

    assert applied == []
    assert deleted == [("consumer-0-1", False)]
    assert result["childPods"] == ["consumer-0-2"]
    assert ledger == {0: 2}


def test_reconcile_prunes_scaled_down_and_unlabelled_pods(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale = _pod(2, 0)
    unlabelled = _pod(0, 0)
    assert unlabelled.metadata is not None
    del unlabelled.metadata.labels[ORDINAL_LABEL]

    applied, deleted, result, _ledger = _reconcile(monkeypatch, [stale, unlabelled], replicas=0)

    assert applied == []
    assert deleted == [("consumer-2-0", False), ("consumer-0-0", False)]
    assert result["desiredReplicas"] == 0


def test_reconcile_keeps_primary_and_canary_sets_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata, spec = _template()
    primary = _existing_pod_from_manifest(
        build_pipeline_pod(
            base_name="consumer",
            template_metadata=metadata,
            template_spec=spec,
            ordinal=0,
            generation=1,
            owner_uid=OWNER_UID,
            owner_name="pipeline",
            owner_namespace="source",
            workload_set=PRIMARY_WORKLOAD_SET,
        ),
        phase="Running",
        ready=True,
    )
    canary = _existing_pod_from_manifest(
        build_pipeline_pod(
            base_name="consumer-canary",
            template_metadata={**metadata, "labels": {"app": "consumer", "env": "canary"}},
            template_spec=spec,
            ordinal=0,
            generation=3,
            owner_uid=OWNER_UID,
            owner_name="pipeline",
            owner_namespace="source",
            workload_set=CANARY_WORKLOAD_SET,
        ),
        phase="Running",
        ready=True,
    )

    applied, deleted, result, ledger = _reconcile(
        monkeypatch,
        [primary, canary],
        workload_set=PRIMARY_WORKLOAD_SET,
    )

    assert applied == []
    assert deleted == []
    assert result["childPods"] == ["consumer-0-1"]
    assert ledger == {0: 1}


def test_delete_obsolete_pod_sets_removes_canary_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary = _pod(0, 1, ready=True, phase="Running")
    canary = _pod(0, 2, ready=True, phase="Running")
    assert canary.metadata is not None
    canary.metadata.name = "consumer-canary-0-2"
    canary.metadata.labels[WORKLOAD_SET_LABEL] = CANARY_WORKLOAD_SET
    missing_set = _pod(1, 0, ready=True, phase="Running")
    assert missing_set.metadata is not None
    del missing_set.metadata.labels[WORKLOAD_SET_LABEL]
    deleted: list[str] = []
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.list_owned_pods",
        lambda *_args, **_kwargs: [primary, canary, missing_set],
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_pod",
        lambda _core, name, _namespace, force=False: deleted.append(name),
    )

    delete_obsolete_pod_sets(
        MagicMock(),
        NAMESPACE,
        OWNER_UID,
        {PRIMARY_WORKLOAD_SET},
        MagicMock(),
    )

    assert deleted == ["consumer-canary-0-2", "consumer-1-0"]


def test_delete_owned_pods_skips_pods_already_terminating(monkeypatch: pytest.MonkeyPatch) -> None:
    pods = [_pod(0, 0), _pod(1, 0, deletion_timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc))]
    deleted: list[str] = []
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.pod_resources.list_owned_pods", lambda *_: pods
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.pod_resources.delete_pod",
        lambda _core, name, _namespace: deleted.append(name),
    )

    delete_owned_pods(MagicMock(), NAMESPACE, OWNER_UID, MagicMock())

    assert deleted == ["consumer-0-0"]


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"0": 3, "2": 5}, {0: 3, 2: 5}),
        (None, {}),
        ({}, {}),
    ],
)
def test_parse_generations_converts_string_ordinals_and_handles_no_ledger_yet(
    data: object, expected: dict[int, int]
) -> None:
    assert _parse_generations(data) == expected


def test_serialize_generations_sorts_ordinals_and_stringifies_keys() -> None:
    assert _serialize_generations({2: 4, 0: 3}) == {"0": 3, "2": 4}

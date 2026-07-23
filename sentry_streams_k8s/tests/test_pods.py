from __future__ import annotations

import copy
from typing import Any
from unittest.mock import MagicMock

import kopf
import pytest

from sentry_streams_k8s.operator.constants import (
    CANARY_WORKLOAD_SET,
    GENERATION_LABEL,
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
    list_owned_pods,
    pod_workload_set,
)
from sentry_streams_k8s.operator.reconcile import (
    _combine_pod_results,
    _parse_generations,
    _reconcile_pod_set,
    _serialize_generations,
    delete_obsolete_pod_sets,
    reconcile_pipeline_pods,
)

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
    deletion_timestamp: str | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "name": f"consumer-{ordinal}-{generation}",
        "labels": {
            ORDINAL_LABEL: str(ordinal),
            GENERATION_LABEL: str(generation),
            WORKLOAD_SET_LABEL: PRIMARY_WORKLOAD_SET,
        },
        "annotations": annotations or {},
    }
    if deletion_timestamp is not None:
        metadata["deletionTimestamp"] = deletion_timestamp
    conditions = [{"type": "Ready", "status": "True"}] if ready else []
    return {"metadata": metadata, "spec": {}, "status": {"phase": phase, "conditions": conditions}}


def _reconcile(
    monkeypatch: pytest.MonkeyPatch,
    pods: list[dict[str, Any]],
    *,
    replicas: int = 1,
    generations: dict[int, int] | None = None,
    template_metadata: dict[str, Any] | None = None,
    template_spec: dict[str, Any] | None = None,
    workload_set: str = PRIMARY_WORKLOAD_SET,
) -> tuple[list[dict[str, Any]], list[tuple[str, bool]], dict[str, Any], dict[int, int]]:
    applied: list[dict[str, Any]] = []
    deleted: list[tuple[str, bool]] = []

    def list_pods(
        _dyn: object,
        _namespace: str,
        _owner_uid: str,
        workload_set: str | None = None,
    ) -> list[dict[str, Any]]:
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
        lambda _dyn, pod, _namespace: applied.append(dict(pod)),
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_pod",
        lambda _dyn, name, _namespace, force=False: deleted.append((name, force)),
    )
    metadata, spec = _template()
    ledger = generations if generations is not None else {}
    result = reconcile_pipeline_pods(
        dyn=MagicMock(),
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


def test_list_owned_pods_selects_owner_and_workload_set() -> None:
    resource = MagicMock()
    resource.get.return_value.items = []
    dyn = MagicMock()
    dyn.resources.get.return_value = resource

    assert list_owned_pods(dyn, NAMESPACE, OWNER_UID, CANARY_WORKLOAD_SET) == []

    resource.get.assert_called_once_with(
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
        )
        for ordinal in range(2)
    ]
    for pod in current:
        pod["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}
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


def _set_result(*child_pods: str) -> dict[str, Any]:
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
            dyn=MagicMock(),
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
            dyn=MagicMock(),
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
    desired = build_pipeline_pod(
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
    duplicate = copy.deepcopy(desired)
    duplicate["metadata"]["name"] = "consumer-0-2"
    duplicate["metadata"]["labels"][GENERATION_LABEL] = "2"
    duplicate["status"] = {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}

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
    del unlabelled["metadata"]["labels"][ORDINAL_LABEL]

    applied, deleted, result, _ledger = _reconcile(monkeypatch, [stale, unlabelled], replicas=0)

    assert applied == []
    assert deleted == [("consumer-2-0", False), ("consumer-0-0", False)]
    assert result["desiredReplicas"] == 0


def test_reconcile_keeps_primary_and_canary_sets_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata, spec = _template()
    primary = build_pipeline_pod(
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        ordinal=0,
        generation=1,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=PRIMARY_WORKLOAD_SET,
    )
    canary = build_pipeline_pod(
        base_name="consumer-canary",
        template_metadata={**metadata, "labels": {"app": "consumer", "env": "canary"}},
        template_spec=spec,
        ordinal=0,
        generation=3,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=CANARY_WORKLOAD_SET,
    )
    for pod in (primary, canary):
        pod["status"] = {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}],
        }

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
    canary["metadata"]["name"] = "consumer-canary-0-2"
    canary["metadata"]["labels"][WORKLOAD_SET_LABEL] = CANARY_WORKLOAD_SET
    missing_set = _pod(1, 0, ready=True, phase="Running")
    del missing_set["metadata"]["labels"][WORKLOAD_SET_LABEL]
    deleted: list[str] = []
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.list_owned_pods",
        lambda *_args, **_kwargs: [primary, canary, missing_set],
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.reconcile.delete_pod",
        lambda _dyn, name, _namespace, force=False: deleted.append(name),
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
    pods = [_pod(0, 0), _pod(1, 0, deletion_timestamp="2026-07-16T00:00:00Z")]
    deleted: list[str] = []
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.pod_resources.list_owned_pods", lambda *_: pods
    )
    monkeypatch.setattr(
        "sentry_streams_k8s.operator.pod_resources.delete_pod",
        lambda _dyn, name, _namespace: deleted.append(name),
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

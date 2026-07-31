from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from typing import Any, cast

import pytest
from kubernetes.client import V1Pod

from sentry_streams_k8s.constants import CANARY_WORKLOAD_SET, PRIMARY_WORKLOAD_SET
from sentry_streams_k8s.consumer_builder import WorkloadSet
from sentry_streams_k8s.k8s_types import V1PodDict
from sentry_streams_k8s.operator.constants import (
    GENERATION_LABEL,
    MANAGED_BY_LABEL,
    ORDINAL_LABEL,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
    SPEC_HASH_ANNOTATION,
    WORKLOAD_SET_LABEL,
)
from sentry_streams_k8s.operator.control_client import RuntimeState
from sentry_streams_k8s.operator.pod_health import PodHealth
from sentry_streams_k8s.operator.pod_resources import (
    build_pipeline_pod,
    delete_owned_pods,
    delete_pod,
    list_owned_pods,
    pod_generation,
    pod_keep_key,
    pod_name,
    pod_ordinal,
    pod_workload_set,
)
from sentry_streams_k8s.operator.reconcile import (
    PodSetResult,
    delete_obsolete_pod_sets,
    reconcile_pipeline_pods,
)
from tests.k8s_fixtures import (
    FakeControlClient,
    FakeCoreV1Api,
    make_condition,
    make_pod,
)

NAMESPACE = "workloads"
OWNER_UID = "owner-uid"
LOGGER = logging.getLogger(__name__)


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
        OWNER_UID_LABEL: OWNER_UID,
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


def _build(
    *,
    generation: int = 4,
    spec: dict[str, Any] | None = None,
    workload_set: str = PRIMARY_WORKLOAD_SET,
) -> V1PodDict:
    template_metadata, template_spec = _template()
    return build_pipeline_pod(
        base_name="consumer",
        template_metadata=template_metadata,
        template_spec=spec if spec is not None else template_spec,
        ordinal=2,
        generation=generation,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=workload_set,
    )


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

    assert pod["apiVersion"] == "v1"
    assert pod["kind"] == "Pod"
    assert pod["metadata"]["name"] == "consumer-2-4"
    assert pod["metadata"]["labels"] == {
        # The template's own labels survive rather than being clobbered:
        "app": "consumer",
        MANAGED_BY_LABEL: "streaming-operator",
        OWNER_UID_LABEL: OWNER_UID,
        WORKLOAD_SET_LABEL: PRIMARY_WORKLOAD_SET,
        ORDINAL_LABEL: "2",
        GENERATION_LABEL: "4",
    }
    annotations = dict(pod["metadata"]["annotations"])
    assert annotations.pop(SPEC_HASH_ANNOTATION)
    assert annotations == {
        "configVersion": "one",
        OWNER_NAME_ANNOTATION: "pipeline",
        OWNER_NAMESPACE_ANNOTATION: "source",
    }
    # The operator replaces unhealthy Pods rather than letting kubelet restart them:
    assert pod["spec"]["restartPolicy"] == "Never"

    assert "restartPolicy" not in spec
    assert metadata == _template()[0]


def test_spec_hash_survives_a_generation_bump_but_tracks_the_spec() -> None:
    # A replacement Pod for the same desired state must hash the same, or every
    # generation bump would look like a spec change and replace the Pod again:
    assert (
        _build(generation=4)["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
        == _build(generation=5)["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
    )

    changed = _build(
        spec={"containers": [{"name": "consumer", "image": "example/consumer:v2"}]},
    )
    assert (
        changed["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
        != _build()["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
    )


def test_spec_hash_tracks_the_workload_set() -> None:
    assert (
        _build(workload_set=CANARY_WORKLOAD_SET)["metadata"]["annotations"][SPEC_HASH_ANNOTATION]
        != _build(workload_set=PRIMARY_WORKLOAD_SET)["metadata"]["annotations"][
            SPEC_HASH_ANNOTATION
        ]
    )


@pytest.mark.parametrize(
    ("workload_set", "label_selector"),
    [
        (
            CANARY_WORKLOAD_SET,
            (
                f"pipeline.streams.sentry.io/owner-uid={OWNER_UID},"
                f"streams.sentry.io/workload-set={CANARY_WORKLOAD_SET}"
            ),
        ),
        (None, f"pipeline.streams.sentry.io/owner-uid={OWNER_UID}"),
    ],
)
def test_list_owned_pods_selects_owner_and_optional_workload_set(
    workload_set: str | None,
    label_selector: str,
) -> None:
    core = FakeCoreV1Api()

    assert list_owned_pods(core, NAMESPACE, OWNER_UID, workload_set) == []

    assert core.pod_list_calls == [(NAMESPACE, label_selector)]


def test_delete_pod_only_drops_the_grace_period_when_forced() -> None:
    core = FakeCoreV1Api()

    delete_pod(core, "consumer-0-0", NAMESPACE)
    delete_pod(core, "consumer-0-0", NAMESPACE, force=True)

    assert core.deleted_pods == [
        ("consumer-0-0", False),
        ("consumer-0-0", True),
    ]


def test_delete_owned_pods_skips_pods_already_terminating() -> None:
    pods = [_pod(0, 0), _pod(1, 0, deletion_timestamp=datetime(2026, 7, 16, tzinfo=timezone.utc))]
    core = FakeCoreV1Api(pods=pods)

    delete_owned_pods(core, NAMESPACE, OWNER_UID, LOGGER)

    assert core.deleted_pods == [("consumer-0-0", False)]


def test_metadata_readers_parse_operator_labels() -> None:
    labelled = _pod(3, 7)
    assert pod_name(labelled) == "consumer-3-7"
    assert pod_ordinal(labelled) == 3
    assert pod_generation(labelled) == 7
    assert pod_workload_set(labelled) == PRIMARY_WORKLOAD_SET


def test_pod_keep_key_prefers_ready_then_the_newest_generation() -> None:
    ready_old = _pod(0, 1, ready=True)
    unready_new = _pod(0, 9)

    # Readiness dominates: a ready Pod is kept over a newer unready one.
    assert pod_keep_key(ready_old, PodHealth(name=pod_name(ready_old), ready=True)) > pod_keep_key(
        unready_new, PodHealth(name=pod_name(unready_new))
    )

    older = _pod(0, 1)
    newer = _pod(0, 2)
    assert pod_keep_key(newer, PodHealth(name=pod_name(newer))) > pod_keep_key(
        older, PodHealth(name=pod_name(older))
    )


def _manifest(
    ordinal: int,
    generation: int,
    *,
    base_name: str = "consumer",
    workload_set: str = PRIMARY_WORKLOAD_SET,
    template_metadata: dict[str, Any] | None = None,
) -> V1PodDict:
    metadata, spec = _template()
    return build_pipeline_pod(
        base_name=base_name,
        template_metadata=template_metadata or metadata,
        template_spec=spec,
        ordinal=ordinal,
        generation=generation,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        workload_set=workload_set,
    )


def _observed(
    manifest: V1PodDict,
    *,
    phase: str = "Pending",
    ready: bool = False,
    container_statuses: list[Any] | None = None,
    start_time: datetime | None = None,
    deletion_timestamp: datetime | None = None,
    pod_ip: str | None = None,
) -> V1Pod:
    metadata = manifest["metadata"]
    return make_pod(
        name=metadata["name"],
        labels=metadata.get("labels"),
        annotations=metadata.get("annotations"),
        phase=phase,
        conditions=[make_condition("Ready", "True")] if ready else [],
        container_statuses=container_statuses,
        start_time=start_time,
        deletion_timestamp=deletion_timestamp,
        pod_ip=pod_ip,
    )


def _workload(name: str = "consumer", replicas: int = 1) -> WorkloadSet:
    metadata, spec = _template()
    return WorkloadSet(
        name=name,
        replicas=replicas,
        labels=dict(metadata["labels"]),
        pod_template={"metadata": metadata, "spec": spec},
    )


def _reconcile(
    pods: list[V1Pod],
    *,
    replicas: int = 1,
    generations: dict[int, int] | None = None,
    workload_set: str = PRIMARY_WORKLOAD_SET,
    control: FakeControlClient | None = None,
) -> tuple[list[V1PodDict], list[tuple[str, bool]], PodSetResult, dict[int, int]]:
    core = FakeCoreV1Api(pods=pods)
    metadata, spec = _template()
    ledger = generations if generations is not None else {}
    result = reconcile_pipeline_pods(
        core=core,
        workload_namespace=NAMESPACE,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        replicas=replicas,
        generations=ledger,
        logger=LOGGER,
        workload_set=workload_set,
        control=cast(Any, control if control is not None else FakeControlClient()),
    )
    return core.applied_pods, core.deleted_pods, result, ledger


def test_reconcile_creates_all_missing_ordinals() -> None:
    applied, deleted, result, ledger = _reconcile([], replicas=2)

    assert [pod["metadata"]["name"] for pod in applied] == ["consumer-0-0", "consumer-1-0"]
    assert deleted == []
    assert ledger == {0: 0, 1: 0}
    assert result["childPods"] == ["consumer-0-0", "consumer-1-0"]
    assert result["desiredReplicas"] == 2
    assert result["readyReplicas"] == 0

    current = [
        _observed(_manifest(ordinal, 0), phase="Running", ready=True) for ordinal in range(2)
    ]
    applied, deleted, result, ledger = _reconcile(current, replicas=2)

    assert applied == []
    assert deleted == []
    assert ledger == {0: 0, 1: 0}
    assert result["readyReplicas"] == 2


def test_reconcile_scales_down() -> None:
    stale = _pod(2, 0)

    applied, deleted, result, _ledger = _reconcile([stale], replicas=0)

    assert applied == []
    assert deleted == [("consumer-2-0", False)]
    assert result["desiredReplicas"] == 0


def test_reconcile_replaces_a_failed_pod_with_the_next_generation() -> None:
    current = _pod(0, 3, phase="Failed")

    applied, deleted, result, ledger = _reconcile([current], generations={0: 7})

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


def test_reconcile_keeps_outdated_consuming_until_the_replacement_exists() -> None:
    outdated = _observed(_manifest(0, 1), phase="Running", ready=True, pod_ip="10.0.0.1")
    assert outdated.metadata is not None
    outdated.metadata.annotations[SPEC_HASH_ANNOTATION] = "old"
    core = FakeCoreV1Api(pods=[outdated])
    metadata, spec = _template()
    control = FakeControlClient(states={"10.0.0.1": RuntimeState.CONSUMING})

    reconcile_pipeline_pods(
        core=core,
        workload_namespace=NAMESPACE,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        replicas=1,
        generations={},
        logger=LOGGER,
        workload_set=PRIMARY_WORKLOAD_SET,
        control=cast(Any, control),
    )

    assert core.operations == ["apply:consumer-0-2"]
    assert control.stopped == []
    assert control.started == []


def test_reconcile_keeps_the_best_duplicate_and_deletes_the_rest() -> None:
    outdated = _pod(0, 1, ready=True, phase="Running", annotations={SPEC_HASH_ANNOTATION: "old"})
    duplicate_manifest = copy.deepcopy(_manifest(0, 0))
    duplicate_manifest["metadata"]["name"] = "consumer-0-2"
    duplicate_manifest["metadata"]["labels"][GENERATION_LABEL] = "2"
    duplicate = _observed(duplicate_manifest, phase="Running", ready=True)

    applied, deleted, result, ledger = _reconcile([outdated, duplicate])

    assert applied == []
    assert deleted == [("consumer-0-1", False)]
    assert result["childPods"] == ["consumer-0-2"]
    assert ledger == {0: 2}


def test_reconcile_starts_a_ready_pod_that_has_no_predecessor() -> None:
    pod = _observed(_manifest(0, 0), phase="Running", ready=True, pod_ip="10.0.0.9")
    control = FakeControlClient(states={"10.0.0.9": RuntimeState.IDLE})

    applied, deleted, _result, _ledger = _reconcile([pod], control=control)

    assert applied == []
    assert deleted == []
    assert control.started == [("10.0.0.9", "consumer-0")]


def test_reconcile_leaves_an_already_consuming_pod_alone() -> None:
    pod = _observed(_manifest(0, 0), phase="Running", ready=True, pod_ip="10.0.0.9")
    control = FakeControlClient(states={"10.0.0.9": RuntimeState.CONSUMING})

    applied, deleted, _result, _ledger = _reconcile([pod], control=control)

    assert applied == [] and deleted == []
    assert control.started == [] and control.stopped == []


def test_reconcile_hands_partitions_over_before_deleting_the_outdated_pod() -> None:
    outdated = _observed(_manifest(0, 1), phase="Running", ready=True, pod_ip="10.0.0.1")
    assert outdated.metadata is not None
    outdated.metadata.annotations[SPEC_HASH_ANNOTATION] = "old"
    replacement = _observed(_manifest(0, 2), phase="Running", ready=True, pod_ip="10.0.0.2")
    core = FakeCoreV1Api(pods=[outdated, replacement])
    metadata, spec = _template()
    control = FakeControlClient(
        states={"10.0.0.1": RuntimeState.CONSUMING, "10.0.0.2": RuntimeState.IDLE}
    )

    reconcile_pipeline_pods(
        core=core,
        workload_namespace=NAMESPACE,
        owner_uid=OWNER_UID,
        owner_name="pipeline",
        owner_namespace="source",
        base_name="consumer",
        template_metadata=metadata,
        template_spec=spec,
        replicas=1,
        generations={},
        logger=LOGGER,
        workload_set=PRIMARY_WORKLOAD_SET,
        control=cast(Any, control),
    )

    assert control.stopped == ["10.0.0.1"]
    assert control.started == [("10.0.0.2", "consumer-0")]
    assert core.operations == ["delete:consumer-0-1"]


def test_reconcile_does_not_hand_over_to_an_unready_replacement() -> None:
    outdated = _observed(_manifest(0, 1), phase="Running", ready=True, pod_ip="10.0.0.1")
    assert outdated.metadata is not None
    outdated.metadata.annotations[SPEC_HASH_ANNOTATION] = "old"
    replacement = _observed(_manifest(0, 2), phase="Pending", ready=False, pod_ip="10.0.0.2")
    control = FakeControlClient(
        states={"10.0.0.1": RuntimeState.CONSUMING, "10.0.0.2": RuntimeState.IDLE}
    )

    applied, deleted, _result, _ledger = _reconcile([outdated, replacement], control=control)

    assert applied == [] and deleted == []
    assert control.stopped == [] and control.started == []


def test_reconcile_deletes_an_unreachable_predecessor_and_waits() -> None:
    outdated = _observed(_manifest(0, 1), phase="Running", ready=True, pod_ip="10.0.0.1")
    assert outdated.metadata is not None
    outdated.metadata.annotations[SPEC_HASH_ANNOTATION] = "old"
    replacement = _observed(_manifest(0, 2), phase="Running", ready=True, pod_ip="10.0.0.2")
    control = FakeControlClient(states={"10.0.0.2": RuntimeState.IDLE}, unreachable={"10.0.0.1"})

    applied, deleted, _result, _ledger = _reconcile([outdated, replacement], control=control)

    assert applied == []
    assert deleted == [("consumer-0-1", False)]
    assert control.started == []


def test_reconcile_waits_for_a_terminating_predecessor_to_finish_committing() -> None:
    outdated = _observed(
        _manifest(0, 1),
        phase="Running",
        ready=True,
        pod_ip="10.0.0.1",
        deletion_timestamp=datetime.now(timezone.utc),
    )
    assert outdated.metadata is not None
    outdated.metadata.annotations[SPEC_HASH_ANNOTATION] = "old"
    replacement = _observed(_manifest(0, 2), phase="Running", ready=True, pod_ip="10.0.0.2")
    control = FakeControlClient(
        states={"10.0.0.1": RuntimeState.STOPPING, "10.0.0.2": RuntimeState.IDLE}
    )

    applied, deleted, _result, _ledger = _reconcile([outdated, replacement], control=control)

    assert applied == [] and deleted == []
    assert control.started == []


def test_delete_obsolete_pod_sets_removes_canary_when_disabled() -> None:
    primary = _pod(0, 1, ready=True, phase="Running")
    canary = _pod(0, 2, ready=True, phase="Running")
    assert canary.metadata is not None
    canary.metadata.name = "consumer-canary-0-2"
    canary.metadata.labels[WORKLOAD_SET_LABEL] = CANARY_WORKLOAD_SET
    core = FakeCoreV1Api(pods=[primary, canary])

    delete_obsolete_pod_sets(
        core,
        NAMESPACE,
        OWNER_UID,
        {PRIMARY_WORKLOAD_SET},
        LOGGER,
    )

    assert core.deleted_pods == [("consumer-canary-0-2", False)]

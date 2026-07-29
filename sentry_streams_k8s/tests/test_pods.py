from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from kubernetes.client import V1Pod

from sentry_streams_k8s.constants import CANARY_WORKLOAD_SET, PRIMARY_WORKLOAD_SET
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
from tests.k8s_fixtures import make_condition, make_pod

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
                f"streams.sentry.io/owner-uid={OWNER_UID},"
                f"streams.sentry.io/workload-set={CANARY_WORKLOAD_SET}"
            ),
        ),
        (None, f"streams.sentry.io/owner-uid={OWNER_UID}"),
    ],
)
def test_list_owned_pods_selects_owner_and_optional_workload_set(
    workload_set: str | None,
    label_selector: str,
) -> None:
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = []

    assert list_owned_pods(core, NAMESPACE, OWNER_UID, workload_set) == []

    core.list_namespaced_pod.assert_called_once_with(
        namespace=NAMESPACE,
        label_selector=label_selector,
    )


def test_delete_pod_only_drops_the_grace_period_when_forced() -> None:
    core = MagicMock()

    delete_pod(core, "consumer-0-0", NAMESPACE)

    core.delete_namespaced_pod.assert_called_once_with(
        name="consumer-0-0",
        namespace=NAMESPACE,
    )

    core.reset_mock()
    delete_pod(core, "consumer-0-0", NAMESPACE, force=True)

    kwargs = core.delete_namespaced_pod.call_args.kwargs
    assert kwargs["name"] == "consumer-0-0"
    assert kwargs["namespace"] == NAMESPACE
    assert kwargs["body"].grace_period_seconds == 0


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


def test_metadata_readers_parse_operator_labels() -> None:
    labelled = _pod(3, 7)
    assert pod_name(labelled) == "consumer-3-7"
    assert pod_ordinal(labelled) == 3
    assert pod_generation(labelled) == 7
    assert pod_workload_set(labelled) == PRIMARY_WORKLOAD_SET

    garbage = make_pod(labels={ORDINAL_LABEL: "one", GENERATION_LABEL: "latest"})
    assert pod_ordinal(garbage) is None
    assert pod_generation(garbage) == 0


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

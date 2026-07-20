from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes.client.exceptions import ApiException
from kubernetes.dynamic.exceptions import NotFoundError

from sentry_streams_k8s.operator.constants import (
    FIELD_MANAGER,
    MANAGED_BY_LABEL,
    OWNER_NAME_ANNOTATION,
    OWNER_NAMESPACE_ANNOTATION,
    OWNER_UID_LABEL,
)
from sentry_streams_k8s.operator.generations import (
    LEDGER_DATA_KEY,
    LEDGER_RESOURCE_LABEL,
    LEDGER_RESOURCE_VALUE,
    load_generations,
    save_generations,
)


def _dynamic_with_configmap(configmap: object) -> tuple[MagicMock, MagicMock]:
    resource = MagicMock()
    resource.get.return_value = configmap
    dyn = MagicMock()
    dyn.resources.get.return_value = resource
    return dyn, resource


def test_load_generations_returns_empty_for_missing_configmap() -> None:
    dyn, resource = _dynamic_with_configmap(None)
    resource.get.side_effect = NotFoundError(ApiException(status=404))

    assert load_generations(dyn, "workloads", "consumer-generations") == {}


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({LEDGER_DATA_KEY: '{"0": 3, "2": 5}'}, {0: 3, 2: 5}),
        ({LEDGER_DATA_KEY: "[]"}, {}),
        ({LEDGER_DATA_KEY: "not json"}, {}),
        ({LEDGER_DATA_KEY: '{"0": 3, "bad": "nope"}'}, {}),
        ({}, {}),
    ],
)
def test_load_generations_handles_valid_and_degraded_ledger_data(
    data: dict[str, str], expected: dict[int, int]
) -> None:
    dyn, _resource = _dynamic_with_configmap(SimpleNamespace(to_dict=lambda: {"data": data}))

    assert load_generations(dyn, "workloads", "consumer-generations") == expected


def test_load_generations_reraises_non_404_api_errors() -> None:
    dyn, resource = _dynamic_with_configmap(None)
    resource.get.side_effect = ApiException(status=500)

    with pytest.raises(ApiException):
        load_generations(dyn, "workloads", "consumer-generations")


def test_save_generations_uses_server_side_apply_with_owned_ledger() -> None:
    dyn, resource = _dynamic_with_configmap(None)

    save_generations(
        dyn,
        "workloads",
        "consumer-generations",
        owner_uid="uid",
        owner_name="pipeline",
        owner_namespace="source",
        generations={2: 4, 0: 3},
    )

    resource.server_side_apply.assert_called_once_with(
        body={
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "consumer-generations",
                "labels": {
                    OWNER_UID_LABEL: "uid",
                    MANAGED_BY_LABEL: FIELD_MANAGER,
                    LEDGER_RESOURCE_LABEL: LEDGER_RESOURCE_VALUE,
                },
                "annotations": {
                    OWNER_NAME_ANNOTATION: "pipeline",
                    OWNER_NAMESPACE_ANNOTATION: "source",
                },
            },
            "data": {LEDGER_DATA_KEY: '{"0": 3, "2": 4}'},
        },
        name="consumer-generations",
        namespace="workloads",
        field_manager=FIELD_MANAGER,
        force_conflicts=True,
    )

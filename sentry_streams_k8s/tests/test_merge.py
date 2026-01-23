"""Tests for the merge module and deepmerge function."""

from typing import Any

import pytest

from sentry_streams_k8s.merge import TypeMismatchError, deepmerge


@pytest.mark.parametrize(
    ("base", "override", "expected", "test_id"),
    [
        # Simple types - override replaces base (same types only)
        (
            {
                "string": "base_value",
                "int": 1,
                "bool": True,
                "float": 1.0,
                "other": 1,
            },
            {
                "string": "override_value",
                "int": 2,
                "bool": False,
                "float": 2.5,
            },
            {
                "string": "override_value",
                "int": 2,
                "bool": False,
                "float": 2.5,
                "other": 1,
            },
            "simple_types",
        ),
        # Nested dicts - recursive merge
        (
            {
                "level1": {
                    "level2": {
                        "base_key": "base_value",
                        "shared_key": "base_shared",
                    }
                }
            },
            {
                "level1": {
                    "level2": {
                        "override_key": "override_value",
                        "shared_key": "override_shared",
                    }
                }
            },
            {
                "level1": {
                    "level2": {
                        "base_key": "base_value",
                        "override_key": "override_value",
                        "shared_key": "override_shared",
                    }
                }
            },
            "nested_dicts",
        ),
        # Lists - concatenation
        (
            {
                "volumes": [
                    {"name": "vol1", "mountPath": "/vol1"},
                    {"name": "vol2", "mountPath": "/vol2"},
                ]
            },
            {
                "volumes": [
                    {"name": "vol3", "mountPath": "/vol3"},
                ]
            },
            {
                "volumes": [
                    {"name": "vol1", "mountPath": "/vol1"},
                    {"name": "vol2", "mountPath": "/vol2"},
                    {"name": "vol3", "mountPath": "/vol3"},
                ]
            },
            "lists_append",
        ),
        # Complex nested structure
        (
            {
                "metadata": {
                    "name": "base-name",
                    "labels": {
                        "app": "base-app",
                        "env": "base-env",
                    },
                    "annotations": {
                        "base-annotation": "value",
                    },
                },
                "spec": {
                    "replicas": 1,
                    "containers": [
                        {"name": "container1", "image": "image1"},
                    ],
                },
            },
            {
                "metadata": {
                    "name": "override-name",
                    "labels": {
                        "env": "override-env",
                        "version": "v1",
                    },
                },
                "spec": {
                    "containers": [
                        {"name": "container2", "image": "image2"},
                    ],
                    "volumes": [
                        {"name": "volume1"},
                    ],
                },
            },
            {
                "metadata": {
                    "name": "override-name",
                    "labels": {
                        "app": "base-app",
                        "env": "override-env",
                        "version": "v1",
                    },
                    "annotations": {
                        "base-annotation": "value",
                    },
                },
                "spec": {
                    "replicas": 1,
                    "containers": [
                        {"name": "container1", "image": "image1"},
                        {"name": "container2", "image": "image2"},
                    ],
                    "volumes": [
                        {"name": "volume1"},
                    ],
                },
            },
            "complex",
        ),
        # Empty base dict
        (
            {},
            {"key": "value"},
            {"key": "value"},
            "empty_base",
        ),
        # Empty override dict
        (
            {"key": "value"},
            {},
            {"key": "value"},
            "empty_override",
        ),
        # Both empty
        (
            {},
            {},
            {},
            "both_empty",
        ),
        # Keys only in override
        (
            {"a": 1},
            {"b": 2, "c": 3},
            {"a": 1, "b": 2, "c": 3},
            "override_only_keys",
        ),
        # Keys only in base
        (
            {"a": 1, "b": 2, "c": 3},
            {"d": 4},
            {"a": 1, "b": 2, "c": 3, "d": 4},
            "base_only_keys",
        ),
        # Empty base list
        (
            {"items": []},
            {"items": [1, 2, 3]},
            {"items": [1, 2, 3]},
            "empty_base_list",
        ),
        # Empty override list
        (
            {"items": [1, 2, 3]},
            {"items": []},
            {"items": [1, 2, 3]},
            "empty_override_list",
        ),
        # Both lists empty
        (
            {"items": []},
            {"items": []},
            {"items": []},
            "both_lists_empty",
        ),
    ],
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_deepmerge_success_cases(
    base: dict[str, Any],
    override: dict[str, Any],
    expected: dict[str, Any],
    test_id: str,
) -> None:
    """Test successful deepmerge operations with various input combinations."""
    result = deepmerge(base, override)
    assert result == expected


def test_deepmerge_does_not_mutate_inputs() -> None:
    """Test that deepmerge does not mutate the input dictionaries."""
    base = {
        "dict": {"key": "value"},
        "list": [1, 2, 3],
    }
    override = {
        "dict": {"key2": "value2"},
        "list": [4, 5],
    }

    base_copy = {
        "dict": {"key": "value"},
        "list": [1, 2, 3],
    }
    override_copy = {
        "dict": {"key2": "value2"},
        "list": [4, 5],
    }

    deepmerge(base, override)

    # Check that inputs are not mutated
    assert base == base_copy
    assert override == override_copy


@pytest.mark.parametrize(
    ("base", "override", "expected_error_msg", "test_id"),
    [
        # dict -> string
        (
            {"key": {"nested": "dict"}},
            {"key": "string"},
            r"Cannot merge key 'key': base type is <class 'dict'> but override type is <class 'str'>",
            "dict_to_string",
        ),
        # list -> dict
        (
            {"key": [1, 2, 3]},
            {"key": {"nested": "dict"}},
            r"Cannot merge key 'key': base type is <class 'list'> but override type is <class 'dict'>",
            "list_to_dict",
        ),
        # string -> list
        (
            {"key": "string"},
            {"key": [1, 2, 3]},
            r"Cannot merge key 'key': base type is <class 'str'> but override type is <class 'list'>",
            "string_to_list",
        ),
        # dict -> list
        (
            {"key": {"nested": "dict"}},
            {"key": [1, 2, 3]},
            r"Cannot merge key 'key': base type is <class 'dict'> but override type is <class 'list'>",
            "dict_to_list",
        ),
    ],
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_deepmerge_mismatched_types(
    base: dict[str, Any],
    override: dict[str, Any],
    expected_error_msg: str,
    test_id: str,
) -> None:
    """Test that type mismatches raise TypeMismatchError."""
    with pytest.raises(TypeMismatchError, match=expected_error_msg):
        deepmerge(base, override)


def test_deepmerge_kubernetes_deployment_example() -> None:
    """Test a realistic Kubernetes deployment merging scenario."""
    base = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "labels": {},
        },
        "spec": {
            "replicas": 1,
            "template": {
                "spec": {
                    "containers": [],
                    "volumes": [],
                },
            },
        },
    }

    user_template = {
        "metadata": {
            "namespace": "my-namespace",
            "labels": {
                "team": "my-team",
            },
        },
        "spec": {
            "replicas": 3,
            "template": {
                "spec": {
                    "nodeSelector": {
                        "disktype": "ssd",
                    },
                    "volumes": [
                        {"name": "user-volume"},
                    ],
                },
            },
        },
    }

    pipeline_additions = {
        "metadata": {
            "name": "my-deployment",
            "labels": {
                "pipeline": "my-pipeline",
            },
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {"name": "streaming-consumer", "image": "my-image"},
                    ],
                    "volumes": [
                        {"name": "pipeline-config"},
                    ],
                },
            },
        },
    }

    # Two-stage merge
    stage1 = deepmerge(base, user_template)
    result = deepmerge(stage1, pipeline_additions)

    assert result == {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "my-deployment",
            "namespace": "my-namespace",
            "labels": {
                "team": "my-team",
                "pipeline": "my-pipeline",
            },
        },
        "spec": {
            "replicas": 3,
            "template": {
                "spec": {
                    "nodeSelector": {
                        "disktype": "ssd",
                    },
                    "containers": [
                        {"name": "streaming-consumer", "image": "my-image"},
                    ],
                    "volumes": [
                        {"name": "user-volume"},
                        {"name": "pipeline-config"},
                    ],
                },
            },
        },
    }

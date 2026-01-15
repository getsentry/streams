"""Dictionary merging utilities for Kubernetes manifest templates."""

import copy
from typing import Any


class TypeMismatchError(TypeError):
    """Raised when attempting to merge incompatible types in deepmerge."""

    pass


def deepmerge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Deep merge two dictionaries with specific semantics for Kubernetes manifests.

    Merge semantics:
    - Simple types (str, int, bool, None): override replaces base
    - Dictionaries: recursively merge
    - Lists: concatenate (append override elements to base)
    - Type mismatches (e.g., dict + list, dict + str): raises TypeMismatchError

    Returns:
        A new dictionary with merged values (base and override are not mutated)

    Raises:
        TypeMismatchError: When attempting to merge incompatible types
            (e.g., trying to merge a dict with a list, or a list with a string)

    Examples:
        >>> base = {"a": 1, "b": {"c": 2}}
        >>> override = {"b": {"d": 3}, "e": 4}
        >>> deepmerge(base, override)
        {'a': 1, 'b': {'c': 2, 'd': 3}, 'e': 4}

        >>> base = {"volumes": [{"name": "vol1"}]}
        >>> override = {"volumes": [{"name": "vol2"}]}
        >>> deepmerge(base, override)
        {'volumes': [{'name': 'vol1'}, {'name': 'vol2'}]}

        >>> base = {"key": {"nested": "value"}}
        >>> override = {"key": "string"}
        >>> deepmerge(base, override)
        Traceback (most recent call last):
            ...
        TypeMismatchError: Cannot merge key 'key': base type is dict but override type is str
    """
    result = copy.deepcopy(base)

    for key, override_value in override.items():
        if key not in result:
            result[key] = copy.deepcopy(override_value)
        else:
            base_value = result[key]

            # Both base and override have this key
            if isinstance(base_value, dict) and isinstance(override_value, dict):
                result[key] = deepmerge(base_value, override_value)
            elif isinstance(base_value, list) and isinstance(override_value, list):
                result[key] = base_value + copy.deepcopy(override_value)
            elif type(base_value) is not type(override_value):
                raise TypeMismatchError(
                    f"Cannot merge key '{key}': base type is {type(base_value)} but override type is {type(override_value)}"
                )
            else:
                result[key] = copy.deepcopy(override_value)

    return result

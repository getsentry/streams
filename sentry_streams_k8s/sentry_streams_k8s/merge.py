"""Dictionary merging utilities for Kubernetes manifest templates."""

import copy
from typing import Any


class TypeMismatchError(TypeError):
    """Raised when attempting to merge incompatible types in deepmerge."""

    pass


class ScalarOverwriteError(ValueError):
    """Raised when attempting to overwrite a scalar value during deepmerge."""

    pass


def deepmerge(
    base: dict[str, Any],
    override: dict[str, Any],
    fail_on_scalar_overwrite: bool = False,
    _path: list[str] | None = None,
) -> dict[str, Any]:
    """
    Deep merge two dictionaries with specific semantics for Kubernetes manifests.

    Merge semantics:
    - Simple types (str, int, bool, None): override replaces base
    - Dictionaries: recursively merge
    - Lists: concatenate (append override elements to base)
    - Type mismatches (e.g., dict + list, dict + str): raises TypeMismatchError


    Raises:
        TypeMismatchError: When attempting to merge incompatible types
            (e.g., trying to merge a dict with a list, or a list with a string)
        ScalarOverwriteError: When fail_on_scalar_overwrite is True and attempting
            to overwrite a scalar value with a different scalar value

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
    if _path is None:
        _path = []

    result = copy.deepcopy(base)

    for key, override_value in override.items():
        current_path = _path + [key]
        path_str = ".".join(current_path)

        if key not in result:
            result[key] = copy.deepcopy(override_value)
        else:
            base_value = result[key]

            # Both base and override have this key
            if isinstance(base_value, dict) and isinstance(override_value, dict):
                result[key] = deepmerge(
                    base_value,
                    override_value,
                    fail_on_scalar_overwrite=fail_on_scalar_overwrite,
                    _path=current_path,
                )
            elif isinstance(base_value, list) and isinstance(override_value, list):
                result[key] = base_value + copy.deepcopy(override_value)
            elif type(base_value) is not type(override_value):
                raise TypeMismatchError(
                    f"Cannot merge key '{key}': base type is {type(base_value)} but override type is {type(override_value)}"
                )
            else:
                # Scalar to scalar replacement
                if fail_on_scalar_overwrite and base_value != override_value:
                    raise ScalarOverwriteError(
                        f"Cannot overwrite scalar at '{path_str}': would change {base_value!r} to {override_value!r}"
                    )
                result[key] = copy.deepcopy(override_value)

    return result

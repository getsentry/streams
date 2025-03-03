import importlib.util as utils
import sys
from importlib import import_module
from types import ModuleType


def get_module(mod: str) -> ModuleType:
    if mod in sys.modules:
        module = sys.modules[mod]

    elif utils.find_spec(mod) is not None:
        module = import_module(mod)

    else:
        raise ImportError(f"Can't find module {mod}")

    return module

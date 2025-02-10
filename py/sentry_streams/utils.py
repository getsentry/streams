import importlib.util as utils
import sys
from types import ModuleType


def get_module(mod: str) -> ModuleType:
    if mod in sys.modules:
        module = sys.modules[mod]

    elif (spec := utils.find_spec(mod)) is not None:
        module = utils.module_from_spec(spec)

    else:
        raise ImportError(f"Can't find module {mod}")

    return module

import importlib.util as utils
import sys
from enum import Enum
from importlib import import_module
from typing import Union, assert_never, cast

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter


class AdapterType(Enum):
    DUMMY = "dummy"


def load_adapter(adapter_type: Union[AdapterType, str], config: PipelineConfig) -> StreamAdapter:
    """
    Loads a StreamAdapter to run a pipeline.

    Adapters can be loaded by statically identifying them or dynamically
    by providing the path to the module.

    Static adapters are the recommended way, though, at present, we still
    have a pyFlink library that requires java to be installed to build the
    wheels on python > 3.11.
    Requiring Java in the development environment is not ideal so we will
    move the Flink adapter out.

    If we manage to import pyFlink without requiring Java or move away from
    Flink, the dynamic loading will not be needed.

    #TODO: Actually move out Flink otherwise everything stated above makes
    # no sense.
    """
    if isinstance(adapter_type, AdapterType):
        if adapter_type == AdapterType.DUMMY:
            from sentry_streams.dummy.dummy_adapter import DummyAdapter

            return DummyAdapter.build(config)
        else:
            assert_never()
    else:
        mod, cls = adapter_type.rsplit(".", 1)

        try:
            if mod in sys.modules:
                module = sys.modules[mod]

            elif utils.find_spec(mod) is not None:
                module = import_module(mod)

            else:
                raise ImportError(f"Can't find module {mod}")

        except ImportError:
            raise

        imported_cls = getattr(module, cls)
        return cast(StreamAdapter, imported_cls.build(config))

    raise ValueError("Dynamic adapter loader is not supported yet.")

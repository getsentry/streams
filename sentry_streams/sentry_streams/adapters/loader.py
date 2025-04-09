import importlib.util as utils
import sys
from importlib import import_module
from typing import Optional, TypeVar, cast

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter

Stream = TypeVar("Stream")
Sink = TypeVar("Sink")


def load_adapter(
    adapter_type: str, config: PipelineConfig, segment_id: Optional[int] = None
) -> StreamAdapter[Stream, Sink]:
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
    if segment_id is not None:
        config = {"env": config["env"], **config["pipeline"]["segments"][segment_id]}

    if adapter_type == "dummy":
        from sentry_streams.dummy.dummy_adapter import DummyAdapter

        return DummyAdapter.build(config)
    if adapter_type == "arroyo":
        from sentry_streams.adapters.arroyo import ArroyoAdapter

        # TODO: The runner deserves a refactoring. The way it is designed
        # it is impossible to create adapters that materialize the type of
        # the `Stream` generic and be able to return a generic here. In order
        # to make it possible the generic would have to be covariant. But we
        # use the generic attribute both as a parameter and return value.
        # So we need to move responsibilities from iterate_edges to the adapter
        # to have a sane type structure.
        return cast(StreamAdapter[Stream, Sink], ArroyoAdapter.build(config))

    if adapter_type == "rust_arroyo":
        from sentry_streams.adapters.arroyo import RustArroyoAdapter

        # TODO: Fix this type as above.
        return cast(StreamAdapter[Stream, Sink], RustArroyoAdapter.build(config))
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

        return cast(StreamAdapter[Stream, Sink], imported_cls.build(config))

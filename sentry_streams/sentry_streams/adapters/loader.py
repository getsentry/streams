from enum import Enum
from typing import Union, assert_never

from sentry_streams.adapters.stream_adapter import PipelineConfig, StreamAdapter


class AdapterType(Enum):
    FLINK = "flink"
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
        if adapter_type == AdapterType.FLINK:
            from sentry_streams.flink.flink_adapter import FlinkAdapter

            return FlinkAdapter.build(config)
        elif adapter_type == AdapterType.DUMMY:
            from sentry_streams.dummy.dummy_adapter import DummyAdapter

            return DummyAdapter.build(config)
        else:
            assert_never()

    raise ValueError("Dynamic adapter loader is not supported yet.")

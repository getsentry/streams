from typing import Tuple

from sentry_streams.adapters.arroyo.rust_step import Committable, SingleMessageOperatorDelegate
from sentry_streams.pipeline.message import PipelineMessage, RustMessage


class FakeOperatorDelegate(SingleMessageOperatorDelegate):
    def __init__(self) -> None:
        pass

    def _process_message(self, msg: RustMessage, committable: Committable) -> RustMessage | None:
        return msg


class FakeOperatorDelegateFactory:
    def build(self):
        return FakeOperatorDelegate()

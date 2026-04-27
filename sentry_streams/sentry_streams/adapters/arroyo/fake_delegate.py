from copy import deepcopy
from typing import Tuple

from sentry_streams.adapters.arroyo.rust_step import Committable, SingleMessageOperatorDelegate
from sentry_streams.examples.transform_metrics import do_nothing
from sentry_streams.pipeline.message import PipelineMessage, PyMessage, RustMessage


class FakeOperatorDelegate(SingleMessageOperatorDelegate):
    def _process_message(self, msg: RustMessage, committable: Committable) -> RustMessage | None:
        copy = deepcopy(msg.payload)
        payload = {"size": len(copy)}

        return PyMessage(payload, msg.headers, msg.timestamp, msg.schema).to_inner()


class FakeOperatorDelegateFactory:
    def build(self) -> FakeOperatorDelegate:
        return FakeOperatorDelegate()

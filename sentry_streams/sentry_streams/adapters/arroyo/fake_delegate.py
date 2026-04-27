from copy import deepcopy
from typing import Iterable, MutableSequence, Set, Tuple

from sentry_streams.adapters.arroyo.rust_step import (
    Committable,
    RustOperatorDelegate,
    SingleMessageOperatorDelegate,
)
from sentry_streams.examples.transform_metrics import do_nothing
from sentry_streams.pipeline.message import PipelineMessage, PyMessage, RustMessage
from sentry_streams.rust_streams import PyWatermark


class FakeOperatorDelegate(RustOperatorDelegate):
    def __init__(
        self,
    ) -> None:
        self.__watermarks: Set[PyWatermark] = set()
        # globbed_committable is the combined committable of all messages polled
        # from the OutputRetriever
        self.__globbed_committable: Committable = {}
        self.__messages: MutableSequence[Tuple[PipelineMessage, Committable]] = []

    def __should_send_watermark(self, watermark: PyWatermark) -> bool:
        """
        Returns True if:
        - all partitions in `watermark`'s payload are present in `self.__globbed_committable`
        - all offsets for those partitions in `watermark` are less than or equal to the corresponding
        offsets in `self.__globbed_committable`
        """
        try:
            for partition, offset in watermark.committable.items():

                if self.__globbed_committable[partition] < offset:
                    return False
        except KeyError:
            return False
        return True

    def __yield_messages(self) -> MutableSequence[Tuple[PipelineMessage, Committable]]:
        """
        Yields messages polled from the OutputRetriever, as well as any stored watermarks that
        can be sent after each message.
        As currently implemented, watermarks can move backwards in message order (as watermarks
        are always yielded after messages), but can never move earlier (meaning we don't commit messages before
        they're finished processing).

        Currently, if no new messages are received, watermarks will not be sent further down the pipeline from a delegate.
        """
        # TODO: ensure watermarks leave the delegate in the same order they entered it
        ret: MutableSequence[Tuple[PipelineMessage, Committable]] = []
        for message, committable in self.__messages:
            ret.append((message, committable))
            self.__globbed_committable.update(committable)
            watermarks = self.__watermarks.copy()
            for wm in watermarks:
                if self.__should_send_watermark(wm):
                    ret.append((wm, wm.committable))
                    self.__watermarks.remove(wm)
        self.__messages = []
        return ret

    def submit(self, message: PipelineMessage, committable: Committable) -> None:
        if isinstance(message, PyWatermark):
            self.__watermarks.add(message)
        else:
            processed = PyMessage(
                {"size": len(message.payload)}, message.headers, message.timestamp, message.schema
            )
            self.__messages.append((processed.to_inner(), committable))

    def poll(self) -> Iterable[Tuple[PipelineMessage, Committable]]:
        return self.__yield_messages()

    def flush(
        self, timeout: float | None = None
    ) -> MutableSequence[Tuple[PipelineMessage, Committable]]:
        return self.__yield_messages()


class FakeOperatorDelegateFactory:
    def build(self) -> FakeOperatorDelegate:
        return FakeOperatorDelegate()

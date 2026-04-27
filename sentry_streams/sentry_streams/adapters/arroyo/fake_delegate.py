from copy import deepcopy
import time
from typing import Any, Iterable, MutableSequence, Sequence, Set, Tuple

from sentry_streams.adapters.arroyo.rust_step import (
    Committable,
    RustOperatorDelegate,
    SingleMessageOperatorDelegate,
)
from sentry_streams.examples.transform_metrics import do_nothing
from sentry_streams.metrics.stats import get_stats
from sentry_streams.pipeline.message import PipelineMessage, PyMessage, RustMessage
from sentry_streams.rust_streams import PyWatermark


class Messagebatch:

    def __init__(self, size: int, time_sec: float) -> None:
        self.size = size
        self.time_sec = time_sec
        self.messages: MutableSequence[Any] = []
        self.committable: Committable = {}
        self._last_flush_time: float | None = None

    def add_message(self, message: RustMessage, committable: Committable) -> None:
        if self._last_flush_time is None:
            self._last_flush_time = time.time()
        self.messages.append(message.payload)
        self.committable.update(committable)

    def flush(self) -> Tuple[Sequence[Any], Committable] | None:
        if self._last_flush_time is None:
            self._last_flush_time = time.time()
        if len(self.messages) < self.size or time.time() - self._last_flush_time < self.time_sec:
            return None

        messages = self.messages
        self.messages = []
        self.committable = {}
        self._last_flush_time = time.time()
        return messages, self.committable


class FakeOperatorDelegate(RustOperatorDelegate):
    def __init__(
        self,
    ) -> None:
        self.__watermarks: Set[PyWatermark] = set()
        # globbed_committable is the combined committable of all messages polled
        # from the OutputRetriever
        self.__globbed_committable: Committable = {}
        self.__batch = Messagebatch(size=50000, time_sec=3.0)
        self.stats = get_stats()

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
        flushed = self.__batch.flush()

        if not flushed:
            return ret

        msg = PyMessage(flushed[0], [], time.time(), None).to_inner()
        ret.append((msg, flushed[1]))
        self.__globbed_committable.update(flushed[1])
        watermarks = self.__watermarks.copy()
        for wm in watermarks:
            if self.__should_send_watermark(wm):
                ret.append((wm, wm.committable))
                self.__watermarks.remove(wm)
        return ret

    def submit(self, message: PipelineMessage, committable: Committable) -> None:
        self.stats.step_exec("fake batch")
        if isinstance(message, PyWatermark):
            self.__watermarks.add(message)
        else:
            self.__batch.add_message(message, committable)

    def poll(self) -> Iterable[Tuple[PipelineMessage, Committable]]:
        return self.__yield_messages()

    def flush(
        self, timeout: float | None = None
    ) -> MutableSequence[Tuple[PipelineMessage, Committable]]:
        return self.__yield_messages()


class FakeOperatorDelegateFactory:
    def build(self) -> FakeOperatorDelegate:
        return FakeOperatorDelegate()

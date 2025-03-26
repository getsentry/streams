import logging
import math
from typing import Any, Callable, Generic, Optional, TypeVar, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.reduce import Reduce
from arroyo.types import FilteredPayload, Message

from sentry_streams.pipeline.function_template import Accumulator

logger = logging.getLogger(__name__)


TPayload = TypeVar("TPayload")
TResult = TypeVar("TResult")


def create_reduce(window_size: Any, arroyo_acc: Any, acc: Any, next_step: Any) -> Reduce[Any, Any]:

    match window_size:
        case int():
            final_acc = arroyo_acc(acc)

            return Reduce(
                window_size, float("inf"), final_acc.accumulator, final_acc.initial_value, next_step
            )

        case _:
            raise TypeError(
                f"({type(window_size)} is not a supported MeasurementUnit type combination for SlidingWindow"
            )


class WindowedReduce(
    ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
):
    def __init__(
        self,
        window_size: Union[int, float],
        window_slide: Union[int, float],
        arroyo_acc: Any,
        acc: Callable[[], Accumulator[Any, Any]],
        next_step: ProcessingStrategy[TResult],
    ) -> None:

        # create the structure to keep track of all windows
        # initialize N windows

        # generalize this depending on whether its by time or by count
        window_count = math.ceil(window_size / window_slide)

        # maintain a list of Reduces

        self.message_count = 0
        self.window_count = window_count
        self.window_size = window_size
        self.window_slide = window_slide
        self.window_start_ind = 0

        # 3 arrays
        self.windows = [
            create_reduce(window_size, arroyo_acc, acc, next_step) for _ in range(self.window_count)
        ]
        self.start_inds = [i * self.window_slide for i in range(self.window_count)]
        self.last_seen = [0 for _ in range(self.window_count)]

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        # figure out how a message gets added to the tracked windows

        # each message should fall into (potentially) multiple windows
        # count

        # [0, 1, 2] -- new start index is always last ind + slide -- refresh this as soon as we complete a window
        #       [2, 3, 4]
        #              (((( [4, 5, 6] ))))

        # [0, 1, 2, 3, 4]
        #     [1, 2, 3, 4, 5]
        #         [2, 3, 4, 5, 6]
        #             [3, 4, 5, 6, 7]
        #                [4, 5, 6, 7, 8]

        # array of starting indices = [...]
        # matching array of Reduces
        # matching array of last seen

        logger.info(f"message id {self.message_count}")
        # first_window_ind = self.message_count // self.window_size
        # total_messages_in_windows = self.window_count * self.window_size
        # Find the index of the message

        for i in range(self.window_count):

            start_ind = self.start_inds[i]
            logger.info(start_ind)

            if self.message_count >= start_ind:
                if self.message_count <= (start_ind + self.window_size - 1):
                    logger.info(
                        f"message index {self.message_count}, cur_start {start_ind}, window_id {i}"
                    )
                    self.windows[i].submit(message)
                    self.last_seen[i] = self.message_count

                    # time to move the window forward
                    if self.last_seen[i] == self.start_inds[i] + self.window_size - 1:
                        self.start_inds[i] = self.last_seen[i] + self.window_slide
                        self.last_seen[i] = 0

        self.message_count += 1

    def poll(self) -> None:
        for i in range(self.window_count):
            self.windows[i].poll()

    def close(self) -> None:
        for i in range(self.window_count):
            self.windows[i].close()

    def terminate(self) -> None:
        for i in range(self.window_count):
            self.windows[i].terminate()

    def join(self, timeout: Optional[float] = None) -> None:
        for i in range(self.window_count):
            self.windows[i].join(timeout)

# import logging
# import math
# import time
# from typing import Any, Callable, Generic, Optional, TypeVar, Union

# from arroyo.processing.strategies.abstract import ProcessingStrategy
# from arroyo.processing.strategies.reduce import Reduce
# from arroyo.types import FilteredPayload, Message

# from sentry_streams.pipeline.function_template import Accumulator

# logger = logging.getLogger(__name__)


# TPayload = TypeVar("TPayload")
# TResult = TypeVar("TResult")


# def create_reduce(
#     window_size: Union[int, float], arroyo_acc: Any, acc: Any, next_step: Any
# ) -> Reduce[Any, Any]:

#     match window_size:
#         case int():
#             final_acc = arroyo_acc(acc)

#             return Reduce(
#                 window_size, float("inf"), final_acc.accumulator, final_acc.initial_value, next_step
#             )

#         case float():
#             final_acc = arroyo_acc(acc)

#             # Unfortunate
#             return Reduce(
#                 1000000000, window_size, final_acc.accumulator, final_acc.initial_value, next_step
#             )


# class WindowedReduce(
#     ProcessingStrategy[Union[FilteredPayload, TPayload]], Generic[TPayload, TResult]
# ):
#     def __init__(
#         self,
#         window_size: Union[int, float],
#         window_slide: Union[int, float],
#         arroyo_acc: Any,
#         acc: Callable[[], Accumulator[Any, Any]],
#         next_step: ProcessingStrategy[TResult],
#     ) -> None:

#         window_count = math.ceil(window_size / window_slide)
#         self.message_count = None
#         self.window_count = window_count
#         self.window_size = window_size
#         self.window_slide = window_slide

#         self.windows = [
#             create_reduce(window_size, arroyo_acc, acc, next_step) for _ in range(self.window_count)
#         ]

#         # initializers
#         if isinstance(self.window_size, int):
#             self.message_count = 1

#         else:
#             self.start_time = time.time()

#         self.window_loop = self.window_count * self.window_slide

#         self.boundaries = []
#         for i in range(self.window_count):
#             greater = (self.window_size + i * self.window_slide) % self.window_loop

#             if self.message_count:
#                 less = i * self.window_slide + 1
#             else:
#                 less = i * self.window_slide

#             self.boundaries.append((greater, less))

#     def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:

#         # count based
#         if self.message_count:
#             msg_id = self.message_count % self.window_loop

#         # time based
#         else:
#             cur_time = time.time() - self.start_time
#             msg_id = cur_time % self.window_loop

#         message_tracker = self.message_count if self.message_count else cur_time

#         # actually submit to the right Reduces
#         for i in range(len(self.windows)):

#             if message_tracker > i * self.window_slide:
#                 if self.boundaries[i][0] < self.boundaries[i][1]:
#                     if not (msg_id > self.boundaries[i][0] and msg_id < self.boundaries[i][1]):
#                         logger.info(f"message index {message_tracker}, window_id {i}")
#                         self.windows[i].submit(message)

#                 else:
#                     if not (msg_id > self.boundaries[i][0] or msg_id < self.boundaries[i][1]):
#                         logger.info(f"message index {message_tracker}, window_id {i}")
#                         self.windows[i].submit(message)

#         if self.message_count:
#             self.message_count += 1

#     def poll(self) -> None:
#         for i in range(self.window_count):
#             self.windows[i].poll()

#     def close(self) -> None:
#         for i in range(self.window_count):
#             self.windows[i].close()

#     def terminate(self) -> None:
#         for i in range(self.window_count):
#             self.windows[i].terminate()

#     def join(self, timeout: Optional[float] = None) -> None:
#         for i in range(self.window_count):
#             self.windows[i].join(timeout)

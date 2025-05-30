import time
from typing import Optional, TypeVar, Union, cast

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Value

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.message import PyMessage as StreamsMessage

TPayload = TypeVar("TPayload")


class MessageWrapper(ProcessingStrategy[Union[FilteredPayload, TPayload]]):
    """
    Custom processing strategy which can wrap payloads coming from the previous step
    into a Message. In the case that the previous step already forwards a Message
    or a FilteredPayload, this strategy will simply forward that as well to the
    next step.
    """

    def __init__(
        self,
        route: Route,
        next_step: ProcessingStrategy[Union[FilteredPayload, RoutedValue]],
    ) -> None:
        self.__next_step = next_step
        self.__route = route

    def submit(self, message: Message[Union[FilteredPayload, TPayload]]) -> None:
        now = time.time()
        if not isinstance(message.payload, FilteredPayload):

            if isinstance(message.payload, RoutedValue):
                # No need to wrap a StreamsMessage in StreamsMessage() again
                # This case occurs when prior strategy is forwarding a message that belongs on a separate route
                assert isinstance(message.payload.payload, StreamsMessage)
                self.__next_step.submit(cast(Message[Union[FilteredPayload, RoutedValue]], message))
            else:
                msg = StreamsMessage(message.payload, [], now, None)

                routed_msg: Message[RoutedValue] = Message(
                    Value(
                        committable=message.value.committable,
                        payload=RoutedValue(self.__route, msg),
                    )
                )
                self.__next_step.submit(routed_msg)

        else:
            self.__next_step.submit(cast(Message[FilteredPayload], message))

    def poll(self) -> None:
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

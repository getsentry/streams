from typing import Optional, Sequence, Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Value

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue


class Broadcaster(ProcessingStrategy[Union[FilteredPayload, RoutedValue]]):
    """
    Custom processing strategy which duplicates a message once per downstream branch
    and updates the waypoints of each copy to correspond to one of the branches.
    Duplicates keep the timestamp from the original message.
    """

    def __init__(
        self,
        route: Route,
        downstream_branches: Sequence[str],
        next_step: ProcessingStrategy[Union[FilteredPayload, RoutedValue]],
    ) -> None:
        self.__next_step = next_step
        self.__route = route
        self.__downstream_branches = downstream_branches

    def submit(self, message: Message[Union[FilteredPayload, RoutedValue]]) -> None:
        message_payload = message.value.payload
        if isinstance(message_payload, RoutedValue) and message_payload.route == self.__route:
            for branch in self.__downstream_branches:
                msg_copy = Message(
                    Value(
                        committable=message.value.committable,
                        timestamp=message.value.timestamp,
                        payload=RoutedValue(
                            route=Route(
                                source=message_payload.route.source,
                                waypoints=[*message_payload.route.waypoints, *[branch]],
                            ),
                            payload=message_payload.payload,
                        ),
                    )
                )
                self.__next_step.submit(msg_copy)
        else:
            self.__next_step.submit(message)

    def poll(self) -> None:
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

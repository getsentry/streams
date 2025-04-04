from collections import deque
from copy import deepcopy
from time import time
from typing import Deque, Optional, Sequence, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
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
        # If we get MessageRejected from the next step, we put the pending messages here
        self.__pending: Deque[Message[RoutedValue]] = deque()

    def __flush_pending(self) -> None:
        while self.__pending:
            try:
                message = self.__pending[0]
                self.__next_step.submit(message)
                self.__pending.popleft()
            except MessageRejected:
                break

    def submit(self, message: Message[Union[FilteredPayload, RoutedValue]]) -> None:
        if self.__pending:
            raise MessageRejected

        # if any downstream branch raises MessageRejected, we need to propagate
        # the error back to the previous step
        raise_message_rejected = False
        message_payload = message.value.payload
        if isinstance(message_payload, RoutedValue) and message_payload.route == self.__route:
            for branch in self.__downstream_branches:
                msg_copy = deepcopy(message)
                copy_value = msg_copy.value
                copy_payload = cast(RoutedValue, copy_value.payload)
                routed_copy = Message(
                    Value(
                        committable=copy_value.committable,
                        timestamp=copy_value.timestamp,
                        payload=RoutedValue(
                            route=Route(
                                source=copy_payload.route.source,
                                waypoints=[*copy_payload.route.waypoints, branch],
                            ),
                            payload=copy_payload.payload,
                        ),
                    )
                )
                try:
                    self.__next_step.submit(routed_copy)
                except MessageRejected:
                    self.__pending.append(routed_copy)
                    raise_message_rejected = True
            if raise_message_rejected:
                raise MessageRejected()
        else:
            self.__next_step.submit(message)

    def poll(self) -> None:
        self.__flush_pending()
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time() + timeout if timeout is not None else None
        while deadline is None or time() < deadline:
            if self.__pending:
                self.__flush_pending()
            else:
                break

        self.__next_step.close()
        self.__next_step.join(timeout=max(deadline - time(), 0) if deadline is not None else None)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

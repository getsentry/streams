from copy import deepcopy
from dataclasses import dataclass
from time import time
from typing import Mapping, MutableMapping, Optional, Sequence, Union, cast

from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import FilteredPayload, Message, Partition, Value

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue


@dataclass(eq=True)
class Messageidentifier:
    route: Route
    committable: Mapping[Partition, int]

    def __hash__(self) -> int:
        return hash((self.route.source, *self.route.waypoints, str(self.committable)))


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
        self.__failed_msgs: MutableMapping[Messageidentifier, Message[RoutedValue]] = {}

    def __retry_failed_msg(
        self, msg: Message[RoutedValue], msg_identifier: Messageidentifier
    ) -> None:
        """
        Retries a message which got MessageRejected previously.
        """
        self.__next_step.submit(msg)
        del self.__failed_msgs[msg_identifier]

    def __submit_to_next_step(
        self, msg: Message[RoutedValue], msg_identifier: Messageidentifier
    ) -> None:
        """
        Submits to the next step.
        If the next step raises MessageRejected, this records the failed message
        and raises MessageRejected to propagate the error.
        """
        try:
            self.__next_step.submit(msg)
        except MessageRejected:
            self.__failed_msgs[msg_identifier] = msg
            raise MessageRejected()

    def __handle_submit(self, msg: Message[RoutedValue]) -> None:
        msg_identifier = Messageidentifier(
            route=msg.value.payload.route,
            committable=msg.value.committable,
        )
        if msg_identifier in self.__failed_msgs:
            self.__retry_failed_msg(msg, msg_identifier)
        else:
            self.__submit_to_next_step(msg, msg_identifier)

    def submit(self, message: Message[Union[FilteredPayload, RoutedValue]]) -> None:
        if (
            isinstance(message.value.payload, RoutedValue)
            and message.value.payload.route == self.__route
        ):
            for branch in self.__downstream_branches:
                routed = cast(Message[RoutedValue], message)
                routed_msg = routed.payload
                routed_copy = Message(
                    Value(
                        committable=deepcopy(message.value.committable),
                        timestamp=message.value.timestamp,
                        payload=RoutedValue(
                            route=Route(
                                source=routed_msg.route.source,
                                waypoints=[*deepcopy(routed_msg.route.waypoints), branch],
                            ),
                            payload=routed_msg.payload.deepcopy(),
                        ),
                    )
                )
                self.__handle_submit(routed_copy)
        else:
            # If message isn't a RoutedValue, just submit it to the next step
            self.__next_step.submit(message)

    def poll(self) -> None:
        failed_msgs = {id: msg for id, msg in self.__failed_msgs.items()}
        for id, msg in failed_msgs.items():
            self.__retry_failed_msg(msg, id)
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        deadline = time() + timeout if timeout is not None else None
        while deadline is None or time() < deadline:
            if self.__failed_msgs:
                for id, msg in self.__failed_msgs.items():
                    self.__retry_failed_msg(msg, id)
            else:
                break

        self.__next_step.close()
        self.__next_step.join(timeout=max(deadline - time(), 0) if deadline is not None else None)

    def close(self) -> None:
        self.__next_step.close()

    def terminate(self) -> None:
        self.__next_step.terminate()

from typing import Any, Optional, Union

from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.types import FilteredPayload, Message

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue


class Forwarder(ProcessingStrategy[Any]):
    """
    Either produces an incoming message to a given Producer if the Route of the message
    matches this strategy's Route, or forwards the message to the next strategy provided.

    `producer_override` is used in tests to pass a mock producer.
    """

    def __init__(
        self,
        route: Route,
        produce_step: Produce[Any],
        next_step: ProcessingStrategy[Union[FilteredPayload, RoutedValue]],
    ) -> None:
        self.__produce_step = produce_step
        self.__next_step = next_step
        self.__route = route

    def submit(self, message: Message[Union[FilteredPayload, RoutedValue]]) -> None:
        message_payload = message.value.payload
        if isinstance(message_payload, RoutedValue) and message_payload.route == self.__route:
            kafka_payload = message.value.replace(
                KafkaPayload(None, str(message_payload.payload).encode("utf-8"), [])
            )
            self.__produce_step.submit(Message(kafka_payload))
        else:
            self.__next_step.submit(message)

    def poll(self) -> None:
        self.__produce_step.poll()
        self.__next_step.poll()

    def join(self, timeout: Optional[float] = None) -> None:
        self.__produce_step.join(timeout)
        self.__next_step.join(timeout)

    def close(self) -> None:
        self.__produce_step.close()
        self.__next_step.close()

    def terminate(self) -> None:
        self.__produce_step.terminate()
        self.__next_step.terminate()

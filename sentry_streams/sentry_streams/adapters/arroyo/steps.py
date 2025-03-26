import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Union

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import FilteredPayload, Message, Topic

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.translator import build_arroyo_windowed_reduce
from sentry_streams.pipeline.pipeline import Filter, Map, Reduce

logger = logging.getLogger(__name__)


@dataclass
class ArroyoStep(ABC):
    """
    Represents a primitive in Arroyo. This is the intermediate representation
    the Arroyo adapter uses to build the application in reverse order with
    respect to how the steps are wired up in the pipeline.

    Arroyo consumers have to be built wiring up strategies from the end to
    the beginning. The streaming pipeline is defined from the beginning to
    the end, so when building the Arroyo application we need to reverse the
    order of the steps.
    """

    route: Route

    @abstractmethod
    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        raise NotImplementedError


def process_message(
    route: Route,
    message: Message[Union[FilteredPayload, RoutedValue]],
    process_routed_payload: Callable[[RoutedValue], Union[FilteredPayload, RoutedValue]],
) -> Union[FilteredPayload, RoutedValue]:
    """
    General logic to manage a routed message in RunTask steps.
    If forwards FilteredMessages and messages for a different route as they are.
    It sends the messages that match the `route` parameter to the
    `process_routed_payload` function.
    """
    payload = message.payload
    if isinstance(payload, FilteredPayload):
        return payload

    if payload.route != route:
        return payload

    return process_routed_payload(payload)


@dataclass
class MapStep(ArroyoStep):
    """
    Represents a Map transformation in the streaming pipeline.
    This translates to a RunTask step in arroyo where a function
    is provided to transform the message payload into a different
    one.
    """

    pipeline_step: Map

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def transformer(
            message: Message[Union[FilteredPayload, RoutedValue]],
        ) -> Union[FilteredPayload, RoutedValue]:
            return process_message(
                self.route,
                message,
                lambda payload: RoutedValue(
                    route=payload.route,
                    payload=self.pipeline_step.resolved_function(payload.payload),
                ),
            )

        return RunTask(
            transformer,
            next,
        )


@dataclass
class FilterStep(ArroyoStep):
    """
    Represent a Map transformation in the streaming pipeline.
    This translates to a RunTask step in arroyo where a function
    is provided to transform the message payload into a different
    one.
    """

    pipeline_step: Filter

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def transformer(
            message: Message[Union[FilteredPayload, RoutedValue]],
        ) -> Union[FilteredPayload, RoutedValue]:
            return process_message(
                self.route,
                message,
                lambda payload: (
                    payload
                    if self.pipeline_step.resolved_function(payload.payload)
                    else FilteredPayload()
                ),
            )

        return RunTask(
            transformer,
            next,
        )


@dataclass
class StreamSinkStep(ArroyoStep):
    """
    Represents an Arroyo producer. This is mapped from a StreamSink in the pipeline.

    It filters out messages for the route not specified on this step and unpacks
    the routed message into the original Arroyo payload.
    """

    producer: Producer[Any]
    topic_name: str

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def extract_value(message: Message[Union[FilteredPayload, RoutedValue]]) -> Any:
            message_payload = message.value.payload
            if isinstance(message_payload, RoutedValue) and message_payload.route == self.route:
                return KafkaPayload(None, str(message_payload.payload).encode("utf-8"), [])
            else:
                return FilteredPayload()

        return RunTask(
            extract_value,
            Produce(self.producer, Topic(self.topic_name), next),
        )


@dataclass
class ReduceStep(ArroyoStep):

    pipeline_step: Reduce[Any, Any, Any]

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:

        logger.info(next)

        windowed_reduce: ProcessingStrategy[Union[FilteredPayload, RoutedValue]] = (
            build_arroyo_windowed_reduce(
                self.pipeline_step.windowing, self.pipeline_step.aggregate_fn, next
            )
        )

        (windowed_reduce)
        return windowed_reduce


# 2025-03-25 17:24:37 - INFO - BrokerValue(_BrokerValue__payload=Rou│
# tedValue(route=Route(source='myinput', waypoints=[]), payload={'te│
# st': 'hello world', 'type': 'event'}), partition=Partition(topic=T│
# opic(name='events'), index=2), offset=39, timestamp=datetime.datet│
# ime(2025, 3, 26, 0, 24, 37, 81000))

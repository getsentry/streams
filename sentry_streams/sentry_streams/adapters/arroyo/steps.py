from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Union

from arroyo.backends.abstract import Producer
from arroyo.backends.kafka.consumer import KafkaPayload
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import (
    FilteredPayload,
    Message,
    Topic,
)

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.pipeline import Filter, Map, Router, RoutingFuncReturnType


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
    Represents a Filter transformation in the streaming pipeline.
    This translates to a RunTask step in arroyo where a message is filtered
    based on the result of a filter function.
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
class RouterStep(ArroyoStep):
    """
    Represents a Router which can direct a message to one of multiple
    downstream branches based on the output of a routing function.

    Since Arroyo has no concept of 'branches', this updates the `waypoints` list within
    a message's `Route` object based on the result of the routing function.
    """

    # TODO: fix generic typing here
    pipeline_step: Router[RoutingFuncReturnType]  # type: ignore

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def append_branch_to_waypoints(
            payload: RoutedValue,
        ) -> RoutedValue:
            routing_func = self.pipeline_step.routing_function
            routing_table = self.pipeline_step.routing_table
            branch = routing_func(payload.payload)
            branch_name = routing_table[branch].name
            payload.route.waypoints.append(branch_name)
            return payload

        def transformer(
            message: Message[Union[FilteredPayload, RoutedValue]],
        ) -> Union[FilteredPayload, RoutedValue]:
            return process_message(
                self.route,
                message,
                append_branch_to_waypoints,
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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from arroyo.backends.abstract import Producer
from arroyo.processing.strategies import Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import (
    FilteredPayload,
    Message,
    Topic,
)

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.pipeline import Filter, Map


@dataclass
class ArroyoStep(ABC):
    """
    Represent a primitive in Arroyo. This is the intermediate representation
    the Arroyo adapter uses to build the application in reverse order with
    respect to how the steps are wired up in the pipeline.
    """

    route: Route

    @abstractmethod
    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        raise NotImplementedError


class MapStep(ArroyoStep):
    """
    Represent a Map transformation in the streaming pipeline.
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
            payload = message.value.payload
            if isinstance(payload, FilteredPayload):
                return payload

            if payload.route != self.route:
                return payload

            return RoutedValue(
                route=payload.route, payload=self.pipeline_step.resolved_function(message.payload)
            )

        return RunTask(
            transformer,
            next,
        )


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
            payload = message.value.payload
            if isinstance(payload, FilteredPayload):
                return payload

            if payload.route != self.route:
                return payload

            should_forward = self.pipeline_step.resolved_function(payload)
            if should_forward:
                return payload
            else:
                return FilteredPayload()

        return RunTask(
            transformer,
            next,
        )


class KafkaSink(ArroyoStep):
    """
    Represents an Arroyo producer. This is mapped from a KafkaSink in the pipeline.
    """

    producer: Producer[Any]
    topic: Topic

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        return Produce(
            self.producer,
            self.topic,
            next,
        )

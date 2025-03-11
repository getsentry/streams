from abc import ABC, abstractmethod

from dataclasses import dataclass
from sentry_streams.adapters.arroyo.routes import RoutedValue, Route
from sentry_streams.pipeline.pipeline import Step, Map

from arroyo.types import (
    FilteredPayload,
    TStrategyPayload,
)
from typing import Union

from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTask


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
    Represents a map step in the Arroyo pipeline.
    """

    pipeline_step: Map

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]]
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def transformer(
            message: Union[FilteredPayload, RoutedValue],
        ) -> Union[FilteredPayload, RoutedValue]:
            if isinstance(message, FilteredPayload):
                return message

            if message.route == self.route:
                return message

            return self.pipeline_step.function(message.payload)

        return RunTask(
            transformer,
            next,
        )

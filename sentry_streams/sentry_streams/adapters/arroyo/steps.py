import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar, Union

from arroyo.backends.abstract import Producer
from arroyo.processing.strategies import CommitOffsets, Produce
from arroyo.processing.strategies.abstract import ProcessingStrategy
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import Commit, FilteredPayload, Message, Topic

from sentry_streams.adapters.arroyo.broadcaster import Broadcaster
from sentry_streams.adapters.arroyo.forwarder import Forwarder
from sentry_streams.adapters.arroyo.msg_wrapper import MessageWrapper
from sentry_streams.adapters.arroyo.reduce import build_arroyo_windowed_reduce
from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.pipeline.message import PyMessage as StreamsMessage
from sentry_streams.pipeline.pipeline import (
    Broadcast,
    Filter,
    Map,
    Reduce,
    Router,
    RoutingFuncReturnType,
)

logger = logging.getLogger(__name__)

TPayload = TypeVar("TPayload")


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

    We pass the `commit` param as SinkStep requires that to build the CommitOffsets step
    for its Producers.
    """

    route: Route

    @abstractmethod
    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        raise NotImplementedError


# NOTE: All of the steps below now:
# Perform operations / evaluations on a StreamsMessage
# Receive a raw payload, which is NOT wrapped in a StreamsMessage (as the user provides this type signature)
# Wrap the raw payload in a StreamsMessage to send it along to the next step


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
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def transformer(
            message: Message[Union[FilteredPayload, RoutedValue]],
        ) -> Union[FilteredPayload, RoutedValue]:
            return process_message(
                self.route,
                message,
                lambda routed_value: RoutedValue(
                    route=routed_value.route,
                    payload=StreamsMessage(
                        self.pipeline_step.resolved_function(routed_value.payload),
                        routed_value.payload.headers,
                        routed_value.payload.timestamp,
                        routed_value.payload.schema,
                    ),
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
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def transformer(
            message: Message[Union[FilteredPayload, RoutedValue]],
        ) -> Union[FilteredPayload, RoutedValue]:
            return process_message(
                self.route,
                message,
                lambda routed_value: (
                    RoutedValue(
                        self.route,
                        StreamsMessage(
                            routed_value.payload.payload,
                            routed_value.payload.headers,
                            routed_value.payload.timestamp,
                            routed_value.payload.schema,
                        ),
                    )
                    if self.pipeline_step.resolved_function(
                        routed_value.payload
                    )  # The function used for filtering takes in a StreamsMessage
                    else FilteredPayload()
                ),
            )

        return RunTask(
            transformer,
            next,
        )


@dataclass
class BroadcastStep(ArroyoStep):
    """
    BroadcastStep forwards a copy of an incoming  message downstream for each downstream branch,
    each copy having a Route matching one of the downstream branches.
    """

    pipeline_step: Broadcast

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        return Broadcaster(
            route=self.route,
            downstream_branches=[branch.name for branch in self.pipeline_step.routes],
            next_step=next,
        )


@dataclass
class RouterStep(ArroyoStep, Generic[RoutingFuncReturnType]):
    """
    Represents a Router which can direct a message to one of multiple
    downstream branches based on the output of a routing function.

    Since Arroyo has no concept of 'branches', this updates the `waypoints` list within
    a message's `Route` object based on the result of the routing function.
    """

    pipeline_step: Router[RoutingFuncReturnType]

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        def append_branch_to_waypoints(
            payload: RoutedValue,
        ) -> RoutedValue:
            routing_func = self.pipeline_step.routing_function
            routing_table = self.pipeline_step.routing_table
            result_branch = routing_func(payload.payload)
            result_branch_name = routing_table[result_branch].name
            payload.route.waypoints.append(result_branch_name)

            streams_msg = payload.payload
            msg = StreamsMessage(
                streams_msg.payload, streams_msg.headers, streams_msg.timestamp, streams_msg.schema
            )

            return RoutedValue(payload.route, msg)

        return RunTask(
            lambda message: process_message(
                self.route,
                message,
                append_branch_to_waypoints,
            ),
            next,
        )


@dataclass
class StreamSinkStep(ArroyoStep):
    """
    StreamSinkStep is backed by the Forwarder custom strategy, which either produces
    messages to a topic via an arroyo Producer or forwards messages to the next downstream
    step.
    This allows the use of multiple sinks, each at the end of a different branch of a Router step.
    """

    producer: Producer[Any]
    topic_name: str

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> Forwarder:
        return Forwarder(
            route=self.route,
            produce_step=Produce(self.producer, Topic(self.topic_name), CommitOffsets(commit)),
            next_step=next,
        )


@dataclass
class ReduceStep(ArroyoStep):
    pipeline_step: Reduce[Any, Any, Any]

    def build(
        self, next: ProcessingStrategy[Union[FilteredPayload, RoutedValue]], commit: Commit
    ) -> ProcessingStrategy[Union[FilteredPayload, RoutedValue]]:
        # TODO: Support group by keys

        msg_wrapper: ProcessingStrategy[Union[FilteredPayload, Any]] = MessageWrapper(
            self.route, next
        )  # Since the Reduce step produces aggregated raw payloads, we need to wrap them
        # in a Message (a StreamsMessage) to prepare it for the next step. The next step
        # expects a Message (a StreamsMessage).

        windowed_reduce: ProcessingStrategy[Union[FilteredPayload, Any]] = (
            build_arroyo_windowed_reduce(
                self.pipeline_step.windowing,
                self.pipeline_step.aggregate_fn,
                msg_wrapper,
                self.route,
            )
        )

        return windowed_reduce

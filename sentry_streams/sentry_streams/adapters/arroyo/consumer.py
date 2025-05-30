import logging
import time
from dataclasses import dataclass, field
from typing import Any, Mapping, MutableSequence, Optional, Tuple, Union

from arroyo.backends.kafka.consumer import Headers, KafkaPayload
from arroyo.processing.strategies import CommitOffsets
from arroyo.processing.strategies.abstract import (
    ProcessingStrategy,
    ProcessingStrategyFactory,
)
from arroyo.processing.strategies.run_task import RunTask
from arroyo.types import Commit, FilteredPayload, Message, Partition

from sentry_streams.adapters.arroyo.routes import Route, RoutedValue
from sentry_streams.adapters.arroyo.steps import ArroyoStep
from sentry_streams.pipeline.message import PyMessage as StreamsMessage

logger = logging.getLogger(__name__)


@dataclass
class ArroyoConsumer:
    """
    Intermediate representation of A single Arroyo application composed
    of multiple steps.

    Arroyo does not support branches. The streaming platform does, so
    we need to fake it in arroyo. This is done by making the branched
    pipeline a sequence and make all the messages go through all the
    steps for all the branches. The route is used to filter out the
    messages that do not belong to the branch.

    Building an Arroyo application is done from the last step to the
    first step. This is because every step references the following one.
    The streaming platform allows you to define the pipeline in sequence
    from the first to last step. This intermediate representation also
    collects the pipeline to be built in reverse order in Arroyo.
    """

    source: str
    stream_name: str
    schema: str
    header_filter: Optional[Tuple[str, bytes]] = None
    steps: MutableSequence[ArroyoStep] = field(default_factory=list)

    def add_step(self, step: ArroyoStep) -> None:
        """
        Append a pipeline step to the Arroyo consumer.
        """
        assert step.route.source == self.source
        self.steps.append(step)

    def build_strategy(self, commit: Commit) -> ProcessingStrategy[Any]:
        """
        Build the Arroyo consumer wiring up the steps in reverse order.

        It also adds a strategy at the beginning that makes each payload
        a RoutedValue that contains the route the message is supposed to
        follow.
        """

        def add_route(message: Message[KafkaPayload]) -> Union[FilteredPayload, RoutedValue]:
            headers: Headers = message.payload.headers
            if self.header_filter and self.header_filter not in headers:
                return FilteredPayload()

            broker_timestamp = message.timestamp.timestamp() if message.timestamp else time.time()
            value = message.payload.value

            return RoutedValue(
                route=Route(source=self.source, waypoints=[]),
                payload=StreamsMessage(
                    payload=value, headers=headers, timestamp=broker_timestamp, schema=self.schema
                ),
            )

        strategy: ProcessingStrategy[Any] = CommitOffsets(commit)
        for step in reversed(self.steps):
            strategy = step.build(strategy, commit)

        return RunTask(
            add_route,
            strategy,
        )


class ArroyoStreamingFactory(ProcessingStrategyFactory[Any]):
    def __init__(self, consumer: ArroyoConsumer) -> None:
        self.consumer = consumer

    def create_with_partitions(
        self,
        commit: Commit,
        _: Mapping[Partition, int],
    ) -> ProcessingStrategy[Any]:

        return self.consumer.build_strategy(commit)

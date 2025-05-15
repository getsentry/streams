from abc import ABC, abstractmethod
from typing import Generic, Iterable, TypeVar

from arroyo.types import Partition, Topic

from sentry_streams.pipeline.message import Message

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


class Operator(ABC, Generic[TIn, TOut]):
    """
    A python implementation of a streaming platform step that is run
    by the Rust Adapter.

    This class is meant to allow people to write streaming primitives
    in Python and run them on the Rust runtime. This is not meant to
    be implemented directly by the user of the streaming platform.

    Ideally this class could be used to facilitate the porting of the
    runtime to Rust. Eventually we should not have anything running with
    this interface.

    This interface is meant to support multiple streaming processing
    patterns:
    - synchronous / asynchronous processing.
      Submit provides a message to the step and the step can return
      a transformed message immediately. On the other hand poll is
      called periodically and can trigger asynchronous processing of a
      message.
    - 1:1 / 1:n / n:1 / n:m processing as all methods allows the implementation
      to return multiple output messages.

    Contrarily to Arroyo this class does not have access to the following
    processing steps to send message to.
    All the messages for the next step are supposed to be returned by
    submit, poll or join. The Rust adapter takes care to send them to
    the next step in the pipeline.

    Following the arroyo interface and providing this class with the
    following step to send message to would have been considerably more
    complex.
    """

    @abstractmethod
    def submit(self, message: Message[TIn]) -> None:
        """
        Send a message to this step for processing.

        This method accumulate the message with work to be done.
        The result of the processing is performed by the `poll` method.
        This separation makes errors management on the Rust side.
        Ideally we would allow submit to return results as well.

        The rust code interprets MessageRejected as backpressure and
        InvalidMessage as a message that cannot be processed to be
        sent to DLQ.
        """
        raise NotImplementedError

    @abstractmethod
    def poll(self) -> Iterable[Message[TOut]]:
        """
        Triggers asynchronous processing. This method is called periodically
        every time we poll from Kafka.

        If this step processes messages asynchronously or if it performs
        aggregations, the result of processing a message cannot be provided
        by the `submit` method itself.

        When the results are ready this method will provide the processing
        results as a return value.
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self) -> Iterable[Message[TOut]]:
        """
        Wait for all processing to be completed and returns the results of
        the in flight processing. It also closes and clean up all the resource
        used by this step.
        """
        raise NotImplementedError


def create_partition() -> Partition:
    return Partition(
        Topic("topic"),
        0,
    )

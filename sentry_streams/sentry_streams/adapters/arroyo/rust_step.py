from abc import ABC, abstractmethod
from typing import Generic, Iterable, Tuple, TypeVar

from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import MessageRejected

from sentry_streams.pipeline.message import Message

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


# This represents a set of committable offsets. These have to be
# moved between Rust and Python so we do cannot use the native
# Arroyo objects as they are not exposed to Python.
# We could create dedicated pyo3 objects but that would be easy
# to confuse with the Arroyo ones. This has a different structure
# thus harder to confuse.
Committable = dict[Tuple[str, int], int]


class RustOperatorDelegate(ABC, Generic[TIn, TOut]):
    """
    A python implementation of a streaming platform step that is run
    by the Rust Adapter. The Rust Arroyo Processing strategy would
    delegate calls to the strategy method to implementations of this
    class.

    This class is meant to allow people to write streaming primitives
    in Python and run them on the Rust runtime. This is not meant to
    be implemented directly by the user of the streaming platform.

    Ideally this class could be used to facilitate the porting of the
    runtime to Rust. Eventually we should not have anything running with
    this interface.

    The `submit` method receives messages to be processed. It does not
    return anything. The `poll` method performs the processing and
    returns the message/s for the following strategy. This separation
    allows aggregation use cases like Reduce to work.

    Contrarily to Arroyo this class does not have access to the following
    processing steps to send message to.
    All the messages for the next step are supposed to be returned by
    poll or join. The Rust adapter takes care to send them to
    the next step in the pipeline.

    Following the arroyo interface and providing this class with the
    following step to send message to would have been considerably more
    complex.
    """

    @abstractmethod
    def submit(self, message: Message[TIn], committable: Committable) -> None:
        """
        Send a message to this step for processing.

        This method accumulate the message with work to be done.
        The result of the processing is performed by the `poll` method.
        This separation makes errors management on the Rust side easier.
        Ideally we would allow submit to return results as well.

        The rust code interprets MessageRejected as backpressure and
        InvalidMessage as a message that cannot be processed to be
        sent to DLQ.

        The `committable` parameters contains the offsets represented by
        the message. It is up to the implementation of this class to
        decide what committable to return.
        """
        raise NotImplementedError

    @abstractmethod
    def poll(self) -> Iterable[Tuple[Message[TOut], Committable]]:
        """
        Triggers asynchronous processing. This method is called periodically
        every time we poll from Kafka.

        When the results are ready this method will provide the processing
        results as a return value together with the committable of each
        returned message.
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float | None = None) -> Iterable[Tuple[Message[TOut], Committable]]:
        """
        Wait for all processing to be completed and returns the results of
        the in flight processing. It also closes and clean up all the resource
        used by this step.
        """
        raise NotImplementedError


class SingleMessageOperatorDelegate(
    Generic[TIn, TOut],
    RustOperatorDelegate[TIn, TOut],
    ABC,
):
    """
    Helper class to support 1:1 synchronous message processing through
    the RustOperatorDelegate.
    This class is meant to implement simple strategies like filters
    where we just need to provide a pure processing function that
    processes one message and returns either a message or nothing.
    """

    def __init__(self) -> None:
        self.__message: Message[TIn] | None = None
        self.__committable: Committable | None = None

    @abstractmethod
    def _process_message(self, msg: Message[TIn], committable: Committable) -> Message[TOut] | None:
        """
        Processes one message at a time. It receives the offsets to commit
        if needed by the processing but it does not allow the delegate to
        change the returned offsets.

        It can raise MessageRejected or InvalidMessage.
        """
        raise NotImplementedError

    def __prepare_output(self) -> Iterable[Tuple[Message[TOut], Committable]]:
        if self.__message is None:
            return []
        assert self.__committable is not None

        try:
            processed = self._process_message(self.__message, self.__committable)
            if processed is None:
                return []
            return [(processed, self.__committable)]
        except InvalidMessage:
            raise
        finally:
            self.__message = None
            self.__committable = None

    def submit(self, message: Message[TIn], committable: Committable) -> None:
        if self.__message is not None:
            raise MessageRejected()
        self.__message = message
        self.__committable = committable

    def poll(self) -> Iterable[Tuple[Message[TOut], Committable]]:
        return self.__prepare_output()

    def flush(self, timeout: float | None = None) -> Iterable[Tuple[Message[TOut], Committable]]:
        return self.__prepare_output()

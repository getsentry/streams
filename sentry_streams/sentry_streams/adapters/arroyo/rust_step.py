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
    A RustOperatorDelegate is an interface to be implemented to build
    streaming platform operators in Python and wire them up to the
    Rust Streaming Adapter.

    This delegate runs in a Rust Arroyo strategy which delegates message
    processing to instances of this class following this process:
    1. The rust strategy receives a message via `submit`
    2. The rust strategy forwards the message to the delegate (the instance
       of this class), which takes it over.
    3. The StreamingProcessor calls `poll` on the Rust Arroyo strategy.
    4. The rust strategy calls poll on the delegate that may or may
       not return processed messages.
    5. If the delegate returns messages, the Rust strategy forwards them
       to the Arroyo next step.

    This class does not provides exactly the same Arroyo strategy
    interface. Instead it provides something easier to manage in Rust.

    - It is not the responsibility of the methods of this class to forward
      messages to the following steps in the pipeline. `poll` and `flush`
      return messages to the Rust code which then forwards them to the
      next strategy. This allows this delegate not to have access to the
      next step which is in Rust.

    - `submit` accepts work, while `poll` performs the processing on the
      messages accepted by `submit`. This interface allows implementations
      to support both 1:0..1, 1:n, n:0..1 processing strategies. It is also
      inherently asynchronous as only poll can return messages.
    """

    @abstractmethod
    def submit(self, message: Message[TIn], committable: Committable) -> None:
        """
        Send a message to this step for processing.

        Sending messages to process is separate from the processing itself.
        This makes error management on the Rust side easier. So this
        method stores messages to process, while `poll` performs the
        processing and returns the result.

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


class RustOperatorFactory(ABC, Generic[TIn, TOut]):
    """
    Like for all Arroyo processing strategies, the framework needs to be
    able to tear down and rebuild the processing strategy on its own when
    needed. This can happen at startup or at every rebalancing.

    This is the class passed to the Rust runtime so that the runtime can
    re-instantiate the RustOperatorDelegate without knowing which parameters
    to pass.

    This is a class rather than a function as these factory are often stateful.
    Example of the state they may hold across multiple instantiations of
    the RustOperatorDelegate are pre-initialized ProcessPools.
    """

    @abstractmethod
    def build(self) -> RustOperatorDelegate[TIn, TOut]:
        """
        Builds a RustOperatorDelegate that can be used to process messages
        in the Rust Streaming Adapter.
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

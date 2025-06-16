from abc import ABC, abstractmethod
from typing import Callable, Generic, Iterable, MutableSequence, Tuple, TypeVar

from arroyo.dlq import InvalidMessage
from arroyo.processing.strategies.abstract import MessageRejected, ProcessingStrategy
from arroyo.types import Message as ArroyoMessage

from sentry_streams.pipeline.message import RustMessage

TIn = TypeVar("TIn")
TOut = TypeVar("TOut")


# This represents a set of committable offsets. These have to be
# moved between Rust and Python so we do cannot use the native
# Arroyo objects as they are not exposed to Python.
# We could create dedicated pyo3 objects but that would be easy
# to confuse with the Arroyo ones. This has a different structure
# thus harder to confuse.
Committable = dict[Tuple[str, int], int]


class RustOperatorDelegate(ABC):
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
    def submit(self, message: RustMessage, committable: Committable) -> None:
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
    def poll(self) -> Iterable[Tuple[RustMessage, Committable]]:
        """
        Triggers asynchronous processing. This method is called periodically
        every time we poll from Kafka.

        When the results are ready this method will provide the processing
        results as a return value together with the committable of each
        returned message.
        """
        raise NotImplementedError

    @abstractmethod
    def flush(self, timeout: float | None = None) -> Iterable[Tuple[RustMessage, Committable]]:
        """
        Wait for all processing to be completed and returns the results of
        the in flight processing. It also closes and clean up all the resource
        used by this step.
        """
        raise NotImplementedError


class RustOperatorFactory(ABC):
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
    def build(self) -> RustOperatorDelegate:
        """
        Builds a RustOperatorDelegate that can be used to process messages
        in the Rust Streaming Adapter.
        """
        raise NotImplementedError


class SingleMessageOperatorDelegate(
    RustOperatorDelegate,
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
        self.__message: RustMessage | None = None
        self.__committable: Committable | None = None

    @abstractmethod
    def _process_message(self, msg: RustMessage, committable: Committable) -> RustMessage | None:
        """
        Processes one message at a time. It receives the offsets to commit
        if needed by the processing but it does not allow the delegate to
        change the returned offsets.

        It can raise MessageRejected or InvalidMessage.
        """
        raise NotImplementedError

    def __prepare_output(self) -> Iterable[Tuple[RustMessage, Committable]]:
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

    def submit(self, message: RustMessage, committable: Committable) -> None:
        if self.__message is not None:
            raise MessageRejected()
        self.__message = message
        self.__committable = committable

    def poll(self) -> Iterable[Tuple[RustMessage, Committable]]:
        return self.__prepare_output()

    def flush(self, timeout: float | None = None) -> Iterable[Tuple[RustMessage, Committable]]:
        return self.__prepare_output()


TStrategyIn = TypeVar("TStrategyIn")
TStrategyOut = TypeVar("TStrategyOut")


class OutputRetriever(ProcessingStrategy[TStrategyOut], Generic[TStrategyOut]):
    """
    This is an Arroyo strategy to be wired to another strategy used inside
    a `RustOperatorDelegate`. This strategy collects the result and return it to the
    Rust code.

    Arroyo strategies are provided the following step and are expected to
    hand the result directly to it. This does not work for `RustOperatorDelegate`
    which is expected to return the result as return value of poll and flush.

    In order to wrap an existing Arroyo strategy in a `RustOperatorDelegate` we
    need to provide an instance of this class to the existing strategy to
    collect the results and send it them back to Rust as `poll` return value.

    the strategy wrapped by the delegate does not always provide messages in
    a format that we can return to the Rust Runtime. For example, existing Arroyo
    strategies may return something like ArroyoMsg[FilteredPayload, Something].
    A transformer can be provided to this class to turn the output into
    a Tuple of `RustMessage` and `Committable`.
    """

    def __init__(
        self,
        out_transformer: Callable[
            [ArroyoMessage[TStrategyOut]], Tuple[RustMessage, Committable] | None
        ],
    ) -> None:
        self.__out_transformer = out_transformer
        self.__pending_messages: MutableSequence[Tuple[RustMessage, Committable]] = []

    def submit(self, message: ArroyoMessage[TStrategyOut]) -> None:
        transformed = self.__out_transformer(message)
        if transformed is not None:
            self.__pending_messages.append((transformed[0], transformed[1]))

    def poll(self) -> None:
        pass

    def join(self, timeout: float | None = None) -> None:
        pass

    def close(self) -> None:
        pass

    def terminate(self) -> None:
        pass

    def fetch(self) -> Iterable[Tuple[RustMessage, Committable]]:
        """
        Fetches the output messages from the processing strategy.
        """
        ret = self.__pending_messages
        self.__pending_messages = []
        return ret


class ArroyoStrategyDelegate(RustOperatorDelegate, Generic[TStrategyIn, TStrategyOut]):
    """
    This delegate wraps an existing Python Arroyo strategy so that it can be
    used as it is in a Rust consumer.
    Objects of this class are provided a strategy to wrap, a Callable to
    transform input messages if a transformation is needed and an
    `OutputRetriever` to collect the results from the inner strategy and
    make them available to rust.

    The message flow looks like this:
    1. The Rust Arroyo Strategy receives a streaming message to process.
    2. The Rust Strategy hands it to this class via the `submit` method.
    3. The `submit` method transforms the input message into a Python
       Arroyo message for the wrapped strategy via the `in_transformer`.
    4. The transformed message is passed to the wrapped strategy via the
       `submit` method.
    5. The inner strategy does the processing and sends the results to the
       next step, which is the `OutputRetriever`.
    6. The `OutputRetriever` transforms the results into a format that
       can be managed by the Rust consumer.
    7. The rust code fetches the transformed results via the `poll` method
       of this class.

    This additional complexity is needed to adapt a Python Arroyo Strategy
    (the reduce one) to the Rust Arroyo Runtime:
    - We cannot run a Python strategy as it is on Rust. Rust `ProcessingStrategy`
      cannot be exposed to python.
    - The Python Strategy cannot return results directly to Rust.
      It can only pass them to the next step (like all Arroyo strategies).
      So it needs a next step that can provide the results to Rust.
    - The Arroyo strategy is an Arroyo strategy, so it needs to be
      fed with Arroyo messages, thus the adaptation logic from the
      new Streaming platform message that the Rust code deals with.
    """

    def __init__(
        self,
        inner: ProcessingStrategy[TStrategyIn],
        in_transformer: Callable[[RustMessage, Committable], ArroyoMessage[TStrategyIn]],
        retriever: OutputRetriever[TStrategyOut],
    ) -> None:
        self.__inner = inner
        self.__in_transformer = in_transformer
        self.__retriever = retriever

    def submit(self, message: RustMessage, committable: Committable) -> None:
        arroyo_msg = self.__in_transformer(message, committable)
        self.__inner.submit(arroyo_msg)

    def poll(self) -> Iterable[Tuple[RustMessage, Committable]]:
        self.__inner.poll()
        return self.__retriever.fetch()

    def flush(self, timeout: float | None = None) -> Iterable[Tuple[RustMessage, Committable]]:
        self.__inner.join(timeout)
        return self.__retriever.fetch()

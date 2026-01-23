class InvalidMessageError(Exception):
    """
    The exception to indicate that the message passed by the previous step is
    invalid, and the message should not be retried.

    If the DLQ is configured and supported by the runtime, the original raw
    messages associated with the invalid messages will be placed into the DLQ.
    """

    pass


class InvalidPipelineError(Exception):
    """
    Exception raised when a pipeline is invalid or misconfigured.

    This includes cases like:
    - Pipeline branches that don't terminate with Sink steps
    - Malformed pipeline graph structure
    """

    pass

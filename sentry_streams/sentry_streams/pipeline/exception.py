class InvalidMessageError(Exception):
    """
    The exception to indicate that the message passed by the previous step is
    invalid, and the message should not be retried.

    If the DLQ is configured and supported by the runtime, the original raw
    messages associated with the invalid messages will be placed into the DLQ.
    """

    pass


class DlqHandledError(Exception):
    """
    Exception raised after a message has been successfully sent to the DLQ.

    This exception signals to the Rust runtime that the message has been
    handled (sent to DLQ) and should be skipped rather than retried or
    causing a crash. The Rust layer will treat this like a filter returning
    False - the message is dropped but processing continues normally.
    """

    pass

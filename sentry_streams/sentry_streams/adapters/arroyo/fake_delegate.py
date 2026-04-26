class FakeOperatorDelegate:
    def __init__(self):
        self.payload = None
        self.committable = None

    def submit(self, payload, committable):
        self.committable = committable
        # Handle watermark messages (PyWatermark objects)
        if hasattr(payload, "committable"):
            self.payload = payload
            return
        if payload.payload == "ok":
            self.payload = payload
            return
        elif payload.payload == "reject":
            from arroyo.processing.strategies import MessageRejected

            raise MessageRejected()
        elif payload.payload == "invalid":
            from arroyo.dlq import InvalidMessage
            from arroyo import Partition, Topic

            raise InvalidMessage(Partition(Topic("topic"), 0), 42)

    def poll(self):
        return [(self.payload, self.committable), (self.payload, self.committable)]

    def flush(self, timeout: float | None = None):
        return [(self.payload, self.committable)]


class FakeOperatorDelegateFactory:
    def build(self):
        return RustOperatorDelegate()

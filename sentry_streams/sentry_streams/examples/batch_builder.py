import json

from sentry_streams.pipeline.function_template import InputType


def build_batch_str(batch: list[InputType]) -> str:

    d = {"batch": batch}

    return json.dumps(d)


def build_message_str(message: str) -> str:

    d = {"message": message}

    return json.dumps(d)

"""
Module to be loaded by kopf runner. Configures an instance of StreamsOperator from provided
environment variable(s).
"""

import logging

from sentry_streams_operator.op import StreamsOperator

streams_operator = StreamsOperator.from_env()
streams_operator.register_handlers()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    streams_operator.configure(streams_operator.logger)

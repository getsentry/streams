"""
Flink Worker gRPC Service Implementation

This module provides the implementation of the FlinkWorkerService gRPC service.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

import grpc
from .flink_worker_pb2 import Message, ProcessMessageRequest, ProcessMessageResponse
from .flink_worker_pb2_grpc import FlinkWorkerServiceServicer, add_FlinkWorkerServiceServicer_to_server

logger = logging.getLogger(__name__)


class FlinkWorkerService(FlinkWorkerServiceServicer):
    """
    Implementation of the FlinkWorkerService gRPC service.

    This service processes messages and returns a list of processed messages.
    The Message class can be subclassed to add custom functionality.
    """

    def ProcessMessage(
        self, request: ProcessMessageRequest, context: grpc.ServicerContext
    ) -> ProcessMessageResponse:
        """
        Process a single message and return a list of processed messages.

        Args:
            request: The ProcessMessageRequest containing the message and segment_id
            context: The gRPC service context

        Returns:
            ProcessMessageResponse containing a list of processed messages
        """
        try:
            message = request.message
            segment_id = request.segment_id

            logger.info(f"Processing message for segment {segment_id}")
            logger.debug(f"Message payload length: {len(message.payload)}")
            logger.debug(f"Message headers: {message.headers}")
            logger.debug(f"Message timestamp: {message.timestamp}")

            # For now, return the original message as-is
            # In a real implementation, this would contain the actual processing logic
            processed_messages = [message]

            # Add a simple header to indicate processing
            processed_message = Message()
            processed_message.CopyFrom(message)
            processed_message.headers["processed"] = "true"
            processed_message.headers["segment_id"] = str(segment_id)

            processed_messages.append(processed_message)

            logger.info(f"Successfully processed message for segment {segment_id}")
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return ProcessMessageResponse(messages=[])


def create_server(port: int = 50051) -> grpc.Server:
    """
    Create and configure the gRPC server.

    Args:
        port: The port to bind the server to

    Returns:
        A configured gRPC server
    """
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    add_FlinkWorkerServiceServicer_to_server(FlinkWorkerService(), server)

    # Bind to the specified port
    server.add_insecure_port(f"[::]:{port}")

    return server

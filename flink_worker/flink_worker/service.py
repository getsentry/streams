"""
Flink Worker gRPC Service Implementation

This module provides the implementation of the FlinkWorkerService gRPC service.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

import time
import grpc
from flink_worker.flink_worker_pb2 import (
    Message, ProcessMessageRequest, ProcessMessageResponse, ProcessWatermarkRequest,
    AddToWindowRequest, TriggerWindowRequest
)
from flink_worker.flink_worker_pb2_grpc import FlinkWorkerServiceServicer, add_FlinkWorkerServiceServicer_to_server
from google.protobuf import empty_pb2

logger = logging.getLogger(__name__)


class FlinkWorkerService(FlinkWorkerServiceServicer):
    """
    Implementation of the FlinkWorkerService gRPC service.

    This service processes messages and returns a list of processed messages.
    The Message class can be subclassed to add custom functionality.
    """

    def __init__(self):
        """Initialize the service with an in-memory window storage."""
        self.windows = {}  # Simple in-memory storage for windows
        logger.info("FlinkWorkerService initialized with window storage")

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
            processed_messages = []
            # Add a simple header to indicate processing
            processed_message = Message()
            processed_message.CopyFrom(message)
            processed = f"{message.payload.decode()} processed".encode("utf-8")
            processed_message.payload = processed
            processed_message.headers["processed"] = "true"
            processed_message.headers["segment_id"] = str(segment_id)

            processed_messages.append(processed_message)

            logger.info(f"Successfully processed message for segment {segment_id}")
            time.sleep(0.5)
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return ProcessMessageResponse(messages=[])

    def ProcessWatermark(
        self, request: ProcessWatermarkRequest, context: grpc.ServicerContext
    ) -> ProcessMessageResponse:
        """
        Process a watermark and return a list of processed messages.

        Args:
            request: The ProcessWatermarkRequest containing the timestamp, headers and segment_id
            context: The gRPC service context

        Returns:
            ProcessMessageResponse containing a list of processed messages
        """
        try:
            timestamp = request.timestamp
            headers = request.headers
            segment_id = request.segment_id

            logger.info(f"Processing watermark for segment {segment_id}")
            logger.debug(f"Watermark timestamp: {timestamp}")
            logger.debug(f"Watermark headers: {headers}")

            # Create a message from the watermark data
            # In a real implementation, this would contain the actual watermark processing logic
            processed_messages = []

            # Create a message with the watermark information
            watermark_message = Message()
            watermark_message.timestamp = timestamp
            watermark_message.headers.update(headers)
            watermark_message.headers["watermark"] = "true"
            watermark_message.headers["segment_id"] = str(segment_id)
            watermark_message.headers["processed"] = "true"

            # Set a simple payload indicating this is a watermark
            watermark_message.payload = f"watermark_{timestamp}".encode("utf-8")

            processed_messages.append(watermark_message)

            logger.info(f"Successfully processed watermark for segment {segment_id}")
            time.sleep(0.1)  # Shorter delay for watermarks
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.error(f"Error processing watermark: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return ProcessMessageResponse(messages=[])

    def AddToWindow(
        self, request: AddToWindowRequest, context: grpc.ServicerContext
    ) -> empty_pb2.Empty:
        """
        Add a message to a window.

        Args:
            request: The AddToWindowRequest containing the message, segment_id, and window_id
            context: The gRPC service context

        Returns:
            Empty response indicating success
        """
        try:
            message = request.message
            segment_id = request.segment_id
            window_id = request.window_id

            # Create a unique key for the window
            window_key = f"{window_id.partition_key}_{window_id.window_start_time}_{segment_id}"

            logger.info(f"Adding message to window {window_key} for segment {segment_id}")
            logger.debug(f"Window partition key: {window_id.partition_key}")
            logger.debug(f"Window start time: {window_id.window_start_time}")

            # Initialize the window if it doesn't exist
            if window_key not in self.windows:
                self.windows[window_key] = []

            # Add the message to the window
            self.windows[window_key].append(message)

            logger.info(f"Successfully added message to window {window_key}")
            return empty_pb2.Empty()

        except Exception as e:
            logger.error(f"Error adding message to window: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return empty_pb2.Empty()

    def TriggerWindow(
        self, request: TriggerWindowRequest, context: grpc.ServicerContext
    ) -> ProcessMessageResponse:
        """
        Trigger a window and return the accumulated messages.

        Args:
            request: The TriggerWindowRequest containing the window_id and segment_id
            context: The gRPC service context

        Returns:
            ProcessMessageResponse containing the accumulated messages from the window
        """
        try:
            window_id = request.window_id
            segment_id = request.segment_id

            # Create the same unique key for the window
            window_key = f"{window_id.partition_key}_{window_id.window_start_time}_{segment_id}"

            logger.info(f"Triggering window {window_key} for segment {segment_id}")
            logger.debug(f"Window partition key: {window_id.partition_key}")
            logger.debug(f"Window start time: {window_id.window_start_time}")

            # Get the messages from the window
            if window_key in self.windows:
                window_messages = self.windows[window_key]
                logger.info(f"Found {len(window_messages)} messages in window {window_key}")

                # Process the window messages (simple aggregation for now)
                processed_messages = []

                output_msg = Message()
                output_msg.headers["window_triggered"] = "true"
                output_msg.headers["window_key"] = window_key
                output_msg.headers["segment_id"] = str(segment_id)
                output_msg.payload = f"window_{window_key}_triggered".encode("utf-8")
                processed_messages.append(output_msg)

                # Clear the window after triggering
                del self.windows[window_key]
                logger.info(f"Window {window_key} cleared after triggering")

                return ProcessMessageResponse(messages=processed_messages)
            else:
                logger.warning(f"Window {window_key} not found")
                return ProcessMessageResponse(messages=[])

        except Exception as e:
            logger.error(f"Error triggering window: {e}")
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

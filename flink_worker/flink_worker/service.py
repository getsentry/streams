"""
Flink Worker gRPC Service Implementation

This module provides the implementation of the FlinkWorkerService gRPC service.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

from typing import MutableMapping
import grpc
from .segment import ProcessingSegment, WindowSegment
from flink_worker.flink_worker_pb2 import (
    ProcessMessageRequest, ProcessMessageResponse, ProcessWatermarkRequest,
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

    def __init__(
        self,
        segments: MutableMapping[int, ProcessingSegment],
        window_segments: MutableMapping[int, WindowSegment]
    ):
        """Initialize the service with an in-memory window storage."""
        self.segments = segments
        self.window_segments = window_segments
        self.counters: MutableMapping[int, int] = {}

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

        message = request.message
        segment_id = request.segment_id

        logger.info(f"Processing message for segment {segment_id} {self.counters.get(segment_id, 0)}")
        self.counters[segment_id] = self.counters.get(segment_id, 0) + 1
        logger.debug(f"Message payload length: {len(message.payload)}")
        logger.debug(f"Message headers: {message.headers}")
        logger.debug(f"Message timestamp: {message.timestamp}")

        if segment_id not in self.segments:
            logger.error(f"Invalid segment when processing message: {segment_id}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Invalid segment: {str(segment_id)}")
            return ProcessMessageResponse(messages=[])

        try:
            processed_messages = self.segments[segment_id].process(message)
            logger.debug(f"Processed message for segment {segment_id}. {len(processed_messages)} returned")
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.exception(f"Error processing message: {e}", exc_info=True)
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

        segment_id = request.segment_id
        watermark_timestamp = request.timestamp

        logger.info(f"Processing watermark for segment {segment_id}")
        logger.debug(f"Watermark timestamp: {watermark_timestamp}")

        if segment_id not in self.segments:
            logger.error(f"Invalid segment when processing message: {segment_id}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Invalid segment: {str(segment_id)}")
            return ProcessMessageResponse(messages=[])

        try:
            processed_messages = self.segments[segment_id].watermark(watermark_timestamp)
            logger.debug(f"Processed message for segment {segment_id}. {len(processed_messages)} returned")
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.exception(f"Error processing message: {e}", exc_info=True)
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
        message = request.message
        segment_id = request.segment_id
        window_id = request.window_id
        partition_key = window_id.partition_key
        window_start_time = window_id.window_start_time

        if segment_id not in self.window_segments:
            logger.error(f"Invalid segment when adding to window: {segment_id}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Invalid segment: {str(segment_id)}")
            return empty_pb2.Empty()

        try:
            self.window_segments[segment_id].add(message, window_start_time, partition_key)
            logger.debug(f"Added message to window {window_id} for segment {segment_id}")
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
        segment_id = request.segment_id
        window_id = request.window_id

        if segment_id not in self.window_segments:
            logger.error(f"Invalid segment when triggering window: {segment_id}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Invalid segment: {str(segment_id)}")
            return ProcessMessageResponse(messages=[])

        try:
            processed_messages = self.window_segments[segment_id].trigger(
                window_id.window_start_time,
                window_id.partition_key
            )
            logger.debug(f"Triggered window for segment {segment_id}. {len(processed_messages)} messages returned")
            return ProcessMessageResponse(messages=processed_messages)

        except Exception as e:
            logger.error(f"Error triggering window: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return ProcessMessageResponse(messages=[])


def create_server(segments: MutableMapping[int, ProcessingSegment], window_segments: MutableMapping[int, WindowSegment], port: int = 50051, ) -> grpc.Server:
    """
    Create and configure the gRPC server.

    Args:
        port: The port to bind the server to

    Returns:
        A configured gRPC server
    """
    server = grpc.server(ThreadPoolExecutor(max_workers=1))
    add_FlinkWorkerServiceServicer_to_server(FlinkWorkerService(segments, window_segments), server)

    # Bind to the specified port
    server.add_insecure_port(f"[::]:{port}")

    return server

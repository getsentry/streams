"""
Flink Worker gRPC Client

This module provides a CLI client for testing the Flink Worker gRPC service.
"""

import argparse
import json
import logging
import sys
from typing import Dict

import grpc

from .flink_worker_pb2 import Message, ProcessMessageRequest, ProcessWatermarkRequest
from .flink_worker_pb2_grpc import FlinkWorkerServiceStub

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FlinkWorkerClient:
    """Client for interacting with the Flink Worker gRPC service."""

    def __init__(self, host: str = "localhost", port: int = 50051):
        """
        Initialize the client.

        Args:
            host: The host address of the gRPC server
            port: The port of the gRPC server
        """
        self.address = f"{host}:{port}"
        self.channel = None
        self.stub = None

    def connect(self) -> None:
        """Establish connection to the gRPC server."""
        try:
            self.channel = grpc.insecure_channel(self.address)
            self.stub = FlinkWorkerServiceStub(self.channel)
            logger.info(f"Connected to Flink Worker service at {self.address}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.address}: {e}")
            raise

    def disconnect(self) -> None:
        """Close the connection to the gRPC server."""
        if self.channel:
            self.channel.close()
            logger.info("Disconnected from Flink Worker service")

    def process_message(
        self,
        payload: bytes,
        headers: Dict[str, str],
        timestamp: int,
        segment_id: int
    ) -> None:
        """
        Send a message to the service for processing.

        Args:
            payload: The message payload as bytes
            headers: A dictionary of string key-value pairs
            timestamp: Unix timestamp
            segment_id: The segment ID as an unsigned integer
        """
        try:
            # Create the message
            message = Message(
                payload=payload,
                headers=headers,
                timestamp=timestamp
            )

            # Create the request
            request = ProcessMessageRequest(
                message=message,
                segment_id=segment_id
            )

            logger.info(f"Sending message for segment {segment_id}")
            logger.debug(f"Payload length: {len(payload)}")
            logger.debug(f"Headers: {headers}")
            logger.debug(f"Timestamp: {timestamp}")

            # Make the gRPC call
            response = self.stub.ProcessMessage(request)

            logger.info(f"Received {len(response.messages)} processed messages")

            # Display the results
            for i, msg in enumerate(response.messages):
                print(f"\n--- Processed Message {i + 1} ---")
                print(f"Payload length: {len(msg.payload)}")
                print(f"Headers: {dict(msg.headers)}")
                print(f"Timestamp: {msg.timestamp}")

                # Try to decode payload as text if it looks like text
                try:
                    payload_text = msg.payload.decode('utf-8')
                    if payload_text.isprintable():
                        print(f"Payload (text): {payload_text}")
                    else:
                        print(f"Payload (hex): {msg.payload.hex()[:100]}...")
                except UnicodeDecodeError:
                    print(f"Payload (hex): {msg.payload.hex()[:100]}...")

        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_watermark(
        self,
        timestamp: int,
        headers: Dict[str, str],
        segment_id: int
    ) -> None:
        """
        Send a watermark to the service for processing.

        Args:
            timestamp: Unix timestamp for the watermark
            headers: A dictionary of string key-value pairs
            segment_id: The segment ID as an unsigned integer
        """
        try:
            # Create the request
            request = ProcessWatermarkRequest(
                timestamp=timestamp,
                headers=headers,
                segment_id=segment_id
            )

            logger.info(f"Sending watermark for segment {segment_id}")
            logger.debug(f"Timestamp: {timestamp}")
            logger.debug(f"Headers: {headers}")

            # Make the gRPC call
            response = self.stub.ProcessWatermark(request)

            logger.info(f"Received {len(response.messages)} processed watermark messages")

            # Display the results
            for i, msg in enumerate(response.messages):
                print(f"\n--- Processed Watermark Message {i + 1} ---")
                print(f"Payload length: {len(msg.payload)}")
                print(f"Headers: {dict(msg.headers)}")
                print(f"Timestamp: {msg.timestamp}")

                # Try to decode payload as text if it looks like text
                try:
                    payload_text = msg.payload.decode('utf-8')
                    if payload_text.isprintable():
                        print(f"Payload (text): {payload_text}")
                    else:
                        print(f"Payload (hex): {msg.payload.hex()[:100]}...")
                except UnicodeDecodeError:
                    print(f"Payload (hex): {msg.payload.hex()[:100]}...")

        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e.code()} - {e.details()}")
        except Exception as e:
            logger.error(f"Error processing watermark: {e}")


def main() -> None:
    """Main entry point for the Flink Worker gRPC client."""
    parser = argparse.ArgumentParser(description="Flink Worker gRPC Client")
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host address of the gRPC server (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=50051,
        help="Port of the gRPC server (default: 50051)"
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["message", "watermark"],
        default="message",
        help="Processing mode: 'message' or 'watermark' (default: 'message')"
    )
    parser.add_argument(
        "--payload",
        type=str,
        default="Hello, Flink Worker!",
        help="Message payload (default: 'Hello, Flink Worker!') - only used in message mode"
    )
    parser.add_argument(
        "--headers",
        type=str,
        default='{"source": "cli", "type": "test"}',
        help="JSON string of headers (default: '{\"source\": \"cli\", \"type\": \"test\"}')"
    )
    parser.add_argument(
        "--timestamp",
        type=int,
        help="Unix timestamp (default: current time)"
    )
    parser.add_argument(
        "--segment-id",
        type=int,
        default=1,
        help="Segment ID (default: 1)"
    )

    args = parser.parse_args()

    # Parse headers JSON
    try:
        headers = json.loads(args.headers)
        if not isinstance(headers, dict):
            raise ValueError("Headers must be a JSON object")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid headers JSON: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Invalid headers: {e}")
        sys.exit(1)

    # Use current timestamp if not provided
    if args.timestamp is None:
        import time
        args.timestamp = int(time.time())

    # Create and use the client
    client = FlinkWorkerClient(args.host, args.port)

    try:
        client.connect()

        if args.mode == "watermark":
            client.process_watermark(args.timestamp, headers, args.segment_id)
        else:
            # Convert payload to bytes for message mode
            payload = args.payload.encode('utf-8')
            client.process_message(payload, headers, args.timestamp, args.segment_id)

    except Exception as e:
        logger.error(f"Client error: {e}")
        sys.exit(1)
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()

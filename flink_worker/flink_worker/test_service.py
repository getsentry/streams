"""
Simple test script for the Flink Worker gRPC service.

This script tests the service by starting it in a separate thread and then
sending a test message to it.
"""

import time

import grpc

from flink_worker.flink_worker_pb2 import Message, ProcessMessageRequest, ProcessWatermarkRequest
from flink_worker.flink_worker_pb2_grpc import FlinkWorkerServiceStub
from flink_worker.service import create_server


def test_service():
    """Test the Flink Worker service."""
    print("🚀 Starting Flink Worker gRPC service test...")

    # Create and start the server
    server = create_server(port=50052)  # Use different port to avoid conflicts
    server.start()

    print("✅ Server started on port 50052")

    # Wait a moment for server to be ready
    time.sleep(1)

    try:
        # Create a channel and stub
        channel = grpc.insecure_channel('localhost:50052')
        stub = FlinkWorkerServiceStub(channel)

        print("✅ Connected to server")

        # Test 1: Process Message
        print("\n📝 Testing ProcessMessage...")

        # Create a test message
        test_message = Message(
            payload=b"Hello, Flink Worker! This is a test message.",
            headers={
                "source": "test_script",
                "type": "greeting",
                "priority": "high"
            },
            timestamp=int(time.time())
        )

        # Create the request
        request = ProcessMessageRequest(
            message=test_message,
            segment_id=42
        )

        print("📤 Sending test message...")
        print(f"   Payload: {test_message.payload}")
        print(f"   Headers: {dict(test_message.headers)}")
        print(f"   Timestamp: {test_message.timestamp}")
        print(f"   Segment ID: {request.segment_id}")

        # Send the message
        response = stub.ProcessMessage(request)

        print("📥 Received response:")
        print(f"   Number of messages: {len(response.messages)}")

        # Display each processed message
        for i, msg in enumerate(response.messages):
            print(f"\n--- Message {i + 1} ---")
            print(f"   Payload: {msg.payload}")
            print(f"   Headers: {dict(msg.headers)}")
            print(f"   Timestamp: {msg.timestamp}")

        # Test 2: Process Watermark
        print("\n💧 Testing ProcessWatermark...")

        # Create a test watermark request
        watermark_request = ProcessWatermarkRequest(
            timestamp=int(time.time()),
            headers={
                "source": "test_script",
                "type": "watermark",
                "priority": "normal"
            },
            segment_id=42
        )

        print("📤 Sending test watermark...")
        print(f"   Timestamp: {watermark_request.timestamp}")
        print(f"   Headers: {dict(watermark_request.headers)}")
        print(f"   Segment ID: {watermark_request.segment_id}")

        # Send the watermark
        watermark_response = stub.ProcessWatermark(watermark_request)

        print("📥 Received watermark response:")
        print(f"   Number of messages: {len(watermark_response.messages)}")

        # Display each processed watermark message
        for i, msg in enumerate(watermark_response.messages):
            print(f"\n--- Watermark Message {i + 1} ---")
            print(f"   Payload: {msg.payload}")
            print(f"   Headers: {dict(msg.headers)}")
            print(f"   Timestamp: {msg.timestamp}")

        print("\n✅ All tests completed successfully!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        raise
    finally:
        # Clean up
        server.stop(0)
        print("🛑 Server stopped")


if __name__ == "__main__":
    test_service()

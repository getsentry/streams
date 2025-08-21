"""
Flink Worker gRPC Server Entry Point

This module provides the main entry point for running the Flink Worker gRPC server.
"""

import argparse
import logging
import signal
import sys
from typing import Optional

from flink_worker.app_segments import AppendEntry, BatchingSegment

from .service import create_server

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def signal_handler(signum: int, frame: Optional[object]) -> None:
    """Handle shutdown signals gracefully."""
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)


def main() -> None:
    """Main entry point for the Flink Worker gRPC server."""
    parser = argparse.ArgumentParser(description="Flink Worker gRPC Server")
    parser.add_argument(
        "--port",
        type=int,
        default=50051,
        help="Port to bind the server to (default: 50051)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="[::]",
        help="Host to bind the server to (default: [::])"
    )

    args = parser.parse_args()

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Create and start the server
        server = create_server({
            0: AppendEntry(),
            1: BatchingSegment(batch_size=5, batch_time_sec=4),
        }, {}, args.port)
        server.start()

        logger.info(f"Flink Worker gRPC server started on {args.host}:{args.port}")
        logger.info("Press Ctrl+C to stop the server")

        # Keep the server running
        server.wait_for_termination()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    except Exception as e:
        logger.error(f"Error starting server: {e}")
        sys.exit(1)
    finally:
        if 'server' in locals():
            server.stop(0)
            logger.info("Server stopped")


if __name__ == "__main__":
    main()

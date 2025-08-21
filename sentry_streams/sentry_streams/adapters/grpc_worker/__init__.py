"""
gRPC Worker Adapter for sentry_streams

This package provides integration with the flink_worker gRPC service.
"""

from sentry_streams.adapters.grpc_worker.adapter import GRPCWorkerAdapter

__all__ = ["GRPCWorkerAdapter"]

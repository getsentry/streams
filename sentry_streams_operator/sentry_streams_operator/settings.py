import importlib
import os
from dataclasses import dataclass
from typing import cast


@dataclass
class Settings:
    # Kubernetes namespace in which to operate.
    namespace: str = os.getenv("SENTRY_STREAMS_OPERATOR_NAMESPACE", "streams-operator")

    # Kuberntes client uses different method to configure itself when running from withing the
    # cluster (default) and expects a properly configured RBAC and service account with secret token
    # mounted. When running from outside the cluster it will use the currently configured kubectl
    # context.
    incluster: bool = not os.getenv("SENTRY_STREAMS_OPERATOR_INCLUSTER") in ("False", "false")

    # Streaming application container name.
    container_name: str = os.getenv("SENTRY_STREAMS_OPERATOR_CONTAINER_NAME", "segment")

    # Streaming application container image.
    container_image: str = os.getenv("SENTRY_STREAMS_OPERATOR_CONTAINER_IMAGE", "sleep")


def load_settings(path: str) -> Settings:
    module_name, obj_name = path.rsplit(",", maxsplit=1)
    return cast(Settings, getattr(importlib.import_module(module_name), obj_name))


def load_settings_from_envvar() -> Settings:
    """Load settings object from envvar or use default values."""
    settings_path = os.getenv("SENTRY_STREAMS_OPERATOR_SETTNGS")
    if settings_path:
        return load_settings(settings_path)
    else:
        return Settings()

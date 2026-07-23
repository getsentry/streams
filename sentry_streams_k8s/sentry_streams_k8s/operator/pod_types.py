from __future__ import annotations

from typing import Any, Literal, NotRequired, TypedDict


class PodManifestMetadata(TypedDict):
    name: str
    labels: NotRequired[dict[str, str]]
    annotations: NotRequired[dict[str, str]]


class PodManifest(TypedDict):
    apiVersion: Literal["v1"]
    kind: Literal["Pod"]
    metadata: PodManifestMetadata
    spec: dict[str, Any]

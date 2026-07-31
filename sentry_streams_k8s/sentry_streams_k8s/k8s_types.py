"""
The K8s Python client has proper types releasing soon. When released,
we can replace these with from_dict methods (e.g. V1Deployment.from_dict).
"""

from __future__ import annotations

from typing import Any, Literal, NotRequired, Required, TypedDict


class V1ObjectMetaDict(TypedDict, total=False):
    name: Required[str]
    namespace: str
    labels: dict[str, str]
    annotations: dict[str, str]


class V1PodDict(TypedDict, total=False):
    apiVersion: Required[str]
    kind: Required[Literal["Pod"]]
    metadata: Required[V1ObjectMetaDict]
    spec: Required[dict[str, Any]]


class V1DeploymentDict(TypedDict, total=False):
    apiVersion: Required[str]
    kind: Required[Literal["Deployment"]]
    metadata: Required[V1ObjectMetaDict]
    spec: dict[str, Any]


class V1ConfigMapDict(TypedDict, total=False):
    apiVersion: Required[str]
    kind: Required[Literal["ConfigMap"]]
    metadata: Required[V1ObjectMetaDict]
    data: dict[str, str]


class V1ConditionDict(TypedDict):
    type: str
    status: Literal["True", "False", "Unknown"]
    reason: str
    message: str
    lastTransitionTime: str
    observedGeneration: NotRequired[int]

from __future__ import annotations

GROUP = "streams.sentry.io"
VERSION = "v1alpha1"
PLURAL = "streamingpipelines"

FIELD_MANAGER = "streaming-operator"
WORKLOAD_NAMESPACE_ENV = "WORKLOAD_NAMESPACE"

# SSA through API clients is tagged with the _content_type
# override and is required to keep tracking field ownership.

APPLY_PATCH_CONTENT_TYPE = "application/apply-patch+yaml"

# The CR and its workloads may live in different namespaces, so we cannot use
# ownerReferences (Kubernetes forbids cross-namespace owning) or kopf.adopt.
# Instead every workload carries the owning CR's UID as a label with
# name/namespace annotations so the operator can delete resources itself:

OWNER_UID_LABEL = "pipeline.streams.sentry.io/owner-uid"
OWNER_NAME_ANNOTATION = "pipeline.streams.sentry.io/owner-name"
OWNER_NAMESPACE_ANNOTATION = "pipeline.streams.sentry.io/owner-namespace"

from __future__ import annotations

from typing import Any

GROUP = "streams.sentry.io"
VERSION = "v1alpha1"
PLURAL = "streamingpipelines"

FIELD_MANAGER = "streaming-operator"
WORKLOAD_NAMESPACE_ENV = "WORKLOAD_NAMESPACE"

# The CR and its workloads may live in different namespaces, so we cannot use
# ownerReferences (Kubernetes forbids cross-namespace owning) or kopf.adopt.
# Instead every workload carries the owning CR's UID as a label with
# name/namespace annotations so the operator can delete resources itself:

OWNER_UID_LABEL = "streams.sentry.io/owner-uid"
OWNER_NAME_ANNOTATION = "streams.sentry.io/owner-name"
OWNER_NAMESPACE_ANNOTATION = "streams.sentry.io/owner-namespace"

# Marks every Pod the operator manages so the cluster-wide Pod event watcher can
# select operator-owned Pods without knowing their individual UIDs:

MANAGED_BY_LABEL = "streams.sentry.io/managed-by"

# Identifies independently reconciled Pod sets owned by the same pipeline:

WORKLOAD_SET_LABEL = "streams.sentry.io/workload-set"

PRIMARY_WORKLOAD_SET = "primary"
CANARY_WORKLOAD_SET = "canary"

ALL_WORKLOAD_SETS = (PRIMARY_WORKLOAD_SET, CANARY_WORKLOAD_SET)

# The ordinal is the stable replica number. The generation increments each time a replica's
# Pod is replaced so the new Pod's name never collides with a still-terminating old one:

ORDINAL_LABEL = "streams.sentry.io/ordinal"
GENERATION_LABEL = "streams.sentry.io/generation"

# Pod names are cannot exceed 63 characters, so we reserve space for the
# largest possible replicas + generation and give the rest to the base name:

MAX_POD_NAME_LENGTH = 63

MAX_REPLICAS = 999
MAX_GENERATION = 9999

MAX_BASE_NAME_LENGTH = MAX_POD_NAME_LENGTH - len(f"-{MAX_REPLICAS}-{MAX_GENERATION}")

# Hash of the desired Pod state. Used to detect config/spec changes:

SPEC_HASH_ANNOTATION = "streams.sentry.io/spec-hash"

# Waiting states that may recover when the operator creates a new Pod:

UNHEALTHY_WAITING_REASONS = frozenset({"ErrImagePull", "ImagePullBackOff"})

# Waiting states caused by an invalid workload specification.
# Should raise a PermanentError and not trigger replacement:

PERMANENT_WAITING_REASONS = frozenset({"InvalidImageName"})

# WAITING_GRACE: A Pod stuck in a bad waiting state is terminated after this long.
# TERMINATING_GRACE: A Pod stuck in a terminating state for this long is force-deleted.

POD_WAITING_GRACE_SECONDS = 300
POD_TERMINATING_GRACE_SECONDS = 600

# How often the daemon re-runs a full reconcile as a safety net behind the watch:

HEALTH_SCAN_INTERVAL_SECONDS = 60

# Bound full reconciliations across pipelines so changes
# to many CRs cannot overload Kubernetes API requests:

MAX_CONCURRENT_RECONCILES = 4

Logger = Any

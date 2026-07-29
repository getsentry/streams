from __future__ import annotations

# Identifies independently reconciled Pod sets owned by the same pipeline:

PRIMARY_WORKLOAD_SET = "primary"
CANARY_WORKLOAD_SET = "canary"

ALL_WORKLOAD_SETS = (PRIMARY_WORKLOAD_SET, CANARY_WORKLOAD_SET)

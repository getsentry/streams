from typing import Any

from sentry_streams.pipeline.exception import InvalidPipelineError
from sentry_streams.pipeline.pipeline import Pipeline, Sink


def validate_all_branches_have_sinks(pipeline: Pipeline[Any]) -> None:
    """
    Validates that all branches in a pipeline terminate with a Sink step.

    Raises:
        InvalidPipelineError: If any branch doesn't end with a Sink step
    """
    # Find all leaf nodes (nodes with no outgoing edges)
    leaf_nodes = [
        step_name
        for step_name in pipeline.steps.keys()
        if not pipeline.outgoing_edges.get(step_name)
    ]

    # Check each leaf is a Sink
    non_sink_leaves = [name for name in leaf_nodes if not isinstance(pipeline.steps[name], Sink)]

    if non_sink_leaves:
        raise InvalidPipelineError(
            f"All pipeline branches must terminate with a Sink step. "
            f"The following steps are leaves but not sinks: {', '.join(non_sink_leaves)}"
        )

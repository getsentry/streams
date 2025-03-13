from dataclasses import dataclass
from typing import MutableMapping, Sequence

from sentry_streams.adapters.arroyo.routes import Route, RoutedStep
from sentry_streams.pipeline.pipeline import Step


@dataclass
class Pipes:
    """
    Intermediate representation of multiple Arroyo applications.

    Arroyo applications represent a single consumer. For the streaming
    platform we need multiple pipelines in the same application. This
    represent multiple arroyo applications.

    Arroyo does not support branches. The streaming platform does so
    we need to fake it in arroyo. This is done by making the branched
    pipeline a sequence and make all the messages go through all the
    steps for all the branches. The route is used to filter out the
    messages that do not belong to the branch.

    Building an Arroyo application is done from the last step to the
    first step. This is because every step references the following one.
    The streaming platform allows you to define the pipeline in sequence
    from the first to last step. This intermediate representation also
    collects the pipeline to be built in reverse order in Arroyo.
    """

    pipelines: MutableMapping[str, Sequence[RoutedStep]]

    def add_step(self, route: Route, step: Step) -> None:
        """
        Append a pipeline step from
        """
        pass

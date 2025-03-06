from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, TypeVar

MeasurementUnit = TypeVar("MeasurementUnit", int, timedelta)


@dataclass
class Window(Generic[MeasurementUnit]):
    """
    A generic representation of a Window.
    Each Window can have a trigger plugged in.
    """


@dataclass
class SlidingWindow(Window[MeasurementUnit]):
    """
    A sliding window which is configured
    by counts or by event time. Both size and slide can be
    in terms of number of elements, or both can be in terms
    of a duration in event time.

    The window slide determines how frequently
    a window is started. (e.g.every 10 elements).
    Windows can overlap.
    """

    window_size: MeasurementUnit
    window_slide: MeasurementUnit


@dataclass
class TumblingWindow(Window[MeasurementUnit]):
    """
    A fixed-size window with no overlap.
    Size is in terms of number of elements (an integer),
    or in terms of event time (timedelta).
    """

    window_size: MeasurementUnit

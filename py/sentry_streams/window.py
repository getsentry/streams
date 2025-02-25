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
class SlidingCountWindow(Window[MeasurementUnit]):
    """
    A sliding window which is configured
    by counts. Size and slide are both
    in terms of number of elements.

    The window slide determines how
    frequently a window is started. (e.g.
    every 10 elements). Windows can
    overlap.
    """

    window_size: MeasurementUnit
    window_slide: MeasurementUnit


@dataclass
class SlidingEventTimeWindow(Window[MeasurementUnit]):
    """
    A sliding window where size and slide
    are both in terms of event time.

    The window slide determines how
    frequently a window is started. (
    e.g. every 5 minutes). Windows
    can overlap.
    """

    window_size: MeasurementUnit
    window_slide: MeasurementUnit


@dataclass
class TumblingCountWindow(Window[MeasurementUnit]):
    """
    A fixed-size window with no overlap.
    Size is in terms of number of elements.
    """

    window_size: MeasurementUnit


@dataclass
class TumblingEventTimeWindow(Window[MeasurementUnit]):
    """
    A fixed-size window with no overlap.
    Size is in terms of event time passed.
    """

    window_size: MeasurementUnit

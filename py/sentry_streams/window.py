from dataclasses import dataclass
from datetime import timedelta


@dataclass
class Trigger:
    """
    A generic representation of a window trigger.
    The trigger defines when a window is marked
    as completed in order to fire a function
    or computation on the window.
    """


@dataclass
class EventTimeTrigger(Trigger):
    """
    Triggers based on progress of
    event time of event data. Once the
    watermark exceeds the end of the
    window, window function is triggered.
    """


@dataclass
class CountingTrigger(Trigger):
    """
    Specifically triggers window functions
    (for example, aggregates) when the number
    of elements in a window exceeds the count.
    """

    count: int


@dataclass
class IntervalTrigger(Trigger):
    """
    Triggers continuously
    based on intervals. In this
    case, event time watermarks
    define the interval boundaries.
    """

    interval: timedelta


@dataclass
class Window:
    """
    A generic representation of a Window.
    Each Window can have a trigger plugged in.
    """

    trigger: Trigger


@dataclass
class SlidingCountWindow(Window):
    """
    A sliding window which is configured
    by counts. Size and slide are both
    in terms of number of elements.

    The window slide determines how
    frequently a window is started. (e.g.
    every 10 elements). Windows can
    overlap.
    """

    window_size: int
    window_slide: int


@dataclass
class SlidingEventTimeWindow(Window):
    """
    A sliding window where size and slide
    are both in terms of event time.

    The window slide determines how
    frequently a window is started. (
    e.g. every 5 minutes). Windows
    can overlap.
    """

    window_size: timedelta
    window_slide: timedelta


@dataclass
class TumblingCountWindow(Window):
    """
    A fixed-size window with no overlap.
    Size is in terms of number of elements.
    """

    window_size: int


@dataclass
class TumblingEventTimeWindow(Window):
    """
    A fixed-size window with no overlap.
    Size is in terms of event time passed.
    """

    window_size: timedelta

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
    The default window trigger.
    """


@dataclass
class CountingTrigger(Trigger):
    count: int


@dataclass
class IntervalTrigger(Trigger):
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
    window_size: int
    window_slide: int


@dataclass
class SlidingEventTimeWindow(Window):
    window_size: timedelta
    window_slide: timedelta


@dataclass
class TumblingCountWindow(Window):
    window_size: int


@dataclass
class TumblingEventTimeWindow(Window):
    window_size: timedelta

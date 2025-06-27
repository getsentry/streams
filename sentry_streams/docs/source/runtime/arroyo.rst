Arroyo Runtime
=================

The Arroyo runtime uses the `getsentry/arroyo <https://github.com/getsentry>`_ library as its runtime backend.

Docs for Arroyo can be found here: https://getsentry.github.io/arroyo/

The ``arroyo`` adapter which translates the pipeline into a series of arroyo steps can be found here:
https://github.com/getsentry/streams/tree/main/sentry_streams/sentry_streams/adapters/arroyo.

====================
Rust Arroyo Runtime
====================

The Rust Arroyo runtime operates similarly to the standard Arroyo runtime,
but uses the rust version of arroyo: https://github.com/getsentry/arroyo/tree/main/rust-arroyo.

The ``rust_arroyo`` adapter can be found here: https://github.com/getsentry/streams/tree/main/sentry_streams/src.

=======
Routes
=======
The streaming platform has multiple steps which can 'route' message to different downstream branches:

- the `Router <https://github.com/getsentry/streams/blob/4808eb17863e296d76800cc0d12aca82bddc4509/sentry_streams/sentry_streams/pipeline/pipeline.py#L305-L320>`_ step forwards messages to one of several given downstream steps
- the `Broadcast <https://github.com/getsentry/streams/blob/4808eb17863e296d76800cc0d12aca82bddc4509/sentry_streams/sentry_streams/pipeline/pipeline.py#L324-L335>`_ step sends a copy of a message to each downstream step

However, arroyo has no concept of branching routes like this. In arroyo, each step in a pipeline is
sequential and messages visit every step in order. To implement downstream branches, the first step in every
pipeline wraps the message in a ``RoutedValue`` class.

The ``RoutedValue`` contains a ``Route`` object, which consists of a ``source`` (indicating the source step of the message) and a list of ``waypoints``,
which represent the downstream 'branch' that a message should be processed by. Each step in the arroyo pipeline also
has a corresponding ``Route`` object. At the start of each arroyo step, the step checks if the message ``Route`` matches the step's ``Route``.
If they match, the message is processed by the step, otherwise the message is forwarded to the next step in the pipeline without processing.

The arroyo ``Router`` step adds the value returned by the routing function to a message's ``waypoints`` list.
The arroyo ``Broadcast`` step creates a copy of a message for each downstream branch, and adds the corresponding
branch's name to that message's ``waypoints`` list.

============
Watermarks
============
Introducing the concept of downstream branches into arroyo breaks the standard commit policy.
Normally, arroyo will commit the offsets of messages which reach the end of the pipeline once per second.
However, since a ``Broadcast`` step creates multiple copies of a message, we can't commit the offset of a message
as soon as the commit step receives it because the message isn't fully processed until *each copy* of the message
reaches the commit step.

To solve this, we introduce Watermarks - internal messages which are regularly emitted into the pipeline (by default
a watermark is send every 10 seconds).
Each watermark message contains the combined `committable <https://getsentry.github.io/arroyo/strategies/index.html#arroyo.types.Message.committable>`_
of all messages which were picked up by the consumer since the last watermark message was sent.

The end step of a consumer created by the arroyo runtime will be a custom commit step which commits the offsets
stored in a watermark once it has received ``N`` copies of that watermark, where ``N`` is the number of possible
branches in the pipeline.
For example, if a pipeline contains a ``Router`` which routes messages to one of 2 downstream branches, then
in one of those branches there is a ``Broadcast`` step that forwards messages to 3 downstream branches, the commit
offsets step will only commit the offsets stored in a watermark after it receives a total of 4 copies of that
watermark::

    Source
      |
  ┎-Router-┓
  |        |
  1   ┎Broadcast┓
      |    |    |
      2    3    4

*Fig 1: Example pipeline with branching routes. One watermark from each route is required to commit.*

Watermark messages are sent by a ``WatermarkEmitter`` step, which is automatically added at the start of a pipeline.
By default watermark messages are emitted every 10 seconds.

Currently, watermarks only exist in the ``rust_arroyo`` runtime and are a work in progress.

Most steps in the pipeline immediately forward watermarks to the next step in the pipeline, but some
steps have special beahviour when they encounter a watermark:

- a ``Broadcast`` step will duplicate a watermark message for each downstream branch (the same as it does when
  it sees a normal message)
- when a ``Router`` step encounters a watermark message, it has the same behaviour as the ``Broadcast`` step
  (submits a copy of the watermark for each downstream branch)

  - this is because we don't know which downstream branches have received messages since the last watermark,
    so we just send a watermark to all of them
- a ``Reduce`` step will store the committables of all received watermark messages until the reduce window closes,
  after which it will forward the combined committables of all watermarks received
- in ``rust-arroyo``, the ``PythonAdapter`` step will move the watermark from a rust ``Watermark`` struct into
  a ``PyWatermark`` python class, after which the internal Python step will handle the watermark
- TODO: how should a multiprocess step handle a watermark message?

===================
Watermark Progress
===================
Watermarks are still a work in progress. Currently they are only being implemented for the ``rust_arroyo``
adapter, with Python arroyo support coming later.

Current progress:

☑ ``WatermarkEmitter`` step sends watermarks downstream to the rest of the pipeline

☑ Watermark messages are sent with the committable of each message consumed since the last watermark message

☑ Watermarks can be passed into Python code via a ``PythonAdapter`` step

☐ An ``Unfold`` step needs to be implemented for ``rust_arroyo`` which will take in a single message and
submit multiple messages (needed for broadcast/router/flatmap)

☐ ``Reduce`` and ``Multiprocess`` need to handle watermark messages

☐ The ``Router`` step needs to be rewritten to use the above ``Unfold`` step instead of just being an arroyo
``RunTask``

☐ The custom commit step that commits based on # of watermarks received needs to be implemented (for now,
the arroyo runtime uses the standard once-per-second commit step)

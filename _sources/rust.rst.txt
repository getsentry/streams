Rust applications
=================

Hybrid applications
-------------------

PR: https://github.com/getsentry/streams/pull/177

**User story:** I want to rewrite a pipeline step in getsentry monolith
in Rust.

Currently Rust-ification within the monolith is being done by adding new
pyo3-based Python dependencies to getsentry’s requirements. We’ll go the
same path, users can define pipeline steps using pyo3, but using our
helper functions/”framework.”

Here is how a function definition works:

.. code:: rust

   // mypackage/src/lib.rs
   sentry_streams::rust_function!(
       RustTransformMsg,
       IngestMetric,
       TransformedIngestMetric,
       |msg: Message<IngestMetric>| -> TransformedIngestMetric {
           let (payload, _) = msg.take();
           TransformedIngestMetric {
               metric_type: payload.metric_type,
               name: payload.name,
               value: payload.value,
               tags: payload.tags,
               timestamp: payload.timestamp,
               transformed: true,
           }
       }
   );

This would be packaged up in a pyo3-based crate, and then can be
referenced from the regular pipeline definition like this:

.. code:: python

   .apply(Map("transform", function=my_package.RustTransformMsg()))

Message payloads
~~~~~~~~~~~~~~~~

``IngestMetric`` and ``TransformedIngestMetric`` types have to be
defined by the user in both Rust and Python.

.. code:: rust

   // mypackage/src/lib.rs
   #[derive(Serialize, Deserialize)
   struct IngestMetric { ... }

.. code:: python

   class IngestMetric(TypedDict): ...

The user has to write their own Python ``.pyi`` stub file to declare
that ``RustTransformMsg`` takes ``IngestMetric`` and returns
``TransformedIngestMetric``:

.. code:: python

   # mypackage/mypackage.pyi
   class RustTransformMsg(RustFunction[IngestMetric, Any]):
       def __init__(self) -> None: ...
       def __call__(self, msg: Message[IngestMetric]) -> Any: ...

Then, the user has to define how conversion works between these types.
They can implement this function manually, or use a builtin conversion
method provided by us. We currently only provide one builtin conversion
by round-tripping via JSON:

.. code:: rust

   // mypackage/src/lib.rs
   sentry_streams::convert_via_json!(IngestMetric);

…and the same procedure has to be repeated for the output type
``TransformedIngestMetric``.

What happens at runtime
~~~~~~~~~~~~~~~~~~~~~~~

The ``rust_function`` macro currently just generates a simple Python
function for the given Rust function. The GIL *is* released while the
user’s Rust code is running, but there is still some GIL overhead when
entering and exiting the function.

In the future we can transparently optimize this without users having to
change their applications. For example, batching function calls to
amortize GIL overhead. We would then only hold the GIL while entering
and exiting the batch.

What we want to improve in the future
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- improve performance of calling convention/reduce overhead

  - take inspiration from
    https://github.com/ealmloff/sledgehammer_bindgen

- automatically generate type stubs for user’s Rust code — pyo3 does
  have something like that, but it doesn’t work perfectly (exposes
  internals of our Rust macro)
- improve ergonomics of message types and their conversion, add protobuf
  or msgpack as a way to roundtrip
- each team at sentry would have to maintain a new python package for
  their Rust functions, set up pyo3 and CI from scratch, etc. we can
  streamline this.

  - we already have: ``sentry_relay`` (relay integration), ``ophio``
    (grouping engine), ``vroomrs`` (profiles), ``symbolic`` (stacktrace
    processing)
  - easiest: we provide a “monorepo” and “monopackage” where all rust
    functions for getsentry go. we maintain CI for this monorepo.
  - medium: repository template
  - also, ideally this is aligned with devinfra’s “golden path” for
    python devenv
  - in practice some team will have to provide support for questions
    about pyo3, since its entire API surface is exposed to product teams
    (although we can templatize and abstract a lot)

Pure-Rust pipelines
-------------------

A lot of the complexity mentioned above is only really necessary for
when you want to mix Python and Rust code. For pure-Rust applications,
we could do something entirely different:

- The runner does not have to be started from Python at all. If we
  started it from Rust, we would have a much easier time optimizing
  function calls.
- The pipeline definition does not have to be Python. We could have it
  be YAML or even Rust as well.
- Type stubs are not really necessary. We can easily validate that the
  types match during startup, or if the pipeline definition is in Rust,
  let the compiler do that job for us.

Any of these will however split the ecosystem. I think we have plenty of
ergonomic improvements we can make even for hybrid applications, that
would benefit pure-Rust users as well. We should focus on those first.

Meeting notes July 24, 2025
===========================

- a better pure-rust story

  - we have too much boilerplate, and now especially for pure rust apps
  - build a rust runner, and try to get rid of as much pyo3 junk as
    possible

    - reference: `The rust arroyo
      runtime <https://www.notion.so/The-rust-arroyo-runtime-2228b10e4b5d806dbe9ccd4e70c93aa2?pvs=21>`__

  - maybe hybrid will get better through this rearchitecture
  - maybe denormalize Parse steps into Map (@Filippo Pacifici)

  .. code:: rust


     // mypackage/src/lib.rs as pyo3
     use sentry_streams;

     sentry_streams::rust_function!(...);

     sentry_streams::main_function!();

     // or, in bin target:
     pub use sentry_streams::main;

  .. code:: python


     mypackage.run_streams()

  concerns:

  - user can freely downgrade/upgrade verison, since they “own” the
    runtime (as they are statically linking it)
  - ability to opt out of message conversion trait requirements

- message type conversion

  - boilerplate is an issue

    - integration with existing schema repos, or copy schema-to-type
      generation into streams for “inline schemas”

  - better performance

- better runtime semantics for rust functions

  - map chains, but in rust?
  - no multiprocessing!

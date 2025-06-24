Docker Setup
===========

This section describes how to use Docker to run Sentry Streams applications using a multi-stage build.

Building the Image
-----------------

Build the development image (includes py-spy for profiling):

.. code-block:: bash

   docker build -t sentry-streams:dev --target dev .

Build the base image (production dependencies only):

.. code-block:: bash

   docker build -t sentry-streams:base --target base .

Usage
-----

Using the pyspy_runner (dev stage - default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The development stage includes py-spy and is the default target:

.. code-block:: bash

   # Build and run with profiling
   docker build -t sentry-streams --target dev .
   docker run --rm sentry-streams profile.svg -n SimpleMap --adapter rust_arroyo --config config.yaml app.py

   # Example from kafkaload.txt
   docker run --rm sentry-streams profile.svg \
     -n SimpleMap \
     --adapter rust_arroyo \
     --config deployment_config/simple_map_filter.yaml \
     examples/simple_map_filter.py

Using the regular runner (base stage)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To run without profiling, use the base stage:

.. code-block:: bash

   # Build base image
   docker build -t sentry-streams:base --target base .

   # Run without profiling (no py-spy available)
   docker run --rm --entrypoint bin/runner sentry-streams:base \
     -n SimpleMap \
     --adapter rust_arroyo \
     --config deployment_config/simple_map_filter.yaml \
     examples/simple_map_filter.py

Interactive shell
~~~~~~~~~~~~~~~~

For development or debugging:

.. code-block:: bash

   # Dev stage with all tools
   docker run --rm -it --target dev sentry-streams /bin/bash

   # Base stage (production only)
   docker run --rm -it --target base sentry-streams /bin/bash

Available Scripts
----------------

- ``bin/pyspy_runner`` - Runs the application with py-spy profiling (dev stage only)
- ``bin/runner`` - Runs the application directly without profiling (both stages)

Build Stages
-----------

Base Stage
~~~~~~~~~~

- Production Python dependencies only
- Rust toolchain installed via rustup
- Rust code built and installed
- Smaller image size
- No profiling tools

Dev Stage
~~~~~~~~~

- Includes all dev dependencies (py-spy, pytest, mypy, etc.)
- Built on top of base stage
- Larger image size
- Full development environment

Notes
-----

- The base stage uses ``uv sync --frozen --no-dev`` for production dependencies only
- The dev stage uses ``uv sync --frozen --group dev`` to add development dependencies
- Rust toolchain is installed using rustup in the base stage
- Rust code is built in the base stage and available in both stages
- The dev stage is the default target when building without specifying ``--target``

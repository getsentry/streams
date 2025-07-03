How to set up the development environment
=========================================

This guide helps you to set up the development environment and troubleshoot common problems.

.. warning::

   There is a lot of work to be done to automate the creation of the development
   environment for sentry_streams. Work is on going for this. At present some parts cannot
   be validated automatically.

Requirements
------------

This repo contains two libraries: `sentry_flink` and `sentry_streams`. We will focus on
`sentry_streams` as it is the only one that is packaged and released.

The `sentry_streams` library contains both Python and Rust code. Everything is packaged as
a Python package that contains also a native library (the rust one).

In order to have a working development environment, you need to have both a python venv
and a rust toolchain. The Rust library is built via `maturin` and the python library is,
at the time of writing, managed via `uv`.

This is what we require:

- Python >= 3.11
- Rust >= 1.83.0
- direnv
- cmake >= 3.5 (needed to build librdkafka)

Set up the environment
----------------------

1. Allow direnv to work on the project root. We use direnv to manage multiple environment
   variables to make maturin and pyo3 work. Without those env vars set you will not
   be able to build the library or run tests.

2. Install the rust tool chain with `rustup <https://rustup.rs/>`_

3. Have python >= 3.11 installed.

  a. MacOs note: There are multiple ways to install the python environment on MacOS.
     We tested successfully the installation via `brew`. Other ways, like `pyenv` and
     `uv`, should work as well but we saw incompatibilities with the way `maturin` expects
     environment variables to be set. See below for more details if you want to use
     `pyenv` or `uv` to install python.

4. Run `make install-dev` from the root of the repo. This will instal `uv` and build
   both `sentry_streams` and `sentry_flink`.

This should be it. After this try to run python tests `make tests-streams` and the
Rust ones: `cargo test`. If something fails here, your environment does not work.
Read below to fix it.

If you need to clean everything up and restart: `make reset`.


What can go wrong
-----------------

In this repo we both call Rust code from Python with pyO3 and call Python code from Rust.
This has a number of implications:

- The Rust library needs to compile successfully for the Python code to run and the
  tests to succeed.

- The Rust code uses the already started Python interpreter when we run the pipeline or
  when we run Python tests. This case is generally not problematic, no matter how the
  virtual environment is configured, as long as Python tests can run, the rust code can
  call the Python code.

- The Rust tests need to start a Python interpreter when we test parts of the Rust code
  that call into Python code. This is where things can break depending on how the interpreter
  is installed and the venv is setup.

Some environment variables are needed to successfully build the library. These are set by direnv.
They are explained here to give a better understanding in case of issues.

- `CMAKE_POLICY_VERSION_MINIMUM=3.5`` - This is needed to build librdkafka on
  newer versions of cmake

Needed environment variables to allow Rust to start a Python interpreter properly:

- `PYTHONPATH=.` - This is needed in order to make the Python interpreter started by
  Rust find the virtual environment.

- `PYO3_PYTHON` - This is needed to make the Python interpreter find the right `site-packages`
  directory when started by the Rust code. See `rust-envvars <https://github.com/getsentry/streams/blob/main/scripts/rust-envvars>`_


Common Errors
-------------

These are common problems we observed

`sys.path` does not contain the venv `site-packages` directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

      panic message: `"Unable to import: PyErr { type: <class 'ModuleNotFoundError'>, value: ModuleNotFoundError(\"No module named 'sentry_kafka_schemas'\"),
 traceback: Some(\"Traceback (most recent call last):
   File \\\"<string>\\\", line 1, in <module>
   File \\\"/Users/filippopacifici/code/streams/sentry_
  streams/sentry_streams/pipeline/__init__.py\\\", line 1, in <module>
    from sentry_streams.pipeline.chain import (
    File \\\"/Users/filippopacifici/co
  de/streams/sentry_streams/sentry_streams/pipeline/chain.py\\\", line 29, in <module>
    from sentry_streams.pipeline.msg_codecs import (
    File \\\"/Use
  rs/filippopacifici/code/streams/sentry_streams/sentry_streams/pipeline/msg_codecs.py\\\", line 6, in <module>
    from sentry_kafka_schemas import get_codec\\n\") }"`


Any other errors where the sentry_streams python code fails to import a
3rd party package points to the same issue.

It means that the interpreter started by Rust is able to find the the project
code as it fails when the sentry_streams code tries to import 3rd parties.

This should only happen when Rust starts the python interpreter (tests).

We need to manually set the `sys.path` variable in the Python interpreter when
Rust starts it. This is done by this code `testutils.rs <https://github.com/getsentry/streams/blob/main/sentry_streams/src/testutils.rs#L140-L141>`_.
`STREAMS_TEST_PYTHONEXECUTABLE` and `STREAMS_TEST_PYTHONPATH` have to be set.

Cannot find Python standard library packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This generally presents itself as not being able to load some standard library
package.

.. code-block:: bash

  Could not find platform independent libraries <prefix>
  Could not find platform dependent libraries <exec_prefix>
  Python path configuration:
    PYTHONHOME = (not set)
    PYTHONPATH = '.'
    program name = 'python3'
    isolated = 0
    environment = 1
    user site = 1
    safe_path = 0
    import site = 1
    is in build tree = 0
    stdlib dir = '/install/lib/python3.11'
    sys._base_executable = '/Users/untitaker/projects/streams/sentry_streams/target/debug/deps/rust_streams-7b3bb705f1a0bf53'
    sys.base_prefix = '/install'
    sys.base_exec_prefix = '/install'
    sys.platlibdir = 'lib'
    sys.executable = '/Users/untitaker/projects/streams/sentry_streams/target/debug/deps/rust_streams-7b3bb705f1a0bf53'
    sys.prefix = '/install'
    sys.exec_prefix = '/install'
    sys.path = [
      '/Users/untitaker/projects/streams/sentry_streams',
      '/install/lib/python311.zip',
      '/install/lib/python3.11',
      '/install/lib/python3.11/lib-dynload',
    ]
  Fatal Python error: init_fs_encoding: failed to get the Python codec of the filesystem encoding
  Python runtime state: core initialized
  ModuleNotFoundError: No module named 'encodings'


The `PYTHON_HOME` environment variables should be preventing this and it is
set by direnv. In case of `uv` installed python `.python-version` should make
the environment point to the right version.

Still we saw this some times with `uv` installed python. Check which python version
`uv` is using via `uv python list`. Verify that the one you are using is referring
to a directory that exists.

If the problem persists consider not using a python interpreter not installed by
`uv`.

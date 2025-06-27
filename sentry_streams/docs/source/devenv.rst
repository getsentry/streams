How to set up the development environment
=========================================




Erroros
=======

If you see this

```
      panic message: `"Unable to import: PyErr { type: <class 'ModuleNotFoundError'>, value: ModuleNotFoundError(\"No module named 'sentry_kafka_schemas'\"),
 traceback: Some(\"Traceback (most recent call last):\\n  File \\\"<string>\\\", line 1, in <module>\\n  File \\\"/Users/filippopacifici/code/streams/sentry_
streams/sentry_streams/pipeline/__init__.py\\\", line 1, in <module>\\n    from sentry_streams.pipeline.chain import (\\n  File \\\"/Users/filippopacifici/co
de/streams/sentry_streams/sentry_streams/pipeline/chain.py\\\", line 29, in <module>\\n    from sentry_streams.pipeline.msg_codecs import (\\n  File \\\"/Use
rs/filippopacifici/code/streams/sentry_streams/sentry_streams/pipeline/msg_codecs.py\\\", line 6, in <module>\\n    from sentry_kafka_schemas import get_code
c\\n\") }"`
```

```
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
```

- uninstall all uv installed python and keep those installed by brew
- the uv ones have .local/share/uv in the path
- uv installed ones cannot find the standard library via PYTHON_HOME



Move .python-version

Check sys.path

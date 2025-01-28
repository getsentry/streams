# streams

The Sentry Streaming Platform

# Run pyFlink application locally.

PyFlink application can run with an embedded flink without having to run
the Flink Server in a stand alone way. Just run the application in the
Python interpreter.

Run `direnv allow` and run `python py/sentry_streams/example.py`.

We need a better way to do this, but at this point of the project it
is alright.

Alternatives for the dev environment:

- Package a docker image with flink and the libraries. Run it as a
  devservice in session mode and deploy the application when testing
  them.
- Package the jars in the python package

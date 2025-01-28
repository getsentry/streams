# streams

The Sentry Streaming Platform

# Run pyFlink application locally.

PyFlink application can run with an embedded flink without having to run
the Flink Server in a stand alone way. Just run the application in the
Python interpreter.

There is an example in `py/sentry_streams/example.py`. Just run this
file. .... And things will fail.

You need the Kafka connector jar which is not part of the Flink
distribution. There is a script `flink-jar-download.sh` in the
scripts directory that downloads the JAR you need from the mvn registry
and puts them in the `flink_libs` directory.

Run the script. Then you just need to run the example in the sentry_streams
package while passing the `FLINK_LIBS` environment variable pointing to
the libs directory:

`FLINK_LIBS=~/code/streams/flink_libs python sentry_streams/example.py`

We need a better way to do this, but at this point of the project it
is alright.

Alternatives for the dev environment:

- Package a docker image with flink and the libraries. Run it as a
  devservice in session mode and deploy the application when testing
  them.
- Package the jars in the python package

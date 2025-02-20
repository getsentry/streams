# streams

The Sentry Streaming Platform

# Run pyFlink application locally.

PyFlink application can run with an embedded flink without having to run
the Flink Server in a stand alone way. Just run the application in the
Python interpreter.

Run `direnv allow`.

See [here](./platforms/flink/README.md) for the steps to run Flink in a container.

Run

```
docker exec -it kafka \
    kafka-topics \
    --bootstrap-server localhost:29092 \
    --topic events \
    --create
```

Run `echo hello world | kcat -P -b 127.0.0.1:9092 -t events` to send some events and see them printed.

If you have Java installed you can skip the container and just run the
application python file directly. That will start Flink.

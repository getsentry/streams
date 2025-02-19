# Apache Flink Development Environment

Our streaming API supports Flink. Flink runs on Java. We do not want to require
every engineer who wants to run a streaming application locally to install a JDK.

This directory contains everything needed to run streaming applications on a
Flink container without needing Java locally on the developer's laptop.
The container packages the Flink distribution, the Kafka connector and the
python dependencies the application needs. It also mounts a local directory
containing the application code to run so that application changes do not require
Docker image to be rebuilt.

## How to use it

This guide explains how to set up a running Flink and deploy an application built
inside the sentry_streams python projects. In order to use this in a different
code base a different image must be built.

1. Create a virtual environment with `make install-dev` (the Makefile is in the
   root of the project). Activate the environment.

2. Ensure you do not have a Kafka running. If you have one stop it.

3. Start the docker-compose bundle in `platforms/flink` and make it rebuild the image:
   `docker-compose up --build`. Add `-d` if you want to run docker compose in
   detached mode.

4. Verify that all containers are running:

```
flink-taskmanager-1
jobmanager
kafka
```

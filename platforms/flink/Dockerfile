ARG FLINK_VERSION=1.20

FROM flink:${FLINK_VERSION} AS build_base

ARG FLINK_VERSION=1.20
ARG KAFKA_CONNECTOR_VERSION=3.4.0
ARG PYTHON_VERSION=3.11

RUN echo Python version ${PYTHON_VERSION}

# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y python${PYTHON_VERSION} python3-pip python${PYTHON_VERSION}-distutils && \
    rm -rf /var/lib/apt/lists/*


ENV FILE_NAME=flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-${FLINK_VERSION}.jar

RUN curl -o /opt/flink/lib/${FILE_NAME} \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}-${FLINK_VERSION}/${FILE_NAME}


FROM build_base
# Adds the dependencies for the application we want to use this image for.
# TODO: Invert the dependencies. Add Flink to the image of the application
# that we need to run in Flink. This would be less disruptive for the systems
# that need to run streaming applications now that we are still testing
# platforms.
ARG REQUIREMENTS=requirements.txt
COPY ${REQUIREMENTS} ./
RUN python3.11 -m pip install -r requirements.txt

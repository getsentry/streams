ARG FLINK_VERSION=1.20

FROM flink:${FLINK_VERSION} AS build_base

ARG FLINK_VERSION=1.20
ARG KAFKA_CONNECTOR_VERSION=3.4.0
ARG PYTHON_VERSION=3.11

# The upstream flink image does not contain the JDK but the JRE only.
# In order to run pyflink we need the JDK ... so we get a JDK.
RUN rm -rf $JAVA_HOME
RUN /bin/sh -c set -eux; ARCH="$(dpkg --print-architecture)"; \
    case "${ARCH}" in  \
    aarch64|arm64) ESUM='8c3146035b99c55ab26a2982f4b9abd2bf600582361cf9c732539f713d271faf'; BINARY_URL='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.26%2B4/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.26_4.tar.gz'; \
    ;; amd64|i386:x86-64) ESUM='60ea98daa09834fdd3162ca91ddc8d92a155ab3121204f6f643176ee0c2d0d5e'; BINARY_URL='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.26%2B4/OpenJDK11U-jdk_x64_linux_hotspot_11.0.26_4.tar.gz'; \
    ;; armhf|arm) ESUM='a64b005b84b173e294078fec34660ed3429d8c60726a5fb5c140e13b9e0c79fa'; BINARY_URL='https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.26%2B4/OpenJDK11U-jdk_arm_linux_hotspot_11.0.26_4.tar.gz'; \
    ;; *) echo "Unsupported arch: ${ARCH}"; exit 1; \
    ;; esac; \
    wget --progress=dot:giga -O /tmp/openjdk.tar.gz ${BINARY_URL}; \
    echo "${ESUM} */tmp/openjdk.tar.gz" | sha256sum -c -; \
    mkdir -p "$JAVA_HOME"; \
    tar --extract --file /tmp/openjdk.tar.gz --directory "$JAVA_HOME" --strip-components 1 --no-same-owner ; \
    rm -f /tmp/openjdk.tar.gz ${JAVA_HOME}/lib/src.zip; \
    find "$JAVA_HOME/lib" -name '*.so' -exec dirname '{}' ';' | sort -u > /etc/ld.so.conf.d/docker-openjdk.conf; \
    ldconfig; \
    java -Xshare:dump;

RUN echo Python version ${PYTHON_VERSION}
# install python3 and pip3
RUN apt-get update -y && \
    apt-get install -y \
    python${PYTHON_VERSION} \
    python3-pip python${PYTHON_VERSION}-distutils \
    python${PYTHON_VERSION}-dev \
    jq && \
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
RUN python3.11 -m pip install yq
ARG PY_PROJECT=sentry_flink/pyproject.toml
COPY ${PY_PROJECT} ./
RUN tomlq -r '.project.dependencies[] | select(.path | not)' pyproject.toml | jq -r '.' > ./requirements.txt
RUN cat ./requirements.txt
RUN python3.11 -m pip install -r ./requirements.txt

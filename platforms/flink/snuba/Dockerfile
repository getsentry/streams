FROM getsentry/snuba:nightly AS build_base

USER root

RUN set -ex; \
    \
    buildDeps=' \
    curl \
    wget \
    gnupg \
    fontconfig \
    tzdata \
    locales \
    '; \
    apt-get update; \
    apt-get install -y $buildDeps --no-install-recommends


ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

ENV JAVA_VERSION=jdk-11.0.26+4

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

RUN set -eux; \
    echo "Verifying install ..."; \
    fileEncoding="$(echo 'System.out.println(System.getProperty("file.encoding"))' | jshell -s -)"; [ "$fileEncoding" = 'UTF-8' ]; rm -rf ~/.java; \
    echo "javac --version"; javac --version; \
    echo "java --version"; java --version; \
    echo "Complete."

FROM build_base AS build_flink

# Install dependencies
RUN set -ex; \
    apt-get update; \
    apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev; \
    rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -ex; \
    wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
    wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
    export GNUPGHOME="$(mktemp -d)"; \
    for server in ha.pool.sks-keyservers.net $(shuf -e \
    hkp://p80.pool.sks-keyservers.net:80 \
    keyserver.ubuntu.com \
    hkp://keyserver.ubuntu.com:80 \
    pgp.mit.edu) ; do \
    gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
    done && \
    gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
    chmod +x /usr/local/bin/gosu; \
    gosu nobody true

# Configure Flink version
ENV FLINK_TGZ_URL=https://dlcdn.apache.org/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz \
    FLINK_ASC_URL=https://downloads.apache.org/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz.asc \
    GPG_KEY=5575E80D59BBB73C15A479B88C1FC56D16B0029D \
    CHECK_GPG=true

# Prepare environment
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink
WORKDIR $FLINK_HOME

# Install Flink
RUN set -ex; \
    wget -nv -O flink.tgz "$FLINK_TGZ_URL"; \
    \
    if [ "$CHECK_GPG" = "true" ]; then \
    wget -nv -O flink.tgz.asc "$FLINK_ASC_URL"; \
    export GNUPGHOME="$(mktemp -d)"; \
    for server in ha.pool.sks-keyservers.net $(shuf -e \
    hkp://p80.pool.sks-keyservers.net:80 \
    keyserver.ubuntu.com \
    hkp://keyserver.ubuntu.com:80 \
    pgp.mit.edu) ; do \
    gpg --batch --keyserver "$server" --recv-keys "$GPG_KEY" && break || : ; \
    done && \
    gpg --batch --verify flink.tgz.asc flink.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" flink.tgz.asc; \
    fi; \
    \
    tar -xf flink.tgz --strip-components=1; \
    rm flink.tgz; \
    \
    chown -R flink:flink .; \
    \
    # Replace default REST/RPC endpoint bind address to use the container's network interface \
    CONF_FILE="$FLINK_HOME/conf/flink-conf.yaml"; \
    if [ ! -e "$FLINK_HOME/conf/flink-conf.yaml" ]; then \
    CONF_FILE="${FLINK_HOME}/conf/config.yaml"; \
    /bin/bash "$FLINK_HOME/bin/config-parser-utils.sh" "${FLINK_HOME}/conf" "${FLINK_HOME}/bin" "${FLINK_HOME}/lib" \
    "-repKV" "rest.address,localhost,0.0.0.0" \
    "-repKV" "rest.bind-address,localhost,0.0.0.0" \
    "-repKV" "jobmanager.bind-host,localhost,0.0.0.0" \
    "-repKV" "taskmanager.bind-host,localhost,0.0.0.0" \
    "-rmKV" "taskmanager.host=localhost"; \
    else \
    sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' "$CONF_FILE"; \
    sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' "$CONF_FILE"; \
    sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' "$CONF_FILE"; \
    sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' "$CONF_FILE"; \
    sed -i '/taskmanager.host: localhost/d' "$CONF_FILE"; \
    fi;

ARG FLINK_VERSION=1.20
ARG KAFKA_CONNECTOR_VERSION=3.4.0


ENV FILE_NAME=flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-${FLINK_VERSION}.jar

RUN curl -o /opt/flink/lib/${FILE_NAME} \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}-${FLINK_VERSION}/${FILE_NAME}


# Configure container
COPY ./platforms/flink/snuba/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]

FROM build_flink
# Adds the dependencies for the application we want to use this image for.
# TODO: Invert the dependencies. Add Flink to the image of the application
# that we need to run in Flink. This would be less disruptive for the systems
# that need to run streaming applications now that we are still testing
# platforms.
RUN apt-get update -y && \
    apt-get install -y \
    jq && \
    rm -rf /var/lib/apt/lists/*

RUN python3.11 -m pip install yq
ARG PY_PROJECT=py/pyproject.toml
COPY ${PY_PROJECT} ./
RUN tomlq -r '.project.dependencies' pyproject.toml | jq -r '.[]' > ./requirements.txt
RUN cat ./requirements.txt
RUN python3.11 -m pip install -r ./requirements.txt

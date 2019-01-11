FROM maven:3.5.4-jdk-8 as build

ENV GRADLE_USER_HOME=/home/gradle/.gradle_novolume

ENV project=connect \
    version=5.1.0-sunbit \
    src=/home/gradle/src

# Work around permissions issues, so relevant files are writable by us:
# https://github.com/moby/moby/issues/6119
RUN mkdir -p ${src} \
    ${src}/${project}/build

WORKDIR ${src}


COPY gradle/ gradle/
COPY gradlew gradle.properties wrapper.gradle ./

COPY build.gradle settings.gradle ./

# Required for build
COPY checkstyle/ checkstyle/
COPY config/ config/

# Dependencies for build
COPY bin/ bin/
COPY clients/ clients/
COPY connect/ connect/
COPY core/ core/
COPY streams/ streams/
COPY tools/ tools/

RUN ./gradlew -s --no-daemon :connect:runtime:build

###############

# We need a glibc-based Alpine build since RocksDB's JNI libs depend on glibc.
FROM anapsix/alpine-java:8u192b12_server-jre_unlimited

ENV project=connect \
    version=5.1.0-sunbit \
    target=/home/gradle/src

# Dependencies
COPY --from=build ${target}/config /etc/kafka

RUN mkdir -p /opt/kafka \
    && ln -sf /etc/kafka /opt/kafka/config

COPY --from=build ${target}/bin /opt/kafka/bin

COPY --from=build ${target}/clients/build/libs /opt/kafka/clients/build/libs

COPY --from=build ${target}/connect/runtime/build/libs /opt/kafka/connect/runtime/build/libs
COPY --from=build ${target}/connect/runtime/build/dependant-libs /opt/kafka/connect/runtime/build/dependant-libs

COPY --from=build ${target}/connect/api/build/libs /opt/kafka/connect/api/build/libs
COPY --from=build ${target}/connect/api/build/dependant-libs /opt/kafka/connect/api/build/dependant-libs

COPY --from=build ${target}/connect/json/build/libs /opt/kafka/connect/json/build/libs
COPY --from=build ${target}/connect/json/build/dependant-libs /opt/kafka/connect/json/build/dependant-libs

COPY --from=build ${target}/connect/transforms/build/libs /opt/kafka/connect/transforms/build/libs
COPY --from=build ${target}/connect/transforms/build/dependant-libs /opt/kafka/connect/transforms/build/dependant-libs
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG jdk_version
FROM $jdk_version AS build-native-image

WORKDIR /build

COPY native/ native

ARG KAFKA_MODE
ARG GRAALVM_URL="https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-21.0.1/graalvm-community-jdk-21.0.1_linux-aarch64_bin.tar.gz"

ENV NATIVE_IMAGE_PATH="/build/graalvm/bin/native-image"
ENV NATIVE_CONFIGS_DIR="/build/native/native-image-configs"
ENV KAFKA_LIBS_DIR="/build/kafka/libs"
ENV KAFKA_BIN_DIR="/build/kafka-binary"
ENV TARGET_PATH="$KAFKA_BIN_DIR/kafka.Kafka"

RUN mkdir $KAFKA_BIN_DIR

RUN if [ "$KAFKA_MODE" = "native" ]; then \
      apt update && apt install -y sudo build-essential libz-dev zlib1g-dev curl jq coreutils libffi-dev cmake pkg-config libfuse-dev && apt-get -y clean ; \
      mkdir graalvm ; \
      curl -L "$GRAALVM_URL" -o graalvm.tar.gz ; \
      tar -xzf graalvm.tar.gz -C graalvm --strip-components=1 ; \
      mkdir kafka ; \
      tar xfz native/kafka.tgz -C kafka --strip-components 1 ; \
      rm graalvm.tar.gz kafka.tgz ; \
      /build/native/native_command.sh $NATIVE_IMAGE_PATH $NATIVE_CONFIGS_DIR $KAFKA_LIBS_DIR $TARGET_PATH ; \
    fi

FROM $jdk_version

MAINTAINER Apache Kafka dev@kafka.apache.org
VOLUME ["/opt/kafka-dev"]

# Set the timezone.
ENV TZ="/usr/share/zoneinfo/America/Los_Angeles"

# Do not ask for confirmations when running apt-get, etc.
ENV DEBIAN_FRONTEND noninteractive

# Set the ducker.creator label so that we know that this is a ducker image.  This will make it
# visible to 'ducker purge'.  The ducker.creator label also lets us know what UNIX user built this
# image.
ARG ducker_creator=default
LABEL ducker.creator=$ducker_creator

# Update Linux and install necessary utilities.
RUN apt update && apt install -y sudo git netcat iptables rsync unzip wget curl jq coreutils openssh-server net-tools vim python3-pip python3-dev libffi-dev libssl-dev cmake pkg-config libfuse-dev iperf traceroute iproute2 iputils-ping && apt-get -y clean
RUN python3 -m pip install -U pip==21.1.1;
# NOTE: ducktape 0.12.0 supports py 3.9, 3.10, 3.11 and 3.12
COPY requirements.txt requirements.txt
RUN pip3 install --upgrade -r requirements.txt

COPY --from=build-native-image /build/kafka-binary/ /opt/kafka-binary/
# Set up ssh
COPY ./ssh-config /root/.ssh/config
# NOTE: The paramiko library supports the PEM-format private key, but does not support the RFC4716 format.
RUN ssh-keygen -m PEM -q -t rsa -N '' -f /root/.ssh/id_rsa && cp -f /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys
RUN echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# Install binary test dependencies.
# we use the same versions as in vagrant/base.sh
ARG KAFKA_MIRROR="https://s3-us-west-2.amazonaws.com/kafka-packages"
RUN mkdir -p "/opt/kafka-2.1.1" && chmod a+rw /opt/kafka-2.1.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.1.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.1.1"
RUN mkdir -p "/opt/kafka-2.2.2" && chmod a+rw /opt/kafka-2.2.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.2.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.2.2"
RUN mkdir -p "/opt/kafka-2.3.1" && chmod a+rw /opt/kafka-2.3.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.3.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.3.1"
RUN mkdir -p "/opt/kafka-2.4.1" && chmod a+rw /opt/kafka-2.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.4.1"
RUN mkdir -p "/opt/kafka-2.5.1" && chmod a+rw /opt/kafka-2.5.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.5.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.5.1"
RUN mkdir -p "/opt/kafka-2.6.3" && chmod a+rw /opt/kafka-2.6.3 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.6.3.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.6.3"
RUN mkdir -p "/opt/kafka-2.7.2" && chmod a+rw /opt/kafka-2.7.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.7.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.7.2"
RUN mkdir -p "/opt/kafka-2.8.2" && chmod a+rw /opt/kafka-2.8.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-2.8.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-2.8.2"
RUN mkdir -p "/opt/kafka-3.0.2" && chmod a+rw /opt/kafka-3.0.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.0.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.0.2"
RUN mkdir -p "/opt/kafka-3.1.2" && chmod a+rw /opt/kafka-3.1.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.1.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.1.2"
RUN mkdir -p "/opt/kafka-3.2.3" && chmod a+rw /opt/kafka-3.2.3 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.2.3.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.2.3"
RUN mkdir -p "/opt/kafka-3.3.2" && chmod a+rw /opt/kafka-3.3.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.3.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.3.2"
RUN mkdir -p "/opt/kafka-3.4.1" && chmod a+rw /opt/kafka-3.4.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.4.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.4.1"
RUN mkdir -p "/opt/kafka-3.5.2" && chmod a+rw /opt/kafka-3.5.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.5.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.5.2"
RUN mkdir -p "/opt/kafka-3.6.2" && chmod a+rw /opt/kafka-3.6.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.6.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.6.2"
RUN mkdir -p "/opt/kafka-3.7.2" && chmod a+rw /opt/kafka-3.7.2 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.7.2.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.7.2"
RUN mkdir -p "/opt/kafka-3.8.1" && chmod a+rw /opt/kafka-3.8.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.8.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.8.1"
RUN mkdir -p "/opt/kafka-3.9.1" && chmod a+rw /opt/kafka-3.9.1 && curl -s "$KAFKA_MIRROR/kafka_2.12-3.9.1.tgz" | tar xz --strip-components=1 -C "/opt/kafka-3.9.1"
RUN mkdir -p "/opt/kafka-4.0.0" && chmod a+rw /opt/kafka-4.0.0 && curl -s "$KAFKA_MIRROR/kafka_2.13-4.0.0.tgz" | tar xz --strip-components=1 -C "/opt/kafka-4.0.0"


# Streams test dependencies
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.1.1-test.jar" -o /opt/kafka-2.1.1/libs/kafka-streams-2.1.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.2.2-test.jar" -o /opt/kafka-2.2.2/libs/kafka-streams-2.2.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.3.1-test.jar" -o /opt/kafka-2.3.1/libs/kafka-streams-2.3.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.4.1-test.jar" -o /opt/kafka-2.4.1/libs/kafka-streams-2.4.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.5.1-test.jar" -o /opt/kafka-2.5.1/libs/kafka-streams-2.5.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.6.3-test.jar" -o /opt/kafka-2.6.3/libs/kafka-streams-2.6.3-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.7.2-test.jar" -o /opt/kafka-2.7.2/libs/kafka-streams-2.7.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-2.8.2-test.jar" -o /opt/kafka-2.8.2/libs/kafka-streams-2.8.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.0.2-test.jar" -o /opt/kafka-3.0.2/libs/kafka-streams-3.0.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.1.2-test.jar" -o /opt/kafka-3.1.2/libs/kafka-streams-3.1.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.2.3-test.jar" -o /opt/kafka-3.2.3/libs/kafka-streams-3.2.3-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.3.2-test.jar" -o /opt/kafka-3.3.2/libs/kafka-streams-3.3.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.4.1-test.jar" -o /opt/kafka-3.4.1/libs/kafka-streams-3.4.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.5.2-test.jar" -o /opt/kafka-3.5.2/libs/kafka-streams-3.5.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.6.2-test.jar" -o /opt/kafka-3.6.2/libs/kafka-streams-3.6.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.7.2-test.jar" -o /opt/kafka-3.7.2/libs/kafka-streams-3.7.2-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.8.1-test.jar" -o /opt/kafka-3.8.1/libs/kafka-streams-3.8.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-3.9.1-test.jar" -o /opt/kafka-3.9.1/libs/kafka-streams-3.9.1-test.jar
RUN curl -s "$KAFKA_MIRROR/kafka-streams-4.0.0-test.jar" -o /opt/kafka-4.0.0/libs/kafka-streams-4.0.0-test.jar

# To ensure the Kafka cluster starts successfully under JDK 17, we need to update the Zookeeper 
# client from version 3.4.x to 3.5.7 in Kafka versions 2.1.1, 2.2.2, and 2.3.1, as the older Zookeeper 
# client is incompatible with JDK 17. See KAFKA-17888 for more details.
RUN curl -s "https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.5.7/zookeeper-3.5.7.jar" -o /opt/zookeeper-3.5.7.jar
RUN curl -s "https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper-jute/3.5.7/zookeeper-jute-3.5.7.jar" -o /opt/zookeeper-jute-3.5.7.jar
RUN rm -f /opt/kafka-2.1.1/libs/zookeeper-* && rm -f /opt/kafka-2.2.2/libs/zookeeper-* && rm -f /opt/kafka-2.3.1/libs/zookeeper-*
RUN cp /opt/zookeeper-3.5.7.jar /opt/kafka-2.1.1/libs/zookeeper-3.5.7.jar && chmod a+rw /opt/kafka-2.1.1/libs/zookeeper-3.5.7.jar
RUN cp /opt/zookeeper-3.5.7.jar /opt/kafka-2.2.2/libs/zookeeper-3.5.7.jar && chmod a+rw /opt/kafka-2.2.2/libs/zookeeper-3.5.7.jar
RUN cp /opt/zookeeper-3.5.7.jar /opt/kafka-2.3.1/libs/zookeeper-3.5.7.jar && chmod a+rw /opt/kafka-2.3.1/libs/zookeeper-3.5.7.jar
RUN cp /opt/zookeeper-jute-3.5.7.jar /opt/kafka-2.1.1/libs/zookeeper-jute-3.5.7.jar && chmod a+rw /opt/kafka-2.1.1/libs/zookeeper-jute-3.5.7.jar
RUN cp /opt/zookeeper-jute-3.5.7.jar /opt/kafka-2.2.2/libs/zookeeper-jute-3.5.7.jar && chmod a+rw /opt/kafka-2.2.2/libs/zookeeper-jute-3.5.7.jar
RUN cp /opt/zookeeper-jute-3.5.7.jar /opt/kafka-2.3.1/libs/zookeeper-jute-3.5.7.jar && chmod a+rw /opt/kafka-2.3.1/libs/zookeeper-jute-3.5.7.jar
# The version of Kibosh to use for testing.
# If you update this, also update vagrant/base.sh
ARG KIBOSH_VERSION="8841dd392e6fbf02986e2fb1f1ebf04df344b65a"

# Aligning uid inside/outside docker enables containers to modify files of kafka source (mounted in /opt/kafka-dev")
# By default, the outside user id is 1000 (UID_MIN). The known exception in QA is travis which gives non-1000 id.
ARG UID="1000"

# Install Kibosh
RUN apt-get install fuse -y
RUN cd /opt && git clone -q  https://github.com/confluentinc/kibosh.git && cd "/opt/kibosh" && git reset --hard $KIBOSH_VERSION && mkdir "/opt/kibosh/build" && cd "/opt/kibosh/build" && ../configure && make -j 2

# Set up the ducker user.
RUN useradd -u $UID -ms /bin/bash ducker \
  && mkdir -p /home/ducker/ \
  && rsync -aiq /root/.ssh/ /home/ducker/.ssh \
  && chown -R ducker /home/ducker/ /mnt/ /var/log/ \
  && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment \
  && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile \
  && echo 'ducker ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers

USER ducker

CMD sudo service ssh start && tail -f /dev/null

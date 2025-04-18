#!/bin/bash
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

if [ $# -lt 1 ];
then
        echo "USAGE: $0 [-daemon] connect-standalone.properties [connector1.properties connector2.json ...]"
        exit 1
fi

base_dir=$(dirname $0)

###
### Classpath additions for Confluent Platform releases (LSB-style layout)
###
#cd -P deals with symlink from /bin to /usr/bin
java_base_dir=$( cd -P "$base_dir/../share/java" && pwd )

# confluent-common: required by kafka-serde-tools
# kafka-serde-tools (e.g. Avro serializer): bundled with confluent-schema-registry package
for library in "kafka" "confluent-common" "kafka-serde-tools" "monitoring-interceptors"; do
  dir="$java_base_dir/$library"
  if [ -d "$dir" ]; then
    classpath_prefix="$CLASSPATH:"
    if [ "x$CLASSPATH" = "x" ]; then
      classpath_prefix=""
    fi
    CLASSPATH="$classpath_prefix$dir/*"
  fi
done

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  LOG4J_CONFIG_NORMAL_INSTALL="/etc/kafka/connect-log4j2.yaml"
  LOG4J_CONFIG_ZIP_INSTALL="$base_dir/../etc/kafka/connect-log4j2.yaml"
  if [ -e "$LOG4J_CONFIG_NORMAL_INSTALL" ]; then # Normal install layout
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${LOG4J_CONFIG_NORMAL_INSTALL}"
  elif [ -e "${LOG4J_CONFIG_ZIP_INSTALL}" ]; then # Simple zip file layout
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=:${LOG4J_CONFIG_ZIP_INSTALL}"
  else # Fallback to normal default
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=$base_dir/../config/connect-log4j2.yaml"
  fi
fi
export KAFKA_LOG4J_OPTS

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
  export KAFKA_HEAP_OPTS="-Xms256M -Xmx2G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name connectStandalone'}

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac

export CLASSPATH
exec $(dirname $0)/kafka-run-class.sh $EXTRA_ARGS org.apache.kafka.connect.cli.ConnectStandalone "$@"

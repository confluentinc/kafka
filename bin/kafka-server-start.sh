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
	echo "USAGE: $0 [-daemon] server.properties [--override property=value]*"
	exit 1
fi
base_dir=$(dirname $0)

if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  LOG4J_CONFIG_NORMAL_INSTALL="/etc/kafka/log4j2.yaml"
  LOG4J_CONFIG_ZIP_INSTALL="$base_dir/../etc/kafka/log4j2.yaml"
  if [ -e "$LOG4J_CONFIG_NORMAL_INSTALL" ]; then # Normal install layout
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${LOG4J_CONFIG_NORMAL_INSTALL}"
  elif [ -e "${LOG4J_CONFIG_ZIP_INSTALL}" ]; then # Simple zip file layout
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${LOG4J_CONFIG_ZIP_INSTALL}"
  else # Fallback to normal default
    KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=$base_dir/../config/log4j2.yaml"
  fi
fi
export KAFKA_LOG4J_OPTS

if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
fi

EXTRA_ARGS=${EXTRA_ARGS-'-name kafkaServer -loggc'}

COMMAND=$1
case $COMMAND in
  -daemon)
    EXTRA_ARGS="-daemon "$EXTRA_ARGS
    shift
    ;;
  *)
    ;;
esac

exec $base_dir/kafka-run-class.sh $EXTRA_ARGS kafka.Kafka "$@"

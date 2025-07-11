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
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

# WINDOWS_OS_FORMAT == 1 if Cygwin or MinGW is detected, else 0.
if [[ $(uname -a) =~ "CYGWIN" || $(uname -a) =~ "MINGW" || $(uname -a) =~ "MSYS" ]]; then
  WINDOWS_OS_FORMAT=1
  export MSYS2_ARG_CONV_EXCL="-Xlog:gc*:file=;-Dlog4j2.configurationFile=;$MSYS2_ARG_CONV_EXCL"
else
  WINDOWS_OS_FORMAT=0
fi

if [ -z "$INCLUDE_TEST_JARS" ]; then
  INCLUDE_TEST_JARS=false
fi

# Exclude jars not necessary for running commands.
regex="(-(test|test-sources|src|scaladoc|javadoc)\.jar|jar.asc|connect-file.*\.jar)$"
should_include_file() {
  if [ "$INCLUDE_TEST_JARS" = true ]; then
    return 0
  fi
  file=$1
  if [ -z "$(echo "$file" | grep -E "$regex")" ] ; then
    return 0
  else
    return 1
  fi
}

base_dir=$(dirname $0)/..

if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.13.16
  if [[ -f "$base_dir/gradle.properties" ]]; then
    SCALA_VERSION=`grep "^scalaVersion=" "$base_dir/gradle.properties" | cut -d= -f 2`
  fi
fi

if [ -z "$SCALA_BINARY_VERSION" ]; then
  SCALA_BINARY_VERSION=$(echo $SCALA_VERSION | cut -f 1-2 -d '.')
fi

# run ./gradlew copyDependantLibs to get all dependant jars in a local dir
shopt -s nullglob
if [ -z "$UPGRADE_KAFKA_STREAMS_TEST_VERSION" ]; then
  for dir in "$base_dir"/core/build/dependant-libs-${SCALA_VERSION}*;
  do
    CLASSPATH="$CLASSPATH:$dir/*"
  done
fi

for file in "$base_dir"/examples/build/libs/kafka-examples*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

if [ -z "$UPGRADE_KAFKA_STREAMS_TEST_VERSION" ]; then
  clients_lib_dir=$(dirname $0)/../clients/build/libs
  streams_lib_dir=$(dirname $0)/../streams/build/libs
  streams_dependant_clients_lib_dir=$(dirname $0)/../streams/build/dependant-libs-${SCALA_VERSION}
else
  clients_lib_dir=/opt/kafka-$UPGRADE_KAFKA_STREAMS_TEST_VERSION/libs
  streams_lib_dir=$clients_lib_dir
  streams_dependant_clients_lib_dir=$streams_lib_dir
fi


for file in "$clients_lib_dir"/kafka-clients*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

for file in "$streams_lib_dir"/kafka-streams*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

if [ -z "$UPGRADE_KAFKA_STREAMS_TEST_VERSION" ]; then
  for file in "$base_dir"/streams/examples/build/libs/kafka-streams-examples*.jar;
  do
    if should_include_file "$file"; then
      CLASSPATH="$CLASSPATH":"$file"
    fi
  done
else
  VERSION_NO_DOTS=`echo $UPGRADE_KAFKA_STREAMS_TEST_VERSION | sed 's/\.//g'`
  SHORT_VERSION_NO_DOTS=${VERSION_NO_DOTS:0:((${#VERSION_NO_DOTS} - 1))} # remove last char, ie, bug-fix number
  for file in "$base_dir"/streams/upgrade-system-tests-$SHORT_VERSION_NO_DOTS/build/libs/kafka-streams-upgrade-system-tests*.jar;
  do
    if should_include_file "$file"; then
      CLASSPATH="$file":"$CLASSPATH"
    fi
  done
fi

for file in "$streams_dependant_clients_lib_dir"/rocksdb*.jar;
do
  CLASSPATH="$CLASSPATH":"$file"
done

for file in "$streams_dependant_clients_lib_dir"/*hamcrest*.jar;
do
  CLASSPATH="$CLASSPATH":"$file"
done

for file in "$base_dir"/shell/build/libs/kafka-shell*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

for dir in "$base_dir"/shell/build/dependant-libs-${SCALA_VERSION}*;
do
  CLASSPATH="$CLASSPATH:$dir/*"
done

for file in "$base_dir"/tools/build/libs/kafka-tools*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

for dir in "$base_dir"/tools/build/dependant-libs-${SCALA_VERSION}*;
do
  CLASSPATH="$CLASSPATH:$dir/*"
done

for file in "$base_dir"/trogdor/build/libs/trogdor-*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

for dir in "$base_dir"/trogdor/build/dependant-libs-${SCALA_VERSION}*;
do
  CLASSPATH="$CLASSPATH:$dir/*"
done

for cc_pkg in "api" "transforms" "runtime" "mirror" "mirror-client" "json" "tools" "basic-auth-extension"
do
  for file in "$base_dir"/connect/${cc_pkg}/build/libs/connect-${cc_pkg}*.jar;
  do
    if should_include_file "$file"; then
      CLASSPATH="$CLASSPATH":"$file"
    fi
  done
  if [ -d "$base_dir/connect/${cc_pkg}/build/dependant-libs" ] ; then
    CLASSPATH="$CLASSPATH:$base_dir/connect/${cc_pkg}/build/dependant-libs/*"
  fi
done

# classpath addition for release
for file in "$base_dir"/libs/*;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done

# CONFLUENT: classpath addition for releases with LSB-style layout
CLASSPATH="$CLASSPATH":"$base_dir/share/java/kafka/*"

# classpath for telemetry
CLASSPATH="$CLASSPATH":"$base_dir/share/java/confluent-telemetry/*"

for file in "$base_dir"/core/build/libs/kafka_${SCALA_BINARY_VERSION}*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done
shopt -u nullglob

if [ -z "$CLASSPATH" ] ; then
  echo "Classpath is empty. Please build the project first e.g. by running './gradlew jar -PscalaVersion=$SCALA_VERSION'"
  exit 1
fi

# JMX settings
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
  if ! echo "$KAFKA_JMX_OPTS" | grep -qF -- '-Dcom.sun.management.jmxremote.rmi.port=' ; then
    # If unset, set the RMI port to address issues with monitoring Kafka running in containers
    KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
  fi
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  LOG4J_CONFIG_NORMAL_INSTALL="/etc/kafka/tools-log4j2.yaml"
  LOG4J_CONFIG_ZIP_INSTALL="$base_dir/etc/kafka/tools-log4j2.yaml"
  if [ -e "$LOG4J_CONFIG_NORMAL_INSTALL" ]; then # Normal install layout
    LOG4J_DIR="${LOG4J_CONFIG_NORMAL_INSTALL}"
  elif [ -e "${LOG4J_CONFIG_ZIP_INSTALL}" ]; then # Simple zip file layout
    LOG4J_DIR="${LOG4J_CONFIG_ZIP_INSTALL}"
  else # Fallback to normal default
    LOG4J_DIR="$base_dir/config/tools-log4j2.yaml"
  fi
  # If Cygwin is detected, LOG4J_DIR is converted to Windows format.
  (( WINDOWS_OS_FORMAT )) && LOG4J_DIR=$(cygpath --path --mixed "${LOG4J_DIR}")
  KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=${LOG4J_DIR}"
else
  if echo "$KAFKA_LOG4J_OPTS" | grep -E "log4j\.[^[:space:]]+(\.properties|\.xml)$" >/dev/null; then
      # Enable Log4j 1.x configuration compatibility mode for Log4j 2
      export LOG4J_COMPATIBILITY=true
      echo DEPRECATED: A Log4j 1.x configuration file has been detected, which is no longer recommended. >&2
      echo To use a Log4j 2.x configuration, please see https://logging.apache.org/log4j/2.x/migrate-from-log4j1.html#Log4j2ConfigurationFormat for details about Log4j configuration file migration. >&2
      echo You can also use the \$KAFKA_HOME/config/tools-log4j2.yaml file as a starting point. Make sure to remove the Log4j 1.x configuration after completing the migration. >&2
  fi
  # create logs directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
  fi
fi

# If Cygwin is detected, LOG_DIR is converted to Windows format.
(( WINDOWS_OS_FORMAT )) && LOG_DIR=$(cygpath --path --mixed "${LOG_DIR}")
KAFKA_LOG4J_CMD_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

# Generic jvm settings you want to add
if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS=""
fi

# Set Debug options if enabled
if [ "x$KAFKA_DEBUG" != "x" ]; then

    # Use default ports
    DEFAULT_JAVA_DEBUG_PORT="5005"

    if [ -z "$JAVA_DEBUG_PORT" ]; then
        JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
    fi

    # Use the defaults if JAVA_DEBUG_OPTS was not set
    DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT"
    if [ -z "$JAVA_DEBUG_OPTS" ]; then
        JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
    fi

    echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    KAFKA_OPTS="$JAVA_DEBUG_OPTS $KAFKA_OPTS"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  KAFKA_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
# MaxInlineLevel=15 is the default since JDK 14 and can be removed once older JDKs are no longer supported
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true"
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$KAFKA_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX

  # The first segment of the version number, which is '1' for releases before Java 9
  # it then becomes '9', '10', ...
  # Some examples of the first line of `java --version`:
  # 8 -> java version "1.8.0_152"
  # 9.0.4 -> java version "9.0.4"
  # 10 -> java version "10" 2018-03-20
  # 10.0.1 -> java version "10.0.1" 2018-04-17
  # We need to match to the end of the line to prevent sed from printing the characters that do not match
  JAVA_MAJOR_VERSION=$("$JAVA" -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
  if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
    KAFKA_GC_LOG_OPTS="-Xlog:gc*:file=$LOG_DIR/$GC_LOG_FILE_NAME:time,tags:filecount=10,filesize=100M"
  else
    KAFKA_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
  fi
fi

# Remove a possible colon prefix from the classpath (happens at lines like `CLASSPATH="$CLASSPATH:$file"` when CLASSPATH is blank)
# Syntax used on the right side is native Bash string manipulation; for more details see
# http://tldp.org/LDP/abs/html/string-manipulation.html, specifically the section titled "Substring Removal"
CLASSPATH=${CLASSPATH#:}

# If Cygwin is detected, classpath is converted to Windows format.
(( WINDOWS_OS_FORMAT )) && CLASSPATH=$(cygpath --path --mixed "${CLASSPATH}")

# If KAFKA_MODE=native, it will bring up Kafka in the native mode.
# It expects the Kafka executable binary to be present at $base_dir/kafka.Kafka.
# This is specifically used to run system tests on native Kafka - by bringing up Kafka in the native mode.
if [[ "x$KAFKA_MODE" == "xnative" ]] && [[ "$*" == *"kafka.Kafka"* ]]; then
  exec $base_dir/kafka.Kafka start --config "$2"  $KAFKA_LOG4J_CMD_OPTS $KAFKA_JMX_OPTS $KAFKA_OPTS
else
  # Launch mode
  if [ "x$DAEMON_MODE" = "xtrue" ]; then
    nohup "$JAVA" $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_CMD_OPTS -cp "$CLASSPATH" $KAFKA_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
  else
    exec "$JAVA" $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_CMD_OPTS -cp "$CLASSPATH" $KAFKA_OPTS "$@"
  fi
fi

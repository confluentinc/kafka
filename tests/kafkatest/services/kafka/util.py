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

from collections import namedtuple

from kafkatest.utils.remote_account import java_version
from kafkatest.version import LATEST_0_8_2, LATEST_0_9, LATEST_0_10_0, LATEST_0_10_1, LATEST_0_10_2, LATEST_0_11_0, LATEST_1_0

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])

new_jdk_not_supported = frozenset([str(LATEST_0_8_2), str(LATEST_0_9), str(LATEST_0_10_0), str(LATEST_0_10_1), str(LATEST_0_10_2), str(LATEST_0_11_0), str(LATEST_1_0)])

# the types of metadata quorums we support
zk_quorum = 'ZK' # ZooKeeper, used through the KIP-500 bridge release
inproc_raft_quorum = 'INPROC_RAFT' # co-located KIP-500 Controllers, used during/after the KIP-500 bridge release
remote_raft_quorum = 'REMOTE_RAFT' # separate KIP-500 Controllers, used during/after the KIP-500 bridge release

# How we will parameterize tests that exercise all quorum styles
all_quorum_styles = [zk_quorum, remote_raft_quorum, inproc_raft_quorum]
# How we will parameterize tests that are unrelated to upgrades
non_upgrade_quorums = [zk_quorum, remote_raft_quorum]
# How we will parameterize upgrade-related tests
upgrade_quorums = [zk_quorum, non_upgrade_quorums, remote_raft_quorum]

def test_uses_zk(test_context):
    # A test is using ZooKeeper if it doesn't specify a metadata quorum or if it explicitly specifies ZooKeeper
    return test_context.injected_args.get('metadata_quorum', zk_quorum) == zk_quorum

def fix_opts_for_new_jvm(node):
    # Startup scripts for early versions of Kafka contains options
    # that not supported on latest versions of JVM like -XX:+PrintGCDateStamps or -XX:UseParNewGC.
    # When system test run on JVM that doesn't support these options
    # we should setup environment variables with correct options.
    java_ver = java_version(node)
    if java_ver <= 9:
        return ""

    cmd = ""
    if node.version == LATEST_0_8_2 or node.version == LATEST_0_9 or node.version == LATEST_0_10_0 or node.version == LATEST_0_10_1 or node.version == LATEST_0_10_2 or node.version == LATEST_0_11_0 or node.version == LATEST_1_0:
        cmd += "export KAFKA_GC_LOG_OPTS=\"-Xlog:gc*:file=kafka-gc.log:time,tags:filecount=10,filesize=102400\"; "
        cmd += "export KAFKA_JVM_PERFORMANCE_OPTS=\"-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -XX:MaxInlineLevel=15 -Djava.awt.headless=true\"; "
    return cmd



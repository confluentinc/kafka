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

# the types of metadata quorums we support
zk_quorum = 'ZK' # ZooKeeper, used through the KIP-500 bridge release
inproc_raft_quorum = 'INPROC_RAFT' # co-located KIP-500 Controllers, used during/after the KIP-500 bridge release
remote_raft_quorum = 'REMOTE_RAFT' # separate KIP-500 Controllers, used during/after the KIP-500 bridge release

# How we will parameterize tests that exercise all quorum styles
all_quorum_styles = [zk_quorum, remote_raft_quorum, inproc_raft_quorum]
# How we will parameterize tests that exercise all Raft quorum styles
all_raft_quorums = [remote_raft_quorum, inproc_raft_quorum]
# How we will parameterize tests that are unrelated to upgrades
non_upgrade_quorums = [zk_quorum, remote_raft_quorum]
# How we will parameterize upgrade-related tests
upgrade_quorums = all_quorum_styles

def quorum_type_for_test(test_context):
    # A test uses ZooKeeper if it doesn't specify a metadata quorum or if it explicitly specifies ZooKeeper
    default_quorum_type = zk_quorum
    retval = default_quorum_type if not test_context.injected_args else test_context.injected_args.get('metadata_quorum', default_quorum_type)
    if retval not in all_quorum_styles:
        raise Exception("Unknown metadata_quorum value provided for the test: %s" % retval)
    return retval

def get_validated_quorum_type(test_context, zk, remote_kafka):
    """
    Validates that all parameters are consistent and returns the corresponding quorum type.
    Raises an exception if the given parameters are inconsistent.

    :param test_context: the test context
    :param zk: the ZooKeeper service, if any
    :param remote_kafka: the Kafka service will run just a Raft-based controller when not None
    :return: the quorum type for the Kafka cluster with the given parameters
    """
    quorum_type = quorum_type_for_test(test_context)
    # Perform validations so we can definitively depend on the input parameters and the returned quorum type
    has_usable_zk = (zk and not zk.ignored)
    if has_usable_zk and quorum_type != zk_quorum:
        raise Exception("Cannot use ZooKeeper while specifying a Raft metadata quorum (should not happen)")
    if remote_kafka and quorum_type != remote_raft_quorum:
        raise Exception("Cannot specify a remote Kafka service unless using a remote Raft metadata quorum (should not happen)")
    return quorum_type

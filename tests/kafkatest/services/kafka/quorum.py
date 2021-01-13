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
zk = 'ZK' # ZooKeeper, used before/during the KIP-500 bridge release(s)
colocated_raft = 'COLOCATED_RAFT' # co-located KIP-500 Controllers, used during/after the KIP-500 bridge release(s)
remote_raft = 'REMOTE_RAFT' # separate KIP-500 Controllers, used during/after the KIP-500 bridge release(s)

# How we will parameterize tests that exercise all quorum styles
all = [zk, remote_raft, colocated_raft]
# How we will parameterize tests that exercise all Raft quorum styles
all_raft = [remote_raft, colocated_raft]
# How we will parameterize tests that are unrelated to upgrades:
#   [“ZK”] before the KIP-500 bridge release(s)
#   [“ZK”, “REMOTE_RAFT”] during the KIP-500 bridge release(s)
#   [“REMOTE_RAFT”] after the KIP-500 bridge release(s)
non_upgrade_quorums = [zk, remote_raft]

def for_test(test_context):
    # A test uses ZooKeeper if it doesn't specify a metadata quorum or if it explicitly specifies ZooKeeper
    default_quorum_type = zk
    arg_name = 'metadata_quorum'
    retval = default_quorum_type if not test_context.injected_args else test_context.injected_args.get(arg_name, default_quorum_type)
    if retval not in all:
        raise Exception("Unknown %s value provided for the test: %s" % (arg_name, retval))
    return retval

class Info:
    """
    Exposes quorum-related information for an instance of KafkaService

    Attributes
    ----------

    kafka : KafkaService
        The service for which this instance exposes quorum-related
        information
    quorum_type : str
        COLOCATED_RAFT, REMOTE_RAFT, or ZK
    using_zk : bool
        True iff quorum_type == ZK
    using_raft : bool
        False iff quorum_type == ZK
    has_broker_role : bool
        True iff using_raft and the Kafka service doesn't itself have
        a remote Kafka service (meaning it is not a remote controller)
    has_controller_role : bool
        True iff quorum_type == COLOCATED_RAFT or the Kafka service
        itself has a remote Kafka service (meaning it is a remote
        controller)
    has_combined_broker_and_controller_roles :
        True iff quorum_type == COLOCATED_RAFT

    """

    def __init__(self, kafka, context):
        """

        :param kafka : KafkaService
            The service for which this instance exposes quorum-related information
        :param context : TestContext
            The test context within which the given Kafka service is being instantiated
        """
        quorum_type = for_test(context)
        if quorum_type != zk and kafka.zk:
            raise Exception("Cannot use ZooKeeper while specifying a Raft metadata quorum (should not happen)")
        if kafka.remote_kafka and quorum_type != remote_raft:
            raise Exception("Cannot specify a remote Kafka service unless using a remote Raft metadata quorum (should not happen)")
        self.kafka = kafka
        self.quorum_type = quorum_type
        self.using_zk = quorum_type == zk
        self.using_raft = not self.using_zk
        self.has_broker_role = self.using_raft and not kafka.remote_kafka
        self.has_controller_role = quorum_type == colocated_raft or kafka.remote_kafka
        self.has_combined_broker_and_controller_roles = quorum_type == colocated_raft

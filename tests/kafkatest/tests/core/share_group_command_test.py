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


from ducktape.utils.util import wait_until
from ducktape.tests.test import Test
from ducktape.mark import matrix
from ducktape.mark.resource import cluster

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.console_share_consumer import ConsoleShareConsumer
from kafkatest.services.security.security_config import SecurityConfig

import os
import re

TOPIC = "topic-share-group-command"


class ShareGroupCommandTest(Test):
    """
    Tests ShareGroupCommand
    """
    # Root directory for persistent output
    PERSISTENT_ROOT = "/mnt/share_group_command"
    COMMAND_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "command.properties")

    def __init__(self, test_context):
        super(ShareGroupCommandTest, self).__init__(test_context)
        self.num_brokers = 1
        self.topics = {
            TOPIC: {'partitions': 1, 'replication-factor': 1}
        }

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            None, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol, topics=self.topics,
            controller_num_nodes_override=self.num_brokers)
        self.kafka.start()

    def start_share_consumer(self):
        self.share_consumer = ConsoleShareConsumer(self.test_context, num_nodes=self.num_brokers, kafka=self.kafka, topic=TOPIC,
                                             share_consumer_timeout_ms=None)
        self.share_consumer.start()

    def setup_and_verify(self, security_protocol, group=None, describe_members=False):
        self.start_kafka(security_protocol, security_protocol)
        self.start_share_consumer()
        share_consumer_node = self.share_consumer.nodes[0]
        wait_until(lambda: self.share_consumer.alive(share_consumer_node),
                   timeout_sec=20, backoff_sec=.2, err_msg="Share consumer was too slow to start")
        kafka_node = self.kafka.nodes[0]
        if security_protocol is not SecurityConfig.PLAINTEXT:
            prop_file = str(self.kafka.security_config.client_config())
            self.logger.debug(prop_file)
            kafka_node.account.ssh("mkdir -p %s" % self.PERSISTENT_ROOT, allow_fail=False)
            kafka_node.account.create_file(self.COMMAND_CONFIG_FILE, prop_file)

        # Verify ShareConsumerGroupCommand lists expected consumer groups
        command_config_file = self.COMMAND_CONFIG_FILE

        if group:
            if describe_members:
                def has_expected_share_group_member():
                    output = self.kafka.describe_share_group_members(group=group, node=kafka_node, command_config=command_config_file)
                    return len(output) == 1 and all("test-share-group" in line for line in output)
                wait_until(has_expected_share_group_member, timeout_sec=10, err_msg="Timed out waiting to describe members of the share group.")
            else:
                wait_until(lambda: re.search("topic-share-group-command",self.kafka.describe_share_group(group=group, node=kafka_node, command_config=command_config_file)), timeout_sec=10,
                        err_msg="Timed out waiting to describe expected share groups.")
        else:
            wait_until(lambda: "test-share-group" in self.kafka.list_share_groups(node=kafka_node, command_config=command_config_file), timeout_sec=10,
                       err_msg="Timed out waiting to list expected share groups.")

        self.share_consumer.stop()

    @cluster(num_nodes=3)
    @matrix(
        security_protocol=['PLAINTEXT', 'SSL'],
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True]
    )
    def test_list_share_groups(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.isolated_kraft, use_share_groups=True):
        """
        Tests if ShareGroupCommand is listing correct share groups
        :return: None
        """
        self.setup_and_verify(security_protocol)

    @cluster(num_nodes=3)
    @matrix(
        security_protocol=['PLAINTEXT', 'SSL'],
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_describe_share_group(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.isolated_kraft, use_share_groups=True):
        """
        Tests if ShareGroupCommand is describing a share group correctly
        :return: None
        """
        self.setup_and_verify(security_protocol, group="test-share-group")

    @cluster(num_nodes=3)
    @matrix(
        security_protocol=['PLAINTEXT', 'SSL'],
        metadata_quorum=[quorum.isolated_kraft],
        use_share_groups=[True],
    )
    def test_describe_share_group_members(self, security_protocol='PLAINTEXT', metadata_quorum=quorum.isolated_kraft, use_share_groups=True):
        """
        Tests if ShareGroupCommand is describing the members of a share group correctly
        :return: None
        """
        self.setup_and_verify(security_protocol, group="test-share-group", describe_members=True)

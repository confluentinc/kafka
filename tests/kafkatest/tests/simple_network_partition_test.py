# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer

import time

from kafkatest.process_signal import SIGSTOP, SIGCONT, SIGKILL
from concurrent import futures

# Death "permanence"
BOUNCE = "bounce"  # Kill and revive
KILL = "kill"      # Kill without reviving

# node types
LEADER = "leader"
CONTROLLER = "controller"


class SimpleNetworkPartitionTest(Test):
    """
    Somewhat ad-hoc network partition test. This roughly reproduces the failure mode outlined here:
    https://aphyr.com/posts/293-call-me-maybe-kafka

    Setup
    - 2 Kafka brokers
    - 1 topic with replication-factor 2 and min.insync.replicas=1
    - Producer with required acks = 1

    Test
    - Find leader and follower for topic
    - Start producer
    - After some time, partition leader and follower
    - Continue writes so that follower falls behind
    - Kill leader
    - Continue writes
    - Verify that some of the writes were lost

    """

    def __init__(self, test_context):
        super(SimpleNetworkPartitionTest, self).__init__(test_context=test_context)

        # Kafka and Zookeeper
        self.zk = ZookeeperService(test_context, num_nodes=1)
        topic_config = {"partitions": 1, "replication-factor": 2, "configs": {"min.insync.replicas": 1}}
        self.topic = "my_topic"
        self.kafka = KafkaService(test_context, num_nodes=2, zk=self.zk, topics={self.topic: topic_config})

        # Producer and Consumer
        self.producer_throughput = 1000
        self.producer = VerifiableProducer(self.test_context, 1, self.kafka, self.topic,
                                           throughput=self.producer_throughput,
                                           configs={"acks": 1, "batch.size": 1000, "buffer.memory": 10000})
        self.consumer = ConsoleConsumer(self.test_context, 1, self.kafka, self.topic, consumer_timeout_ms=3000)

        self.leader = None
        self.follower = None

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def test_replication(self):

        try:
            self.leader = self.kafka.leader(self.topic, 0)
            leader_idx = self.kafka.idx(self.leader)
            follower_idx = 1 + leader_idx % 2  # 1 -> 2, and 2 -> 1
            self.follower = self.kafka.get_node(follower_idx)

            # Immediately partition leader from follower
            self.partition_follower_leader(self.follower, self.leader)


            # self.kafka.stop_node(self.follower)
            # self.kafka.signal_node(self.follower, sig=SIGSTOP)

            self.producer.start()

            # Produce for a little while (say, 10,000 messages)
            # Perhaps check directly that follower has fallen out of isr
            # Could do this by.... parsing the describe topic command ;)
            time.sleep(5)

            # Kill leader and do not revive (clean kill is fine)
            leader_pid = self.kafka.pids(self.leader)[0]
            self.kafka.signal_node(self.leader, sig=SIGKILL)
            wait_until(lambda: not self.leader.account.alive(leader_pid), timeout_sec=3, backoff_sec=.25)

            # self.kafka.signal_node(self.follower, sig=SIGCONT)
            # self.kafka.start_node(self.follower)
            if not wait_until(lambda: self.follower == self.kafka.leader(self.topic, 0), timeout_sec=10, backoff_sec=.5):
                raise RuntimeError("Leader reelection did not take place in a reasonable amount of time.")

            acked = self.producer.num_acked
            self.logger.info("Num currently acked: " + str(acked))
            self.logger.info("Waiting for more messages to be acked...")
            if not wait_until(lambda: self.producer.num_acked > acked + 1000, timeout_sec=60, backoff_sec=.5):
                raise RuntimeError("Former follower is now leader, but new messages are not being acknowledged.")
            self.stop_producer()

            # consume
            self.consumer.start()
            self.consumer.wait()
            self.messages_consumed = self.consumer.messages_consumed[1]

            if len(self.messages_consumed) == 0:
                raise RuntimeError("No messages consumed under topic %s" % self.topic)

            # Check produced vs consumed
            # validation should fail
            self.logger.debug("Validating...")
            success, msg = self.validate()
            assert not success, "Somehow every acked message was consumed despite the network partition."
            self.logger.info("Test failed in the *expected* way:" + msg)
        except BaseException as e:
            raise e
        finally:
            self.logger.info("follower: " + str(self.follower))
            if self.follower and self.follower.account:
                # self.follower.account.ssh("sudo iptables -D INPUT -p tcp --source %s -j DROP" %
                #     self.leader.account.hostname, allow_fail=True)
                self.clear_iptables(self.follower)
            if self.leader and self.leader.account:
                # self.leader.account.ssh("sudo iptables -D INPUT -p tcp --source %s -j DROP" %
                #     self.follower.account.hostname, allow_fail=True)
                self.clear_iptables(self.leader)

    def partition_follower_leader(self, follower, leader):
        follower.account.ssh("sudo iptables -A INPUT -p tcp --source %s -j DROP" %
            leader.account.hostname)
        # leader.account.ssh("sudo iptables -A INPUT -p tcp --source %s -j DROP" %
        #     follower.account.hostname)

    def start_producer(self):
        self.producer.start()

        if not wait_until(lambda: self.producer.num_acked > 5, timeout_sec=5):
            raise RuntimeError("Producer failed to start in a reasonable amount of time: %s" % str(self.producer))

    def stop_producer(self):
        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = (executor.submit(self.producer.stop))

            if not wait_until(future.done, timeout_sec=60, backoff_sec=1):
                raise RuntimeError("Producer did not stop in a reasonable amount of time.")

    def validate(self):
        """Check that produced messages were consumed."""

        success = True
        msg = ""
        consumed = self.messages_consumed
        acked = self.producer.acked

        if len(set(consumed)) != len(consumed):
            # There are duplicates. This is ok, so report it but don't fail the test
            msg += "There are duplicate messages in the log\n"

        if not set(consumed).issuperset(set(acked)):
            # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
            acked_minus_consumed = set(acked) - set(consumed)
            success = False
            msg += "At least one acked message did not appear in the consumed messages for topic %s. acked_minus_consumed: %s" % (self.topic, str(acked_minus_consumed))

        self.logger.info("num consumed: " + str(len(consumed)))
        self.logger.info("num acked:    " + str(len(acked)))
        return success, msg

    def clear_iptables(self, node):
        cmds = ["iptables -F",
                "iptables -X",
                "iptables -t nat -F",
                "iptables -t nat -X",
                "iptables -t mangle -F",
                "iptables -t mangle -X",
                "iptables -P INPUT ACCEPT",
                "iptables -P FORWARD ACCEPT",
                "iptables -P OUTPUT ACCEPT"]

        cmds = ["sudo " + cmd for cmd in cmds]
        node.account.ssh("; ".join(cmds), allow_fail=True)

        # lines = node.account.ssh_capture("sudo iptables -L")

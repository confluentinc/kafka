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
from ducktape.mark import matrix
from ducktape.mark import parametrize

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer

from kafkatest.process_signal import SIGTERM, SIGKILL, SIGSTOP

from concurrent import futures

# Death "permanence"
BOUNCE = "bounce"  # Kill and revive
KILL = "kill"      # Kill without reviving

# node types
LEADER = "leader"
CONTROLLER = "controller"


class ReplicationTest(Test):
    """Replication tests.
    These tests verify that replication provides simple durability guarantees by checking that data acked by
    brokers is still available for consumption in the face of various failure scenarios.

    Each test produces to three separate topics while driving various kinds of failure on a particular topic and partition.
    This exercises replication and leader/controller failover while producing across multiple topics, each with multiple partitions.

    Failures are induced on either the leader of a topic/partition or on the controller. As a consequence of the
    way brokers distribute leaders across the broker cluster, this will have the side effect of exercising
    failure of followers as wel..

    We then validate by consuming all messages from each topic, and checking that every acked message shows up in
    the downstream consumer for each topic.
    """

    def __init__(self, test_context):
        super(ReplicationTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=1)

        # Important to disable unclean leader election
        topic_config = {"partitions": 3, "replication-factor": 3,
                        "configs": {"min.insync.replicas": 2, "unclean.leader.election.enable": "false"}}
        self.topic_names = ["topic1", "topic2", "topic3"]
        self.topics = {topic: topic_config.copy() for topic in self.topic_names}
        self.topic_to_fail = "topic1"  # We'll induce failures on this topic

        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics=self.topics)
        self.producer_throughput = 10000

        self.producers = [VerifiableProducer(self.test_context, 1, self.kafka, topic,
                                             throughput=self.producer_throughput,
                                             # close_timeout_ms=60000,
                                             # configs={"acks": -1})
                                             configs={"acks": -1, "batch.size": 100, "buffer.memory": 1000})
                          for topic in self.topic_names]
        self.consumers = [ConsoleConsumer(self.test_context, 1, self.kafka, topic, consumer_timeout_ms=3000)
                          for topic in self.topic_names]

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    # @parametrize(configs={"acks": "1"})
    @parametrize(configs={"compression.type": "gzip"})
    @matrix(failure=[SIGTERM, SIGSTOP, SIGKILL])
    @matrix(node_type=[LEADER, CONTROLLER])
    # @matrix(failure_permanence=[BOUNCE, KILL])
    def test_replication(self, failure=SIGTERM, failure_permanence=BOUNCE,
                         node_type=LEADER, configs={}, num_bounce=4):
        """This is the top-level test template.

        The steps are:
            Produce messages in the background while driving some failure condition
            When done driving failures, immediately stop producing
            Consume all messages
            Validate that messages acked by brokers were consumed

        Note that consuming is a bit tricky, at least with console consumer. The goal is to consume all messages
        (foreach partition) in the topic. In this case, waiting for the last message may cause the consumer to stop
        too soon since console consumer is consuming multiple partitions from a single thread and therefore we lose
        ordering guarantees.

        Waiting on a count of consumed messages can be unreliable: if we stop consuming when num_consumed == num_acked,
        we might exit early if some messages are duplicated (though not an issue here since producer retries==0)

        Therefore rely here on the consumer.timeout.ms setting which times out on the interval between successively
        consumed messages. Since we run the producer to completion before running the consumer, this is a reliable
        indicator that nothing is left to consume.

        """
        self.num_bounce = num_bounce

        for producer in self.producers:
            producer.configs.update(configs.copy())

        # Produce in a background thread while driving broker failures
        self.logger.debug("Producing messages...")
        self.start_producers()

        self.logger.debug("Driving failures...")

        assert self.kafka.all_alive()

        node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)
        assert node_to_signal != None
        leader_or_controller_change = \
            lambda: node_to_signal != self.fetch_broker_node(self.topic_to_fail, 0, node_type) and self.kafka.dead(node_to_signal)

        self.kafka.bounce_node(
            node_to_signal, sig=SIGTERM, condition=leader_or_controller_change,
            condition_timeout_sec=30, condition_backoff_sec=.5)

        self.drive_failures(failure, failure_permanence, node_type)
        self.stop_producers()

        self.acked = [producer.acked for producer in self.producers]

        # Consume all messages
        self.logger.debug("Consuming messages...")
        self.messages_consumed = []
        for consumer in self.consumers:
            consumer.start()
            consumer.wait()
            self.messages_consumed.append(consumer.messages_consumed[1])

        # Check produced vs consumed
        self.logger.debug("Validating...")
        success, msg = self.validate()

        if not success:
            for producer in self.producers:
                self.mark_for_collect(producer)

        assert success, msg

    def fetch_broker_node(self, topic, partition, node_type):
        if node_type == LEADER:
            leader = self.kafka.leader(topic=topic, partition=partition)
            self.logger.info("Leader for topic %s and partition %d is currently: %d" % (topic, partition, self.kafka.idx(leader)))
            return leader
        elif node_type == CONTROLLER:
            controller = self.kafka.controller()
            self.logger.info("Controller is currently: %d" % (self.kafka.idx(controller)))
            return controller
        else:
            raise RuntimeError("Unsupported node type.")

    def start_producers(self):
        for producer in self.producers:
            producer.start()

        for producer in self.producers:
            if not wait_until(lambda: producer.num_acked > 5, timeout_sec=5):
                raise RuntimeError("Producer failed to start in a reasonable amount of time: %s" % str(producer))

    def stop_producers(self):
        stop_futures = []
        with futures.ThreadPoolExecutor(max_workers=len(self.topic_names)) as executor:
            for producer in self.producers:
                stop_futures.append(executor.submit(producer.stop))

            def done():
                """:return True iff all futures are done."""
                done_futures = [f.done() for f in stop_futures]
                return reduce(lambda x, y: x and y, done_futures, True)

            if not wait_until(lambda: done(), timeout_sec=30, backoff_sec=1):
                raise RuntimeError("Producers did not stop in a reasonable amount of time.")

    def drive_failures(self, failure, failure_permanence, node_type):

        if failure_permanence == KILL:
            node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)
            self.kafka.signal_node(node_to_signal, sig=failure)

        elif failure_permanence == BOUNCE:
            self.bounce(failure, node_type, self.num_bounce)
        else:
            raise RuntimeError("Invalid failure type")

    def bounce(self, signal, node_type, num_bounce):

        for i in range(num_bounce):
            assert self.kafka.all_alive()
            node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)
            self.logger.debug("Will bounce " + str(node_to_signal.account))
            assert node_to_signal != None

            def leader_or_controller_change():
                changed = node_to_signal is not None \
                    and node_to_signal != self.fetch_broker_node(self.topic_to_fail, 0, node_type)
                if signal == SIGSTOP:
                    # Check for deadness doesn't really work on a paused process
                    return changed
                else:
                    # Make sure the new controller/leader is elected *and* the old process is dead
                    # Otherwise the process will almost certainly fail to restart
                    return changed and self.kafka.dead(node_to_signal)

            self.kafka.bounce_node(
                node_to_signal, sig=signal, condition=leader_or_controller_change,
                condition_timeout_sec=30, condition_backoff_sec=.5)

        assert self.kafka.all_alive()

    def kill_permanently(self, failure, node_type):
        """Fail by sending a signal to the process, but don't revive."""
        node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)
        self.kafka.signal_node(node_to_signal, failure)
        num_acked = self.producers[0].num_acked

        # Make sure that writes are eventually able to go through again after the failure
        wait_until(lambda: self.producers[0].num_acked > num_acked + 100, timeout_sec=6)

    def validate(self):
        """Check that produced messages were consumed."""

        success = True
        msg = ""
        for i in range(len(self.topic_names)):
            consumed = self.messages_consumed[i]
            acked = self.acked[i]
            topic = self.topic_names[i]

            if len(consumed) == 0:
                msg += "No messages consumed under topic %s" % topic
                success = False

            if len(set(consumed)) != len(consumed):
                # There are duplicates. This is ok, so report it but don't fail the test
                msg += "There are duplicate messages in the log\n"

            if not set(consumed).issuperset(set(acked)):
                # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
                acked_minus_consumed = set(acked) - set(consumed)
                success = False
                msg += "At least one acked message did not appear in the consumed messages for topic %s: " % topic
                msg += "num acked from topic %s: %d. " % (topic, len(acked))
                msg += "num consumed from topic %s:  %d. " % (topic, len(consumed))

        if not success:
            # Collect all the data logs if there was a failure
            self.mark_for_collect(self.kafka)

        return success, msg

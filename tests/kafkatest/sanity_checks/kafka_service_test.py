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


from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize


from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.process_signal import SIGTERM, SIGKILL, SIGSTOP


class KafkaServiceTest(Test):
    def __init__(self, test_context):
        super(KafkaServiceTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.kafka = KafkaService(self.test_context, num_nodes=1, zk=self.zk)

    @parametrize(num_nodes=1)
    @parametrize(num_nodes=3)
    def test_start(self, num_nodes=1):
        """Check that simply starting the service works"""

        self.zk.start()
        self.kafka.num_nodes = num_nodes
        self.kafka.start()

        assert self.kafka.all_alive()
        for node in self.kafka.nodes:
            assert len(self.kafka.pids(node)) == 1

    def test_stop_node(self):
        """Check that stopping a node works, even with a SIGSTOPed process."""
        self.zk.start()
        self.kafka.start()

        node = self.kafka.nodes[0]
        self.kafka.signal_node(node, sig=SIGSTOP)
        self.kafka.stop_node(self.kafka.nodes[0])

        if not wait_until(lambda: self.kafka.dead(node), timeout_sec=5, backoff_sec=.5):
            raise RuntimeError("Kafka node failed to stop in a reasonable amount of time.")

    @parametrize(sig=SIGTERM)
    @parametrize(sig=SIGKILL)
    def test_signal_node(self, sig=SIGTERM):
        self.zk.start()
        self.kafka.start()
        assert self.kafka.all_alive()

        node = self.kafka.nodes[0]
        self.kafka.signal_node(node, sig=sig)
        if not wait_until(lambda: self.kafka.dead(node), timeout_sec=5, backoff_sec=.5):
            raise RuntimeError("Kafka node failed to stop in a reasonable amount of time.")

    @parametrize(sig=SIGTERM)
    # @parametrize(sig=SIGKILL)
    def test_bounce_node(self, sig=SIGTERM):
        self.zk.start()
        self.kafka.num_nodes = 3
        self.kafka.start()

        node = self.kafka.nodes[0]
        old_pid = self.kafka.pids(node)

        self.kafka.bounce_node(
            node, sig=sig, condition=lambda: self.kafka.dead(node), condition_timeout_sec=10, condition_backoff_sec=.5)
        assert self.kafka.alive(node)

        new_pid = self.kafka.pids(node)[0]
        assert new_pid != old_pid

    def test_find_controller(self):
        self.zk.start()
        self.kafka.start()

        assert self.kafka.controller() == self.kafka.nodes[0]


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


from ducktape.services.service import Service
from ducktape.utils.util import wait_until

from concurrent import futures
import json
import time


class ZookeeperService(Service):

    logs = {
        "zk_log": {
            "path": "/mnt/zk.log",
            "collect_default": True}
    }

    def __init__(self, context, num_nodes):
        """
        :type context
        """
        super(ZookeeperService, self).__init__(context, num_nodes)

    def start(self):
        super(ZookeeperService, self).start()
        if not wait_until(lambda: self.all_alive(), timeout_sec=20, backoff_sec=.5):
            raise RuntimeError("Timed out waiting for Zookeeper cluster to start.")

    def all_alive(self):
        for node in self.nodes:
            if not self.alive(node):
                return False
        return True

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting ZK node %d on %s", idx, node.account.hostname)

        node.account.ssh("mkdir -p /mnt/zookeeper")
        node.account.ssh("echo %d > /mnt/zookeeper/myid" % idx)

        config_file = self.render('zookeeper.properties')
        self.logger.info("zookeeper.properties:")
        self.logger.info(config_file)
        node.account.create_file("/mnt/zookeeper.properties", config_file)

        cmd = "/opt/kafka/bin/zookeeper-server-start.sh /mnt/zookeeper.properties 1>> %(path)s 2>> %(path)s &" % \
              self.logs["zk_log"]

        with futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(lambda: node.account.ssh(cmd))

    def alive(self, node):
        try:
            # Try opening tcp connection and immediately closing by sending EOF
            node.account.ssh("echo EOF | nc %s %d" % (node.account.hostname, 2181), allow_fail=False)
            return True
        except:
            return False

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        node.account.kill_process("zookeeper", allow_fail=False)

    def clean_node(self, node):
        self.logger.info("Cleaning ZK node %d on %s", self.idx(node), node.account.hostname)
        node.account.ssh("rm -rf /mnt/zookeeper /mnt/zookeeper.properties /mnt/zk.log", allow_fail=False)

    def connect_setting(self):
        return ','.join([node.account.hostname + ':2181' for node in self.nodes])

    def get_data(self, path):
        """
        Get data from zookeeper on the given path. This method assumes the data is in JSON format.

        :param path Zookeeper path to query for data
        """
        cmd = "/opt/kafka/bin/kafka-run-class.sh kafka.tools.ZooKeeperMainWrapper -server %s get %s" % \
              (self.connect_setting(), path)
        data = None
        node = self.nodes[0]
        for line in node.account.ssh_capture(cmd):
            try:
                data = json.loads(line)
                break
            except ValueError:
                pass

        return data
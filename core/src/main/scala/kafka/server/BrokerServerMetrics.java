/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class collects metrics from a broker
 */
public class BrokerServerMetrics implements Runnable {

    /* KafkaServer object */
    private final KafkaServer server;

    /* logger object */
    private static final Logger log = LoggerFactory.getLogger(BrokerServerMetrics.class);

    /* reporting interval in ms */
    private static final long reportInterval = 10*1000;

    /* simple constructor */
    public BrokerServerMetrics(KafkaServer server) {
        this.server = server;
    }

    /**
     * Run and produce stats
     */
    public void run() {

        while(true) {
            try {
                log.info("BrokerID={}:WrittenBytes={}", server.config().brokerId(), BrokerTopicStats.getBrokerAllTopicsStats().bytesInRate().count());
                log.info("BrokerID={}:ReadBytes={}", server.config().brokerId(), BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate().count());
                log.info("BrokerID={}:BytesInRate={}", server.config().brokerId(), BrokerTopicStats.getBrokerAllTopicsStats().bytesInRate().meanRate());
                log.info("BrokerID={}:BytesOutRate={}", server.config().brokerId(), BrokerTopicStats.getBrokerAllTopicsStats().bytesOutRate().meanRate());
                log.info("BrokerID={}:NumPartitions={}", server.config().brokerId(), server.replicaManager().partitionCount().value());
                Thread.sleep(reportInterval);
            } catch (Exception e) {
                log.debug("BrokerID={}:Exiting from BrokerServerMetrics thread", server.config().brokerId());
                break;
            }
        }
    }

}

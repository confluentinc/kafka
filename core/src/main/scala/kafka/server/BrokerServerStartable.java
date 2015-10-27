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

import kafka.utils.SystemTime$;
import scala.Option;
import org.apache.kafka.common.utils.Utils;

/**
 * This class is similar to KafkaServerStartable.scala
 * The difference is that we collect more metrics from the system
 */
public class BrokerServerStartable  {

    /* KafkaServer object */
    private final KafkaServer server;

    /* Thread that collects metrics */
    private final Thread metricThread;

    /* Runnable stats thread */
    private final BrokerServerMetrics brokerServerMetrics;

    public BrokerServerStartable(KafkaConfig serverConfig) {
        final Option<String> none = Option.empty();
        server = new KafkaServer(serverConfig, SystemTime$.MODULE$, none);
        brokerServerMetrics = new BrokerServerMetrics(server);
        metricThread = Utils.daemonThread("BrokerServerMetrics", brokerServerMetrics);
    }

    public void startup() {
        try {
            // start server
            server.startup();

            // start a daemon that monitors various metrics
            metricThread.start();

        } catch (Exception e) {
            System.exit(1);
        }
    }

    public void shutdown() {
        try {
            /* do a final dumping of stats */
            brokerServerMetrics.logStats();
            server.shutdown();
            metricThread.interrupt();
            metricThread.join();
        } catch (Exception e) {
            Runtime.getRuntime().halt(1);
        }
    }

    /**
     * Allow setting broker state from the startable.
     * This is needed when a custom kafka server startable want to emit new states that it introduces.
     */
    public void setServerState(Byte newState) {
        server.brokerState().newState(newState);
    }

    public void awaitShutdown() {
        server.awaitShutdown();
    }

}

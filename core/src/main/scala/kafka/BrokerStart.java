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

package kafka;

import java.util.Properties;
import kafka.metrics.KafkaMetricsReporter$;
import kafka.server.BrokerServerStartable;
import kafka.server.KafkaConfig;
import kafka.utils.VerifiableProperties;

/**
 * Main class that starts a Kafka broker and any associated threads
 * This class is similar to Kafka.scala
 */
public class BrokerStart  {

    public static void main(String[] args) throws Exception {
        try {
            Properties serverProps = Kafka.getPropsFromArgs(args);
            KafkaConfig serverConfig = KafkaConfig.fromProps(serverProps);
            KafkaMetricsReporter$.MODULE$.startReporters(new VerifiableProperties(serverProps));
            final BrokerServerStartable brokerServerStartable = new BrokerServerStartable(serverConfig);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    brokerServerStartable.shutdown();
                }
            });

            brokerServerStartable.startup();
            brokerServerStartable.awaitShutdown();

        } catch (Exception e) {
            System.exit(1);
        }
        System.exit(0);
    }
}

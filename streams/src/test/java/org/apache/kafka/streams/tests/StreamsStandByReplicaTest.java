/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.tests;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StreamsStandByReplicaTest {

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("StreamsStandByReplicaTest are expecting two parameters: " +
                "propFile, additionalConfigs; but only see " + args.length + " parameter");
            Exit.exit(1);
        }

        System.out.println("StreamsTest instance started");

        final String propFileName = args[0];
        final String additionalConfigs = args[1];

        final Properties streamsProperties = Utils.loadProps(propFileName);
        final String kafka = streamsProperties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (kafka == null) {
            System.err.println("No bootstrap kafka servers specified in " + StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
            Exit.exit(1);
        }
        
        streamsProperties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsProperties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProperties.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);

        if (additionalConfigs == null) {
            System.err.println("additional configs are not provided");
            System.err.flush();
            Exit.exit(1);
        }

        final Map<String, String> updated = SystemTestUtil.parseConfigs(additionalConfigs);
        System.out.println("Updating configs with " + updated);

        final String sourceTopic = updated.remove("sourceTopic");
        final String sinkTopic1 = updated.remove("sinkTopic1");
        final String sinkTopic2 = updated.remove("sinkTopic2");

        if (sourceTopic == null || sinkTopic1 == null || sinkTopic2 == null) {
            System.err.printf(
                "one or more required topics null sourceTopic[%s], sinkTopic1[%s], sinkTopic2[%s]%n",
                sourceTopic,
                sinkTopic1,
                sinkTopic2);
            System.err.flush();
            Exit.exit(1);
        }

        streamsProperties.putAll(updated);

        if (!confirmCorrectConfigs(streamsProperties)) {
            System.err.printf(
                    "ERROR: Did not have all required configs expected  to contain %s, %s,  %s,  %s%n",
                    StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG),
                    StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG),
                    StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG),
                    StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG)
            );

            Exit.exit(1);
        }

        final StreamsBuilder builder = new StreamsBuilder();

        final String inMemoryStoreName = "in-memory-store";
        final String persistentMemoryStoreName = "persistent-memory-store";

        final KeyValueBytesStoreSupplier inMemoryStoreSupplier = Stores.inMemoryKeyValueStore(inMemoryStoreName);
        final KeyValueBytesStoreSupplier persistentStoreSupplier = Stores.persistentKeyValueStore(persistentMemoryStoreName);

        final Serde<String> stringSerde = Serdes.String();
        final ValueMapper<Long, String> countMapper = Object::toString;

        final KStream<String, String> inputStream = builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde));

        inputStream.groupByKey().count(Materialized.as(inMemoryStoreSupplier)).toStream().mapValues(countMapper)
            .to(sinkTopic1, Produced.with(stringSerde, stringSerde));

        inputStream.groupByKey().count(Materialized.as(persistentStoreSupplier)).toStream().mapValues(countMapper)
            .to(sinkTopic2, Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProperties);

        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("FATAL: An unexpected exception " + e);
            e.printStackTrace(System.err);
            System.err.flush();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING && oldState == KafkaStreams.State.REBALANCING) {
                final Set<ThreadMetadata> threadMetadata = streams.metadataForLocalThreads();
                for (final ThreadMetadata threadMetadatum : threadMetadata) {
                    System.out.println(
                        "ACTIVE_TASKS:" + threadMetadatum.activeTasks().size()
                        + " STANDBY_TASKS:" + threadMetadatum.standbyTasks().size());
                }
            }
        });

        System.out.println("Start Kafka Streams");
        streams.start();

        Exit.addShutdownHook("streams-shutdown-hook", () -> {
            shutdown(streams);
            System.out.println("Shut down streams now");
        });
    }

    private static void shutdown(final KafkaStreams streams) {
        streams.close(Duration.ofSeconds(10));
    }

    private static boolean confirmCorrectConfigs(final Properties properties) {
        return properties.containsKey(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.RETRIES_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)) &&
               properties.containsKey(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG));
    }

}

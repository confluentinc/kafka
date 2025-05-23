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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.function.Predicate;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

@Timeout(600)
@Tag("integration")
public class StandbyTaskCreationIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private String safeTestName;

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) {
        safeTestName = safeUniqueTestName(testInfo);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final String INPUT_TOPIC = "input-topic";

    private KafkaStreams client1;
    private KafkaStreams client2;
    private volatile boolean client1IsOk = false;
    private volatile boolean client2IsOk = false;

    @AfterEach
    public void after() {
        client1.close(Duration.ofSeconds(60));
        client2.close(Duration.ofSeconds(60));
    }

    private Properties streamsConfiguration(final boolean streamsProtocolEnabled) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        if (streamsProtocolEnabled) {
            streamsConfiguration.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
            CLUSTER.setStandbyReplicas("app-" + safeTestName, 1);
        } else {
            streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        }
        return streamsConfiguration;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldNotCreateAnyStandByTasksForStateStoreWithLoggingDisabled(final boolean streamsProtocolEnabled) throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        final String stateStoreName = "myTransformState";
        final StoreBuilder<KeyValueStore<Integer, Integer>> keyValueStoreBuilder =
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(stateStoreName),
                                        Serdes.Integer(),
                                        Serdes.Integer()).withLoggingDisabled();
        builder.addStateStore(keyValueStoreBuilder);
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            // don't use method reference below as it won't create a new `Processor` instance but re-use the same object
            .process(() -> new Processor<Integer, Integer, Object, Object>() {
                @Override
                public void process(final Record<Integer, Integer> record) {
                    // no-op
                }
            }, stateStoreName);


        final Topology topology = builder.build();
        createClients(topology, streamsConfiguration(streamsProtocolEnabled), topology, streamsConfiguration(streamsProtocolEnabled));

        setStateListenersForVerification(thread -> thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

        startClients();

        waitUntilBothClientAreOK(
            "At least one client did not reach state RUNNING with active tasks but no stand-by tasks"
        );
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldCreateStandByTasksForMaterializedAndOptimizedSourceTables(final boolean streamsProtocolEnabled) throws Exception {
        final Properties streamsConfiguration1 = streamsConfiguration(streamsProtocolEnabled);
        streamsConfiguration1.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        final Properties streamsConfiguration2 = streamsConfiguration(streamsProtocolEnabled);
        streamsConfiguration2.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.Integer()), Materialized.as("source-table"));

        createClients(
            builder.build(streamsConfiguration1),
            streamsConfiguration1,
            builder.build(streamsConfiguration2),
            streamsConfiguration2
        );

        setStateListenersForVerification(thread -> !thread.standbyTasks().isEmpty() && !thread.activeTasks().isEmpty());

        startClients();

        waitUntilBothClientAreOK(
            "At least one client did not reach state RUNNING with active tasks and stand-by tasks"
        );
    }

    private void createClients(final Topology topology1,
                               final Properties streamsConfiguration1,
                               final Topology topology2,
                               final Properties streamsConfiguration2) {

        client1 = new KafkaStreams(topology1, streamsConfiguration1);
        client2 = new KafkaStreams(topology2, streamsConfiguration2);
    }

    private void setStateListenersForVerification(final Predicate<ThreadMetadata> taskCondition) {
        client1.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING &&
                client1.metadataForLocalThreads().stream().allMatch(taskCondition)) {

                client1IsOk = true;
            }
        });
        client2.setStateListener((newState, oldState) -> {
            if (newState == State.RUNNING &&
                client2.metadataForLocalThreads().stream().allMatch(taskCondition)) {

                client2IsOk = true;
            }
        });
    }

    private void startClients() {
        client1.start();
        client2.start();
    }

    private void waitUntilBothClientAreOK(final String message) throws Exception {
        TestUtils.waitForCondition(
            () -> client1IsOk && client2IsOk,
            30 * 1000,
            message + ": "
                + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
                + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
        );
    }
}

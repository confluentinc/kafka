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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
public class HandlingSourceTopicDeletionIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;
    private static final long TIMEOUT = 60000;
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void before() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @AfterEach
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void shouldThrowErrorAfterSourceTopicDeleted(final TestInfo testName) throws InterruptedException {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));

        final String safeTestName = safeUniqueTestName(testName);
        final String appId = "app-" + safeTestName;

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        streamsConfiguration.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 2000);

        final Topology topology = builder.build();
        final AtomicBoolean calledUncaughtExceptionHandler1 = new AtomicBoolean(false);
        final AtomicBoolean calledUncaughtExceptionHandler2 = new AtomicBoolean(false);

        try (final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, streamsConfiguration);
             final KafkaStreams kafkaStreams2 = new KafkaStreams(topology, streamsConfiguration)) {
            
            kafkaStreams1.setUncaughtExceptionHandler(exception -> {
                calledUncaughtExceptionHandler1.set(true);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            kafkaStreams1.start();

            kafkaStreams2.setUncaughtExceptionHandler(exception -> {
                calledUncaughtExceptionHandler2.set(true);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
            kafkaStreams2.start();

            TestUtils.waitForCondition(
                () -> kafkaStreams1.state() == State.RUNNING && kafkaStreams2.state() == State.RUNNING,
                TIMEOUT,
                () -> "Kafka Streams clients did not reach state RUNNING"
            );

            CLUSTER.deleteTopic(INPUT_TOPIC);

            TestUtils.waitForCondition(
                () -> kafkaStreams1.state() == State.ERROR && kafkaStreams2.state() == State.ERROR,
                TIMEOUT,
                () -> "Kafka Streams clients did not reach state ERROR"
            );

            assertThat(calledUncaughtExceptionHandler1.get(), is(true));
            assertThat(calledUncaughtExceptionHandler2.get(), is(true));
        }
    }
}

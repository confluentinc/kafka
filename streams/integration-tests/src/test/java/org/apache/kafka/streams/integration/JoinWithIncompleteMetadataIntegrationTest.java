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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class JoinWithIncompleteMetadataIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final String APP_ID = "join-incomplete-metadata-integration-test";
    private static final Long COMMIT_INTERVAL = 100L;
    static final Properties STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String NON_EXISTENT_INPUT_TOPIC_LEFT = "inputTopicLeft-not-exist";
    static final String OUTPUT_TOPIC = "outputTopic";

    StreamsBuilder builder;
    final ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> value1 + "-" + value2;
    private KTable<Long, String> rightTable;

    @BeforeEach
    public void prepareTopology() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC_RIGHT, OUTPUT_TOPIC);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

        builder = new StreamsBuilder();
        rightTable = builder.table(INPUT_TOPIC_RIGHT);
    }

    @AfterEach
    public void cleanup() throws InterruptedException, IOException {
        CLUSTER.deleteAllTopics();
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testShouldAutoShutdownOnJoinWithIncompleteMetadata(final boolean useNewProtocol) throws InterruptedException {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        
        if (useNewProtocol) {
            STREAMS_CONFIG.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
        }

        final KStream<Long, String> notExistStream = builder.stream(NON_EXISTENT_INPUT_TOPIC_LEFT);

        final KTable<Long, String> aggregatedTable = notExistStream.leftJoin(rightTable, valueJoiner)
                .groupBy((key, value) -> key)
                .reduce((value1, value2) -> value1 + value2);

        // Write the (continuously updating) results to the output topic.
        aggregatedTable.toStream().to(OUTPUT_TOPIC);

        final KafkaStreamsWrapper streams = new KafkaStreamsWrapper(builder.build(), STREAMS_CONFIG);
        final IntegrationTestUtils.StateListenerStub listener = new IntegrationTestUtils.StateListenerStub();
        streams.setStreamThreadStateListener(listener);
        streams.start();

        TestUtils.waitForCondition(listener::transitToPendingShutdownSeen, "Did not seen thread state transited to PENDING_SHUTDOWN");

        streams.close();
        assertTrue(listener.transitToPendingShutdownSeen());
    }
}

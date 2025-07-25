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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KStreamKTableLeftJoinTest {
    private static final KeyValueTimestamp[] EMPTY = new KeyValueTimestamp[0];

    private final String streamTopic = "streamTopic";
    private final String tableTopic = "tableTopic";
    private TestInputTopic<Integer, String> inputStreamTopic;
    private TestInputTopic<Integer, String> inputTableTopic;
    private final int[] expectedKeys = {0, 1, 2, 3};

    private TopologyTestDriver driver;
    private MockApiProcessor<Integer, String, Void, Void> processor;
    private StreamsBuilder builder;

    @BeforeEach
    public void setUp() {
        builder = new StreamsBuilder();

        final KStream<Integer, String> stream;
        final KTable<Integer, String> table;

        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
        stream = builder.stream(streamTopic, consumed);
        table = builder.table(tableTopic, consumed);
        stream.leftJoin(table, MockValueJoiner.TOSTRING_JOINER).process(supplier);

        final Properties props = StreamsTestUtils.getStreamsConfig(Serdes.Integer(), Serdes.String());
        driver = new TopologyTestDriver(builder.build(), props);
        inputStreamTopic = driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        inputTableTopic = driver.createInputTopic(tableTopic, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        processor = supplier.theCapturedProcessor();
    }

    @AfterEach
    public void cleanup() {
        driver.close();
    }

    private void pushToStream(final int messageCount, final String valuePrefix) {
        for (int i = 0; i < messageCount; i++) {
            inputStreamTopic.pipeInput(expectedKeys[i], valuePrefix + expectedKeys[i], i);
        }
    }

    private void pushToTable(final int messageCount, final String valuePrefix) {
        final Random r = new Random(System.currentTimeMillis());
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(
                expectedKeys[i],
                valuePrefix + expectedKeys[i],
                r.nextInt(Integer.MAX_VALUE));
        }
    }

    private void pushNullValueToTable(final int messageCount) {
        for (int i = 0; i < messageCount; i++) {
            inputTableTopic.pipeInput(expectedKeys[i], null);
        }
    }

    @Test
    public void shouldRequireCopartitionedStreams() {
        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(streamTopic, tableTopic), copartitionGroups.iterator().next());
    }

    @Test
    public void shouldJoinWithEmptyTableOnStreamUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+null", 0),
                new KeyValueTimestamp<>(1, "X1+null", 1));
    }

    @Test
    public void shouldNotJoinOnTableUpdates() {
        // push two items to the primary stream. the table is empty
        pushToStream(2, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+null", 0),
                new KeyValueTimestamp<>(1, "X1+null", 1));

        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1),
                new KeyValueTimestamp<>(2, "X2+null", 2),
                new KeyValueTimestamp<>(3, "X3+null", 3));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YY");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+YY0", 0),
                new KeyValueTimestamp<>(1, "X1+YY1", 1),
                new KeyValueTimestamp<>(2, "X2+YY2", 2),
                new KeyValueTimestamp<>(3, "X3+YY3", 3));

        // push all items to the table. this should not produce any item
        pushToTable(4, "YYY");
        processor.checkAndClearProcessResult(EMPTY);
    }

    @Test
    public void shouldJoinRegardlessIfMatchFoundOnStreamUpdates() {
        // push two items to the table. this should not produce any item.
        pushToTable(2, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1),
                new KeyValueTimestamp<>(2, "X2+null", 2),
                new KeyValueTimestamp<>(3, "X3+null", 3));

    }

    @Test
    public void shouldClearTableEntryOnNullValueUpdates() {
        // push all four items to the table. this should not produce any item.
        pushToTable(4, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "X");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "X0+Y0", 0),
                new KeyValueTimestamp<>(1, "X1+Y1", 1),
                new KeyValueTimestamp<>(2, "X2+Y2", 2),
                new KeyValueTimestamp<>(3, "X3+Y3", 3));

        // push two items with null to the table as deletes. this should not produce any item.
        pushNullValueToTable(2);
        processor.checkAndClearProcessResult(EMPTY);

        // push all four items to the primary stream. this should produce four items.
        pushToStream(4, "XX");
        processor.checkAndClearProcessResult(new KeyValueTimestamp<>(0, "XX0+null", 0),
                new KeyValueTimestamp<>(1, "XX1+null", 1),
                new KeyValueTimestamp<>(2, "XX2+Y2", 2),
                new KeyValueTimestamp<>(3, "XX3+Y3", 3));
    }

    @Test
    public void shouldNotDropLeftNullKey() {
        // push all four items to the table. this should not produce any item.
        pushToTable(1, "Y");
        processor.checkAndClearProcessResult(EMPTY);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoin.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(null, "A", 0);

            processor.checkAndClearProcessResult(new KeyValueTimestamp<>(null, "A+null", 0));

            assertTrue(appender.getMessages().isEmpty());
        }

        assertEquals(
            0.0,
            driver.metrics().get(
                new MetricName(
                    "dropped-records-total",
                    "stream-task-metrics",
                    "",
                    mkMap(
                        mkEntry("thread-id", Thread.currentThread().getName()),
                        mkEntry("task-id", "0_0")
                    )
                ))
                .metricValue()
        );
    }

    @Test
    public void shouldLogAndMeterWhenSkippingNullLeftValue() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KStreamKTableJoin.class)) {
            final TestInputTopic<Integer, String> inputTopic =
                driver.createInputTopic(streamTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(1, null);

            assertTrue(appender.getMessages().contains("Skipping record due to null join key or value. topic=[streamTopic] partition=[0] offset=[0]"));
        }

        assertEquals(
            1.0,
            driver.metrics().get(
                new MetricName(
                    "dropped-records-total",
                    "stream-task-metrics",
                    "",
                    mkMap(
                        mkEntry("thread-id", Thread.currentThread().getName()),
                        mkEntry("task-id", "0_0")
                    )
                ))
                .metricValue()
        );
    }
}

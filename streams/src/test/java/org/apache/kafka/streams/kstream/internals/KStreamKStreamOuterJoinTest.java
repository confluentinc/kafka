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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.state.DslKeyValueParams;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.WrappedStateStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.MockApiProcessor;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class KStreamKStreamOuterJoinTest {
    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final Consumed<Integer, String> consumed = Consumed.with(Serdes.Integer(), Serdes.String());
    private final Consumed<Integer, Long> consumed2 = Consumed.with(Serdes.Integer(), Serdes.Long());
    private static final Properties PROPS = StreamsTestUtils.getStreamsConfig(Serdes.String(), Serdes.String());

    @SuppressWarnings("deprecation") // old join semantics; can be removed when `JoinWindows.of()` is removed
    @Test
    public void testOuterJoinDuplicatesWithFixDisabledOldApi() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.of(ofMillis(100L)).grace(ofMillis(10L)),
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(PROPS), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                    driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                    driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // Only 2 window stores should be available
            assertEquals(2, driver.getAllStateStores().size());

            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(0, "A0-0", 0L);
            inputTopic2.pipeInput(0, 10L, 0L);
            inputTopic2.pipeInput(1, 21L, 0L);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 0L),
                new KeyValueTimestamp<>(0, "A0-0+null", 0L),
                new KeyValueTimestamp<>(0, "A0+10", 0L),
                new KeyValueTimestamp<>(0, "A0-0+10", 0L),
                new KeyValueTimestamp<>(1, "null+21", 0L)
            );
        }
    }

    @Test
    public void testOuterJoinDuplicates() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(10L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // verifies non-joined duplicates are emitted when window has closed
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(0, "A0-0", 0L);
            inputTopic2.pipeInput(1, 11L, 0L);
            inputTopic2.pipeInput(1, 12L, 0L);
            inputTopic2.pipeInput(1, 10L, 111L);
            // bump stream-time to trigger outer-join results
            inputTopic2.pipeInput(3, 100L, 211);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+11", 0L),
                new KeyValueTimestamp<>(1, "null+12", 0L),
                new KeyValueTimestamp<>(0, "A0+null", 0L),
                new KeyValueTimestamp<>(0, "A0-0+null", 0L)
            );

            // verifies joined duplicates are emitted
            inputTopic1.pipeInput(2, "A2", 200L);
            inputTopic1.pipeInput(2, "A2-0", 200L);
            inputTopic2.pipeInput(2, 13L, 201L);
            inputTopic2.pipeInput(2, 14L, 201L);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+13", 201L),
                new KeyValueTimestamp<>(2, "A2-0+13", 201L),
                new KeyValueTimestamp<>(2, "A2+14", 201L),
                new KeyValueTimestamp<>(2, "A2-0+14", 201L)
            );

            // this record should expired non-joined records; only null+10 will be emitted because
            // it did not have a join
            inputTopic2.pipeInput(3, 100L, 1500L);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+10", 111L),
                new KeyValueTimestamp<>(3, "null+100", 211)
            );
        }
    }

    @Test
    public void testLeftExpiredNonJoinedRecordsAreEmittedByTheLeftProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic1.pipeInput(0, "A0", windowStart + 1L);
            inputTopic1.pipeInput(1, "A1", windowStart + 2L);
            inputTopic1.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic2.pipeInput(1, 11L, windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+11", windowStart + 3L)
            );

            // Dummy record in left topic will emit expired non-joined records from the left topic
            inputTopic1.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", windowStart + 1L),
                new KeyValueTimestamp<>(0, "A0-0+null", windowStart + 3L)
            );

            // Flush internal non-joined state store by joining the dummy record
            inputTopic2.pipeInput(2, 100L, windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+100", windowStart + 401L)
            );
        }
    }

    @Test
    public void testLeftExpiredNonJoinedRecordsAreEmittedByTheRightProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(0L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic1.pipeInput(0, "A0", windowStart + 1L);
            inputTopic1.pipeInput(1, "A1", windowStart + 2L);
            inputTopic1.pipeInput(0, "A0-0", windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic2.pipeInput(1, 11L, windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+11", windowStart + 3L)
            );

            // Dummy record in right topic will emit expired non-joined records from the left topic
            inputTopic2.pipeInput(2, 100L, windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", windowStart + 1L),
                new KeyValueTimestamp<>(0, "A0-0+null", windowStart + 3L)
            );

            // Flush internal non-joined state store by joining the dummy record
            inputTopic1.pipeInput(2, "dummy", windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+100", windowStart + 402L)
            );
        }
    }

    @Test
    public void testRightExpiredNonJoinedRecordsAreEmittedByTheLeftProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic2.pipeInput(0, 10L, windowStart + 1L);
            inputTopic2.pipeInput(1, 11L, windowStart + 2L);
            inputTopic2.pipeInput(0, 12L, windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic1.pipeInput(1, "A1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+11", windowStart + 3L)
            );

            // Dummy record in left topic will emit expired non-joined records from the right topic
            inputTopic1.pipeInput(2, "dummy", windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "null+10", windowStart + 1L),
                new KeyValueTimestamp<>(0, "null+12", windowStart + 3L)
            );

            // Process the dummy joined record
            inputTopic2.pipeInput(2, 100L, windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+100", windowStart + 402L)
            );
        }
    }

    @Test
    public void testRightExpiredNonJoinedRecordsAreEmittedByTheRightProcessor() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();

        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);
        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(0L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            final long windowStart = 0L;

            // No joins detected; No null-joins emitted
            inputTopic2.pipeInput(0, 10L, windowStart + 1L);
            inputTopic2.pipeInput(1, 11L, windowStart + 2L);
            inputTopic2.pipeInput(0, 12L, windowStart + 3L);
            processor.checkAndClearProcessResult();

            // Join detected; No null-joins emitted
            inputTopic1.pipeInput(1, "A1", windowStart + 3L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+11", windowStart + 3L)
            );

            // Dummy record in right topic will emit expired non-joined records from the right topic
            inputTopic2.pipeInput(2, 100L, windowStart + 401L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "null+10", windowStart + 1L),
                new KeyValueTimestamp<>(0, "null+12", windowStart + 3L)
            );

            // Process the dummy joined record
            inputTopic1.pipeInput(2, "dummy", windowStart + 402L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "dummy+100", windowStart + 402L)
            );
        }
    }

    @Test
    public void testOrdering() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // push two items to the primary stream; the other window is empty; this should not produce any item yet
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 100) }
            // --> w2 = {}
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic1.pipeInput(1, "A1", 100L);
            processor.checkAndClearProcessResult();

            // push one item to the other window that has a join;
            // this should produce the not-joined record first;
            // then the joined record
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 100) }
            // w2 = { }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 100) }
            // --> w2 = { 1:11 (ts: 110) }
            inputTopic2.pipeInput(1, 11L, 110L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 0L),
                new KeyValueTimestamp<>(1, "A1+11", 110L)
            );
        }
    }

    @Test
    public void testGracePeriod() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100), ofMillis(10)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(topic1, topic2), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // push one item to the primary stream; and one item in other stream; this should not produce items because there are no joins
            // and window has not ended
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0) }
            // --> w2 = { 1:11 (ts: 0) }
            inputTopic1.pipeInput(0, "A0", 0L);
            inputTopic2.pipeInput(1, 11L, 0L);
            processor.checkAndClearProcessResult();

            // push one item on each stream with a window time after the previous window ended (not closed); this should not produce
            // joined records because the window has ended, but will not produce non-joined records because the window has not closed.
            // w1 = { 0:A0 (ts: 0) }
            // w2 = { 1:11 (ts: 0) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 101) }
            // --> w2 = { 1:11 (ts: 0), 0:10 (ts: 101) }
            inputTopic1.pipeInput(1, "A1", 101L);
            inputTopic2.pipeInput(0, 10L, 101L);
            processor.checkAndClearProcessResult();

            // push a 100 item to the any stream after the window is closed; this should produced all expired non-joined records because
            // the window has closed
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = { 0:10 (ts: 101), 1:11 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:10 (ts: 101), 1:11 (ts: 101), 0:100 (ts: 112) }
            inputTopic2.pipeInput(0, 100L, 112);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+11", 0L),
                new KeyValueTimestamp<>(0, "A0+null", 0L)
            );
        }
    }

    @Test
    public void testEmitAllNonJoinedResultsForAsymmetricWindow() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(5)).after(ofMillis(20)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(topic1, topic2), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // push one item to the primary stream; this should not produce any items because there are no matching keys
            // and window has not ended
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 29) }
            // --> w2 = {}
            inputTopic1.pipeInput(0, "A0", 29L);
            processor.checkAndClearProcessResult();

            // push another item to the primary stream; this should not produce any items because there are no matching keys
            // and window has not ended
            // w1 = { 0:A0 (ts: 29) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // --> w2 = {}
            inputTopic1.pipeInput(1, "A1", 30L);
            processor.checkAndClearProcessResult();

            // push one item to the other stream; this should not produce any items because there are no matching keys
            // and window has not ended
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 30) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // --> w2 = { 2:12 (ts: 31) }
            inputTopic2.pipeInput(2, 12L, 31L);
            processor.checkAndClearProcessResult();

            // push another item to the other stream; this should produce no inner joined-items because there are no matching keys 
            // and window has not ended
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 30) }
            // w2 = { 2:12 (ts: 31) }
            // --> w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // --> w2 = { 2:12 (ts: 31), 3:13 (ts: 36) }
            inputTopic2.pipeInput(3, 13L, 36L);
            processor.checkAndClearProcessResult();

            // push another item to the other stream; this should produce no inner joined-items because there are no matching keys 
            // and should produce a right-join-item because before window has ended
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 30) }
            // w2 = { 2:12 (ts: 31), 3:13 (ts: 36) }
            // --> w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // --> w2 = { 2:12 (ts: 31), 3:13 (ts: 36), 4:14 (ts: 37) }
            inputTopic2.pipeInput(4, 14L, 37L);
            processor.checkAndClearProcessResult(
                    new KeyValueTimestamp<>(2, "null+12", 31L)
            );

            // push another item to the other stream; this should produce no inner joined-items because there are no matching keys 
            // and should produce a left-join-item because after window has ended
            // and should produce two right-join-items because before window has ended
            // w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // w2 = { 2:10 (ts: 31), 3:13 (ts: 36), 4:14 (ts: 37) }
            // --> w1 = { 0:A0 (ts: 29), 1:A1 (ts: 30) }
            // --> w2 = { 2:12 (ts: 31), 3:13 (ts: 36), 4:14 (ts: 37), 5:15 (ts: 50) }
            inputTopic2.pipeInput(5, 15L, 50L);
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 29L),
                new KeyValueTimestamp<>(3, "null+13", 36L),
                new KeyValueTimestamp<>(4, "null+14", 37L)
            );
        }
    }
    
    @Test
    public void testOuterJoinWithInMemoryCustomSuppliers() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L));

        final WindowBytesStoreSupplier thisStoreSupplier = Stores.inMemoryWindowStore(
            "in-memory-join-store",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()),
            true
        );

        final WindowBytesStoreSupplier otherStoreSupplier = Stores.inMemoryWindowStore(
            "in-memory-join-store-other",
            Duration.ofMillis(joinWindows.size() + joinWindows.gracePeriodMs()),
            Duration.ofMillis(joinWindows.size()),
            true
        );

        final StreamJoined<Integer, String, Long> streamJoined = StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long());

        runOuterJoin(streamJoined.withThisStoreSupplier(thisStoreSupplier).withOtherStoreSupplier(otherStoreSupplier), joinWindows);
    }

    @Test
    public void testOuterJoinWithDefaultSuppliers() {
        final JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L));
        final StreamJoined<Integer, String, Long> streamJoined = StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long());

        runOuterJoin(streamJoined, joinWindows);
    }

    public void runOuterJoin(final StreamJoined<Integer, String, Long> streamJoined,
                             final JoinWindows joinWindows) {
        final StreamsBuilder builder = new StreamsBuilder();

        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            joinWindows,
            streamJoined
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(topic1, topic2), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            // 2 window stores + 1 shared window store should be available
            assertEquals(3, driver.getAllStateStores().size());

            // push two items to the primary stream; the other window is empty; this should not
            // produce any items because window has not expired
            // w1 {}
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult();

            // push two items to the other stream; this should produce two full-joined items
            // w1 = { 0:A0, 1:A1 }
            // w2 {}
            // --> w1 = { 0:A0, 1:A1 }
            // --> w2 = { 0:10, 1:11 }
            for (int i = 0; i < 2; i++) {
                inputTopic2.pipeInput(expectedKeys[i], (long) expectedKeys[i] + 10);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+10", 0L),
                new KeyValueTimestamp<>(1, "A1+11", 0L)
            );

            // push three items to the primary stream; this should produce two full-joined items
            // w1 = { 0:A0, 1:A1 }
            // w2 = { 0:10, 1:11 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:10, 1:11 }
            for (int i = 0; i < 3; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "B" + expectedKeys[i]);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "B0+10", 0L),
                new KeyValueTimestamp<>(1, "B1+11", 0L)
            );

            // push all items to the other stream; this should produce five full-joined items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:10, 1:11 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // --> w2 = { 0:10, 1:11, 0:20, 1:21, 2:22, 3:23 }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, (long) expectedKey + 20);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+20", 0L),
                new KeyValueTimestamp<>(0, "B0+20", 0L),
                new KeyValueTimestamp<>(1, "A1+21", 0L),
                new KeyValueTimestamp<>(1, "B1+21", 0L),
                new KeyValueTimestamp<>(2, "B2+22", 0L)
            );

            // push all four items to the primary stream; this should produce six full-joined items
            // w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2 }
            // w2 = { 0:10, 1:11, 0:20, 1:21, 2:22, 3:23 }
            // --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 0:C0, 1:C1, 2:C2, 3:C3 }
            // --> w2 = { 0:10, 1:11, 0:20, 1:21, 2:22, 3:23 }
            for (final int expectedKey : expectedKeys) {
                inputTopic1.pipeInput(expectedKey, "C" + expectedKey);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "C0+10", 0L),
                new KeyValueTimestamp<>(0, "C0+20", 0L),
                new KeyValueTimestamp<>(1, "C1+11", 0L),
                new KeyValueTimestamp<>(1, "C1+21", 0L),
                new KeyValueTimestamp<>(2, "C2+22", 0L),
                new KeyValueTimestamp<>(3, "C3+23", 0L)
            );

            // push a dummy record that should expire non-joined items; it should not produce any items because
            // all of them are joined
            inputTopic1.pipeInput(0, "dummy", 400L);
            processor.checkAndClearProcessResult();
        }
    }

    @Test
    public void testWindowing() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[]{0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        final Collection<Set<String>> copartitionGroups =
            TopologyWrapper.getInternalTopologyBuilder(builder.build()).copartitionGroups();

        assertEquals(1, copartitionGroups.size());
        assertEquals(Set.of(topic1, topic2), copartitionGroups.iterator().next());

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            final long time = 0L;

            // push two items to the primary stream; the other window is empty; this should not produce items because window has not closed
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time);
            }
            processor.checkAndClearProcessResult();

            // push four items to the other stream; this should produce two full-join items
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
            // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0) }
            for (final int expectedKey : expectedKeys) {
                inputTopic2.pipeInput(expectedKey, (long) expectedKey + 10, time);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+10", 0L),
                new KeyValueTimestamp<>(1, "A1+11", 0L)
            );

            testUpperWindowBound(expectedKeys, driver, processor);
            testLowerWindowBound(expectedKeys, driver, processor);
        }
    }

    @Test
    public void testShouldNotEmitLeftJoinResultForAsymmetricBeforeWindow() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)).before(ZERO),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; this should not produce any items
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult();

            // push one item to the other stream; this should produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:10 (ts: 100) }
            time += 100L;
            inputTopic2.pipeInput(expectedKeys[0], (long) expectedKeys[0] + 10, time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+10", 100L)
            );

            // push one item to the other stream; this should produce one left-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:10 (ts: 100) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:10 (ts: 100), 1:11 (ts: 102) }
            time += 2;
            inputTopic2.pipeInput(expectedKeys[1], (long) expectedKeys[1] + 10, time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+null", 1L)
            );

            // push one item to the other stream; this should produce one right-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:10 (ts: 100), 1:11 (ts: 102) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 0:10 (ts: 100), 1:11 (ts: 102), 2:12 (ts: 103) }
            time += 1;
            inputTopic2.pipeInput(expectedKeys[2], (long) expectedKeys[2] + 10, time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "null+11", 102L)
            );

            // push one item to the first stream; this should not produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 0:10 (ts: 100), 1:11 (ts: 102), 2:12 (ts: 103) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1), 2:A2 (ts: 103) }
            // --> w2 = { 0:10 (ts: 100), 1:11 (ts: 102), 2:12 (ts: 103)  }
            inputTopic1.pipeInput(expectedKeys[2], "A" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+12", 103L)
            );
        }
    }

    @Test
    public void testShouldNotEmitLeftJoinResultForAsymmetricAfterWindow() {
        final StreamsBuilder builder = new StreamsBuilder();
        final int[] expectedKeys = new int[] {0, 1, 2, 3};

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100)).after(ZERO),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();
            long time = 0L;

            // push two items to the primary stream; the other window is empty; 
            // this should produce one left-joined item
            // w1 = {}
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = {}
            for (int i = 0; i < 2; i++) {
                inputTopic1.pipeInput(expectedKeys[i], "A" + expectedKeys[i], time + i);
            }
            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(0, "A0+null", 0L)
            );

            // push one item to the other stream; this should produce one full-join item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = {}
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:11 (ts: 1) }
            time += 1;
            inputTopic2.pipeInput(expectedKeys[1], (long) expectedKeys[1] + 10, time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(1, "A1+11", 1L)
            );

            // push one item to the other stream;
            // this should not produce any item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:11 (ts: 1) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:11 (ts: 1), 2:12 (ts: 101) }
            time += 100;
            inputTopic2.pipeInput(expectedKeys[2], (long) expectedKeys[2] + 10, time);

            processor.checkAndClearProcessResult();

            // push one item to the other stream; this should not produce any item
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:11 (ts: 1), 2:12 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // --> w2 = { 1:11 (ts: 1), 2:12 (ts: 101), 3:13 (ts: 101) }
            inputTopic2.pipeInput(expectedKeys[3], (long) expectedKeys[3] + 10, time);

            processor.checkAndClearProcessResult();

            // push one item to the first stream;
            // this should produce one inner-join item;
            // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1) }
            // w2 = { 1:11 (ts: 1), 2:12 (ts: 101), 3:13 (ts: 101) }
            // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 1), 2:A2 (ts: 201) }
            // --> w2 = { 1:11 (ts: 1), 2:12 (ts: 101), 3:13 (ts: 101) }
            time += 100;
            inputTopic1.pipeInput(expectedKeys[2], "A" + expectedKeys[2], time);

            processor.checkAndClearProcessResult(
                new KeyValueTimestamp<>(2, "A2+12", 201L)
            );
        }
    }

    /**
     * NOTE: Header forwarding is undefined behavior, but we still want to understand the
     * behavior so that we can make decisions about defining it in the future.
     */
    @Test
    public void testShouldForwardCurrentHeaders() {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, Long> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed2);

        joined = stream1.outerJoin(
            stream2,
            MockValueJoiner.TOSTRING_JOINER,
            JoinWindows.ofTimeDifferenceAndGrace(ofMillis(100L), ofMillis(10L)),
            StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.Long())
        );
        joined.process(supplier);

        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(), PROPS)) {
            final TestInputTopic<Integer, String> inputTopic1 =
                driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final TestInputTopic<Integer, Long> inputTopic2 =
                driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
            final MockApiProcessor<Integer, String, Void, Void> processor = supplier.theCapturedProcessor();

            inputTopic1.pipeInput(new TestRecord<>(
                0,
                "A0",
                new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x1})}),
                0L
            ));
            inputTopic2.pipeInput(new TestRecord<>(
                1,
                10L,
                new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x2})}),
                0L
            ));
            // bump stream-time to trigger outer-join results
            inputTopic2.pipeInput(new TestRecord<>(
                3,
                100L,
                new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x3})}),
                (long) 211
            ));

            // Again, header forwarding is undefined, but the current observed behavior is that
            // the headers pass through the forwarding record.
            processor.checkAndClearProcessedRecords(
                new Record<>(
                    1,
                    "null+10",
                    0L,
                    new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x3})})
                ),
                new Record<>(
                    0,
                    "A0+null",
                    0L,
                    new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x3})})
                )
            );

            // verifies joined duplicates are emitted
            inputTopic1.pipeInput(new TestRecord<>(
                2,
                "A2",
                new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x4})}),
                200L
            ));
            inputTopic2.pipeInput(new TestRecord<>(
                2,
                12L,
                new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x5})}),
                200L
            ));

            processor.checkAndClearProcessedRecords(
                new Record<>(
                    2,
                    "A2+12",
                    200L,
                    new RecordHeaders(new Header[]{new RecordHeader("h", new byte[]{0x5})})
                )
            );
        }
    }

    private void testUpperWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockApiProcessor<Integer, String, Void, Void> processor) {
        long time;

        final TestInputTopic<Integer, String> inputTopic1 =
            driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);
        final TestInputTopic<Integer, Long> inputTopic2 =
            driver.createInputTopic(topic2, new IntegerSerializer(), new LongSerializer(), Instant.ofEpochMilli(0L), Duration.ZERO);

        // push four items with larger and increasing timestamp (out of window) to the other stream; this should produced 2 expired non-joined records
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time = 1000L;
        for (int i = 0; i < expectedKeys.length; i++) {
            inputTopic2.pipeInput(expectedKeys[i], (long) expectedKeys[i] + 20, time + i);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "null+12", 0L),
            new KeyValueTimestamp<>(3, "null+13", 0L)
        );

        // push four items with larger timestamp to the primary stream; this should produce four full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time = 1000L + 100L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "B" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "B0+20", 1100L),
            new KeyValueTimestamp<>(1, "B1+21", 1100L),
            new KeyValueTimestamp<>(2, "B2+22", 1100L),
            new KeyValueTimestamp<>(3, "B3+23", 1100L)
        );

        // push four items with increased timestamp to the primary stream; this should produce three full-join items (non-joined item is not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "C" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(1, "C1+21", 1101L),
            new KeyValueTimestamp<>(2, "C2+22", 1101L),
            new KeyValueTimestamp<>(3, "C3+23", 1101L)
        );

        // push four items with increased timestamp to the primary stream; this should produce two full-join items (non-joined items are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "D" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(2, "D2+22", 1102L),
            new KeyValueTimestamp<>(3, "D3+23", 1102L)
        );

        // push four items with increased timestamp to the primary stream; this should produce one full-join items (three non-joined left-join are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "E" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(3, "E3+23", 1103L)
        );

        // push four items with increased timestamp to the primary stream; this should produce no full-join items (four non-joined left-join are not produced yet)
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "F" + expectedKey, time);
        }
        processor.checkAndClearProcessResult();

        // push a dummy record to produce all left-join non-joined items
        time += 301L;
        inputTopic1.pipeInput(0, "dummy", time);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "C0+null", 1101L),
            new KeyValueTimestamp<>(0, "D0+null", 1102L),
            new KeyValueTimestamp<>(1, "D1+null", 1102L),
            new KeyValueTimestamp<>(0, "E0+null", 1103L),
            new KeyValueTimestamp<>(1, "E1+null", 1103L),
            new KeyValueTimestamp<>(2, "E2+null", 1103L),
            new KeyValueTimestamp<>(0, "F0+null", 1104L),
            new KeyValueTimestamp<>(1, "F1+null", 1104L),
            new KeyValueTimestamp<>(2, "F2+null", 1104L),
            new KeyValueTimestamp<>(3, "F3+null", 1104L)
        );
    }

    private void testLowerWindowBound(final int[] expectedKeys,
                                      final TopologyTestDriver driver,
                                      final MockApiProcessor<Integer, String, Void, Void> processor) {
        long time;
        final TestInputTopic<Integer, String> inputTopic1 = driver.createInputTopic(topic1, new IntegerSerializer(), new StringSerializer());

        // push four items with smaller timestamp (before the window) to the primary stream; this should produce four left-join and no full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time = 1000L - 100L - 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "G" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "G0+null", 899L),
            new KeyValueTimestamp<>(1, "G1+null", 899L),
            new KeyValueTimestamp<>(2, "G2+null", 899L),
            new KeyValueTimestamp<>(3, "G3+null", 899L)
        );

        // push four items with increase timestamp to the primary stream; this should produce three left-join and one full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "H" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "H0+20", 1000L),
            new KeyValueTimestamp<>(1, "H1+null", 900L),
            new KeyValueTimestamp<>(2, "H2+null", 900L),
            new KeyValueTimestamp<>(3, "H3+null", 900L)
        );

        // push four items with increase timestamp to the primary stream; this should produce two left-join and two full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "I" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "I0+20", 1000L),
            new KeyValueTimestamp<>(1, "I1+21", 1001L),
            new KeyValueTimestamp<>(2, "I2+null", 901L),
            new KeyValueTimestamp<>(3, "I3+null", 901L)
        );

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "J" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "J0+20", 1000L),
            new KeyValueTimestamp<>(1, "J1+21", 1001L),
            new KeyValueTimestamp<>(2, "J2+22", 1002L),
            new KeyValueTimestamp<>(3, "J3+null", 902L)
        );

        // push four items with increase timestamp to the primary stream; this should produce one left-join and three full-join items
        // w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //        0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //        0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //        0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //        0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //        0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //        0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //        0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //        0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //        0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902) }
        // w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //        0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        // --> w1 = { 0:A0 (ts: 0), 1:A1 (ts: 0),
        //            0:B0 (ts: 1100), 1:B1 (ts: 1100), 2:B2 (ts: 1100), 3:B3 (ts: 1100),
        //            0:C0 (ts: 1101), 1:C1 (ts: 1101), 2:C2 (ts: 1101), 3:C3 (ts: 1101),
        //            0:D0 (ts: 1102), 1:D1 (ts: 1102), 2:D2 (ts: 1102), 3:D3 (ts: 1102),
        //            0:E0 (ts: 1103), 1:E1 (ts: 1103), 2:E2 (ts: 1103), 3:E3 (ts: 1103),
        //            0:F0 (ts: 1104), 1:F1 (ts: 1104), 2:F2 (ts: 1104), 3:F3 (ts: 1104),
        //            0:G0 (ts: 899), 1:G1 (ts: 899), 2:G2 (ts: 899), 3:G3 (ts: 899),
        //            0:H0 (ts: 900), 1:H1 (ts: 900), 2:H2 (ts: 900), 3:H3 (ts: 900),
        //            0:I0 (ts: 901), 1:I1 (ts: 901), 2:I2 (ts: 901), 3:I3 (ts: 901),
        //            0:J0 (ts: 902), 1:J1 (ts: 902), 2:J2 (ts: 902), 3:J3 (ts: 902),
        //            0:K0 (ts: 903), 1:K1 (ts: 903), 2:K2 (ts: 903), 3:K3 (ts: 903) }
        // --> w2 = { 0:10 (ts: 0), 1:11 (ts: 0), 2:12 (ts: 0), 3:13 (ts: 0),
        //            0:20 (ts: 1000), 1:21 (ts: 1001), 2:22 (ts: 1002), 3:23 (ts: 1003) }
        time += 1L;
        for (final int expectedKey : expectedKeys) {
            inputTopic1.pipeInput(expectedKey, "K" + expectedKey, time);
        }
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "K0+20", 1000L),
            new KeyValueTimestamp<>(1, "K1+21", 1001L),
            new KeyValueTimestamp<>(2, "K2+22", 1002L),
            new KeyValueTimestamp<>(3, "K3+23", 1003L)
        );

        // push a dummy record to verify there are no expired records to produce
        // dummy window is behind the max. stream time seen (1205 used in testUpperWindowBound)
        inputTopic1.pipeInput(0, "dummy", time + 200L);
        processor.checkAndClearProcessResult(
            new KeyValueTimestamp<>(0, "dummy+null", 1103L)
        );
    }

    public static class CapturingStoreSuppliers extends BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers {

        final AtomicReference<KeyValueBytesStoreSupplier> capture = new AtomicReference<>();

        @Override
        public KeyValueBytesStoreSupplier keyValueStore(final DslKeyValueParams params) {
            final KeyValueBytesStoreSupplier result = super.keyValueStore(params);
            capture.set(result);
            return result;
        }
    }

    @Test
    public void testShouldJoinWithNonTimestampedStore() {
        final CapturingStoreSuppliers suppliers = new CapturingStoreSuppliers();
        final StreamJoined<Integer, String, String> streamJoined =
                StreamJoined.with(Serdes.Integer(), Serdes.String(), Serdes.String())
                        .withDslStoreSuppliers(suppliers);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<Integer, String> stream1;
        final KStream<Integer, String> stream2;
        final KStream<Integer, String> joined;
        final MockApiProcessorSupplier<Integer, String, Void, Void> supplier = new MockApiProcessorSupplier<>();
        stream1 = builder.stream(topic1, consumed);
        stream2 = builder.stream(topic2, consumed);

        joined = stream1.outerJoin(
                stream2,
                MockValueJoiner.TOSTRING_JOINER,
                JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(100L)),
                streamJoined
        );
        joined.process(supplier);

        // create a TTD so that the topology gets built
        try (final TopologyTestDriver ignored = new TopologyTestDriver(builder.build(PROPS), PROPS)) {
            assertThat("Expected stream joined to supply builders that create non-timestamped stores",
                    !WrappedStateStore.isTimestamped(suppliers.capture.get().get()));
        }
    }
}

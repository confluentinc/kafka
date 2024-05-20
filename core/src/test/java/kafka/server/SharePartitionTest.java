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
package kafka.server;

import kafka.server.SharePartition.AcknowledgementBatch;
import kafka.server.SharePartition.InFlightState;
import kafka.server.SharePartition.RecordState;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.NoOpShareStatePersister;
import org.apache.kafka.server.group.share.PartitionFactory;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.group.share.PersisterStateBatch;
import org.apache.kafka.server.group.share.ReadShareGroupStateResult;
import org.apache.kafka.server.group.share.TopicData;
import org.apache.kafka.server.group.share.WriteShareGroupStateResult;
import org.apache.kafka.server.util.FutureUtils;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static kafka.server.SharePartition.EMPTY_MEMBER_ID;
import static kafka.server.SharePartitionManagerTest.RECORD_LOCK_DURATION_MS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SharePartitionTest {

    private static final String GROUP_ID = "test-group";
    private static final String MEMBER_ID = "member-1";
    private static final int MAX_DELIVERY_COUNT = 5;
    private static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    private static Timer mockTimer;
    private static final Time MOCK_TIME = new MockTime();
    private static final int ACQUISITION_LOCK_TIMEOUT_MS = 100;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("share-group-lock-timeout-test-reaper",
                new SystemTimer("share-group-lock-test-timeout"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
    }

    @Test
    public void testRecordStateValidateTransition() {
        // Null check.
        assertThrows(NullPointerException.class, () -> RecordState.AVAILABLE.validateTransition(null));
        // Same state transition check.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACQUIRED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Acknowledged state.
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ACQUIRED));
        assertThrows(IllegalStateException.class, () -> RecordState.ACKNOWLEDGED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Archived state.
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.AVAILABLE));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.ARCHIVED.validateTransition(RecordState.ARCHIVED));
        // Invalid state transition to any other state from Available state other than Acquired.
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ACKNOWLEDGED));
        assertThrows(IllegalStateException.class, () -> RecordState.AVAILABLE.validateTransition(RecordState.ARCHIVED));

        // Successful transition from Available to Acquired.
        assertEquals(RecordState.ACQUIRED, RecordState.AVAILABLE.validateTransition(RecordState.ACQUIRED));
        // Successful transition from Acquired to any state.
        assertEquals(RecordState.AVAILABLE, RecordState.ACQUIRED.validateTransition(RecordState.AVAILABLE));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.ACQUIRED.validateTransition(RecordState.ACKNOWLEDGED));
        assertEquals(RecordState.ARCHIVED, RecordState.ACQUIRED.validateTransition(RecordState.ARCHIVED));
    }

    @Test
    public void testRecordStateForId() {
        assertEquals(RecordState.AVAILABLE, RecordState.forId((byte) 0));
        assertEquals(RecordState.ACQUIRED, RecordState.forId((byte) 1));
        assertEquals(RecordState.ACKNOWLEDGED, RecordState.forId((byte) 2));
        assertEquals(RecordState.ARCHIVED, RecordState.forId((byte) 4));
        // Invalid check.
        assertThrows(IllegalArgumentException.class, () -> RecordState.forId((byte) 5));
    }

    @Test
    public void testInitialize() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        assertFalse(sharePartition.cachedState().isEmpty());
        assertEquals(5, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(3, sharePartition.stateEpoch());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(11L));

        assertEquals(10, sharePartition.cachedState().get(5L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(2, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());

        assertEquals(15, sharePartition.cachedState().get(11L).lastOffset());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(11L).batchState());
        assertEquals(3, sharePartition.cachedState().get(11L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(11L).offsetState());
    }

    @Test
    public void testInitializeWithEmptyStateBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 5, 10L, Errors.NONE.code(), Collections.emptyList()))))
        );
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(10, sharePartition.startOffset());
        assertEquals(10, sharePartition.endOffset());
        assertEquals(5, sharePartition.stateEpoch());
        assertEquals(10, sharePartition.nextFetchOffset());
    }

    @Test
    public void testInitializeWithInvalidStartOffsetStateBatches() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 6L, Errors.NONE.code(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithInvalidTopicIdResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(Uuid.randomUuid(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithInvalidPartitionResponse() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
            new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                PartitionFactory.newPartitionAllData(1, 3, 5L, Errors.NONE.code(),
                    Arrays.asList(
                        new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                        new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithNoOpShareStatePersister() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        assertTrue(sharePartition.cachedState().isEmpty());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());
        assertEquals(0, sharePartition.stateEpoch());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testInitializeWithNullResponse() {
        Persister persister = Mockito.mock(Persister.class);
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithNullTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(null);
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithEmptyTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.emptyList());
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testInitializeWithReadException() {
        Persister persister = Mockito.mock(Persister.class);
        Mockito.when(persister.readState(Mockito.any())).thenReturn(FutureUtils.failedFuture(new RuntimeException("Read exception")));
        assertThrows(IllegalStateException.class, () -> SharePartitionBuilder.builder().withPersister(persister).build());
    }

    @Test
    public void testAcquireSingleRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 3, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireMultipleRecordsWithOverlapAndNewBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10, 0);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
    }

    @Test
    public void testAcquireSameBatchAgain() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        // No records should be returned as the batch is already acquired.
        assertEquals(0, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send subset of the same batch again, no records should be returned.
        MemoryRecords subsetRecords = memoryRecords(2, 10);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        // No records should be returned as the batch is already acquired.
        assertEquals(0, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());
        // Cache shouldn't be tracking per offset records
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcquireWithEmptyFetchRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, MemoryRecords.EMPTY,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        assertEquals(0, result.join().size());
        assertEquals(0, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcknowledgeSingleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        MemoryRecords records1 = memoryRecords(1, 0);
        MemoryRecords records2 = memoryRecords(1, 1);

        // Another batch is acquired because if there is only 1 batch, and it is acknowledged, the batch will be removed from cachedState
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 10, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

       result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(1, 1, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(1L).batchState());
        assertEquals(1, sharePartition.cachedState().get(1L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(1L).offsetState());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 14, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcknowledgeMultipleRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new AcknowledgementBatch(5, 6, Collections.singletonList((byte) 2)),
                new AcknowledgementBatch(10, 18, Arrays.asList(
                    (byte) 2, (byte) 2, (byte) 2,
                    (byte) 2, (byte) 2, (byte) 0,
                    (byte) 0, (byte) 0, (byte) 1
                ))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcknowledgeMultipleSubsetRecordBatchWithGapOffsets() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(
                // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                //  irrespective of acknowledgement type provided. While acquiring, the log start
                //  offset should be used to determine such gaps.
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 1, (byte) 1, (byte) 1,
                (byte) 0, (byte) 0, (byte) 1,
                (byte) 0, (byte) 1, (byte) 0,
                (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcknowledgeOutOfRangeCachedData() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        // Acknowledge a batch when cache is empty.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(0, 15, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(20, 25, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRequestException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            "member-2",
            Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeWhenOffsetNotAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Acknowledge the same batch again but with ACCEPT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // Re-acquire the same batch and then acknowledge subset with ACCEPT type.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Re-acknowledge the subset batch with REJECT type.
        ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(6, 8, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testAcknowledgeRollbackWithFullBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new AcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new AcknowledgementBatch(15, 19, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcknowledgeRollbackWithSubsetError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-19 should exist.
        assertEquals(3, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Arrays.asList(
                new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                new AcknowledgementBatch(15, 19, Collections.singletonList((byte) 1)),
                // Add another batch which should fail the request.
                new AcknowledgementBatch(16, 19, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // Check the state of the cache. The state should be acquired itself.
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        // Though the last batch is subset but the offset state map will not be exploded as the batch is
        // not in acquired state due to previous batch acknowledgement.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
    }

    @Test
    public void testAcquireReleasedRecord() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(12, 13, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Send the same fetch request batch again but only 2 offsets should come as acquired.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(12, 13, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
    }

    @Test
    public void testAcquireReleasedRecordMultipleBatches() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // Third fetch request with 5 records starting from offset 23, gap of 3 offsets.
        MemoryRecords records3 = memoryRecords(5, 23);
        // Fourth fetch request with 5 records starting from offset 28.
        MemoryRecords records4 = memoryRecords(5, 28);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(28, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 30, 3, records4,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records4, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(33, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(23L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(28L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(23L).offsetState());
        assertNull(sharePartition.cachedState().get(28L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            MEMBER_ID,
            Collections.singletonList(new AcknowledgementBatch(12, 30, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(4, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(15L).batchState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(23L).batchState());
        assertNull(sharePartition.cachedState().get(23L).offsetState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(23L).batchMemberId());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(28L).batchState());
        assertNotNull(sharePartition.cachedState().get(28L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(28L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(29L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(30L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(31L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(32L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(28L).offsetState());

        // Send next batch from offset 12, only 3 records should be acquired.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(12, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Though record2 batch exists to acquire but send batch record3, it should be acquired but
        // next fetch offset should not move.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 40, 3, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from batch 2.
        MemoryRecords subsetRecords = memoryRecords(2, 17);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(17, 18, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Acquire partial records from record 4 to further test if the next fetch offset move
        // accordingly once complete record 2 is also acquired.
        subsetRecords = memoryRecords(1, 28);
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, subsetRecords,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(28, 28, 2).toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(15, sharePartition.nextFetchOffset());

        // Try to acquire complete record 2 though it's already partially acquired, the next fetch
        // offset should move.
        result = sharePartition.acquire(
            MEMBER_ID,
            new FetchPartitionData(Errors.NONE, 20, 3, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        // Offset 15,16 and 19 should be acquired.
        List<AcquiredRecords> expectedAcquiredRecords = expectedAcquiredRecords(15, 16, 2);
        expectedAcquiredRecords.addAll(expectedAcquiredRecords(19, 19, 2));
        assertArrayEquals(expectedAcquiredRecords.toArray(), acquiredRecordsList.toArray());
        // Next fetch offset should not move.
        assertEquals(29, sharePartition.nextFetchOffset());
    }

    @Test
    public void testReleaseSingleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(1, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testReleaseMultipleRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testReleaseMultipleAcknowledgedRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records0 = memoryRecords(5, 0);
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecords records2 = memoryRecords(9, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records0,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records0, 1).toArray(), result.join().toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                    // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                    //  irrespective of acknowledgement type provided. While acquiring, the log start
                    //  offset should be used to determine such gaps.
                    new AcknowledgementBatch(5, 18, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcknowledgedMultipleSubsetRecordBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(
                    // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                    //  irrespective of acknowledgement type provided. While acquiring, the log start
                    //  offset should be used to determine such gaps.
                    (byte) 1, (byte) 1, (byte) 1,
                    (byte) 1, (byte) 1, (byte) 1,
                    (byte) 0, (byte) 0, (byte) 1,
                    (byte) 0, (byte) 1, (byte) 0,
                    (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(1, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(6, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of second batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(10, 18, Arrays.asList(
                    (byte) 1, (byte) 1, (byte) 0, (byte) 0,
                    (byte) 1, (byte) 0, (byte) 1, (byte) 0,
                    (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Release acquired records for "member-1".
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsWithAnotherMemberAndSubsetAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Acknowledging over subset of second batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(10, 18, Arrays.asList(
                    (byte) 1, (byte) 1, (byte) 0, (byte) 0,
                    (byte) 1, (byte) 0, (byte) 1, (byte) 0,
                    (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(21, sharePartition.nextFetchOffset());

        // Release acquired records for "member-1".
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Ack subset of records by "member-2".
        ackResult = sharePartition.acknowledge(
                "member-2",
                Collections.singletonList(new AcknowledgementBatch(5, 5, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(19, sharePartition.nextFetchOffset());

        // Release acquired records for "member-2".
        releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(6, sharePartition.nextFetchOffset());

        // Check cached state.
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testReleaseAcquiredRecordsForEmptyCachedData() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        // Release a batch when cache is empty.
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testReleaseAcquiredRecordsAfterDifferentAcknowledges() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 6, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceeded() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(15, sharePartition.endOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(10, 12, Collections.singletonList((byte) 1)),
                        new AcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                        new AcknowledgementBatch(17, 19, Collections.singletonList((byte) 1))
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(13, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        // Send next batch from offset 13, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(13, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send next batch from offset 15, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(15, 16, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(13, 16, Collections.singletonList((byte) 2))
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(20, sharePartition.nextFetchOffset());
        // cachedPartition will be empty because after the second release, the acquired records will now have moved to
        // ARCHIVE state (maxDeliveryCountExceeded). Also, now since all the records are either in ACKNOWLEDGED or ARCHIVED
        // state, cachedState should be empty.
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetWhileOthersAreAcquiredAgain() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        // First fetch request with 5 records starting from offset 0.
        MemoryRecords records1 = memoryRecords(5, 0);
        MemoryRecords recordsSubset = memoryRecords(2, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Collections.singletonList(
                        new AcknowledgementBatch(0, 1, Collections.singletonList((byte) 2))
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());

        // Send next batch from offset 0, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, recordsSubset,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(0, 1, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ACQUIRED, (short) 2, MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(0, 4, Collections.singletonList((byte) 2))
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(0L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(1L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(2L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(3L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(4L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(0L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecords() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        MemoryRecords records1 = memoryRecords(10, 0);
        MemoryRecords records2 = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(
                        new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 2))
                ));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetAfterReleaseAcquiredRecordsSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // third fetch request with 5 records starting from offset20.
        MemoryRecords records3 = memoryRecords(5, 20);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 50, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                        new AcknowledgementBatch(17, 19, Collections.singletonList((byte) 3)),
                        new AcknowledgementBatch(20, 24, Collections.singletonList((byte) 2))
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(13, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(20L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        // Send next batch from offset 13, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(13, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send next batch from offset 15, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(15, 16, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(20L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ARCHIVED, (short) 2, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());
    }

    @Test
    public void testMaxDeliveryCountLimitExceededForRecordsSubsetCacheCleared() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxDeliveryCount(2).build();
        // First fetch request with 5 records starting from offset 10.
        MemoryRecords records1 = memoryRecords(5, 10);
        // Second fetch request with 5 records starting from offset 15.
        MemoryRecords records2 = memoryRecords(5, 15);
        // third fetch request with 5 records starting from offset20.
        MemoryRecords records3 = memoryRecords(5, 20);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 50, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(15L).offsetState());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                new ArrayList<>(Arrays.asList(
                        new AcknowledgementBatch(10, 12, Collections.singletonList((byte) 1)),
                        new AcknowledgementBatch(13, 16, Collections.singletonList((byte) 2)),
                        new AcknowledgementBatch(17, 19, Collections.singletonList((byte) 3)),
                        new AcknowledgementBatch(20, 24, Collections.singletonList((byte) 2))
                )));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(13, sharePartition.nextFetchOffset());
        assertEquals(3, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(15L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(20L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(20L).batchMemberId());
        assertNull(sharePartition.cachedState().get(20L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(15L).offsetState());

        // Send next batch from offset 13, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(13, 14, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());

        // Send next batch from offset 15, only 2 records should be acquired.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(15, 16, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 40, 3, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records3, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(25, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(25, sharePartition.nextFetchOffset());
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringSingleRecord() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(0, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecords() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.timer().size());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testAcquisitionLockForAcquiringMultipleRecordsWithOverlapAndNewBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(5, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Add records from 0-9 offsets, 5-9 should be acquired and 0-4 should be ignored.
        records = memoryRecords(10, 0);
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(memoryRecords(5, 5), 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringSameBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(5, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200); // TODO: Should use waitUntilTrue.
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertEquals(0, sharePartition.timer().size());

        // Acquire the same batch again.
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(15, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingSingleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(1, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 10, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(0, 0, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(5, 10);
        // Gap from 15-17 offsets.
        recordsBuilder.appendWithOffset(18, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();
        MemoryRecords records3 = memoryRecords(2, 1);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records3, 1).toArray(), result.join().toArray());
        assertEquals(3, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(19, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                // Do not send gap offsets to verify that they are ignored and accepted as per client ack.
                Collections.singletonList(new AcknowledgementBatch(5, 18, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(19, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());

        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(1, sharePartition.nextFetchOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(1L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(10L).batchState());

        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(1L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockForAcquiringSubsetBatchAgain() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(8, 10);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(18, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Acquire subset of records again.
        MemoryRecords records2 = memoryRecords(3, 12);
        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 3, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(3, result.join().size());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(10L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(11L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(13L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).offsetState().get(14L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(17L).state());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(10L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(11L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(12L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(13L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(14L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(15L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(16L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).offsetState().get(17L).state());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnAcknowledgingMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(
                    6, 18, Arrays.asList(
                        // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                        //  irrespective of acknowledgement type provided. While acquiring, the log start
                        //  offset should be used to determine such gaps.
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 1, (byte) 1, (byte) 1,
                        (byte) 0, (byte) 0, (byte) 1,
                        (byte) 0, (byte) 1, (byte) 0,
                        (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockReleaseExceedsMaxDeliveryCount() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
                .withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
                .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
                .build();
        MemoryRecords records1 = memoryRecords(10, 0);
        MemoryRecords records2 = memoryRecords(10, 10);

        // Adding records1 in the sharePartition to make sure that SPSO doesn't move forward when delivery count of records2
        // exceed the max delivery count.
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(10L).batchState());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records2, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(10L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(2, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(10, sharePartition.cachedState().get(10L).firstOffset());
        assertEquals(19, sharePartition.cachedState().get(10L).lastOffset());
        //After the second delivery attempt fails to acknowledge the record correctly, the record should be archived.
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(2, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(10L).offsetState());
        assertNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockReleaseSPSOMovesForward() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
                .withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
                .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
                .build();
        MemoryRecords records = memoryRecords(10, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        MemoryRecords subsetRecords = memoryRecords(5, 0);

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, subsetRecords,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(5L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(6L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(7L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(8L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(0L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(1L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(2L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(3L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(4L).deliveryCount());

        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(2L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(3L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState().get(4L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(9L).acquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertNotNull(sharePartition.cachedState().get(0L).offsetState());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(0L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(1L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(2L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(3L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(4L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(5L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(6L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(7L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(8L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(0L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(1L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(2L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(3L).deliveryCount());
        assertEquals(2, sharePartition.cachedState().get(0L).offsetState().get(4L).deliveryCount());

        assertNull(sharePartition.cachedState().get(0L).offsetState().get(0L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(1L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(2L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(3L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(4L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(0L).offsetState().get(9L).acquisitionLockTimeoutTask());

        // Since only first 5 records from the batch are archived, the batch remains in the cachedState, but the
        // start offset is updated
        assertEquals(5, sharePartition.startOffset());
    }

    @Test
    public void testAcquisitionLockReleaseSPSOMovesForwardAndCachedStateCleared() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder()
                .withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
                .withMaxDeliveryCount(2) // Only 2 delivery attempts will be made before archiving the records
                .build();
        MemoryRecords records = memoryRecords(10, 0);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        List<AcquiredRecords> acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 1).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 3, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());

        acquiredRecordsList = result.join();
        assertArrayEquals(expectedAcquiredRecords(records, 2).toArray(), acquiredRecordsList.toArray());
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(9, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(2, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(0L).offsetState());
        assertNotNull(sharePartition.cachedState().get(0L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(10, sharePartition.nextFetchOffset());
        // After the second failed attempt to acknowledge the record batch successfully, the record batch is archived.
        // Since this is the first batch in the share partition, SPSO moves forward and the cachedState is cleared
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void testAcknowledgeAfterAcquisitionLockTimeout() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Acknowledge with ACCEPT type.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Try acknowledging with REJECT type.
        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnReleasingMultipleRecordBatch() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records = memoryRecords(10, 5);

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertNull(sharePartition.cachedState().get(5L).offsetState());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockOnReleasingAcknowledgedMultipleSubsetRecordBatchWithGapOffsets() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records1 = memoryRecords(2, 5);
        // Untracked gap of 3 offsets from 7-9.
        MemoryRecordsBuilder recordsBuilder = memoryRecordsBuilder(2, 10);
        // Gap from 12-13 offsets.
        recordsBuilder.appendWithOffset(14, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap for 15 offset.
        recordsBuilder.appendWithOffset(16, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        // Gap from 17-19 offsets.
        recordsBuilder.appendWithOffset(20, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        MemoryRecords records2 = recordsBuilder.build();

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records1, 1).toArray(), result.join().toArray());
        assertEquals(7, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertArrayEquals(expectedAcquiredRecords(records2, 1).toArray(), result.join().toArray());
        assertEquals(21, sharePartition.nextFetchOffset());
        assertNotNull(sharePartition.cachedState().get(10L).acquisitionLockTimeoutTask());
        assertEquals(2, sharePartition.timer().size());

        // Acknowledging over subset of both batch with subset of gap offsets.
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(6, 18, Arrays.asList(
                    // TODO: NOTE - untracked gap of 3 offsets from 7-9 has no effect on acknowledgment
                    //  irrespective of acknowledgement type provided. While acquiring, the log start
                    //  offset should be used to determine such gaps.
                    (byte) 1, (byte) 1, (byte) 1,
                    (byte) 1, (byte) 1, (byte) 1,
                    (byte) 0, (byte) 0, (byte) 1,
                    (byte) 0, (byte) 1, (byte) 0,
                    (byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(21, sharePartition.nextFetchOffset());
        assertEquals(2, sharePartition.cachedState().size());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        expectedOffsetStateMap.clear();
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap1 = new HashMap<>();
        expectedOffsetStateMap1.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap1.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap1, sharePartition.cachedState().get(5L).offsetState());

        Map<Long, InFlightState> expectedOffsetStateMap2 = new HashMap<>();
        expectedOffsetStateMap2.put(10L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(11L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(14L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(15L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(16L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(17L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(18L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap2.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap2, sharePartition.cachedState().get(10L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());

        assertNull(sharePartition.cachedState().get(10L).offsetState().get(10L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(11L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(12L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(13L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(14L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(15L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(16L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(17L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(18L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(19L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(10L).offsetState().get(20L).acquisitionLockTimeoutTask());

        assertEquals(0, sharePartition.timer().size());

        // Allowing acquisition lock to expire. This won't change the state since the batches have been released.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        assertEquals(expectedOffsetStateMap1, sharePartition.cachedState().get(5L).offsetState());
        assertEquals(expectedOffsetStateMap2, sharePartition.cachedState().get(10L).offsetState());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcquisitionLockAfterDifferentAcknowledges() throws InterruptedException {
        SharePartition sharePartition = SharePartitionBuilder.builder().withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS).build();
        MemoryRecords records = memoryRecords(5, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        // Cached data with offset 5-9 should exist.
        assertEquals(1, sharePartition.cachedState().size());
        assertNotNull(sharePartition.cachedState().get(5L));
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 6, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(3, sharePartition.timer().size());

        ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        //Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACQUIRED, (short) 1, MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(1, sharePartition.timer().size());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);
        assertEquals(5, sharePartition.nextFetchOffset());
        // Check cached state.
        expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertEquals(0, sharePartition.timer().size());
    }

    @Test
    public void testAcknowledgeSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(7, 5);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(5, 7, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(12, sharePartition.nextFetchOffset());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        // Acknowledge subset with another member.
        ackResult = sharePartition.acknowledge("member-2",
                Collections.singletonList(new AcknowledgementBatch(9, 11, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());
    }

    @Test
    public void testReleaseAcquiredRecordsSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(7, 5);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(12, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(5, 7, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(12, sharePartition.nextFetchOffset());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(5L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        expectedOffsetStateMap.put(5L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(6L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(7L, new InFlightState(RecordState.ACKNOWLEDGED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(8L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(9L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());

        // Release acquired records subset with another member.
        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords("member-2");
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());
        // No change in the offset state map since the member id is different.
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(5L).offsetState());
    }

    @Test
    public void testReacquireSubsetWithAnotherMember() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(12, 10);

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(10, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        sharePartition.acquire("member-1",
                new FetchPartitionData(Errors.NONE, 30, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(22, sharePartition.nextFetchOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge("member-1",
                Arrays.asList(
                    new AcknowledgementBatch(5, 11, Collections.singletonList((byte) 2)),
                    new AcknowledgementBatch(12, 13, Collections.singletonList((byte) 0)),
                    new AcknowledgementBatch(14, 15, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Records 17-20 are released in the acknowledgement
        ackResult = sharePartition.acknowledge("member-1",
                Collections.singletonList(new AcknowledgementBatch(17, 20, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(5, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        sharePartition.acquire("member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(10, sharePartition.nextFetchOffset());

        // Reacquire with another member.
        MemoryRecords records3 = memoryRecords(7, 10);
        sharePartition.acquire("member-2",
                new FetchPartitionData(Errors.NONE, 30, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(),
                        OptionalInt.empty(), false));
        assertEquals(17, sharePartition.nextFetchOffset());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(2, sharePartition.cachedState().get(5L).batchDeliveryCount());

        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchState());
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(10L).batchMemberId());

        // Check cached state.
        Map<Long, InFlightState> expectedOffsetStateMap = new HashMap<>();
        // Records 10-11, 14-15 were reacquired by member-2.
        expectedOffsetStateMap.put(10L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(11L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Records 12-13 were kept as gapOffsets, hence they are not reacquired and are kept in ARCHIVED state.
        expectedOffsetStateMap.put(12L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(13L, new InFlightState(RecordState.ARCHIVED, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(14L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        expectedOffsetStateMap.put(15L, new InFlightState(RecordState.ACQUIRED, (short) 2, "member-2"));
        // Record 16 was not released in the acknowledgements. It was included in the reacquire by member-2,
        // still its ownership is with member-1 and delivery count is 1.
        expectedOffsetStateMap.put(16L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        expectedOffsetStateMap.put(17L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(18L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(19L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(20L, new InFlightState(RecordState.AVAILABLE, (short) 1, EMPTY_MEMBER_ID));
        expectedOffsetStateMap.put(21L, new InFlightState(RecordState.ACQUIRED, (short) 1, "member-1"));
        assertEquals(expectedOffsetStateMap, sharePartition.cachedState().get(10L).offsetState());
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackBatchError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Arrays.asList(
                        new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                        // Acknowledging batch with another member will cause failure and rollback.
                        new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                        new AcknowledgementBatch(15, 19, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        // State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testAcknowledgeWithAnotherMemberRollbackSubsetError() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(5, 5);
        MemoryRecords records2 = memoryRecords(5, 10);
        MemoryRecords records3 = memoryRecords(5, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());

        result = sharePartition.acquire(
                "member-2",
                new FetchPartitionData(Errors.NONE, 20, 0, records3,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Arrays.asList(
                        new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2)),
                        new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 1)),
                        // Acknowledging subset with another member will cause failure and rollback.
                        new AcknowledgementBatch(16, 18, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertTrue(ackResult.join().isPresent());
        assertEquals(InvalidRecordStateException.class, ackResult.join().get().getClass());

        assertEquals(3, sharePartition.cachedState().size());
        // Check the state of the cache. State should be rolled back to the previous state for any changes.
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(5L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(10L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-2", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
    }

    @Test
    public void testCanAcquireTrue() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireFalse() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        assertEquals(0, sharePartition.startOffset());
        assertEquals(0, sharePartition.endOffset());

        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, Collections.singletonList((byte) 1))
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
    }

    @Test
    public void testCanAcquireRecordsReleasedAfterBeingAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, Collections.singletonList((byte) 2))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been released, thus they are still available for being acquired
        assertFalse(sharePartition.canAcquireMore());
    }

    @Test
    public void testCanAcquireRecordsArchivedAfterBeingAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, Collections.singletonList((byte) 3))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been rejected, thus they are removed from the cachedState
        assertTrue(sharePartition.canAcquireMore());
    }

    @Test
    public void testCanAcquireRecordsAcknowledgedAfterBeingAcquired() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(150, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(149, sharePartition.endOffset());

        MemoryRecords records2 = memoryRecords(100, 150);
        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 89, Collections.singletonList((byte) 1))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(90, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        // The records have been accepted, thus they are removed from the cachedState
        assertTrue(sharePartition.canAcquireMore());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeAccept() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        assertEquals(0, sharePartition.cachedState().size());

        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, Collections.singletonList((byte) 1))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireMore());
        // The records have been accepted, thus they are removed from the cachedState
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeReject() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, Collections.singletonList((byte) 3))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(250, sharePartition.nextFetchOffset());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(250, sharePartition.startOffset());
        assertEquals(250, sharePartition.endOffset());
        assertTrue(sharePartition.canAcquireMore());
        // The records have been rejected, thus they are removed from the cachedState
        assertEquals(0, sharePartition.cachedState().size());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementTypeRelease() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(250, 0);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertEquals(250, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 249, Collections.singletonList((byte) 2))
                )
        );
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.nextFetchOffset());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        // The SPSO should only move when the initial records in cachedState are acknowledged with type ACKNOWLEDGE or ARCHIVED
        assertEquals(0, sharePartition.startOffset());
        assertEquals(249, sharePartition.endOffset());
        assertFalse(sharePartition.canAcquireMore());
        // The records have been released, thus they are not removed from the cachedState
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForBatchSubset() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(20).build();
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 12, Collections.singletonList((byte) 1))
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(0L));
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(0L).offsetState().get(12L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(13L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(13, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsFromBeginningForEntireBatch() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(20).build();
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(0, 14, Collections.singletonList((byte) 3))
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(1, sharePartition.cachedState().size());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(15, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAcknowledgementsInBetween() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(20).build();
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                "member-1",
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                "member-1",
                Collections.singletonList(
                        new AcknowledgementBatch(10, 14, Collections.singletonList((byte) 3))
                )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertTrue(sharePartition.cachedState().containsKey(0L));
        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(0L).batchState());
        assertEquals(0, sharePartition.cachedState().get(0L).firstOffset());
        assertEquals(14, sharePartition.cachedState().get(0L).lastOffset());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).offsetState().get(9L).state());
        assertEquals(RecordState.ARCHIVED, sharePartition.cachedState().get(0L).offsetState().get(10L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());

        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateWhenAllRecordsInCachedStateAreAcknowledged() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(20).build();
        MemoryRecords records1 = memoryRecords(15, 0);
        MemoryRecords records2 = memoryRecords(15, 15);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(),
                false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(14, sharePartition.endOffset());
        assertEquals(15, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(),
                false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(15L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(15L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(15L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(29, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            "member-1",
            Collections.singletonList(
                new AcknowledgementBatch(0, 29, Collections.singletonList((byte) 1))
            )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.cachedState().size());

        assertTrue(sharePartition.canAcquireMore());
        assertEquals(30, sharePartition.startOffset());
        assertEquals(30, sharePartition.endOffset());
        assertEquals(30, sharePartition.nextFetchOffset());
    }

    @Test
    public void tesMaybeUpdateCachedStateMultipleAcquisitionsAndAcknowledgements() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(100).build();
        MemoryRecords records1 = memoryRecords(20, 0);
        MemoryRecords records2 = memoryRecords(20, 20);
        MemoryRecords records3 = memoryRecords(20, 40);
        MemoryRecords records4 = memoryRecords(20, 60);
        MemoryRecords records5 = memoryRecords(100, 80);
        MemoryRecords records6 = memoryRecords(20, 180);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records1,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(19, sharePartition.endOffset());
        assertEquals(20, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records2,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(39, sharePartition.endOffset());
        assertEquals(40, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records3,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(0L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(0, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());

        // First Acknowledgement for the first batch of records
        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
            "member-1",
            Collections.singletonList(
                new AcknowledgementBatch(0, 19, Collections.singletonList((byte) 1))
            )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(59, sharePartition.endOffset());
        assertEquals(60, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records4,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(20L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(20L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(20L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(40L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(40L).batchDeliveryCount());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(60L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(60L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(20, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());

        ackResult = sharePartition.acknowledge(
            "member-1",
            Collections.singletonList(
                new AcknowledgementBatch(20, 49, Collections.singletonList((byte) 1))
            )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());

        assertThrows(IllegalStateException.class, () -> sharePartition.cachedState().get(40L).batchState());
        assertEquals(40, sharePartition.cachedState().get(40L).firstOffset());
        assertEquals(59, sharePartition.cachedState().get(40L).lastOffset());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(40L).offsetState().get(49L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(40L).offsetState().get(50L).state());

        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(60L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(60L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(60L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(79, sharePartition.endOffset());
        assertEquals(80, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records5,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(3, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(80L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(80L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(80L).batchDeliveryCount());
        assertFalse(sharePartition.canAcquireMore());
        assertEquals(50, sharePartition.startOffset());
        assertEquals(179, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());

        // Final Acknowledgment, all records are acknowledged here
        ackResult = sharePartition.acknowledge(
            "member-1",
            Collections.singletonList(
                new AcknowledgementBatch(50, 179, Collections.singletonList((byte) 3))
            )
        );

        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(0, sharePartition.cachedState().size());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(180, sharePartition.endOffset());
        assertEquals(180, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
            "member-1",
            new FetchPartitionData(Errors.NONE, 20, 0, records6,
                Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(180L).batchState());
        assertEquals("member-1", sharePartition.cachedState().get(180L).batchMemberId());
        assertEquals(1, sharePartition.cachedState().get(180L).batchDeliveryCount());
        assertTrue(sharePartition.canAcquireMore());
        assertEquals(180, sharePartition.startOffset());
        assertEquals(199, sharePartition.endOffset());
        assertEquals(200, sharePartition.nextFetchOffset());
    }

    @Test
    void testNextFetchOffsetComputesNextOffsetCorrectly() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();
        MemoryRecords records1 = memoryRecords(10, 0);
        MemoryRecords records2 = memoryRecords(10, 10);
        MemoryRecords records3 = memoryRecords(10, 20);

        String memberId1 = "memberId-1";
        String memberId2 = "memberId-2";
        String memberId3 = "memberId-3";

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                memberId1,
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(memberId1, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(false, sharePartition.findNextFetchOffset());
        assertEquals(10, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                memberId2,
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(memberId1, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(10L).batchState());
        assertEquals(memberId2, sharePartition.cachedState().get(10L).batchMemberId());
        assertEquals(false, sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                memberId1,
                Collections.singletonList(new AcknowledgementBatch(5, 9, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(true, sharePartition.findNextFetchOffset());
        assertEquals(5, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                memberId1,
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(true, sharePartition.findNextFetchOffset());
        assertEquals(20, sharePartition.nextFetchOffset());
        assertEquals(false, sharePartition.findNextFetchOffset());
    }

    @Test
    void testNextFetchOffsetWorksCorrectlyWithMultipleConsumers() {
        SharePartition sharePartition = SharePartitionBuilder.builder().withMaxInflightMessages(100).build();
        MemoryRecords records1 = memoryRecords(3, 0);
        MemoryRecords records2 = memoryRecords(2, 3);

        String memberId1 = "member-1";
        String memberId2 = "member-2";

        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                memberId1,
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(memberId1, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(3, sharePartition.nextFetchOffset());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                memberId1,
                Collections.singletonList(new AcknowledgementBatch(0, 2, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(0, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                memberId2,
                new FetchPartitionData(Errors.NONE, 20, 0, records2,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(3L).batchState());
        assertEquals(memberId2, sharePartition.cachedState().get(3L).batchMemberId());
        assertEquals(0, sharePartition.nextFetchOffset());

        result = sharePartition.acquire(
                memberId1,
                new FetchPartitionData(Errors.NONE, 20, 0, records1,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));

        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(3L).batchState());
        assertEquals(memberId1, sharePartition.cachedState().get(0L).batchMemberId());
        assertEquals(memberId2, sharePartition.cachedState().get(3L).batchMemberId());
        assertEquals(5, sharePartition.nextFetchOffset());

        ackResult = sharePartition.acknowledge(
                memberId2,
                Collections.singletonList(new AcknowledgementBatch(3, 4, Collections.singletonList((byte) 2))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        assertEquals(2, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(0L).batchState());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(3L).batchState());
        assertEquals(3, sharePartition.nextFetchOffset());
    }

    @Test
    public void testWriteShareGroupStateWithNullResponse() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));

        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(null));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateWithNullTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(null);
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateWithInvalidTopicsData() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        // TopicsData is empty.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.emptyList());
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));

        // TopicsData contains more results than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Arrays.asList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.emptyList()),
                new TopicData<>(Uuid.randomUuid(), Collections.emptyList())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));

        // TopicsData contains no partition data.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.emptyList())));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));

        // TopicsData contains wrong topicId
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(Uuid.randomUuid(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));

        // TopicsData contains more partition data than expected.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Arrays.asList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code()),
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));

        // TopicsData contains wrong partition
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(1, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateWithWriteException() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(FutureUtils.failedFuture(new RuntimeException("Write exception")));
        assertThrows(IllegalStateException.class, () -> sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateSuccessful() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertTrue(sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NOT_COORDINATOR.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));
        assertFalse(sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testWriteShareGroupStateWithNoOpShareStatePersister() {
        SharePartition sharePartition = SharePartitionBuilder.builder().build();

        List<PersisterStateBatch> stateBatches = Arrays.asList(
                new PersisterStateBatch(5L, 10L, RecordState.AVAILABLE.id, (short) 2),
                new PersisterStateBatch(11L, 15L, RecordState.ARCHIVED.id, (short) 3));

        assertTrue(sharePartition.isWriteShareGroupStateSuccessful(stateBatches));
    }

    @Test
    public void testAcknowledgeBatchWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(10, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(5, 14, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
    }

    @Test
    public void testAcknowledgeOffsetWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(6, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 10, Collections.singletonList((byte) 3))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());
    }

    @Test
    public void testReleaseBatchWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(10, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
    }

    @Test
    public void testReleaseOffsetWithWriteShareGroupStateFailure() {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister).build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true for acknowledge to pass.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(6, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Acknowledging the records succeeded.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        CompletableFuture<Optional<Throwable>> releaseResult = sharePartition.releaseAcquiredRecords(MEMBER_ID);
        assertFalse(releaseResult.isCompletedExceptionally());
        assertFalse(releaseResult.join().isPresent());

        // Due to failure in writeShareGroupState, the cached state should not be updated.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());
    }

    @Test
    public void testAcquisitionLockOnBatchWithWriteShareGroupStateFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
                .withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
                .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(10, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());

        // Allowing acquisition lock to expire.
        Thread.sleep(200);

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).batchState());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).batchMemberId());
        assertEquals(0, sharePartition.timer().size());
        assertNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());
    }

    @Test
    public void testAcquisitionLockOnOffsetWithWriteShareGroupStateFailure() throws InterruptedException {
        Persister persister = Mockito.mock(Persister.class);
        mockPersisterReadStateMethod(persister);
        SharePartition sharePartition = SharePartitionBuilder.builder().withPersister(persister)
                .withAcquisitionLockTimeoutMs(ACQUISITION_LOCK_TIMEOUT_MS)
                .build();

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns true for acknowledge to pass.
        WriteShareGroupStateResult writeShareGroupStateResult = Mockito.mock(WriteShareGroupStateResult.class);
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.NONE.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        MemoryRecords records = memoryRecords(6, 5);
        CompletableFuture<List<AcquiredRecords>> result = sharePartition.acquire(
                MEMBER_ID,
                new FetchPartitionData(Errors.NONE, 20, 0, records,
                        Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false));
        assertFalse(result.isCompletedExceptionally());
        assertEquals(1, result.join().size());
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).batchState());
        assertEquals(1, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).acquisitionLockTimeoutTask());

        CompletableFuture<Optional<Throwable>> ackResult = sharePartition.acknowledge(
                MEMBER_ID,
                Collections.singletonList(new AcknowledgementBatch(8, 9, Collections.singletonList((byte) 1))));
        assertFalse(ackResult.isCompletedExceptionally());
        assertFalse(ackResult.join().isPresent());

        // Acknowledging the records succeeded.
        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.ACQUIRED, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());

        assertEquals(4, sharePartition.timer().size());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertNotNull(sharePartition.cachedState().get(5L).offsetState().get(10L).acquisitionLockTimeoutTask());

        // Mock persister writeState method so that sharePartition.isWriteShareGroupStateSuccessful() returns false.
        Mockito.when(writeShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionErrorData(0, Errors.GROUP_ID_NOT_FOUND.code())))));
        Mockito.when(persister.writeState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(writeShareGroupStateResult));

        // Allowing acquisition lock to expire.
        Thread.sleep(200);

        assertEquals(1, sharePartition.cachedState().size());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).offsetState().get(5L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).offsetState().get(6L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).offsetState().get(7L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(8L).state());
        assertEquals(RecordState.ACKNOWLEDGED, sharePartition.cachedState().get(5L).offsetState().get(9L).state());
        assertEquals(RecordState.AVAILABLE, sharePartition.cachedState().get(5L).offsetState().get(10L).state());

        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(5L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(6L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(7L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(8L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(9L).memberId());
        assertEquals(EMPTY_MEMBER_ID, sharePartition.cachedState().get(5L).offsetState().get(10L).memberId());

        assertEquals(0, sharePartition.timer().size());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(5L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(6L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(7L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(8L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(9L).acquisitionLockTimeoutTask());
        assertNull(sharePartition.cachedState().get(5L).offsetState().get(10L).acquisitionLockTimeoutTask());
    }

    private MemoryRecords memoryRecords(int numOfRecords) {
        return memoryRecords(numOfRecords, 0);
    }

    private MemoryRecords memoryRecords(int numOfRecords, long startOffset) {
        return memoryRecordsBuilder(numOfRecords, startOffset).build();
    }

    private MemoryRecordsBuilder memoryRecordsBuilder(int numOfRecords, long startOffset) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024),
            CompressionType.NONE, TimestampType.CREATE_TIME, startOffset);
        for (int i = 0; i < numOfRecords; i++) {
            builder.appendWithOffset(startOffset + i, 0L, TestUtils.randomString(10).getBytes(), TestUtils.randomString(10).getBytes());
        }
        return builder;
    }

    private List<AcquiredRecords> expectedAcquiredRecords(MemoryRecords memoryRecords, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        memoryRecords.batches().forEach(batch -> acquiredRecordsList.add(new AcquiredRecords()
            .setFirstOffset(batch.baseOffset())
            .setLastOffset(batch.lastOffset())
            .setDeliveryCount((short) deliveryCount)));
        return acquiredRecordsList;
    }

    private List<AcquiredRecords> expectedAcquiredRecords(long baseOffset, long lastOffset, int deliveryCount) {
        List<AcquiredRecords> acquiredRecordsList = new ArrayList<>();
        for (long i = baseOffset; i <= lastOffset; i++) {
            acquiredRecordsList.add(new AcquiredRecords()
                .setFirstOffset(i)
                .setLastOffset(i)
                .setDeliveryCount((short) deliveryCount));
        }
        return acquiredRecordsList;
    }

    public void mockPersisterReadStateMethod(Persister persister) {
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(TOPIC_ID_PARTITION.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionAllData(0, 0, 0L, Errors.NONE.code(),
                                Collections.emptyList())))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));
    }

    private static class SharePartitionBuilder {

        private int acquisitionLockTimeoutMs = RECORD_LOCK_DURATION_MS;
        private int maxDeliveryCount = MAX_DELIVERY_COUNT;
        private int maxInflightMessages = MAX_IN_FLIGHT_MESSAGES;
        private Persister persister = NoOpShareStatePersister.getInstance();

        private SharePartitionBuilder withAcquisitionLockTimeoutMs(int acquisitionLockTimeoutMs) {
            this.acquisitionLockTimeoutMs = acquisitionLockTimeoutMs;
            return this;
        }

        private SharePartitionBuilder withMaxDeliveryCount(int maxDeliveryCount) {
            this.maxDeliveryCount = maxDeliveryCount;
            return this;
        }

        private SharePartitionBuilder withMaxInflightMessages(int maxInflightMessages) {
            this.maxInflightMessages = maxInflightMessages;
            return this;
        }

        private SharePartitionBuilder withPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        public static SharePartitionBuilder builder() {
            return new SharePartitionBuilder();
        }

        public SharePartition build() {
            return new SharePartition(GROUP_ID, TOPIC_ID_PARTITION, maxInflightMessages, maxDeliveryCount,
                acquisitionLockTimeoutMs, mockTimer, MOCK_TIME, persister);
        }
    }
}

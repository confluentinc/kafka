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
package org.apache.kafka.raft;

import org.apache.kafka.common.message.LeaderChangeMessageData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.raft.MockLog.LogEntry;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.apache.kafka.common.record.ControlRecordUtils.deserialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MockLogTest {

    private MockLog log;

    @Before
    public void setup() {
        log = new MockLog();
    }

    @Test
    public void testAppendAsLeaderHelper() {
        int epoch = 2;
        SimpleRecord record = new SimpleRecord("foo".getBytes());
        log.appendAsLeader(Collections.singleton(record), epoch);
        assertEquals(epoch, log.lastFetchedEpoch());
        List<LogEntry> entries = log.readEntries(0, 1);
        assertEquals(Collections.singletonList(LogEntry.with(0, epoch, record)), entries);
        assertEquals(0L, log.startOffset());
        assertEquals(1L, log.endOffset());
    }

    @Test
    public void testTruncate() {
        int epoch = 2;
        SimpleRecord recordOne = new SimpleRecord("one".getBytes());
        SimpleRecord recordTwo = new SimpleRecord("two".getBytes());
        log.appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

        List<LogEntry> entries = log.readEntries(0, 2);
        assertEquals(Arrays.asList(LogEntry.with(0, epoch, recordOne), LogEntry.with(1, epoch, recordTwo)), entries);
        assertEquals(0L, log.startOffset());
        assertEquals(2L, log.endOffset());

        log.truncateTo(1);
        entries = log.readEntries(0, 2);
        assertEquals(Collections.singletonList(LogEntry.with(0, epoch, recordOne)), entries);
        assertEquals(0L, log.startOffset());
        assertEquals(1L, log.endOffset());
    }

    @Test
    public void testUpdateHighWatermark() {
        long newOffset = 5L;
        log.updateHighWatermark(newOffset);
        assertEquals(newOffset, log.highWatermark());
    }

    @Test
    public void testDecrementHighWatermark() {
        log.updateHighWatermark(4);
        assertThrows(IllegalArgumentException.class, () -> log.updateHighWatermark(3));
    }

    @Test
    public void testAssignEpochStartOffset() {
        log.assignEpochStartOffset(2, 0);
        assertEquals(2, log.lastFetchedEpoch());
    }

    @Test
    public void testAssignEpochStartOffsetNotEqualToEndOffset() {
        assertThrows(IllegalArgumentException.class, () -> log.assignEpochStartOffset(2, 1));
    }

    @Test
    public void testAppendAsLeader() {
        // The record passed-in offsets are not going to affect the eventual offsets.
        final long initialOffset = 5L;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        final int currentEpoch = 3;
        log.appendAsLeader(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, recordFoo), currentEpoch);

        assertEquals(0, log.startOffset());
        assertEquals(1, log.endOffset());
        assertEquals(currentEpoch, log.lastFetchedEpoch());

        Records records = log.read(0, OptionalLong.empty());
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(recordFoo.value(), extractRecords.get(0));
        assertEquals(Optional.of(new OffsetAndEpoch(1, currentEpoch)), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testAppendControlRecord() {
        final long initialOffset = 5L;
        final int currentEpoch = 3;
        LeaderChangeMessageData messageData =  new LeaderChangeMessageData().setLeaderId(0);
        log.appendAsLeader(MemoryRecords.withLeaderChangeMessage(
            initialOffset, 2, messageData), currentEpoch);

        assertEquals(0, log.startOffset());
        assertEquals(1, log.endOffset());
        assertEquals(currentEpoch, log.lastFetchedEpoch());

        Records records = log.read(0, OptionalLong.empty());
        for (RecordBatch batch : records.batches()) {
            assertTrue(batch.isControlBatch());
        }
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            LeaderChangeMessageData deserializedData = deserialize(record);
            assertEquals(deserializedData, messageData);
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(Optional.of(new OffsetAndEpoch(1, currentEpoch)), log.endOffsetForEpoch(currentEpoch));
    }

    @Test
    public void testAppendAsFollower() {
        // The record passed-in offsets are not going to affect the eventual offsets.
        final long initialOffset = 5L;
        SimpleRecord recordFoo = new SimpleRecord("foo".getBytes());
        log.appendAsFollower(MemoryRecords.withRecords(initialOffset, CompressionType.NONE, recordFoo));

        assertEquals(0, log.startOffset());
        assertEquals(1, log.endOffset());
        assertEquals(0, log.lastFetchedEpoch());

        Records records = log.read(0, OptionalLong.empty());
        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }

        assertEquals(1, extractRecords.size());
        assertEquals(recordFoo.value(), extractRecords.get(0));
        // The data epoch is not larger than default, so no advance for epoch offsets.
        assertEquals(Optional.empty(), log.endOffsetForEpoch(0));
    }

    @Test
    public void testReadRecords() {
        int epoch = 2;

        ByteBuffer recordOneBuffer = ByteBuffer.allocate(4);
        recordOneBuffer.putInt(1);
        SimpleRecord recordOne = new SimpleRecord(recordOneBuffer);

        ByteBuffer recordTwoBuffer = ByteBuffer.allocate(4);
        recordTwoBuffer.putInt(2);
        SimpleRecord recordTwo = new SimpleRecord(recordTwoBuffer);

        log.appendAsLeader(Arrays.asList(recordOne, recordTwo), epoch);

        Records records = log.read(0, OptionalLong.empty());

        List<ByteBuffer> extractRecords = new ArrayList<>();
        for (Record record : records.records()) {
            extractRecords.add(record.value());
        }
        assertEquals(Arrays.asList(recordOne.value(), recordTwo.value()), extractRecords);
    }
}

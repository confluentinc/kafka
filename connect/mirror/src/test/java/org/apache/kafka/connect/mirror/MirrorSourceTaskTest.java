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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.mirror.OffsetSyncWriter.PartitionState;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MirrorSourceTaskTest {

    @Test
    public void testSerde() {
        byte[] key = new byte[]{'a', 'b', 'c', 'd', 'e'};
        byte[] value = new byte[]{'f', 'g', 'h', 'i', 'j', 'k'};
        Headers headers = new RecordHeaders();
        headers.add("header1", new byte[]{'l', 'm', 'n', 'o'});
        headers.add("header2", new byte[]{'p', 'q', 'r', 's', 't'});
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic1", 2, 3L, 4L,
            TimestampType.CREATE_TIME, 5, 6, key, value, headers, Optional.empty());
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(null, null, "cluster7",
                new DefaultReplicationPolicy(), null);
        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(consumerRecord);
        assertEquals("cluster7.topic1", sourceRecord.topic(),
                "Failure on cluster7.topic1 consumerRecord serde");
        assertEquals(2, sourceRecord.kafkaPartition().intValue(),
                "sourceRecord kafka partition is incorrect");
        assertEquals(new TopicPartition("topic1", 2), MirrorUtils.unwrapPartition(sourceRecord.sourcePartition()),
                "topic1 unwrapped from sourcePartition is incorrect");
        assertEquals(3L, MirrorUtils.unwrapOffset(sourceRecord.sourceOffset()).longValue(),
                "sourceRecord's sourceOffset is incorrect");
        assertEquals(4L, sourceRecord.timestamp().longValue(),
                "sourceRecord's timestamp is incorrect");
        assertEquals(key, sourceRecord.key(), "sourceRecord's key is incorrect");
        assertEquals(value, sourceRecord.value(), "sourceRecord's value is incorrect");
        assertEquals(headers.lastHeader("header1").value(), sourceRecord.headers().lastWithName("header1").value(),
                "sourceRecord's header1 is incorrect");
        assertEquals(headers.lastHeader("header2").value(), sourceRecord.headers().lastWithName("header2").value(),
                "sourceRecord's header2 is incorrect");
    }

    @Test
    public void testOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(50);

        assertTrue(partitionState.update(0, 100), "always emit offset sync on first update");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "upstream offset skipped -> resync");
        partitionState.reset();
        assertFalse(partitionState.update(3, 152), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 153), "one past target offset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 154), "no sync");
        partitionState.reset();
        assertFalse(partitionState.update(6, 203), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(7, 204), "one past target offset");
        partitionState.reset();
        assertTrue(partitionState.update(2, 206), "upstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(3, 207), "no sync");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "downstream reset");
        partitionState.reset();
        assertFalse(partitionState.update(5, 4), "no sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(7, 6), "sync");
        assertTrue(partitionState.update(8, 7), "sync");
        assertTrue(partitionState.update(10, 57), "sync");
        partitionState.reset();
        assertFalse(partitionState.update(11, 58), "sync");
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
    }

    @Test
    public void testZeroOffsetSync() {
        OffsetSyncWriter.PartitionState partitionState = new OffsetSyncWriter.PartitionState(0);

        // if max offset lag is zero, should always emit offset syncs
        assertTrue(partitionState.update(0, 100), "zeroOffsetSync downStreamOffset 100 is incorrect");
        assertTrue(partitionState.shouldSyncOffsets, "should sync offsets");
        partitionState.reset();
        assertFalse(partitionState.shouldSyncOffsets, "should sync offsets to false");
        assertTrue(partitionState.update(2, 102), "zeroOffsetSync downStreamOffset 102 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 153), "zeroOffsetSync downStreamOffset 153 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 154), "zeroOffsetSync downStreamOffset 154 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 155), "zeroOffsetSync downStreamOffset 155 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(6, 207), "zeroOffsetSync downStreamOffset 207 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(2, 208), "zeroOffsetSync downStreamOffset 208 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(3, 209), "zeroOffsetSync downStreamOffset 209 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(4, 3), "zeroOffsetSync downStreamOffset 3 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(5, 4), "zeroOffsetSync downStreamOffset 4 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(7, 6), "zeroOffsetSync downStreamOffset 6 is incorrect");
        assertTrue(partitionState.update(8, 7), "zeroOffsetSync downStreamOffset 7 is incorrect");
        assertTrue(partitionState.update(10, 57), "zeroOffsetSync downStreamOffset 57 is incorrect");
        partitionState.reset();
        assertTrue(partitionState.update(11, 58), "zeroOffsetSync downStreamOffset 58 is incorrect");
    }

    @Test
    public void testPoll() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        byte[] key2 = "123".getBytes();
        byte[] value2 = "456".getBytes();
        List<ConsumerRecord<byte[], byte[]>> consumerRecordsList =  new ArrayList<>();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));
        consumerRecordsList.add(new ConsumerRecord<>(topicName, 1, 1, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key2.length, value2.length, key2, value2, headers, Optional.empty()));
        final TopicPartition tp = new TopicPartition(topicName, 0);
        ConsumerRecords<byte[], byte[]> consumerRecords =
                new ConsumerRecords<>(Map.of(tp, consumerRecordsList), Map.of(tp, new OffsetAndMetadata(2, Optional.empty(), "")));

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        when(consumer.poll(any())).thenReturn(consumerRecords);

        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, null);
        List<SourceRecord> sourceRecords = mirrorSourceTask.poll();

        assertEquals(2, sourceRecords.size());
        for (int i = 0; i < sourceRecords.size(); i++) {
            SourceRecord sourceRecord = sourceRecords.get(i);
            ConsumerRecord<byte[], byte[]> consumerRecord = consumerRecordsList.get(i);
            assertEquals(consumerRecord.key(), sourceRecord.key(),
                    "consumerRecord key does not equal sourceRecord key");
            assertEquals(consumerRecord.value(), sourceRecord.value(),
                    "consumerRecord value does not equal sourceRecord value");
            // We expect that the topicname will be based on the replication policy currently used
            assertEquals(replicationPolicy.formatRemoteTopic(sourceClusterName, topicName),
                    sourceRecord.topic(), "topicName not the same as the current replicationPolicy");
            // We expect that MirrorMaker will keep the same partition assignment
            assertEquals(consumerRecord.partition(), sourceRecord.kafkaPartition().intValue(),
                    "partition assignment not the same as the current replicationPolicy");
            // Check header values
            List<Header> expectedHeaders = new ArrayList<>();
            consumerRecord.headers().forEach(expectedHeaders::add);
            List<org.apache.kafka.connect.header.Header> taskHeaders = new ArrayList<>();
            sourceRecord.headers().forEach(taskHeaders::add);
            compareHeaders(expectedHeaders, taskHeaders);
        }
    }

    @Test
    public void testSeekBehaviorDuringStart() {
        // Setting up mock behavior.
        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> mockConsumer = mock(KafkaConsumer.class);

        SourceTaskContext mockSourceTaskContext = mock(SourceTaskContext.class);
        OffsetStorageReader mockOffsetStorageReader = mock(OffsetStorageReader.class);
        when(mockSourceTaskContext.offsetStorageReader()).thenReturn(mockOffsetStorageReader);

        Set<TopicPartition> topicPartitions = Set.of(
                new TopicPartition("previouslyReplicatedTopic", 8),
                new TopicPartition("previouslyReplicatedTopic1", 0),
                new TopicPartition("previouslyReplicatedTopic", 1),
                new TopicPartition("newTopicToReplicate1", 1),
                new TopicPartition("newTopicToReplicate1", 4),
                new TopicPartition("newTopicToReplicate2", 0)
        );

        long arbitraryCommittedOffset = 4L;
        long offsetToSeek = arbitraryCommittedOffset + 1L;
        when(mockOffsetStorageReader.offset(anyMap())).thenAnswer(testInvocation -> {
            Map<String, Object> topicPartitionOffsetMap = testInvocation.getArgument(0);
            String topicName = topicPartitionOffsetMap.get("topic").toString();

            // Only return the offset for previously replicated topics.
            // For others, there is no value set.
            if (topicName.startsWith("previouslyReplicatedTopic")) {
                topicPartitionOffsetMap.put("offset", arbitraryCommittedOffset);
            }
            return topicPartitionOffsetMap;
        });

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(mockConsumer, null, null,
                new DefaultReplicationPolicy(), null);
        mirrorSourceTask.initialize(mockSourceTaskContext);

        // Call test subject
        mirrorSourceTask.initializeConsumer(topicPartitions);

        // Verifications
        // Ensure all the topic partitions are assigned to consumer
        verify(mockConsumer, times(1)).assign(topicPartitions);

        // Ensure seek is only called for previously committed topic partitions.
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 8), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic", 1), offsetToSeek);
        verify(mockConsumer, times(1))
                .seek(new TopicPartition("previouslyReplicatedTopic1", 0), offsetToSeek);

        verifyNoMoreInteractions(mockConsumer);
    }

    @Test
    public void testCommitRecordWithNullMetadata() {
        // Create a consumer mock
        byte[] key1 = "abc".getBytes();
        byte[] value1 = "fgh".getBytes();
        String topicName = "test";
        String headerKey = "key";
        RecordHeaders headers = new RecordHeaders(new Header[] {
            new RecordHeader(headerKey, "value".getBytes()),
        });

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);

        String sourceClusterName = "cluster1";
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();
        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, null);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, 0, 0, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, key1.length, value1.length, key1, value1, headers, Optional.empty()));

        // Expect that commitRecord will not throw an exception
        mirrorSourceTask.commitRecord(sourceRecord, null);
    }

    @Test
    public void testSendSyncEvent() {
        byte[] recordKey = "key".getBytes();
        byte[] recordValue = "value".getBytes();
        long maxOffsetLag = 50;
        int recordPartition = 0;
        int recordOffset = 0;
        int metadataOffset = 100;
        String topicName = "topic";
        String sourceClusterName = "sourceCluster";

        RecordHeaders headers = new RecordHeaders();
        ReplicationPolicy replicationPolicy = new DefaultReplicationPolicy();

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        MirrorSourceMetrics metrics = mock(MirrorSourceMetrics.class);
        PartitionState partitionState = new PartitionState(maxOffsetLag);
        Map<TopicPartition, PartitionState> partitionStates = new HashMap<>();
        OffsetSyncWriter offsetSyncWriter = mock(OffsetSyncWriter.class);
        when(offsetSyncWriter.maxOffsetLag()).thenReturn(maxOffsetLag);
        doNothing().when(offsetSyncWriter).firePendingOffsetSyncs();
        doNothing().when(offsetSyncWriter).promoteDelayedOffsetSyncs();

        MirrorSourceTask mirrorSourceTask = new MirrorSourceTask(consumer, metrics, sourceClusterName,
                replicationPolicy, offsetSyncWriter);

        SourceRecord sourceRecord = mirrorSourceTask.convertRecord(new ConsumerRecord<>(topicName, recordPartition,
                recordOffset, System.currentTimeMillis(), TimestampType.CREATE_TIME, recordKey.length,
                recordValue.length, recordKey, recordValue, headers, Optional.empty()));

        TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(sourceRecord.sourcePartition());
        partitionStates.put(sourceTopicPartition, partitionState);
        RecordMetadata recordMetadata = new RecordMetadata(sourceTopicPartition, metadataOffset, 0, 0, 0, recordPartition);
        doNothing().when(offsetSyncWriter).maybeQueueOffsetSyncs(eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));

        mirrorSourceTask.commitRecord(sourceRecord, recordMetadata);
        // We should have dispatched this sync to the producer
        verify(offsetSyncWriter, times(1)).maybeQueueOffsetSyncs(eq(sourceTopicPartition), eq((long) recordOffset), eq(recordMetadata.offset()));
        verify(offsetSyncWriter, times(1)).firePendingOffsetSyncs();

        mirrorSourceTask.commit();
        // No more syncs should take place; we've been able to publish all of them so far
        verify(offsetSyncWriter, times(1)).promoteDelayedOffsetSyncs();
        verify(offsetSyncWriter, times(2)).firePendingOffsetSyncs();
    }

    private void compareHeaders(List<Header> expectedHeaders, List<org.apache.kafka.connect.header.Header> taskHeaders) {
        assertEquals(expectedHeaders.size(), taskHeaders.size());
        for (int i = 0; i < expectedHeaders.size(); i++) {
            Header expectedHeader = expectedHeaders.get(i);
            org.apache.kafka.connect.header.Header taskHeader = taskHeaders.get(i);
            assertEquals(expectedHeader.key(), taskHeader.key(),
                    "taskHeader's key expected to equal " + taskHeader.key());
            assertEquals(expectedHeader.value(), taskHeader.value(),
                    "taskHeader's value expected to equal " + taskHeader.value().toString());
        }
    }
}

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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData.AddPartitionsToTxnResult;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TransactionManagerTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = Integer.MAX_VALUE;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 100L;

    private final String transactionalId = "foobar";
    private final int transactionTimeoutMs = 1121;

    private final String topic = "test";
    private static final Uuid TOPIC_ID = Uuid.fromString("y2J9jXHhfIkQ1wK8mMKXx1");
    private final TopicPartition tp0 = new TopicPartition(topic, 0);
    private final TopicPartition tp1 = new TopicPartition(topic, 1);
    private final long producerId = 13131L;
    private final short epoch = 1;
    private final long ongoingProducerId = 999L;
    private final short bumpedOngoingEpoch = 11;
    private final String consumerGroupId = "myConsumerGroup";
    private final String memberId = "member";
    private final int generationId = 5;
    private final String groupInstanceId = "instance";

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final ProducerMetadata metadata = new ProducerMetadata(0, 0, Long.MAX_VALUE, Long.MAX_VALUE,
            logContext, new ClusterResourceListeners(), time);
    private final MockClient client = new MockClient(time, metadata);
    private final ApiVersions apiVersions = new ApiVersions();

    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private TestableTransactionManager transactionManager = null;
    private Node brokerNode = null;
    private long finalizedFeaturesEpoch = 0;

    @BeforeEach
    public void setup() {
        this.metadata.add("test", time.milliseconds());
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, singletonMap("test", 2)));
        this.brokerNode = new Node(0, "localhost", 2211);

        initializeTransactionManager(Optional.of(transactionalId), false, false);
    }

    private void initializeTransactionManager(
        Optional<String> transactionalId,
        boolean transactionV2Enabled
    ) {
        initializeTransactionManager(transactionalId, transactionV2Enabled, false);
    }

    private void initializeTransactionManager(
        Optional<String> transactionalId,
        boolean transactionV2Enabled,
        boolean enable2pc
    ) {
        Metrics metrics = new Metrics(time);

        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
            new ApiVersion()
                .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 6),
            new ApiVersion()
                .setApiKey(ApiKeys.PRODUCE.id)
                .setMinVersion((short) 0)
                .setMaxVersion(transactionV2Enabled ? ApiKeys.PRODUCE.latestVersion() : (short) 11),
            new ApiVersion()
                .setApiKey(ApiKeys.TXN_OFFSET_COMMIT.id)
                .setMinVersion((short) 0)
                .setMaxVersion(transactionV2Enabled ? ApiKeys.TXN_OFFSET_COMMIT.latestVersion() : (short) 4)),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion(transactionV2Enabled ? (short) 2 : (short) 1)
                .setMinVersion((short) 0)),
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel(transactionV2Enabled ? (short) 2 : (short) 1)
                .setMinVersionLevel(transactionV2Enabled ? (short) 2 : (short) 1)),
            finalizedFeaturesEpoch));
        finalizedFeaturesEpoch += 1;
        this.transactionManager = new TestableTransactionManager(logContext, transactionalId.orElse(null),
                transactionTimeoutMs, DEFAULT_RETRY_BACKOFF_MS, apiVersions, enable2pc);


        int batchSize = 16 * 1024;
        int deliveryTimeoutMs = 3000;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";

        this.brokerNode = new Node(0, "localhost", 2211);
        this.accumulator = new RecordAccumulator(logContext, batchSize, Compression.NONE, 0, 0L, 0L,
                deliveryTimeoutMs, metrics, metricGrpName, time, transactionManager,
                new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, true,
                MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(metrics), this.time, REQUEST_TIMEOUT,
                50, transactionManager);
    }

    @Test
    public void testSenderShutdownWithPendingTransactions() throws Exception {
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(tp0);
        FutureRecordMetadata sendFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());

        sender.initiateClose();
        sender.runOnce();

        TransactionalRequestResult result = transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(result::isCompleted);
        runUntil(sendFuture::isDone);
    }

    @Test
    public void testEndTxnNotSentIfIncompleteBatches() {
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        transactionManager.beginCommit();
        assertNull(transactionManager.nextRequest(true));
        assertTrue(transactionManager.nextRequest(false).isEndTxn());
    }

    @Test
    public void testFailIfNotReadyForSendNoProducerId() {
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailIfNotReadyForSendIdempotentProducer(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        transactionManager.maybeAddPartition(tp0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailIfNotReadyForSendIdempotentProducerFatalError(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        transactionManager.transitionToFatalError(new KafkaException());
        assertThrows(KafkaException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testFailIfNotReadyForSendNoOngoingTransaction() {
        doInitTransactions();
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testFailIfNotReadyForSendAfterAbortableError() {
        doInitTransactions();
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        assertThrows(KafkaException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testFailIfNotReadyForSendAfterFatalError() {
        doInitTransactions();
        transactionManager.transitionToFatalError(new KafkaException());
        assertThrows(KafkaException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testHasOngoingTransactionSuccessfulAbort() {
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions();
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartition(partition);
        runUntil(transactionManager::hasOngoingTransaction);

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(partition));

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionSuccessfulCommit() {
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions();
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(partition));

        transactionManager.beginCommit();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionAbortableError() {
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions();
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(partition));

        transactionManager.transitionToAbortableError(new KafkaException());
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.beginAbort();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testHasOngoingTransactionFatalError() {
        TopicPartition partition = new TopicPartition("foo", 0);

        assertFalse(transactionManager.hasOngoingTransaction());
        doInitTransactions();
        assertFalse(transactionManager.hasOngoingTransaction());

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(partition));

        transactionManager.transitionToFatalError(new KafkaException());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMaybeAddPartitionToTransaction() {
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.transactionContainsPartition(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        assertTrue(transactionManager.hasPartitionsToAdd());

        runUntil(() -> transactionManager.transactionContainsPartition(partition));
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.isPartitionPendingAdd(partition));

        // adding the partition again should not have any effect
        transactionManager.maybeAddPartition(partition);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.transactionContainsPartition(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));
    }

    @Test
    public void testMaybeAddPartitionToTransactionInTransactionV2() {
        initializeTransactionManager(Optional.of(transactionalId), true);
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(partition);
        // In V2, the maybeAddPartition should not add the partition to the pending list.
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.transactionContainsPartition(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));

        // Adding the partition again should not have any effect
        transactionManager.maybeAddPartition(partition);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertTrue(transactionManager.transactionContainsPartition(partition));
        assertFalse(transactionManager.isPartitionPendingAdd(partition));
    }

    @Test
    public void testAddPartitionToTransactionOverridesRetryBackoffForConcurrentTransactions() {
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.transactionContainsPartition(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.CONCURRENT_TRANSACTIONS);
        runUntil(() -> !client.hasPendingResponses());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(20, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffForRegularRetriableError() {
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.transactionContainsPartition(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.COORDINATOR_NOT_AVAILABLE);
        runUntil(() -> !client.hasPendingResponses());

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test
    public void testAddPartitionToTransactionRetainsRetryBackoffWhenPartitionsAlreadyAdded() {
        TopicPartition partition = new TopicPartition("foo", 0);
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(partition);
        assertTrue(transactionManager.hasPartitionsToAdd());
        assertFalse(transactionManager.transactionContainsPartition(partition));
        assertTrue(transactionManager.isPartitionPendingAdd(partition));

        prepareAddPartitionsToTxn(partition, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(partition));

        TopicPartition otherPartition = new TopicPartition("foo", 1);
        transactionManager.maybeAddPartition(otherPartition);

        prepareAddPartitionsToTxn(otherPartition, Errors.CONCURRENT_TRANSACTIONS);
        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);
        assertNotNull(handler);
        assertEquals(DEFAULT_RETRY_BACKOFF_MS, handler.retryBackoffMs());
    }

    @Test
    public void testNotReadyForSendBeforeInitTransactions() {
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testNotReadyForSendBeforeBeginTransaction() {
        doInitTransactions();
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testNotReadyForSendAfterAbortableError() {
        doInitTransactions();
        transactionManager.beginTransaction();
        transactionManager.transitionToAbortableError(new KafkaException());
        assertThrows(KafkaException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testNotReadyForSendAfterFatalError() {
        doInitTransactions();
        transactionManager.transitionToFatalError(new KafkaException());
        assertThrows(KafkaException.class, () -> transactionManager.maybeAddPartition(tp0));
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterAbortableError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterAbortableError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil(transactionManager::hasInFlightRequest);
        transactionManager.transitionToAbortableError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPendingPartitionAfterFatalError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithInFlightPartitionAddAfterFatalError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        // Send the AddPartitionsToTxn request and leave it in-flight
        runUntil(transactionManager::hasInFlightRequest);
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterAbortableError() {
        doInitTransactions();

        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToAbortableError(new KafkaException());

        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithAddedPartitionAfterFatalError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        transactionManager.transitionToFatalError(new KafkaException());

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testIsSendToPartitionAllowedWithPartitionNotAdded() {
        doInitTransactions();
        transactionManager.beginTransaction();
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDefaultSequenceNumber(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        assertEquals(transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals(transactionManager.sequenceNumber(tp0), 3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBumpEpochAndResetSequenceNumbersAfterUnknownProducerId(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        ProducerBatch b4 = writeIdempotentBatchWithValue(transactionManager, tp0, "4");
        ProducerBatch b5 = writeIdempotentBatchWithValue(transactionManager, tp0, "5");
        assertEquals(5, transactionManager.sequenceNumber(tp0));

        // First batch succeeds
        long b1AppendTime = time.milliseconds();
        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        b1.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(b1, b1Response);

        // We get an UNKNOWN_PRODUCER_ID, so bump the epoch and set sequence numbers back to 0
        ProduceResponse.PartitionResponse b2Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 500L);
        assertTrue(transactionManager.canRetry(b2Response, b2));

        // Run sender loop to trigger epoch bump
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == 2);
        assertEquals(2, b2.producerEpoch());
        assertEquals(0, b2.baseSequence());
        assertEquals(1, b3.baseSequence());
        assertEquals(2, b4.baseSequence());
        assertEquals(3, b5.baseSequence());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBatchFailureAfterProducerReset(boolean transactionV2Enabled) {
        // This tests a scenario where the producerId is reset while pending requests are still inflight.
        // The partition(s) that triggered the reset will have their sequence number reset, while any others will not
        final short epoch = Short.MAX_VALUE;

        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "1");

        ProduceResponse.PartitionResponse tp0b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, -1, -1, 400L);
        transactionManager.handleCompletedBatch(tp0b1, tp0b1Response);

        ProduceResponse.PartitionResponse tp1b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, -1, -1, 400L);
        transactionManager.handleCompletedBatch(tp1b1, tp1b1Response);

        ProducerBatch tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "2");
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp1));

        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 400L);
        assertTrue(transactionManager.canRetry(b1Response, tp0b1));

        ProduceResponse.PartitionResponse b2Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, -1, -1, 400L);
        transactionManager.handleCompletedBatch(tp1b1, b2Response);

        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(tp0b2, transactionManager.nextBatchBySequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp1));
        assertEquals(tp1b2, transactionManager.nextBatchBySequence(tp1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBatchCompletedAfterProducerReset(boolean transactionV2Enabled) {
        final short epoch = Short.MAX_VALUE;

        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        writeIdempotentBatchWithValue(transactionManager, tp1, "1");

        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        assertEquals(2, transactionManager.sequenceNumber(tp0));

        // The producerId might be reset due to a failure on another partition
        transactionManager.requestIdempotentEpochBumpForPartition(tp1);
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        initializeIdempotentProducerId(producerId + 1, (short) 0);

        // We continue to track the state of tp0 until in-flight requests complete
        ProduceResponse.PartitionResponse b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L);
        transactionManager.handleCompletedBatch(b1, b1Response);

        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(0, transactionManager.lastAckedSequence(tp0).getAsInt());
        assertEquals(b2, transactionManager.nextBatchBySequence(tp0));
        assertEquals(epoch, transactionManager.nextBatchBySequence(tp0).producerEpoch());

        ProduceResponse.PartitionResponse b2Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L);
        transactionManager.handleCompletedBatch(b2, b2Response);

        transactionManager.maybeUpdateProducerIdAndEpoch(tp0);
        assertEquals(0, transactionManager.sequenceNumber(tp0));
        assertFalse(transactionManager.lastAckedSequence(tp0).isPresent());
        assertNull(transactionManager.nextBatchBySequence(tp0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDuplicateSequenceAfterProducerReset(boolean transactionV2Enabled) throws Exception {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        initializeIdempotentProducerId(producerId, epoch);

        Metrics metrics = new Metrics(time);
        final int requestTimeout = 10000;
        final int deliveryTimeout = 15000;

        RecordAccumulator accumulator = new RecordAccumulator(logContext, 16 * 1024, Compression.NONE, 0, 0L, 0L,
                deliveryTimeout, metrics, "", time, transactionManager,
                new BufferPool(1024 * 1024, 16 * 1024, metrics, time, ""));

        Sender sender = new Sender(logContext, this.client, this.metadata, accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(metrics), this.time, requestTimeout,
                0, transactionManager);

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        Future<RecordMetadata> responseFuture1 = accumulator.append(tp0.topic(), tp0.partition(), time.milliseconds(),
                "1".getBytes(), "1".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT, time.milliseconds(),
                TestUtils.singletonCluster()).future;
        sender.runOnce();
        assertEquals(1, transactionManager.sequenceNumber(tp0));

        time.sleep(requestTimeout);
        sender.runOnce();
        assertEquals(0, client.inFlightRequestCount());
        assertTrue(transactionManager.hasInflightBatches(tp0));
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        sender.runOnce(); // retry
        assertEquals(1, client.inFlightRequestCount());
        assertTrue(transactionManager.hasInflightBatches(tp0));
        assertEquals(1, transactionManager.sequenceNumber(tp0));

        time.sleep(5000); // delivery time out
        sender.runOnce();

        // The retried request will remain inflight until the request timeout
        // is reached even though the delivery timeout has expired and the
        // future has completed exceptionally.
        assertTrue(responseFuture1.isDone());
        TestUtils.assertFutureThrows(TimeoutException.class, responseFuture1);
        assertFalse(transactionManager.hasInFlightRequest());
        assertEquals(1, client.inFlightRequestCount());

        sender.runOnce(); // bump the epoch
        assertEquals(epoch + 1, transactionManager.producerIdAndEpoch().epoch);
        assertEquals(0, transactionManager.sequenceNumber(tp0));

        Future<RecordMetadata> responseFuture2 = accumulator.append(tp0.topic(), tp0.partition(), time.milliseconds(),
                "2".getBytes(), "2".getBytes(), Record.EMPTY_HEADERS, null, MAX_BLOCK_TIMEOUT, time.milliseconds(),
                TestUtils.singletonCluster()).future;
        sender.runOnce();
        sender.runOnce();
        assertEquals(0, transactionManager.firstInFlightSequence(tp0));
        assertEquals(1, transactionManager.sequenceNumber(tp0));

        time.sleep(5000); // request time out again
        sender.runOnce();
        assertTrue(transactionManager.hasInflightBatches(tp0)); // the latter batch failed and retried
        assertFalse(responseFuture2.isDone());
    }

    private ProducerBatch writeIdempotentBatchWithValue(TransactionManager manager,
                                                        TopicPartition tp,
                                                        String value) {
        manager.maybeUpdateProducerIdAndEpoch(tp);
        int seq = manager.sequenceNumber(tp);
        manager.incrementSequenceNumber(tp, 1);
        ProducerBatch batch = batchWithValue(tp, value);
        batch.setProducerState(manager.producerIdAndEpoch(), seq, false);
        manager.addInFlightBatch(batch);
        batch.close();
        return batch;
    }

    private ProducerBatch writeTransactionalBatchWithValue(
        TransactionManager manager,
        TopicPartition tp,
        String value
    ) {
        manager.maybeUpdateProducerIdAndEpoch(tp);
        int seq = manager.sequenceNumber(tp);
        manager.incrementSequenceNumber(tp, 1);
        ProducerBatch batch = batchWithValue(tp, value);
        batch.setProducerState(manager.producerIdAndEpoch(), seq, true);
        manager.addInFlightBatch(batch);
        batch.close();
        return batch;
    }

    private ProducerBatch batchWithValue(TopicPartition tp, String value) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(64),
                Compression.NONE, TimestampType.CREATE_TIME, 0L);
        long currentTimeMs = time.milliseconds();
        ProducerBatch batch = new ProducerBatch(tp, builder, currentTimeMs);
        batch.tryAppend(currentTimeMs, new byte[0], value.getBytes(), new Header[0], null, currentTimeMs);
        return batch;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSequenceNumberOverflow(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        assertEquals(transactionManager.sequenceNumber(tp0), 0);
        transactionManager.incrementSequenceNumber(tp0, Integer.MAX_VALUE);
        assertEquals(transactionManager.sequenceNumber(tp0), Integer.MAX_VALUE);
        transactionManager.incrementSequenceNumber(tp0, 100);
        assertEquals(transactionManager.sequenceNumber(tp0), 99);
        transactionManager.incrementSequenceNumber(tp0, Integer.MAX_VALUE);
        assertEquals(transactionManager.sequenceNumber(tp0), 98);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testProducerIdReset(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        initializeIdempotentProducerId(15L, Short.MAX_VALUE);
        assertEquals(transactionManager.sequenceNumber(tp0), 0);
        assertEquals(transactionManager.sequenceNumber(tp1), 0);
        transactionManager.incrementSequenceNumber(tp0, 3);
        assertEquals(transactionManager.sequenceNumber(tp0), 3);
        transactionManager.incrementSequenceNumber(tp1, 3);
        assertEquals(transactionManager.sequenceNumber(tp1), 3);

        transactionManager.requestIdempotentEpochBumpForPartition(tp0);
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(transactionManager.sequenceNumber(tp0), 0);
        assertEquals(transactionManager.sequenceNumber(tp1), 3);
    }

    @Test
    public void testBasicTransaction() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        prepareProduceResponse(Errors.NONE, producerId, epoch);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());
        runUntil(responseFuture::isDone);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));

        assertFalse(transactionManager.hasPendingOffsetCommits());

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);

        runUntil(transactionManager::hasPendingOffsetCommits);
        assertFalse(addOffsetsResult.isCompleted()); // the result doesn't complete until TxnOffsetCommit returns

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.NONE);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);
        assertTrue(transactionManager.hasPendingOffsetCommits());

        runUntil(() -> !transactionManager.hasPendingOffsetCommits());
        assertTrue(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete.

        transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testTransactionManagerEnablesV2() {
        initializeTransactionManager(Optional.of(transactionalId), false);
        doInitTransactions();
        transactionManager.beginTransaction();
        assertFalse(transactionManager.hasFatalError());
        assertFalse(transactionManager.isTransactionV2Enabled());

        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
            new ApiVersion()
                .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3)),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion((short) 2)
                .setMinVersion((short) 0)),
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel((short) 2)
                .setMinVersionLevel((short) 2)),
            2));

        // The manager stays in transaction V2 disabled.
        assertFalse(transactionManager.isTransactionV2Enabled());

        transactionManager.maybeAddPartition(tp1);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(tp1, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        TransactionalRequestResult retryResult = transactionManager.beginCommit();
        assertTrue(transactionManager.hasOngoingTransaction());
        assertTrue(transactionManager.isTransactionV2Enabled());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, (short) (epoch + 1));
        runUntil(() -> !transactionManager.hasOngoingTransaction());
        runUntil(retryResult::isCompleted);
        retryResult.await();
        runUntil(retryResult::isAcked);
        assertFalse(transactionManager.hasOngoingTransaction());

        // After restart the transaction, the V2 is still enabled and epoch is bumped.
        transactionManager.beginTransaction();
        assertTrue(transactionManager.isTransactionV2Enabled());
        assertEquals(epoch + 1, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testTransactionV2AddPartitionAndOffsets() throws InterruptedException {
        initializeTransactionManager(Optional.of(transactionalId), true);
        doInitTransactions();

        transactionManager.beginTransaction();

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareProduceResponse(Errors.NONE, producerId, epoch);
        transactionManager.maybeAddPartition(tp0);
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());
        runUntil(responseFuture::isDone);

        // Now, test adding the offsets.
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));

        assertTrue(transactionManager.hasPendingOffsetCommits());

        // the result doesn't complete until TxnOffsetCommit returns
        assertFalse(addOffsetsResult.isCompleted());

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp1, Errors.NONE);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);
        assertTrue(transactionManager.hasPendingOffsetCommits());

        runUntil(() -> !transactionManager.hasPendingOffsetCommits());
        // We should only be done after both RPCs complete.
        assertTrue(addOffsetsResult.isCompleted());

        transactionManager.beginCommit();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch, producerId, (short) (epoch + 1), false);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
    }

    @Test
    public void testTransactionManagerDisablesV2() {
        Metrics metrics = new Metrics(time);

        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
            new ApiVersion()
                .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3),
            new ApiVersion()
                .setApiKey(ApiKeys.PRODUCE.id)
                .setMinVersion((short) 5)
                .setMaxVersion((short) (ProduceRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 + 1)),
            new ApiVersion()
                .setApiKey(ApiKeys.TXN_OFFSET_COMMIT.id)
                .setMinVersion((short) 1)
                .setMaxVersion((short) (TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2 + 1))),
            Arrays.asList(new ApiVersionsResponseData.SupportedFeatureKey()
                .setName("transaction.version")
                .setMaxVersion((short) 1)
                .setMinVersion((short) 0)),
            Arrays.asList(new ApiVersionsResponseData.FinalizedFeatureKey()
                .setName("transaction.version")
                .setMaxVersionLevel((short) 1)
                .setMinVersionLevel((short) 1)),
            0));
        this.transactionManager = new TestableTransactionManager(logContext, transactionalId,
            transactionTimeoutMs, DEFAULT_RETRY_BACKOFF_MS, apiVersions, false);

        int batchSize = 16 * 1024;
        int deliveryTimeoutMs = 3000;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";

        this.brokerNode = new Node(0, "localhost", 2211);
        this.accumulator = new RecordAccumulator(logContext, batchSize, Compression.NONE, 0, 0L, 0L,
            deliveryTimeoutMs, metrics, metricGrpName, time, transactionManager,
            new BufferPool(totalSize, batchSize, metrics, time, metricGrpName));

        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, true,
            MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(metrics), this.time, REQUEST_TIMEOUT,
            50, transactionManager);

        doInitTransactions();
        assertFalse(transactionManager.isTransactionV2Enabled());
    }

    @Test
    public void testDisconnectAndRetry() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, true, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
    }

    @Test
    public void testInitializeTransactionsTwiceRaisesError() {
        doInitTransactions(producerId, epoch);
        assertTrue(transactionManager.hasProducerId());
        assertThrows(IllegalStateException.class, () -> transactionManager.initializeTransactions(false));
    }

    @Test
    public void testUnsupportedFindCoordinator() {
        transactionManager.initializeTransactions(false);
        client.prepareUnsupportedVersionResponse(body -> {
            FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
            assertEquals(CoordinatorType.forId(findCoordinatorRequest.data().keyType()), CoordinatorType.TRANSACTION);
            assertTrue(findCoordinatorRequest.data().key().isEmpty());
            assertEquals(1, findCoordinatorRequest.data().coordinatorKeys().size());
            assertTrue(findCoordinatorRequest.data().coordinatorKeys().contains(transactionalId));
            return true;
        });

        runUntil(transactionManager::hasFatalError);
        assertTrue(transactionManager.hasFatalError());
        assertInstanceOf(UnsupportedVersionException.class, transactionManager.lastError());
    }

    @Test
    public void testUnsupportedInitTransactions() {
        transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertFalse(transactionManager.hasError());

        client.prepareUnsupportedVersionResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertEquals(initProducerIdRequest.data().transactionalId(), transactionalId);
            assertEquals(initProducerIdRequest.data().transactionTimeoutMs(), transactionTimeoutMs);
            return true;
        });

        runUntil(transactionManager::hasFatalError);
        assertTrue(transactionManager.hasFatalError());
        assertInstanceOf(UnsupportedVersionException.class, transactionManager.lastError());
    }

    @Test
    public void testUnsupportedForMessageFormatInTxnOffsetCommit() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, singletonMap(tp, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT));
        runUntil(transactionManager::hasError);

        assertInstanceOf(UnsupportedForMessageFormatException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(UnsupportedForMessageFormatException.class, sendOffsetsResult.error());
        assertFatalError(UnsupportedForMessageFormatException.class);
    }

    @Test
    public void testFencedInstanceIdInTxnOffsetCommitByGroupMetadata() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final String fencedMemberId = "fenced_member";

        doInitTransactions();

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, 5, fencedMemberId, Optional.of(groupInstanceId)));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data().groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data().producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data().producerEpoch());
            return txnOffsetCommitRequest.data().groupInstanceId().equals(groupInstanceId)
                && !txnOffsetCommitRequest.data().memberId().equals(memberId);
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.FENCED_INSTANCE_ID)));

        runUntil(transactionManager::hasError);
        assertInstanceOf(FencedInstanceIdException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(FencedInstanceIdException.class, sendOffsetsResult.error());
        assertAbortableError(FencedInstanceIdException.class);
    }

    @Test
    public void testUnknownMemberIdInTxnOffsetCommitByGroupMetadata() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final String unknownMemberId = "unknownMember";

        doInitTransactions();

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, 5, unknownMemberId, Optional.empty()));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data().groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data().producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data().producerEpoch());
            return !txnOffsetCommitRequest.data().memberId().equals(memberId);
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.UNKNOWN_MEMBER_ID)));

        runUntil(transactionManager::hasError);
        assertInstanceOf(CommitFailedException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(CommitFailedException.class, sendOffsetsResult.error());
        assertAbortableError(CommitFailedException.class);
    }

    @Test
    public void testIllegalGenerationInTxnOffsetCommitByGroupMetadata() {
        final TopicPartition tp = new TopicPartition("foo", 0);
        final int illegalGenerationId = 1;

        doInitTransactions();

        transactionManager.beginTransaction();

        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)),
            new ConsumerGroupMetadata(consumerGroupId, illegalGenerationId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
                Optional.empty()));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);

        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, singletonMap(tp, Errors.ILLEGAL_GENERATION));
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data().groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data().producerId());
            assertEquals(epoch, txnOffsetCommitRequest.data().producerEpoch());
            return txnOffsetCommitRequest.data().generationId() != generationId;
        }, new TxnOffsetCommitResponse(0, singletonMap(tp, Errors.ILLEGAL_GENERATION)));

        runUntil(transactionManager::hasError);
        assertInstanceOf(CommitFailedException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(CommitFailedException.class, sendOffsetsResult.error());
        assertAbortableError(CommitFailedException.class);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectAfterSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, true, producerId, epoch);
        // send pid to coordinator, should get disconnected before receiving the response, and resend the
        // FindCoordinator and InitPid requests.
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        assertNull(transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);

        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);
        runUntil(initPidResult::isCompleted);

        assertTrue(initPidResult.isCompleted()); // The future should only return after the second round of retries succeed.
        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnDisconnectBeforeSend() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        client.disconnect(brokerNode.idString());
        client.backoff(brokerNode, 100);
        // send pid to coordinator. Should get disconnected before the send and resend the FindCoordinator
        // and InitPid requests.
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);
        time.sleep(110);  // waiting for the backoff period for the node to expire.

        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);

        runUntil(initPidResult::isCompleted);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testLookupCoordinatorOnNotCoordinatorError() {
        // This is called from the initTransactions method in the producer as the first order of business.
        // It finds the coordinator and then gets a PID.
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NOT_COORDINATOR, false, producerId, epoch);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        assertFalse(initPidResult.isCompleted());
        assertFalse(transactionManager.hasProducerId());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertFalse(initPidResult.isCompleted());
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);

        runUntil(initPidResult::isCompleted);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(epoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInFindCoordinator() {
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false,
                CoordinatorType.TRANSACTION, transactionalId);

        runUntil(transactionManager::hasError);

        assertTrue(transactionManager.hasFatalError());
        assertInstanceOf(TransactionalIdAuthorizationException.class, transactionManager.lastError());
        assertFalse(initPidResult.isSuccessful());
        assertThrows(TransactionalIdAuthorizationException.class, initPidResult::await);
        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInInitProducerId() {
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, false, producerId, RecordBatch.NO_PRODUCER_EPOCH);
        runUntil(transactionManager::hasError);
        assertTrue(initPidResult.isCompleted());
        assertFalse(initPidResult.isSuccessful());
        assertThrows(TransactionalIdAuthorizationException.class, initPidResult::await);
        assertAbortableError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInFindCoordinator() {
        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(transactionManager::hasError);
        assertInstanceOf(GroupAuthorizationException.class, transactionManager.lastError());

        runUntil(sendOffsetsResult::isCompleted);
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(GroupAuthorizationException.class, sendOffsetsResult.error());

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testGroupAuthorizationFailureInTxnOffsetCommit() {
        final TopicPartition tp1 = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp1, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, singletonMap(tp1, Errors.GROUP_AUTHORIZATION_FAILED));

        runUntil(transactionManager::hasError);
        assertInstanceOf(GroupAuthorizationException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(GroupAuthorizationException.class, sendOffsetsResult.error());
        assertFalse(transactionManager.hasPendingOffsetCommits());

        GroupAuthorizationException exception = (GroupAuthorizationException) sendOffsetsResult.error();
        assertEquals(consumerGroupId, exception.groupId());

        assertAbortableError(GroupAuthorizationException.class);
    }

    @Test
    public void testFatalErrorWhenProduceResponseWithInvalidPidMapping() throws InterruptedException {
        initializeTransactionManager(Optional.of(transactionalId), true);
        doInitTransactions();

        transactionManager.beginTransaction();
        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        transactionManager.maybeAddPartition(tp0);
        assertFalse(responseFuture.isDone());

        prepareProduceResponse(Errors.INVALID_PRODUCER_ID_MAPPING, producerId, epoch);
        assertFalse(responseFuture.isDone());
        runUntil(responseFuture::isDone);
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddOffsetsToTxn() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED, consumerGroupId, producerId, epoch);
        runUntil(transactionManager::hasError);
        assertInstanceOf(TransactionalIdAuthorizationException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(TransactionalIdAuthorizationException.class, sendOffsetsResult.error());

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testInvalidTxnStateFailureInAddOffsetsToTxn() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
            singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.INVALID_TXN_STATE, consumerGroupId, producerId, epoch);
        runUntil(transactionManager::hasError);
        assertInstanceOf(InvalidTxnStateException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(InvalidTxnStateException.class, sendOffsetsResult.error());

        assertFatalError(InvalidTxnStateException.class);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInTxnOffsetCommit() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, singletonMap(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasError);

        assertInstanceOf(TransactionalIdAuthorizationException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(TransactionalIdAuthorizationException.class, sendOffsetsResult.error());

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testTopicAuthorizationFailureInAddPartitions() throws InterruptedException {
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        transactionManager.maybeAddPartition(tp1);

        FutureRecordMetadata firstPartitionAppend = appendToAccumulator(tp0);
        FutureRecordMetadata secondPartitionAppend = appendToAccumulator(tp1);

        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        errors.put(tp1, Errors.OPERATION_NOT_ATTEMPTED);

        prepareAddPartitionsToTxn(errors);
        runUntil(transactionManager::hasError);

        assertInstanceOf(TopicAuthorizationException.class, transactionManager.lastError());
        assertFalse(transactionManager.isPartitionPendingAdd(tp0));
        assertFalse(transactionManager.isPartitionPendingAdd(tp1));
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.transactionContainsPartition(tp1));
        assertFalse(transactionManager.hasPartitionsToAdd());

        TopicAuthorizationException exception = (TopicAuthorizationException) transactionManager.lastError();
        assertEquals(singleton(tp0.topic()), exception.unauthorizedTopics());
        assertAbortableError(TopicAuthorizationException.class);
        sender.runOnce();

        TestUtils.assertFutureThrows(TransactionAbortedException.class, firstPartitionAppend);
        TestUtils.assertFutureThrows(TransactionAbortedException.class, secondPartitionAppend);
    }

    @Test
    public void testCommitWithTopicAuthorizationFailureInAddPartitionsInFlight() throws InterruptedException {
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);

        doInitTransactions();

        // Begin a transaction, send two records, and begin commit
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        transactionManager.maybeAddPartition(tp1);
        FutureRecordMetadata firstPartitionAppend = appendToAccumulator(tp0);
        FutureRecordMetadata secondPartitionAppend = appendToAccumulator(tp1);
        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // We send the AddPartitionsToTxn request in the first sender call
        sender.runOnce();
        assertFalse(transactionManager.hasError());
        assertFalse(commitResult.isCompleted());
        assertFalse(firstPartitionAppend.isDone());

        // The AddPartitionsToTxn response returns in the next call with the error
        Map<TopicPartition, Errors> errors = new HashMap<>();
        errors.put(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        errors.put(tp1, Errors.OPERATION_NOT_ATTEMPTED);
        AddPartitionsToTxnResult result = AddPartitionsToTxnResponse.resultForTransaction(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID, errors);
        AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData().setResultsByTopicV3AndBelow(result.topicResults()).setThrottleTimeMs(0);
        client.respond(body -> {
            AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) body;
            assertEquals(new HashSet<>(getPartitionsFromV3Request(request)), new HashSet<>(errors.keySet()));
            return true;
        }, new AddPartitionsToTxnResponse(data));

        sender.runOnce();
        assertTrue(transactionManager.hasError());
        assertFalse(commitResult.isCompleted());
        assertFalse(firstPartitionAppend.isDone());
        assertFalse(secondPartitionAppend.isDone());

        // The next call aborts the records, which have not yet been sent. It should
        // not block because there are no requests pending and we still need to cancel
        // the pending transaction commit.
        sender.runOnce();
        assertTrue(commitResult.isCompleted());
        TestUtils.assertFutureThrows(TopicAuthorizationException.class, firstPartitionAppend);
        TestUtils.assertFutureThrows(TopicAuthorizationException.class, secondPartitionAppend);
        assertInstanceOf(TopicAuthorizationException.class, commitResult.error());
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionNotStarted() throws Exception {
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(unauthorizedPartition);

        Future<RecordMetadata> responseFuture = appendToAccumulator(unauthorizedPartition);

        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(() -> !client.hasPendingResponses());

        assertTrue(transactionManager.hasAbortableError());
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        runUntil(responseFuture::isDone);
        assertProduceFutureFailed(responseFuture);

        // No partitions added, so no need to prepare EndTxn response
        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());
        assertTrue(abortResult.isSuccessful());
        abortResult.await();

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);
        assertNotNull(responseFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testRetryAbortTransactionAfterTimeout() throws Exception {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        appendToAccumulator(tp0);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        TransactionalRequestResult result = transactionManager.beginAbort();
        assertThrows(TimeoutException.class, () -> result.await(0, TimeUnit.MILLISECONDS));

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(transactionManager::isReady);
        assertTrue(result.isSuccessful());
        assertFalse(result.isAcked());
        assertFalse(transactionManager.hasOngoingTransaction());

        assertThrows(IllegalStateException.class, () -> transactionManager.initializeTransactions(false));
        assertThrows(IllegalStateException.class, transactionManager::beginTransaction);
        assertThrows(IllegalStateException.class, transactionManager::beginCommit);
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));

        assertSame(result, transactionManager.beginAbort());
        result.await();

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testRetryCommitTransactionAfterTimeout() throws Exception {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        prepareProduceResponse(Errors.NONE, producerId, epoch);

        appendToAccumulator(tp0);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        TransactionalRequestResult result = transactionManager.beginCommit();
        assertThrows(TimeoutException.class, () -> result.await(0, TimeUnit.MILLISECONDS));

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(transactionManager::isReady);
        assertTrue(result.isSuccessful());
        assertFalse(result.isAcked());
        assertFalse(transactionManager.hasOngoingTransaction());

        assertThrows(IllegalStateException.class, () -> transactionManager.initializeTransactions(false));
        assertThrows(IllegalStateException.class, transactionManager::beginTransaction);
        assertThrows(IllegalStateException.class, transactionManager::beginAbort);
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));

        assertSame(result, transactionManager.beginCommit());
        result.await();

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testRetryInitTransactionsAfterTimeout() {
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        assertThrows(TimeoutException.class, () -> result.await(0, TimeUnit.MILLISECONDS));

        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);
        runUntil(transactionManager::hasProducerId);
        assertTrue(result.isSuccessful());
        assertFalse(result.isAcked());

        // At this point, the InitProducerId call has returned, but the user has yet
        // to complete the call to `initTransactions`. Other transitions should be
        // rejected until they do.

        assertThrows(IllegalStateException.class, transactionManager::beginTransaction);
        assertThrows(IllegalStateException.class, transactionManager::beginAbort);
        assertThrows(IllegalStateException.class, transactionManager::beginCommit);
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));

        assertSame(result, transactionManager.initializeTransactions(false));
        result.await();
        assertTrue(result.isAcked());
        assertThrows(IllegalStateException.class, () -> transactionManager.initializeTransactions(false));

        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testRecoveryFromAbortableErrorTransactionStarted() throws Exception {
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        transactionManager.maybeAddPartition(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.transactionContainsPartition(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertFalse(unauthorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        TransactionalRequestResult result = transactionManager.beginAbort();
        runUntil(transactionManager::isReady);
        // neither produce request has been sent, so they should both be failed immediately
        assertProduceFutureFailed(authorizedTopicProduceFuture);
        assertProduceFutureFailed(unauthorizedTopicProduceFuture);
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());
        assertTrue(result.isSuccessful());
        result.await();

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        FutureRecordMetadata nextTransactionFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(nextTransactionFuture::isDone);
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testRecoveryFromAbortableErrorProduceRequestInRetry() throws Exception {
        final TopicPartition unauthorizedPartition = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.NONE);

        Future<RecordMetadata> authorizedTopicProduceFuture = appendToAccumulator(tp0);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        accumulator.beginFlush();
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(authorizedTopicProduceFuture.isDone());
        assertTrue(accumulator.hasIncomplete());

        transactionManager.maybeAddPartition(unauthorizedPartition);
        Future<RecordMetadata> unauthorizedTopicProduceFuture = appendToAccumulator(unauthorizedPartition);
        prepareAddPartitionsToTxn(singletonMap(unauthorizedPartition, Errors.TOPIC_AUTHORIZATION_FAILED));
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.transactionContainsPartition(unauthorizedPartition));
        assertFalse(authorizedTopicProduceFuture.isDone());

        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(authorizedTopicProduceFuture::isDone);

        assertProduceFutureFailed(unauthorizedTopicProduceFuture);
        assertNotNull(authorizedTopicProduceFuture.get());
        assertTrue(authorizedTopicProduceFuture.isDone());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        runUntil(transactionManager::isReady);
        // neither produce request has been sent, so they should both be failed immediately
        assertTrue(transactionManager.isReady());
        assertFalse(transactionManager.hasPartitionsToAdd());
        assertFalse(accumulator.hasIncomplete());
        assertTrue(abortResult.isSuccessful());
        abortResult.await();

        // ensure we can now start a new transaction

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        FutureRecordMetadata nextTransactionFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(singletonMap(tp0, Errors.NONE));
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.hasPartitionsToAdd());

        transactionManager.beginCommit();
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(nextTransactionFuture::isDone);
        assertNotNull(nextTransactionFuture.get());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(transactionManager::isReady);
    }

    @Test
    public void testTransactionalIdAuthorizationFailureInAddPartitions() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp);

        prepareAddPartitionsToTxn(tp, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        runUntil(transactionManager::hasError);
        assertInstanceOf(TransactionalIdAuthorizationException.class, transactionManager.lastError());

        assertFatalError(TransactionalIdAuthorizationException.class);
    }

    @Test
    public void testInvalidTxnStateInAddPartitions() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp);

        prepareAddPartitionsToTxn(tp, Errors.INVALID_TXN_STATE);
        runUntil(transactionManager::hasError);
        assertInstanceOf(InvalidTxnStateException.class, transactionManager.lastError());

        assertFatalError(InvalidTxnStateException.class);
    }

    @Test
    public void testFlushPendingPartitionsOnCommit() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // we have an append, an add partitions request, and now also an endtxn.
        // The order should be:
        //  1. Add Partitions
        //  2. Produce
        //  3. EndTxn.
        assertFalse(transactionManager.transactionContainsPartition(tp0));
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertFalse(responseFuture.isDone());
        assertFalse(commitResult.isCompleted());

        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        assertFalse(commitResult.isCompleted());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertTrue(transactionManager.isCompleting());

        runUntil(commitResult::isCompleted);
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testMultipleAddPartitionsPerForOneProduce() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        // User does one producer.send
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        assertFalse(transactionManager.transactionContainsPartition(tp0));

        // Sender flushes one add partitions. The produce goes next.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        // In the mean time, the user does a second produce to a different partition
        transactionManager.maybeAddPartition(tp1);
        Future<RecordMetadata> secondResponseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);

        assertFalse(transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // The second add partitions should go out here.
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        assertFalse(responseFuture.isDone());
        assertFalse(secondResponseFuture.isDone());

        // Finally we get to the produce.
        runUntil(responseFuture::isDone);
        assertTrue(secondResponseFuture.isDone());
    }

    @ParameterizedTest
    @EnumSource(names = {
        "UNKNOWN_TOPIC_OR_PARTITION",
        "REQUEST_TIMED_OUT",
        "COORDINATOR_LOAD_IN_PROGRESS",
        "CONCURRENT_TRANSACTIONS"
    })
    public void testRetriableErrors(Errors error) {
        // Ensure FindCoordinator retries.
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(error, false, CoordinatorType.TRANSACTION, transactionalId);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        // Ensure InitPid retries.
        prepareInitPidResponse(error, false, producerId, epoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);
        runUntil(transactionManager::hasProducerId);

        result.await();
        transactionManager.beginTransaction();

        // Ensure AddPartitionsToTxn retries. Since CONCURRENT_TRANSACTIONS is handled differently here, we substitute.
        Errors addPartitionsToTxnError = error.equals(Errors.CONCURRENT_TRANSACTIONS) ? Errors.COORDINATOR_LOAD_IN_PROGRESS : error;
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(addPartitionsToTxnError, tp0, epoch, producerId);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        // Ensure txnOffsetCommit retries is tested in testRetriableErrorInTxnOffsetCommit.

        // Ensure EndTxn retries.
        TransactionalRequestResult abortResult = transactionManager.beginCommit();
        prepareEndTxnResponse(error, TransactionResult.COMMIT, producerId, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void testCoordinatorNotAvailable() {
        // Ensure FindCoordinator with COORDINATOR_NOT_AVAILABLE error retries.
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, false, CoordinatorType.TRANSACTION, transactionalId);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);
        runUntil(transactionManager::hasProducerId);

        result.await();
    }

    @Test
    public void testProducerFencedExceptionInInitProducerId() {
        verifyProducerFencedForInitProducerId(Errors.PRODUCER_FENCED);
    }

    @Test
    public void testInvalidProducerEpochConvertToProducerFencedInInitProducerId() {
        verifyProducerFencedForInitProducerId(Errors.INVALID_PRODUCER_EPOCH);
    }

    private void verifyProducerFencedForInitProducerId(Errors error) {
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(error, false, producerId, epoch);

        runUntil(transactionManager::hasError);

        assertThrows(ProducerFencedException.class, result::await);

        assertThrows(ProducerFencedException.class, () -> transactionManager.beginTransaction());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginCommit());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginAbort());
        assertThrows(ProducerFencedException.class, () -> transactionManager.sendOffsetsToTransaction(
            Collections.emptyMap(), new ConsumerGroupMetadata("dummyId")));
    }

    @Test
    public void testProducerFencedInAddPartitionToTxn() throws InterruptedException {
        verifyProducerFencedForAddPartitionsToTxn(Errors.PRODUCER_FENCED);
    }

    @Test
    public void testInvalidProducerEpochConvertToProducerFencedInAddPartitionToTxn() throws InterruptedException {
        verifyProducerFencedForAddPartitionsToTxn(Errors.INVALID_PRODUCER_EPOCH);
    }

    private void verifyProducerFencedForAddPartitionsToTxn(Errors error) throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(error, tp0, epoch, producerId);

        verifyProducerFenced(responseFuture);
    }

    @Test
    public void testProducerFencedInAddOffSetsToTxn() throws InterruptedException {
        verifyProducerFencedForAddOffsetsToTxn(Errors.INVALID_PRODUCER_EPOCH);
    }

    @Test
    public void testInvalidProducerEpochConvertToProducerFencedInAddOffSetsToTxn() throws InterruptedException {
        verifyProducerFencedForAddOffsetsToTxn(Errors.INVALID_PRODUCER_EPOCH);
    }

    private void verifyProducerFencedForAddOffsetsToTxn(Errors error) throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.sendOffsetsToTransaction(Collections.emptyMap(), new ConsumerGroupMetadata(consumerGroupId));

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddOffsetsToTxnResponse(error, consumerGroupId, producerId, epoch);

        verifyProducerFenced(responseFuture);
    }

    private void verifyProducerFenced(Future<RecordMetadata> responseFuture) throws InterruptedException {
        runUntil(responseFuture::isDone);
        assertTrue(transactionManager.hasError());

        try {
            // make sure the produce was expired.
            responseFuture.get();
            fail("Expected to get a ExecutionException from the response");
        } catch (ExecutionException e) {
            assertInstanceOf(ProducerFencedException.class, e.getCause());
        }

        // make sure the exception was thrown directly from the follow-up calls.
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginTransaction());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginCommit());
        assertThrows(ProducerFencedException.class, () -> transactionManager.beginAbort());
        assertThrows(ProducerFencedException.class, () -> transactionManager.sendOffsetsToTransaction(
            Collections.emptyMap(), new ConsumerGroupMetadata("dummyId")));
    }

    @Test
    public void testInvalidProducerEpochConvertToProducerFencedInEndTxn() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        prepareEndTxnResponse(Errors.INVALID_PRODUCER_EPOCH, TransactionResult.COMMIT, producerId, epoch);

        runUntil(commitResult::isCompleted);
        runUntil(responseFuture::isDone);

        assertThrows(KafkaException.class, commitResult::await);
        assertFalse(commitResult.isSuccessful());
        assertTrue(commitResult.isAcked());

        // make sure the exception was thrown directly from the follow-up calls.
        assertThrows(KafkaException.class, () -> transactionManager.beginTransaction());
        assertThrows(KafkaException.class, () -> transactionManager.beginCommit());
        assertThrows(KafkaException.class, () -> transactionManager.beginAbort());
        assertThrows(KafkaException.class, () -> transactionManager.sendOffsetsToTransaction(
            Collections.emptyMap(), new ConsumerGroupMetadata("dummyId")));
    }

    @Test
    public void testInvalidProducerEpochFromProduce() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.INVALID_PRODUCER_EPOCH, producerId, epoch);
        prepareProduceResponse(Errors.NONE, producerId, epoch);

        sender.runOnce();

        runUntil(responseFuture::isDone);
        assertTrue(transactionManager.hasError());

        transactionManager.beginAbort();

        TransactionManager.TxnRequestHandler handler = transactionManager.nextRequest(false);

        // First we will get an EndTxn for abort.
        assertNotNull(handler);
        assertInstanceOf(EndTxnRequest.Builder.class, handler.requestBuilder());

        handler = transactionManager.nextRequest(false);

        // Second we will see an InitPid for handling InvalidProducerEpoch.
        assertNotNull(handler);
        assertInstanceOf(InitProducerIdRequest.Builder.class, handler.requestBuilder());
    }

    @Test
    public void testDisallowCommitOnProduceFailure() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        TransactionalRequestResult commitResult = transactionManager.beginCommit();
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch);

        runUntil(commitResult::isCompleted);  // commit should be cancelled with exception without being sent.

        assertThrows(KafkaException.class, commitResult::await);
        TestUtils.assertFutureThrows(OutOfOrderSequenceException.class, responseFuture);

        // Commit is not allowed, so let's abort and try again.
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, (short) (epoch + 1));
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAllowAbortOnProduceFailure() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch);

        // Because this is a failure that triggers an epoch bump, the abort will trigger an InitProducerId call
        runUntil(transactionManager::hasAbortableError);
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, (short) (epoch + 1));
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testAbortableErrorWhileAbortInProgress() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        runUntil(() -> !accumulator.hasUndrained());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        sendProduceResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, producerId, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(responseFuture::isDone);

        // we do not transition to ABORTABLE_ERROR since we were already aborting
        assertTrue(transactionManager.isAborting());
        assertFalse(transactionManager.hasError());

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testCommitTransactionWithUnsentProduceRequest() throws Exception {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> !client.hasPendingResponses());
        assertTrue(accumulator.hasUndrained());

        // committing the transaction should cause the unsent batch to be flushed
        transactionManager.beginCommit();
        runUntil(() -> !accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // until the produce future returns, we will not send EndTxn
        AtomicInteger numRuns = new AtomicInteger(0);
        runUntil(() -> numRuns.incrementAndGet() >= 4);
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we send EndTxn
        runUntil(transactionManager::hasInFlightRequest);
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);

        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasInFlightRequest());
    }

    @Test
    public void testCommitTransactionWithInFlightProduceRequest() throws Exception {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());
        assertTrue(accumulator.hasUndrained());

        accumulator.beginFlush();
        runUntil(() -> !accumulator.hasUndrained());
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we begin the commit with the produce request still pending
        transactionManager.beginCommit();
        AtomicInteger numRuns = new AtomicInteger(0);
        runUntil(() -> numRuns.incrementAndGet() >= 4);
        assertFalse(accumulator.hasUndrained());
        assertTrue(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());
        assertFalse(responseFuture.isDone());

        // now the produce response returns
        sendProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);
        assertFalse(accumulator.hasUndrained());
        assertFalse(accumulator.hasIncomplete());
        assertFalse(transactionManager.hasInFlightRequest());

        // now we send EndTxn
        runUntil(transactionManager::hasInFlightRequest);
        sendEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(transactionManager::isReady);
        assertFalse(transactionManager.hasInFlightRequest());
    }

    @Test
    public void testFindCoordinatorAllowedInAbortableErrorState() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        runUntil(transactionManager::hasInFlightRequest);

        transactionManager.transitionToAbortableError(new KafkaException());
        sendAddPartitionsToTxnResponse(Errors.NOT_COORDINATOR, tp0, epoch, producerId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) == null);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testCancelUnsentAddPartitionsAndProduceOnAbort() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        // note since no partitions were added to the transaction, no EndTxn will be sent

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        TestUtils.assertFutureThrows(TransactionAbortedException.class, responseFuture);
    }

    @Test
    public void testAbortResendsAddPartitionErrorIfRetried() throws InterruptedException {
        doInitTransactions(producerId, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, producerId);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the AddPartitions
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        TestUtils.assertFutureThrows(TransactionAbortedException.class, responseFuture);
    }

    @Test
    public void testAbortResendsProduceRequestIfRetried() throws Exception {
        doInitTransactions(producerId, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.REQUEST_TIMED_OUT, producerId, epoch);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        // we should resend the ProduceRequest before aborting
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        RecordMetadata recordMetadata = responseFuture.get();
        assertEquals(tp0.topic(), recordMetadata.topic());
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnAddPartitions() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, tp0, epoch, producerId);

        runUntil(() -> !client.hasPendingResponses());
        assertFalse(transactionManager.transactionContainsPartition(tp0));  // The partition should not yet be added.

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        runUntil(responseFuture::isDone);
    }

    @Test
    public void testHandlingOfUnknownTopicPartitionErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test
    public void testHandlingOfCoordinatorLoadingErrorOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.COORDINATOR_LOAD_IN_PROGRESS);
    }

    @Test
    public void testHandlingOfNetworkExceptionOnTxnOffsetCommit() {
        testRetriableErrorInTxnOffsetCommit(Errors.NETWORK_EXCEPTION);
    }

    private void testRetriableErrorInTxnOffsetCommit(Errors error) {
        doInitTransactions();

        transactionManager.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        offsets.put(tp1, new OffsetAndMetadata(1));

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes.

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, error);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse);

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        runUntil(() -> transactionManager.coordinator(CoordinatorType.GROUP) != null);
        assertTrue(transactionManager.hasPendingOffsetCommits());

        runUntil(transactionManager::hasPendingOffsetCommits);  // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete successfully.

        txnOffsetCommitResponse.put(tp1, Errors.NONE);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse);
        runUntil(addOffsetsResult::isCompleted);
        assertTrue(addOffsetsResult.isSuccessful());
    }

    @Test
    public void testHandlingOfProducerFencedErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.PRODUCER_FENCED);
    }

    @Test
    public void testHandlingOfTransactionalIdAuthorizationFailedErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }

    @Test
    public void testHandlingOfInvalidProducerEpochErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.INVALID_PRODUCER_EPOCH, Errors.PRODUCER_FENCED);
    }

    @Test
    public void testHandlingOfUnsupportedForMessageFormatErrorOnTxnOffsetCommit() {
        testFatalErrorInTxnOffsetCommit(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT);
    }

    private void testFatalErrorInTxnOffsetCommit(final Errors error) {
        testFatalErrorInTxnOffsetCommit(error, error);
    }

    private void testFatalErrorInTxnOffsetCommit(final Errors triggeredError, final Errors resultingError) {
        doInitTransactions();

        transactionManager.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        offsets.put(tp1, new OffsetAndMetadata(1));

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId));
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes.

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, triggeredError);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse);

        runUntil(addOffsetsResult::isCompleted);
        assertFalse(addOffsetsResult.isSuccessful());
        assertEquals(resultingError.exception().getClass(), addOffsetsResult.error().getClass());
    }

    @Test
    public void shouldNotAddPartitionsToTransactionWhenTopicAuthorizationFailed() throws Exception {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        runUntil(transactionManager::hasError);
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddPartitionsRequestFailed() {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.TOPIC_AUTHORIZATION_FAILED, tp0, epoch, producerId);
        runUntil(() -> !client.hasPendingResponses());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        assertFalse(abortResult.isCompleted());

        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldNotSendAbortTxnRequestWhenOnlyAddOffsetsRequestFailed() {
        doInitTransactions();

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));

        transactionManager.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.GROUP_AUTHORIZATION_FAILED, consumerGroupId, producerId, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(transactionManager.isReady());
        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
    }

    @Test
    public void shouldFailAbortIfAddOffsetsFailsWithFatalError() {
        doInitTransactions();

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp1, new OffsetAndMetadata(1));

        transactionManager.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareAddOffsetsToTxnResponse(Errors.UNKNOWN_SERVER_ERROR, consumerGroupId, producerId, epoch);

        runUntil(abortResult::isCompleted);
        assertFalse(abortResult.isSuccessful());
        assertTrue(transactionManager.hasFatalError());
    }

    @Test
    public void testSendOffsetsWithGroupMetadata() {
        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS);

        TransactionalRequestResult addOffsetsResult = prepareGroupMetadataCommit(
            () -> prepareTxnOffsetCommitResponse(consumerGroupId, producerId,
                epoch, groupInstanceId, memberId, generationId, txnOffsetCommitResponse));

        sender.runOnce();  // Send TxnOffsetCommitRequest request.

        assertTrue(transactionManager.hasPendingOffsetCommits());  // The TxnOffsetCommit failed.
        assertFalse(addOffsetsResult.isCompleted());  // We should only be done after both RPCs complete successfully.

        txnOffsetCommitResponse.put(tp1, Errors.NONE);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, groupInstanceId, memberId, generationId, txnOffsetCommitResponse);
        sender.runOnce();  // Send TxnOffsetCommitRequest again.

        assertTrue(addOffsetsResult.isCompleted());
        assertTrue(addOffsetsResult.isSuccessful());
    }

    @Test
    public void testSendOffsetWithGroupMetadataFailAsAutoDowngradeTxnCommitNotEnabled() {
        client.setNodeApiVersions(NodeApiVersions.create(ApiKeys.TXN_OFFSET_COMMIT.id, (short) 0, (short) 2));

        Map<TopicPartition, Errors> txnOffsetCommitResponse = new HashMap<>();
        txnOffsetCommitResponse.put(tp0, Errors.NONE);
        txnOffsetCommitResponse.put(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS);

        TransactionalRequestResult addOffsetsResult = prepareGroupMetadataCommit(
            () -> prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, txnOffsetCommitResponse));

        sender.runOnce();

        assertTrue(addOffsetsResult.isCompleted());
        assertFalse(addOffsetsResult.isSuccessful());
        assertInstanceOf(UnsupportedVersionException.class, addOffsetsResult.error());
        assertFatalError(UnsupportedVersionException.class);
    }

    private TransactionalRequestResult prepareGroupMetadataCommit(Runnable prepareTxnCommitResponse) {
        doInitTransactions();

        transactionManager.beginTransaction();
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        offsets.put(tp1, new OffsetAndMetadata(1));

        TransactionalRequestResult addOffsetsResult = transactionManager.sendOffsetsToTransaction(
            offsets, new ConsumerGroupMetadata(consumerGroupId, generationId, memberId, Optional.of(groupInstanceId)));
        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);

        sender.runOnce();  // send AddOffsetsToTxnResult

        assertFalse(addOffsetsResult.isCompleted());  // The request should complete only after the TxnOffsetCommit completes

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnCommitResponse.run();

        assertNull(transactionManager.coordinator(CoordinatorType.GROUP));
        sender.runOnce();  // try to send TxnOffsetCommitRequest, but find we don't have a group coordinator
        sender.runOnce();  // send find coordinator for group request
        assertNotNull(transactionManager.coordinator(CoordinatorType.GROUP));
        assertTrue(transactionManager.hasPendingOffsetCommits());
        return addOffsetsResult;
    }

    @Test
    public void testNoDrainWhenPartitionsPending() throws InterruptedException {
        doInitTransactions();
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        appendToAccumulator(tp0);
        transactionManager.maybeAddPartition(tp1);
        appendToAccumulator(tp1);

        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp1));

        Node node1 = new Node(0, "localhost", 1111);
        Node node2 = new Node(1, "localhost", 1112);
        Map<Integer, Node> nodesById = new HashMap<>();
        nodesById.put(node1.id(), node1);
        nodesById.put(node2.id(), node2);
        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp0, Optional.of(node1.id()), Optional.empty(), null, null, null);
        PartitionMetadata part2Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node2.id()), Optional.empty(), null, null, null);
        MetadataSnapshot metadataCache = new MetadataSnapshot(null, nodesById, Arrays.asList(part1Metadata, part2Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        nodes.add(node2);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(metadataCache, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
        assertTrue(drainedBatches.containsKey(node2.id()));
        assertTrue(drainedBatches.get(node2.id()).isEmpty());
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testAllowDrainInAbortableErrorState() throws InterruptedException {
        doInitTransactions();
        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp1);
        prepareAddPartitionsToTxn(tp1, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxn(tp0, Errors.TOPIC_AUTHORIZATION_FAILED);
        runUntil(transactionManager::hasAbortableError);
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));

        // Try to drain a message destined for tp1, it should get drained.
        Node node1 = new Node(1, "localhost", 1112);
        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp1, Optional.of(node1.id()), Optional.empty(), null, null, null);
        MetadataSnapshot metadataCache = new MetadataSnapshot(null, Collections.singletonMap(node1.id(), node1), singletonList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
        appendToAccumulator(tp1);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(metadataCache, Collections.singleton(node1),
                Integer.MAX_VALUE,
                time.milliseconds());

        // We should drain the appended record since we are in abortable state and the partition has already been
        // added to the transaction.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertEquals(1, drainedBatches.get(node1.id()).size());
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testRaiseErrorWhenNoPartitionsPendingOnDrain() throws InterruptedException {
        doInitTransactions();
        transactionManager.beginTransaction();
        // Don't execute transactionManager.maybeAddPartitionToTransaction(tp0). This should result in an error on drain.
        appendToAccumulator(tp0);
        Node node1 = new Node(0, "localhost", 1111);
        PartitionMetadata part1Metadata = new PartitionMetadata(Errors.NONE, tp0, Optional.of(node1.id()), Optional.empty(), null, null, null);
        MetadataSnapshot metadataCache = new MetadataSnapshot(null, Collections.singletonMap(node1.id(), node1), singletonList(part1Metadata), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());

        Set<Node> nodes = new HashSet<>();
        nodes.add(node1);
        Map<Integer, List<ProducerBatch>> drainedBatches = accumulator.drain(metadataCache, nodes, Integer.MAX_VALUE,
                time.milliseconds());

        // We shouldn't drain batches which haven't been added to the transaction yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());

        // Let's now add the partition, flush and try to drain again.
        transactionManager.maybeAddPartition(tp0);
        accumulator.beginFlush();

        drainedBatches = accumulator.drain(metadataCache, nodes, Integer.MAX_VALUE, time.milliseconds());

        // We still shouldn't drain batches because the partition call didn't complete yet.
        assertTrue(drainedBatches.containsKey(node1.id()));
        assertTrue(drainedBatches.get(node1.id()).isEmpty());
        assertTrue(accumulator.hasUndrained());

        // Now prepare response to complete the partition addition.
        // We should now be able to drain the request.
        prepareAddPartitionsToTxn(tp0, Errors.NONE);
        runUntil(() -> !accumulator.hasUndrained());
    }

    @Test
    public void resendFailedProduceRequestAfterAbortableError() throws Exception {
        doInitTransactions();
        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NOT_LEADER_OR_FOLLOWER, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());

        assertFalse(responseFuture.isDone());

        transactionManager.transitionToAbortableError(new KafkaException());
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);
        assertNotNull(responseFuture.get()); // should throw the exception which caused the transaction to be aborted.
    }

    @Test
    public void testTransitionToAbortableErrorOnBatchExpiry() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.backoff(clusterNode, 100);

        runUntil(responseFuture::isDone);

        // make sure the produce was expired.
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, responseFuture::get).getCause(),
            "Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testTransitionToAbortableErrorOnMultipleBatchExpiry() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        transactionManager.maybeAddPartition(tp1);

        Future<RecordMetadata> firstBatchResponse = appendToAccumulator(tp0);
        Future<RecordMetadata> secondBatchResponse = appendToAccumulator(tp1);

        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        Map<TopicPartition, Errors> partitionErrors = new HashMap<>();
        partitionErrors.put(tp0, Errors.NONE);
        partitionErrors.put(tp1, Errors.NONE);
        prepareAddPartitionsToTxn(partitionErrors);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.transactionContainsPartition(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp1));
        assertFalse(firstBatchResponse.isDone());
        assertFalse(secondBatchResponse.isDone());

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.backoff(clusterNode, 100);

        runUntil(firstBatchResponse::isDone);
        runUntil(secondBatchResponse::isDone);

        // make sure the produce was expired.
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, firstBatchResponse::get).getCause(),
            "Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        // make sure the produce was expired.
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, secondBatchResponse::get).getCause(),
            "Expected to get a TimeoutException since the queued ProducerBatch should have been expired");

        assertTrue(transactionManager.hasAbortableError());
    }

    @Test
    public void testDropCommitOnBatchExpiry() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());

        runUntil(responseFuture::isDone);  // We should try to flush the produce, but expire it instead without sending anything.

        // make sure the produce was expired.
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, responseFuture::get).getCause(),
            "Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        runUntil(commitResult::isCompleted);  // the commit shouldn't be completed without being sent since the produce request failed.
        assertFalse(commitResult.isSuccessful());  // the commit shouldn't succeed since the produce request failed.
        assertInstanceOf(TimeoutException.class, assertThrows(TransactionAbortableException.class, commitResult::await).getCause());

        assertTrue(transactionManager.hasAbortableError());
        assertTrue(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.isCompleting());
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, (short) (epoch + 1));
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        assertFalse(transactionManager.hasOngoingTransaction());
        assertFalse(transactionManager.transactionContainsPartition(tp0));
    }

    @Test
    public void testTransitionToFatalErrorWhenRetriedBatchIsExpired() throws InterruptedException {
        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
                new ApiVersion()
                    .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                    .setMinVersion((short) 0)
                    .setMaxVersion((short) 1),
                new ApiVersion()
                    .setApiKey(ApiKeys.PRODUCE.id)
                    .setMinVersion((short) 0)
                    .setMaxVersion((short) 7)),
                Collections.emptyList(),
                Collections.emptyList(),
                0));

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);

        assertFalse(transactionManager.transactionContainsPartition(tp0));
        assertFalse(transactionManager.isSendToPartitionAllowed(tp0));
        // Check that only addPartitions was sent.
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));
        assertTrue(transactionManager.isSendToPartitionAllowed(tp0));

        prepareProduceResponse(Errors.NOT_LEADER_OR_FOLLOWER, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(responseFuture.isDone());

        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.backoff(clusterNode, 100);

        runUntil(responseFuture::isDone);  // We should try to flush the produce, but expire it instead without sending anything.

        // make sure the produce was expired.
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, responseFuture::get).getCause(),
            "Expected to get a TimeoutException since the queued ProducerBatch should have been expired");
        runUntil(commitResult::isCompleted);
        assertFalse(commitResult.isSuccessful());  // the commit should have been dropped.

        assertTrue(transactionManager.hasFatalError());
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBumpEpochAfterTimeoutWithoutPendingInflightRequests(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        long producerId = 15L;
        short epoch = 5;
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        // Nothing to resolve, so no reset is needed
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());

        TopicPartition tp0 = new TopicPartition("foo", 0);
        assertEquals(Integer.valueOf(0), transactionManager.sequenceNumber(tp0));

        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        assertEquals(Integer.valueOf(1), transactionManager.sequenceNumber(tp0));
        transactionManager.handleCompletedBatch(b1, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        // Marking sequence numbers unresolved without inflight requests is basically a no-op.
        transactionManager.markSequenceUnresolved(b1);
        transactionManager.maybeResolveSequences();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertFalse(transactionManager.hasUnresolvedSequences());

        // We have a new batch which fails with a timeout
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        assertEquals(Integer.valueOf(2), transactionManager.sequenceNumber(tp0));
        transactionManager.markSequenceUnresolved(b2);
        transactionManager.handleFailedBatch(b2, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // We only had one inflight batch, so we should be able to clear the unresolved status
        // and bump the epoch
        transactionManager.maybeResolveSequences();
        assertFalse(transactionManager.hasUnresolvedSequences());

        // Run sender loop to trigger epoch bump
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == 6);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNoProducerIdResetAfterLastInFlightBatchSucceeds(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        long producerId = 15L;
        short epoch = 5;
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        TopicPartition tp0 = new TopicPartition("foo", 0);
        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        assertEquals(3, transactionManager.sequenceNumber(tp0));

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(b1);
        transactionManager.handleFailedBatch(b1, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The reset should not occur until sequence numbers have been resolved
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The second batch fails as well with a timeout
        transactionManager.handleFailedBatch(b2, new TimeoutException(), false);
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The third batch succeeds, which should resolve the sequence number without
        // requiring a producerId reset.
        transactionManager.handleCompletedBatch(b3, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        transactionManager.maybeResolveSequences();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertFalse(transactionManager.hasUnresolvedSequences());
        assertEquals(3, transactionManager.sequenceNumber(tp0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEpochBumpAfterLastInFlightBatchFailsIdempotentProducer(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, epoch);
        initializeIdempotentProducerId(producerId, epoch);

        TopicPartition tp0 = new TopicPartition("foo", 0);
        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        ProducerBatch b3 = writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        assertEquals(Integer.valueOf(3), transactionManager.sequenceNumber(tp0));

        // The first batch fails with a timeout
        transactionManager.markSequenceUnresolved(b1);
        transactionManager.handleFailedBatch(b1, new TimeoutException(), false);
        assertTrue(transactionManager.hasUnresolvedSequences());

        // The second batch succeeds, but sequence numbers are still not resolved
        transactionManager.handleCompletedBatch(b2, new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, time.milliseconds(), 0L));
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
        assertTrue(transactionManager.hasUnresolvedSequences());

        // When the last inflight batch fails, we have to bump the epoch
        transactionManager.handleFailedBatch(b3, new TimeoutException(), false);

        // Run sender loop to trigger epoch bump
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == 2);
        assertFalse(transactionManager.hasUnresolvedSequences());
        assertEquals(0, transactionManager.sequenceNumber(tp0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaybeResolveSequencesTransactionalProducer(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.of(transactionalId), transactionV2Enabled);

        // Initialize transaction with initial producer ID and epoch.
        doInitTransactions(producerId, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        ProducerBatch b1 = writeTransactionalBatchWithValue(transactionManager, tp0, "1");
        assertEquals(Integer.valueOf(1), transactionManager.sequenceNumber(tp0));

        transactionManager.markSequenceUnresolved(b1);
        assertTrue(transactionManager.hasUnresolvedSequences());

        transactionManager.handleFailedBatch(b1, new TimeoutException(), false);
        // Call maybeResolveSequences to trigger resolution logic
        transactionManager.maybeResolveSequences();

        // Verify the type of error state the transaction is in.
        if (transactionManager.isTransactionV2Enabled() || transactionManager.needToTriggerEpochBumpFromClient()) {
            // Expected to throw an abortable error when epoch bumping is allowed
            assertTrue(transactionManager.hasAbortableError());
        } else {
            // Expected to throw a fatal error when epoch bumping is not allowed
            assertTrue(transactionManager.hasFatalError());
        }
    }

    @Test
    public void testEpochUpdateAfterBumpFromEndTxnResponseInV2() throws InterruptedException {
        initializeTransactionManager(Optional.of(transactionalId), true);

        // Initialize transaction with initial producer ID and epoch.
        doInitTransactions(producerId, epoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        // Append record with initial producer ID and epoch.
        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);

        final short bumpedEpoch = epoch + 1;

        // Trigger an EndTxn request by completing the transaction.
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch, producerId, bumpedEpoch, false);
        runUntil(abortResult::isCompleted);

        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(bumpedEpoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testProducerIdAndEpochUpdateAfterOverflowFromEndTxnResponseInV2() throws InterruptedException {
        initializeTransactionManager(Optional.of(transactionalId), true);

        // Initialize transaction with initial producer ID and epoch.
        doInitTransactions(producerId, epoch);

        transactionManager.beginTransaction();
        // Append record with initial producer ID and epoch
        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        transactionManager.maybeAddPartition(tp0);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture::isDone);

        final long newProducerId = producerId + 1;

        // Trigger an EndTxn request by completing the transaction.
        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch, newProducerId, (short) 0, false);
        runUntil(commitResult::isCompleted);

        assertEquals(newProducerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals((short) 0, transactionManager.producerIdAndEpoch().epoch);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testNoFailedBatchHandlingWhenTxnManagerIsInFatalError(boolean transactionV2Enabled) {
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        long producerId = 15L;
        short epoch = 5;
        initializeIdempotentProducerId(producerId, epoch);

        TopicPartition tp0 = new TopicPartition("foo", 0);
        ProducerBatch b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        // Handling b1 should bump the epoch after OutOfOrderSequenceException
        transactionManager.handleFailedBatch(b1, new OutOfOrderSequenceException("out of sequence"), false);
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        ProducerIdAndEpoch idAndEpochAfterFirstBatch = new ProducerIdAndEpoch(producerId, (short) (epoch + 1));
        assertEquals(idAndEpochAfterFirstBatch, transactionManager.producerIdAndEpoch());

        transactionManager.transitionToFatalError(new KafkaException());

        // The second batch should not bump the epoch as txn manager is already in fatal error state
        ProducerBatch b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        transactionManager.handleFailedBatch(b2, new TimeoutException(), true);
        transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();
        assertEquals(idAndEpochAfterFirstBatch, transactionManager.producerIdAndEpoch());
    }

    @Test
    public void testAbortTransactionAndReuseSequenceNumberOnError() throws InterruptedException {
        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
                new ApiVersion()
                        .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                        .setMinVersion((short) 0)
                        .setMaxVersion((short) 1),
                new ApiVersion()
                        .setApiKey(ApiKeys.END_TXN.id)
                        .setMinVersion((short) 0)
                        .setMaxVersion((short) 4),
                new ApiVersion()
                        .setApiKey(ApiKeys.PRODUCE.id)
                        .setMinVersion((short) 0)
                        .setMaxVersion((short) 7)),
                Collections.emptyList(),
                Collections.emptyList(),
                0));

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture0 = appendToAccumulator(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));  // Send AddPartitionsRequest
        runUntil(responseFuture0::isDone);

        Future<RecordMetadata> responseFuture1 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture1::isDone);

        Future<RecordMetadata> responseFuture2 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.TOPIC_AUTHORIZATION_FAILED, producerId, epoch);
        runUntil(responseFuture2::isDone); // Receive abortable error

        assertTrue(transactionManager.hasAbortableError());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        abortResult.await();
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));  // Send AddPartitionsRequest

        assertEquals(2, transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testAbortTransactionAndResetSequenceNumberOnUnknownProducerId() throws InterruptedException {
        // Set the InitProducerId version such that bumping the epoch number is not supported. This will test the case
        // where the sequence number is reset on an UnknownProducerId error, allowing subsequent transactions to
        // append to the log successfully
        // Set the EndTxn version such that sequence is not reset on every end txn.
        apiVersions.update("0", new NodeApiVersions(Arrays.asList(
                new ApiVersion()
                    .setApiKey(ApiKeys.INIT_PRODUCER_ID.id)
                    .setMinVersion((short) 0)
                    .setMaxVersion((short) 1),
                new ApiVersion()
                    .setApiKey(ApiKeys.PRODUCE.id)
                    .setMinVersion((short) 0)
                    .setMaxVersion((short) 7),
                new ApiVersion()
                     .setApiKey(ApiKeys.END_TXN.id)
                     .setMinVersion((short) 0)
                     .setMaxVersion((short) 4)),
                Collections.emptyList(),
                Collections.emptyList(),
                0));

        doInitTransactions();

        transactionManager.beginTransaction();

        transactionManager.maybeAddPartition(tp1);
        Future<RecordMetadata> successPartitionResponseFuture = appendToAccumulator(tp1);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp1, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch, tp1);
        runUntil(successPartitionResponseFuture::isDone);
        assertTrue(transactionManager.transactionContainsPartition(tp1));

        transactionManager.maybeAddPartition(tp0);
        Future<RecordMetadata> responseFuture0 = appendToAccumulator(tp0);
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture0::isDone);
        assertTrue(transactionManager.transactionContainsPartition(tp0));

        Future<RecordMetadata> responseFuture1 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(responseFuture1::isDone);

        Future<RecordMetadata> responseFuture2 = appendToAccumulator(tp0);
        client.prepareResponse(produceRequestMatcher(producerId, epoch, tp0),
                produceResponse(tp0, 0, Errors.UNKNOWN_PRODUCER_ID, 0, 0));
        runUntil(responseFuture2::isDone);

        assertTrue(transactionManager.hasAbortableError());

        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, epoch);
        runUntil(abortResult::isCompleted);
        assertTrue(abortResult.isSuccessful());
        abortResult.await();
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        assertEquals(0, transactionManager.sequenceNumber(tp0));
        assertEquals(1, transactionManager.sequenceNumber(tp1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testBumpTransactionalEpochOnAbortableError(boolean transactionV2Enabled) throws InterruptedException {
        final short initialEpoch = 1;
        final short bumpedEpoch = initialEpoch + 1;

        doInitTransactions(producerId, initialEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        Future<RecordMetadata> responseFuture0 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture0::isDone);

        Future<RecordMetadata> responseFuture1 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture1::isDone);

        Future<RecordMetadata> responseFuture2 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.TOPIC_AUTHORIZATION_FAILED, producerId, initialEpoch);
        runUntil(responseFuture2::isDone);

        assertTrue(transactionManager.hasAbortableError());
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch);
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == bumpedEpoch);

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        abortResult.await();
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        assertEquals(0, transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testBumpTransactionalEpochOnUnknownProducerIdError() throws InterruptedException {
        final short initialEpoch = 1;
        final short bumpedEpoch = 2;

        doInitTransactions(producerId, initialEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        Future<RecordMetadata> responseFuture0 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture0::isDone);

        Future<RecordMetadata> responseFuture1 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture1::isDone);

        Future<RecordMetadata> responseFuture2 = appendToAccumulator(tp0);
        client.prepareResponse(produceRequestMatcher(producerId, initialEpoch, tp0),
                produceResponse(tp0, 0, Errors.UNKNOWN_PRODUCER_ID, 0, 0));
        runUntil(responseFuture2::isDone);

        assertTrue(transactionManager.hasAbortableError());
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch);
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == bumpedEpoch);

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        abortResult.await();
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        assertEquals(0, transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testBumpTransactionalEpochOnTimeout() throws InterruptedException {
        final short initialEpoch = 1;
        final short bumpedEpoch = 2;

        doInitTransactions(producerId, initialEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        Future<RecordMetadata> responseFuture0 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture0::isDone);

        Future<RecordMetadata> responseFuture1 = appendToAccumulator(tp0);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture1::isDone);

        Future<RecordMetadata> responseFuture2 = appendToAccumulator(tp0);
        runUntil(client::hasInFlightRequests); // Send Produce Request

        // Sleep 10 seconds to make sure that the batches in the queue would be expired if they can't be drained.
        time.sleep(10000);
        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        Node clusterNode = metadata.fetch().nodes().get(0);
        client.disconnect(clusterNode.idString());
        client.backoff(clusterNode, 100);

        runUntil(responseFuture2::isDone); // We should try to flush the produce, but expire it instead without sending anything.

        assertTrue(transactionManager.hasAbortableError());
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        sender.runOnce();  // handle the abort
        time.sleep(110);  // Sleep to make sure the node backoff period has passed

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch);
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == bumpedEpoch);

        assertTrue(abortResult.isCompleted());
        assertTrue(abortResult.isSuccessful());
        abortResult.await();
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, bumpedEpoch, producerId);
        runUntil(() -> transactionManager.transactionContainsPartition(tp0));

        assertEquals(0, transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testBumpTransactionalEpochOnRecoverableAddPartitionRequestError() {
        final short initialEpoch = 1;
        final short bumpedEpoch = 2;

        doInitTransactions(producerId, initialEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        prepareAddPartitionsToTxnResponse(Errors.UNKNOWN_PRODUCER_ID, tp0, initialEpoch, producerId);
        runUntil(transactionManager::hasAbortableError);
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch);
        runUntil(abortResult::isCompleted);
        assertEquals(bumpedEpoch, transactionManager.producerIdAndEpoch().epoch);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @Test
    public void testBumpTransactionalEpochOnRecoverableAddOffsetsRequestError() throws InterruptedException {
        final short initialEpoch = 1;
        final short bumpedEpoch = 2;

        doInitTransactions(producerId, initialEpoch);

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, initialEpoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, initialEpoch);
        runUntil(responseFuture::isDone);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(tp0, new OffsetAndMetadata(1));
        transactionManager.sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
        assertFalse(transactionManager.hasPendingOffsetCommits());
        prepareAddOffsetsToTxnResponse(Errors.UNKNOWN_PRODUCER_ID, consumerGroupId, producerId, initialEpoch);
        runUntil(transactionManager::hasAbortableError);  // Send AddOffsetsRequest
        TransactionalRequestResult abortResult = transactionManager.beginAbort();

        prepareEndTxnResponse(Errors.NONE, TransactionResult.ABORT, producerId, initialEpoch);
        prepareInitPidResponse(Errors.NONE, false, producerId, bumpedEpoch);
        runUntil(abortResult::isCompleted);
        assertEquals(bumpedEpoch, transactionManager.producerIdAndEpoch().epoch);
        assertTrue(abortResult.isSuccessful());
        assertTrue(transactionManager.isReady());  // make sure we are ready for a transaction now.
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testHealthyPartitionRetriesDuringEpochBump(boolean transactionV2Enabled) throws InterruptedException {
        // Use a custom Sender to allow multiple inflight requests
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        Sender sender = new Sender(logContext, this.client, this.metadata, this.accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(new Metrics(time)), this.time,
                REQUEST_TIMEOUT, 50, transactionManager);
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        ProducerBatch tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "4");
        ProducerBatch tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "5");
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp1));

        // First batch of each partition succeeds
        long b1AppendTime = time.milliseconds();
        ProduceResponse.PartitionResponse t0b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp0b1.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp0b1, t0b1Response);

        ProduceResponse.PartitionResponse t1b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b1.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b1, t1b1Response);

        // We bump the epoch and set sequence numbers back to 0
        ProduceResponse.PartitionResponse t0b2Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 500L);
        assertTrue(transactionManager.canRetry(t0b2Response, tp0b2));

        // Run sender loop to trigger epoch bump
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == 2);

        // tp0 batches should have had sequence and epoch rewritten, but tp1 batches should not
        assertEquals(tp0b2, transactionManager.nextBatchBySequence(tp0));
        assertEquals(0, transactionManager.firstInFlightSequence(tp0));
        assertEquals(0, tp0b2.baseSequence());
        assertTrue(tp0b2.sequenceHasBeenReset());
        assertEquals(2, tp0b2.producerEpoch());

        assertEquals(tp1b2, transactionManager.nextBatchBySequence(tp1));
        assertEquals(1, transactionManager.firstInFlightSequence(tp1));
        assertEquals(1, tp1b2.baseSequence());
        assertFalse(tp1b2.sequenceHasBeenReset());
        assertEquals(1, tp1b2.producerEpoch());

        // New tp1 batches should not be drained from the accumulator while tp1 has in-flight requests using the old epoch
        appendToAccumulator(tp1);
        sender.runOnce();
        assertEquals(1, accumulator.getDeque(tp1).size());

        // Partition failover occurs and tp1 returns a NOT_LEADER_OR_FOLLOWER error
        // Despite having the old epoch, the batch should retry
        ProduceResponse.PartitionResponse t1b2Response = new ProduceResponse.PartitionResponse(
                Errors.NOT_LEADER_OR_FOLLOWER, -1, -1, 600L);
        assertTrue(transactionManager.canRetry(t1b2Response, tp1b2));
        accumulator.reenqueue(tp1b2, time.milliseconds());

        // The batch with the old epoch should be successfully drained, leaving the new one in the queue
        sender.runOnce();
        assertEquals(1, accumulator.getDeque(tp1).size());
        assertNotEquals(tp1b2, accumulator.getDeque(tp1).peek());
        assertEquals(epoch, tp1b2.producerEpoch());

        // After successfully retrying, there should be no in-flight batches for tp1 and the sequence should be 0
        t1b2Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b2.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b2, t1b2Response);

        transactionManager.maybeUpdateProducerIdAndEpoch(tp1);
        assertFalse(transactionManager.hasInflightBatches(tp1));
        assertEquals(0, transactionManager.sequenceNumber(tp1));

        // The last batch should now be drained and sent
        runUntil(() -> transactionManager.hasInflightBatches(tp1));
        assertTrue(accumulator.getDeque(tp1).isEmpty());
        ProducerBatch tp1b3 = transactionManager.nextBatchBySequence(tp1);
        assertEquals(epoch + 1, tp1b3.producerEpoch());

        ProduceResponse.PartitionResponse t1b3Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b3.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b3, t1b3Response);

        transactionManager.maybeUpdateProducerIdAndEpoch(tp1);
        assertFalse(transactionManager.hasInflightBatches(tp1));
        assertEquals(1, transactionManager.sequenceNumber(tp1));
    }

    @Test
    public void testRetryAbortTransaction() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.ABORT, TransactionResult.ABORT);
    }

    @Test
    public void testRetryCommitTransaction() throws InterruptedException {
        verifyCommitOrAbortTransactionRetriable(TransactionResult.COMMIT, TransactionResult.COMMIT);
    }

    @Test
    public void testRetryAbortTransactionAfterCommitTimeout() {
        assertThrows(IllegalStateException.class, () -> verifyCommitOrAbortTransactionRetriable(TransactionResult.COMMIT, TransactionResult.ABORT));
    }

    @Test
    public void testRetryCommitTransactionAfterAbortTimeout() {
        assertThrows(IllegalStateException.class, () -> verifyCommitOrAbortTransactionRetriable(TransactionResult.ABORT, TransactionResult.COMMIT));
    }

    @Test
    public void testNeedToTriggerEpochBumpFromClientDuringCoordinatorDisconnect() {
        doInitTransactions(0, (short) 0);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertTrue(transactionManager.needToTriggerEpochBumpFromClient());

        apiVersions.remove(transactionManager.coordinator(CoordinatorType.TRANSACTION).idString());
        assertTrue(transactionManager.needToTriggerEpochBumpFromClient());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFailedInflightBatchAfterEpochBump(boolean transactionV2Enabled) throws InterruptedException {
        // Use a custom Sender to allow multiple inflight requests
        initializeTransactionManager(Optional.empty(), transactionV2Enabled);
        Sender sender = new Sender(logContext, this.client, this.metadata, this.accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL, MAX_RETRIES, new SenderMetricsRegistry(new Metrics(time)), this.time,
                REQUEST_TIMEOUT, 50, transactionManager);
        initializeIdempotentProducerId(producerId, epoch);

        ProducerBatch tp0b1 = writeIdempotentBatchWithValue(transactionManager, tp0, "1");
        ProducerBatch tp0b2 = writeIdempotentBatchWithValue(transactionManager, tp0, "2");
        writeIdempotentBatchWithValue(transactionManager, tp0, "3");
        ProducerBatch tp1b1 = writeIdempotentBatchWithValue(transactionManager, tp1, "4");
        ProducerBatch tp1b2 = writeIdempotentBatchWithValue(transactionManager, tp1, "5");
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp1));

        // First batch of each partition succeeds
        long b1AppendTime = time.milliseconds();
        ProduceResponse.PartitionResponse t0b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp0b1.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp0b1, t0b1Response);

        ProduceResponse.PartitionResponse t1b1Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b1.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b1, t1b1Response);

        // We bump the epoch and set sequence numbers back to 0
        ProduceResponse.PartitionResponse t0b2Response = new ProduceResponse.PartitionResponse(
                Errors.UNKNOWN_PRODUCER_ID, -1, -1, 500L);
        assertTrue(transactionManager.canRetry(t0b2Response, tp0b2));

        // Run sender loop to trigger epoch bump
        runUntil(() -> transactionManager.producerIdAndEpoch().epoch == 2);

        // tp0 batches should have had sequence and epoch rewritten, but tp1 batches should not
        assertEquals(tp0b2, transactionManager.nextBatchBySequence(tp0));
        assertEquals(0, transactionManager.firstInFlightSequence(tp0));
        assertEquals(0, tp0b2.baseSequence());
        assertTrue(tp0b2.sequenceHasBeenReset());
        assertEquals(2, tp0b2.producerEpoch());

        assertEquals(tp1b2, transactionManager.nextBatchBySequence(tp1));
        assertEquals(1, transactionManager.firstInFlightSequence(tp1));
        assertEquals(1, tp1b2.baseSequence());
        assertFalse(tp1b2.sequenceHasBeenReset());
        assertEquals(1, tp1b2.producerEpoch());

        // New tp1 batches should not be drained from the accumulator while tp1 has in-flight requests using the old epoch
        appendToAccumulator(tp1);
        sender.runOnce();
        assertEquals(1, accumulator.getDeque(tp1).size());

        // Partition failover occurs and tp1 returns a NOT_LEADER_OR_FOLLOWER error
        // Despite having the old epoch, the batch should retry
        ProduceResponse.PartitionResponse t1b2Response = new ProduceResponse.PartitionResponse(
                Errors.NOT_LEADER_OR_FOLLOWER, -1, -1, 600L);
        assertTrue(transactionManager.canRetry(t1b2Response, tp1b2));
        accumulator.reenqueue(tp1b2, time.milliseconds());

        // The batch with the old epoch should be successfully drained, leaving the new one in the queue
        sender.runOnce();
        assertEquals(1, accumulator.getDeque(tp1).size());
        assertNotEquals(tp1b2, accumulator.getDeque(tp1).peek());
        assertEquals(epoch, tp1b2.producerEpoch());

        // After successfully retrying, there should be no in-flight batches for tp1 and the sequence should be 0
        t1b2Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b2.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b2, t1b2Response);

        transactionManager.maybeUpdateProducerIdAndEpoch(tp1);
        assertFalse(transactionManager.hasInflightBatches(tp1));
        assertEquals(0, transactionManager.sequenceNumber(tp1));

        // The last batch should now be drained and sent
        runUntil(() -> transactionManager.hasInflightBatches(tp1));
        assertTrue(accumulator.getDeque(tp1).isEmpty());
        ProducerBatch tp1b3 = transactionManager.nextBatchBySequence(tp1);
        assertEquals(epoch + 1, tp1b3.producerEpoch());

        ProduceResponse.PartitionResponse t1b3Response = new ProduceResponse.PartitionResponse(
                Errors.NONE, 500L, b1AppendTime, 0L);
        tp1b3.complete(500L, b1AppendTime);
        transactionManager.handleCompletedBatch(tp1b3, t1b3Response);

        assertFalse(transactionManager.hasInflightBatches(tp1));
        assertEquals(1, transactionManager.sequenceNumber(tp1));
    }

    @Test
    public void testBackgroundInvalidStateTransitionIsFatal() {
        doInitTransactions();
        assertTrue(transactionManager.isTransactional());

        transactionManager.setShouldPoisonStateOnInvalidTransitionOverride(true);

        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will result in a poisoning of the transaction manager for all subsequent transactional operations since
        // it was performed in the background.
        assertThrows(IllegalStateException.class, () -> transactionManager.handleFailedBatch(batchWithValue(tp0, "test"), new KafkaException(), false));
        assertTrue(transactionManager.hasFatalError());

        // Validate that all of these operations will fail after the invalid state transition attempt above.
        assertThrows(IllegalStateException.class, () -> transactionManager.beginTransaction());
        assertThrows(IllegalStateException.class, () -> transactionManager.beginAbort());
        assertThrows(IllegalStateException.class, () -> transactionManager.beginCommit());
        assertThrows(IllegalStateException.class, () -> transactionManager.maybeAddPartition(tp0));
        assertThrows(IllegalStateException.class, () -> transactionManager.initializeTransactions(false));
        assertThrows(IllegalStateException.class, () -> transactionManager.sendOffsetsToTransaction(Collections.emptyMap(), new ConsumerGroupMetadata("fake-group-id")));
    }

    @Test
    public void testForegroundInvalidStateTransitionIsRecoverable() {
        // Intentionally perform an operation that will cause an invalid state transition. The detection of this
        // will not poison the transaction manager since it was performed in the foreground.
        assertThrows(IllegalStateException.class, () -> transactionManager.beginAbort());
        assertFalse(transactionManager.hasFatalError());

        // Validate that the transactions can still run after the invalid state transition attempt above.
        doInitTransactions();
        assertTrue(transactionManager.isTransactional());

        transactionManager.beginTransaction();
        assertFalse(transactionManager.hasFatalError());

        transactionManager.maybeAddPartition(tp1);
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareAddPartitionsToTxn(tp1, Errors.NONE);
        runUntil(() -> transactionManager.transactionContainsPartition(tp1));

        TransactionalRequestResult retryResult = transactionManager.beginCommit();
        assertTrue(transactionManager.hasOngoingTransaction());

        prepareEndTxnResponse(Errors.NONE, TransactionResult.COMMIT, producerId, epoch);
        runUntil(() -> !transactionManager.hasOngoingTransaction());
        runUntil(retryResult::isCompleted);
        retryResult.await();
        runUntil(retryResult::isAcked);
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testTransactionAbortableExceptionInInitProducerId() {
        TransactionalRequestResult initPidResult = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.TRANSACTION_ABORTABLE, false, producerId, RecordBatch.NO_PRODUCER_EPOCH);
        runUntil(transactionManager::hasError);
        assertTrue(initPidResult.isCompleted());
        assertFalse(initPidResult.isSuccessful());
        assertThrows(TransactionAbortableException.class, initPidResult::await);
        assertAbortableError(TransactionAbortableException.class);
    }

    @Test
    public void testTransactionAbortableExceptionInAddPartitions() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp);

        prepareAddPartitionsToTxn(tp, Errors.TRANSACTION_ABORTABLE);
        runUntil(transactionManager::hasError);
        assertInstanceOf(TransactionAbortableException.class, transactionManager.lastError());

        assertAbortableError(TransactionAbortableException.class);
    }

    @Test
    public void testTransactionAbortableExceptionInFindCoordinator() {
        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(new TopicPartition("foo", 0), new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        runUntil(() -> !transactionManager.hasPartitionsToAdd());

        prepareFindCoordinatorResponse(Errors.TRANSACTION_ABORTABLE, false, CoordinatorType.GROUP, consumerGroupId);
        runUntil(transactionManager::hasError);
        assertInstanceOf(TransactionAbortableException.class, transactionManager.lastError());

        runUntil(sendOffsetsResult::isCompleted);
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(TransactionAbortableException.class, sendOffsetsResult.error());

        assertAbortableError(TransactionAbortableException.class);
    }

    @Test
    public void testTransactionAbortableExceptionInEndTxn() throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        TransactionalRequestResult commitResult = transactionManager.beginCommit();

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);

        assertFalse(responseFuture.isDone());
        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        prepareEndTxnResponse(Errors.TRANSACTION_ABORTABLE, TransactionResult.COMMIT, producerId, epoch);

        runUntil(commitResult::isCompleted);
        runUntil(responseFuture::isDone);

        assertThrows(KafkaException.class, commitResult::await);
        assertFalse(commitResult.isSuccessful());
        assertTrue(commitResult.isAcked());

        assertAbortableError(TransactionAbortableException.class);
    }

    @Test
    public void testTransactionAbortableExceptionInAddOffsetsToTxn() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.TRANSACTION_ABORTABLE, consumerGroupId, producerId, epoch);
        runUntil(transactionManager::hasError);
        assertInstanceOf(TransactionAbortableException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(TransactionAbortableException.class, sendOffsetsResult.error());

        assertAbortableError(TransactionAbortableException.class);
    }

    @Test
    public void testTransactionAbortableExceptionInTxnOffsetCommit() {
        final TopicPartition tp = new TopicPartition("foo", 0);

        doInitTransactions();

        transactionManager.beginTransaction();
        TransactionalRequestResult sendOffsetsResult = transactionManager.sendOffsetsToTransaction(
                singletonMap(tp, new OffsetAndMetadata(39L)), new ConsumerGroupMetadata(consumerGroupId));

        prepareAddOffsetsToTxnResponse(Errors.NONE, consumerGroupId, producerId, epoch);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.GROUP, consumerGroupId);
        prepareTxnOffsetCommitResponse(consumerGroupId, producerId, epoch, singletonMap(tp, Errors.TRANSACTION_ABORTABLE));
        runUntil(transactionManager::hasError);

        assertInstanceOf(TransactionAbortableException.class, transactionManager.lastError());
        assertTrue(sendOffsetsResult.isCompleted());
        assertFalse(sendOffsetsResult.isSuccessful());
        assertInstanceOf(TransactionAbortableException.class, sendOffsetsResult.error());
        assertAbortableError(TransactionAbortableException.class);
    }

    private FutureRecordMetadata appendToAccumulator(TopicPartition tp) throws InterruptedException {
        final long nowMs = time.milliseconds();
        return accumulator.append(tp.topic(), tp.partition(), nowMs, "key".getBytes(), "value".getBytes(), Record.EMPTY_HEADERS,
                null, MAX_BLOCK_TIMEOUT, nowMs, TestUtils.singletonCluster()).future;
    }

    private void verifyCommitOrAbortTransactionRetriable(TransactionResult firstTransactionResult,
                                                         TransactionResult retryTransactionResult) throws InterruptedException {
        doInitTransactions();

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);

        appendToAccumulator(tp0);

        prepareAddPartitionsToTxnResponse(Errors.NONE, tp0, epoch, producerId);
        prepareProduceResponse(Errors.NONE, producerId, epoch);
        runUntil(() -> !client.hasPendingResponses());

        TransactionalRequestResult result = firstTransactionResult == TransactionResult.COMMIT ?
                transactionManager.beginCommit() : transactionManager.beginAbort();
        prepareEndTxnResponse(Errors.NONE, firstTransactionResult, producerId, epoch, producerId, epoch, true);
        runUntil(() -> !client.hasPendingResponses());
        assertFalse(result.isCompleted());
        assertThrows(TimeoutException.class, () -> result.await(MAX_BLOCK_TIMEOUT, TimeUnit.MILLISECONDS));

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> !client.hasPendingResponses());
        TransactionalRequestResult retryResult = retryTransactionResult == TransactionResult.COMMIT ?
                transactionManager.beginCommit() : transactionManager.beginAbort();
        assertEquals(retryResult, result); // check if cached result is reused.

        prepareEndTxnResponse(Errors.NONE, retryTransactionResult, producerId, epoch);
        runUntil(retryResult::isCompleted);
        assertFalse(transactionManager.hasOngoingTransaction());
    }

    @Test
    public void testInitializeTransactionsWithKeepPreparedTxn() {
        doInitTransactionsWith2PCEnabled(true);
        runUntil(transactionManager::hasProducerId);

        // Expect a bumped epoch in the response.
        assertTrue(transactionManager.hasProducerId());
        assertFalse(transactionManager.hasOngoingTransaction());
        assertEquals(ongoingProducerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(bumpedOngoingEpoch, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testPrepareTransaction() {
        doInitTransactionsWith2PCEnabled(false);
        runUntil(transactionManager::hasProducerId);

        // Begin a transaction
        transactionManager.beginTransaction();
        assertTrue(transactionManager.hasOngoingTransaction());

        // Add a partition to the transaction
        transactionManager.maybeAddPartition(tp0);

        // Capture the current producer ID and epoch before preparing the response
        long producerId = transactionManager.producerIdAndEpoch().producerId;
        short epoch = transactionManager.producerIdAndEpoch().epoch;

        // Simulate a produce request
        try {
            // Prepare the response before sending to ensure it's ready
            prepareProduceResponse(Errors.NONE, producerId, epoch);

            appendToAccumulator(tp0);
            // Wait until the request is processed
            runUntil(() -> !client.hasPendingResponses());
        } catch (InterruptedException e) {
            fail("Unexpected interruption: " + e);
        }

        transactionManager.prepareTransaction();
        assertTrue(transactionManager.isPrepared());

        ProducerIdAndEpoch preparedState = transactionManager.preparedTransactionState();
        // Validate the state contains the correct producer ID and epoch
        assertEquals(producerId, preparedState.producerId);
        assertEquals(epoch, preparedState.epoch);
    }

    @Test
    public void testInitPidResponseWithKeepPreparedTrueAndOngoingTransaction() {
        // Initialize transaction manager with 2PC enabled
        initializeTransactionManager(Optional.of(transactionalId), true, true);
        
        // Start initializeTransactions with keepPreparedTxn=true
        TransactionalRequestResult result = transactionManager.initializeTransactions(true);
        
        // Prepare coordinator response
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        
        // Simulate InitProducerId response with ongoing transaction
        long ongoingPid = 12345L;
        short ongoingEpoch = 5;
        prepareInitPidResponse(
            Errors.NONE,
            false,
            producerId,
            epoch,
            true,
            true,
            ongoingPid,
            ongoingEpoch
        );
        
        runUntil(transactionManager::hasProducerId);
        transactionManager.maybeUpdateTransactionV2Enabled(true);
        
        result.await();
        assertTrue(result.isSuccessful());
        
        // Verify transaction manager transitioned to PREPARED_TRANSACTION state
        assertTrue(transactionManager.isPrepared());
        
        // Verify preparedTxnState was set with ongoing producer ID and epoch
        ProducerIdAndEpoch preparedState = transactionManager.preparedTransactionState();
        assertNotNull(preparedState);
        assertEquals(ongoingPid, preparedState.producerId);
        assertEquals(ongoingEpoch, preparedState.epoch);
    }

    @Test
    public void testInitPidResponseWithKeepPreparedTrueAndNoOngoingTransaction() {
        // Initialize transaction manager without 2PC enabled
        // keepPrepared can be true even when enable2Pc is false, and we expect the same behavior
        initializeTransactionManager(Optional.of(transactionalId), true, false);
        
        // Start initializeTransactions with keepPreparedTxn=true
        TransactionalRequestResult result = transactionManager.initializeTransactions(true);
        
        // Prepare coordinator response
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        
        // Simulate InitProducerId response without ongoing transaction
        prepareInitPidResponse(
            Errors.NONE,
            false,
            producerId,
            epoch,
            true,
            false,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH
        );
        
        runUntil(transactionManager::hasProducerId);
        transactionManager.maybeUpdateTransactionV2Enabled(true);
        
        result.await();
        assertTrue(result.isSuccessful());
        
        // Verify transaction manager transitioned to READY state (not PREPARED_TRANSACTION)
        assertFalse(transactionManager.isPrepared());
        assertTrue(transactionManager.isReady());
        
        // Verify preparedTxnState was not set or is empty
        ProducerIdAndEpoch preparedState = transactionManager.preparedTransactionState();
        assertEquals(ProducerIdAndEpoch.NONE, preparedState);
    }

    private void prepareAddPartitionsToTxn(final Map<TopicPartition, Errors> errors) {
        AddPartitionsToTxnResult result = AddPartitionsToTxnResponse.resultForTransaction(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID, errors);
        AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData().setResultsByTopicV3AndBelow(result.topicResults()).setThrottleTimeMs(0);
        client.prepareResponse(body -> {
            AddPartitionsToTxnRequest request = (AddPartitionsToTxnRequest) body;
            assertEquals(new HashSet<>(getPartitionsFromV3Request(request)), new HashSet<>(errors.keySet()));
            return true;
        }, new AddPartitionsToTxnResponse(data));
    }

    private void prepareAddPartitionsToTxn(final TopicPartition tp, final Errors error) {
        prepareAddPartitionsToTxn(Collections.singletonMap(tp, error));
    }

    private void prepareFindCoordinatorResponse(Errors error, boolean shouldDisconnect,
                                                final CoordinatorType coordinatorType,
                                                final String coordinatorKey) {
        client.prepareResponse(body -> {
            FindCoordinatorRequest findCoordinatorRequest = (FindCoordinatorRequest) body;
            assertEquals(coordinatorType, CoordinatorType.forId(findCoordinatorRequest.data().keyType()));
            String key = findCoordinatorRequest.data().coordinatorKeys().isEmpty()
                    ? findCoordinatorRequest.data().key()
                    : findCoordinatorRequest.data().coordinatorKeys().get(0);
            assertEquals(coordinatorKey, key);
            return true;
        }, FindCoordinatorResponse.prepareResponse(error, coordinatorKey, brokerNode), shouldDisconnect);
    }

    private void prepareInitPidResponse(
        Errors error,
        boolean shouldDisconnect,
        long producerId,
        short producerEpoch
    ) {
        prepareInitPidResponse(error, shouldDisconnect, producerId, producerEpoch, false, false, -1, (short) -1);
    }

    private void prepareInitPidResponse(
        Errors error,
        boolean shouldDisconnect,
        long producerId,
        short producerEpoch,
        boolean keepPreparedTxn,
        boolean enable2Pc,
        long ongoingProducerId,
        short ongoingProducerEpoch
    ) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
            .setErrorCode(error.code())
            .setProducerEpoch(producerEpoch)
            .setProducerId(producerId)
            .setThrottleTimeMs(0)
            .setOngoingTxnProducerId(ongoingProducerId)
            .setOngoingTxnProducerEpoch(ongoingProducerEpoch);

        client.prepareResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertEquals(transactionalId, initProducerIdRequest.data().transactionalId());
            assertEquals(transactionTimeoutMs, initProducerIdRequest.data().transactionTimeoutMs());
            assertEquals(keepPreparedTxn, initProducerIdRequest.data().keepPreparedTxn());
            assertEquals(enable2Pc, initProducerIdRequest.data().enable2Pc());
            return true;
        }, new InitProducerIdResponse(responseData), shouldDisconnect);
    }

    private void sendProduceResponse(Errors error, final long producerId, final short producerEpoch) {
        sendProduceResponse(error, producerId, producerEpoch, tp0);
    }

    private void sendProduceResponse(Errors error, final long producerId, final short producerEpoch, TopicPartition tp) {
        client.respond(produceRequestMatcher(producerId, producerEpoch, tp), produceResponse(tp, 0, error, 0));
    }

    private void prepareProduceResponse(Errors error, final long producerId, final short producerEpoch) {
        prepareProduceResponse(error, producerId, producerEpoch, tp0);
    }

    private void prepareProduceResponse(Errors error, final long producerId, final short producerEpoch, TopicPartition tp) {
        client.prepareResponse(produceRequestMatcher(producerId, producerEpoch, tp), produceResponse(tp, 0, error, 0));
    }

    private MockClient.RequestMatcher produceRequestMatcher(final long producerId, final short epoch, TopicPartition tp) {
        return body -> {
            ProduceRequest produceRequest = (ProduceRequest) body;
            MemoryRecords records = produceRequest.data().topicData()
                    .stream()
                    .filter(t -> t.name().equals(tp.topic()))
                    .findAny()
                    .get()
                    .partitionData()
                    .stream()
                    .filter(p -> p.index() == tp.partition())
                    .map(p -> (MemoryRecords) p.records())
                    .findAny().get();
            assertNotNull(records);
            Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
            assertTrue(batchIterator.hasNext());
            MutableRecordBatch batch = batchIterator.next();
            assertFalse(batchIterator.hasNext());
            assertTrue(batch.isTransactional());
            assertEquals(producerId, batch.producerId());
            assertEquals(epoch, batch.producerEpoch());
            assertEquals(transactionalId, produceRequest.transactionalId());
            return true;
        };
    }

    private void prepareAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                   final short epoch, final long producerId) {
        AddPartitionsToTxnResult result = AddPartitionsToTxnResponse.resultForTransaction(
                AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID, singletonMap(topicPartition, error));
        client.prepareResponse(addPartitionsRequestMatcher(topicPartition, epoch, producerId),
                new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                        .setThrottleTimeMs(0)
                        .setResultsByTopicV3AndBelow(result.topicResults())));
    }

    private void sendAddPartitionsToTxnResponse(Errors error, final TopicPartition topicPartition,
                                                final short epoch, final long producerId) {
        AddPartitionsToTxnResult result = AddPartitionsToTxnResponse.resultForTransaction(
                AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID, singletonMap(topicPartition, error));
        client.respond(addPartitionsRequestMatcher(topicPartition, epoch, producerId),
                new AddPartitionsToTxnResponse(new AddPartitionsToTxnResponseData()
                        .setThrottleTimeMs(0)
                        .setResultsByTopicV3AndBelow(result.topicResults())));
    }

    private MockClient.RequestMatcher addPartitionsRequestMatcher(final TopicPartition topicPartition,
                                                                  final short epoch, final long producerId) {
        return body -> {
            AddPartitionsToTxnRequest addPartitionsToTxnRequest = (AddPartitionsToTxnRequest) body;
            assertEquals(producerId, addPartitionsToTxnRequest.data().v3AndBelowProducerId());
            assertEquals(epoch, addPartitionsToTxnRequest.data().v3AndBelowProducerEpoch());
            assertEquals(singletonList(topicPartition), getPartitionsFromV3Request(addPartitionsToTxnRequest));
            assertEquals(transactionalId, addPartitionsToTxnRequest.data().v3AndBelowTransactionalId());
            return true;
        };
    }
    
    private List<TopicPartition> getPartitionsFromV3Request(AddPartitionsToTxnRequest request) {
        return AddPartitionsToTxnRequest.getPartitions(request.data().v3AndBelowTopics());
    }

    /**
     * Prepares an `EndTxnResponse` for a transactional producer.
     * This method can be used when Transaction V2 is NOT enabled, which
     * means the producer ID and producer epoch will NOT be a part of
     * the End Txn Response.
     * An error stating to use the right method and provide the expected producer ID and epoch
     * will be thrown if the End Txn API version is >= 5.
     *
     * @param error                 The error to be set in the response.
     * @param result                The transaction result.
     * @param requestProducerId     The producer ID in the request.
     * @param requestProducerEpoch  The producer epoch in the request.
     */
    private void prepareEndTxnResponse(
        Errors error,
        final TransactionResult result,
        final long requestProducerId,
        final short requestProducerEpoch
    ) {
        EndTxnResponseData responseData = new EndTxnResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(0);

        client.prepareResponse(body -> {
            EndTxnRequest endTxnRequest = (EndTxnRequest) body;

            assertEquals(transactionalId, endTxnRequest.data().transactionalId());
            assertEquals(requestProducerId, endTxnRequest.data().producerId());
            assertEquals(requestProducerEpoch, endTxnRequest.data().producerEpoch());
            assertEquals(result, endTxnRequest.result());

            int requestVersion = endTxnRequest.version();
            if (requestVersion >= 5) {
                fail("ExpectedProducerId and ExpectedEpochId must be provided when transaction V2 is enabled. Use the appropriate method.");
            }
            return true;
        }, new EndTxnResponse(responseData));
    }

    /**
     * Prepares an `EndTxnResponse` for a transactional producer.
     * This method should be used when Transaction V2 is enabled, which
     * means the producer ID and producer epoch will be a part of
     * the End Txn Response.
     *
     * @param error                 The error to be set in the response.
     * @param result                The transaction result.
     * @param requestProducerId     The producer ID in the request.
     * @param requestEpochId        The producer epoch in the request.
     * @param expectedProducerId    The expected producer ID to set in the response if the API version is >= 5.
     * @param expectedEpochId       The expected producer epoch to set in the response if the API version is >= 5.
     * @param shouldDisconnect      Whether to simulate a disconnection after sending the response.
     */
    private void prepareEndTxnResponse(
        Errors error,
        final TransactionResult result,
        final long requestProducerId,
        final short requestEpochId,
        final long expectedProducerId,
        final short expectedEpochId,
        boolean shouldDisconnect
    ) {
        EndTxnResponseData responseData = new EndTxnResponseData()
            .setErrorCode(error.code())
            .setThrottleTimeMs(0);

        client.prepareResponse(body -> {
            EndTxnRequest endTxnRequest = (EndTxnRequest) body;

            assertEquals(transactionalId, endTxnRequest.data().transactionalId());
            assertEquals(requestProducerId, endTxnRequest.data().producerId());
            assertEquals(requestEpochId, endTxnRequest.data().producerEpoch());
            assertEquals(result, endTxnRequest.result());

            int requestVersion = endTxnRequest.version(); // Extract request version
            if (requestVersion >= 5) {
                responseData.setProducerId(expectedProducerId);
                responseData.setProducerEpoch(expectedEpochId);
            }
            return true;
        }, new EndTxnResponse(responseData), shouldDisconnect);
    }

    private void sendEndTxnResponse(Errors error, final TransactionResult result, final long producerId, final short epoch) {
        client.respond(endTxnMatcher(result, producerId, epoch), new EndTxnResponse(
            new EndTxnResponseData()
                .setErrorCode(error.code())
                .setThrottleTimeMs(0)
        ));
    }

    private MockClient.RequestMatcher endTxnMatcher(final TransactionResult result, final long producerId, final short epoch) {
        return body -> {
            EndTxnRequest endTxnRequest = (EndTxnRequest) body;
            assertEquals(transactionalId, endTxnRequest.data().transactionalId());
            assertEquals(producerId, endTxnRequest.data().producerId());
            assertEquals(epoch, endTxnRequest.data().producerEpoch());
            assertEquals(result, endTxnRequest.result());
            return true;
        };
    }

    private void prepareAddOffsetsToTxnResponse(final Errors error,
                                                final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch) {
        client.prepareResponse(body -> {
            AddOffsetsToTxnRequest addOffsetsToTxnRequest = (AddOffsetsToTxnRequest) body;
            assertEquals(consumerGroupId, addOffsetsToTxnRequest.data().groupId());
            assertEquals(transactionalId, addOffsetsToTxnRequest.data().transactionalId());
            assertEquals(producerId, addOffsetsToTxnRequest.data().producerId());
            assertEquals(producerEpoch, addOffsetsToTxnRequest.data().producerEpoch());
            return true;
        }, new AddOffsetsToTxnResponse(
            new AddOffsetsToTxnResponseData()
                .setErrorCode(error.code()))
        );
    }

    private void prepareTxnOffsetCommitResponse(final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch,
                                                Map<TopicPartition, Errors> txnOffsetCommitResponse) {
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data().groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data().producerId());
            assertEquals(producerEpoch, txnOffsetCommitRequest.data().producerEpoch());
            return true;
        }, new TxnOffsetCommitResponse(0, txnOffsetCommitResponse));
    }

    private void prepareTxnOffsetCommitResponse(final String consumerGroupId,
                                                final long producerId,
                                                final short producerEpoch,
                                                final String groupInstanceId,
                                                final String memberId,
                                                final int generationId,
                                                Map<TopicPartition, Errors> txnOffsetCommitResponse) {
        client.prepareResponse(request -> {
            TxnOffsetCommitRequest txnOffsetCommitRequest = (TxnOffsetCommitRequest) request;
            assertEquals(consumerGroupId, txnOffsetCommitRequest.data().groupId());
            assertEquals(producerId, txnOffsetCommitRequest.data().producerId());
            assertEquals(producerEpoch, txnOffsetCommitRequest.data().producerEpoch());
            assertEquals(groupInstanceId, txnOffsetCommitRequest.data().groupInstanceId());
            assertEquals(memberId, txnOffsetCommitRequest.data().memberId());
            assertEquals(generationId, txnOffsetCommitRequest.data().generationId());
            return true;
        }, new TxnOffsetCommitResponse(0, txnOffsetCommitResponse));
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        return produceResponse(tp, offset, error, throttleTimeMs, 10);
    }

    @SuppressWarnings("deprecation")
    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs, int logStartOffset) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset, RecordBatch.NO_TIMESTAMP, logStartOffset);
        Map<TopicIdPartition, ProduceResponse.PartitionResponse> partResp = singletonMap(new TopicIdPartition(TOPIC_ID, tp), resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private void initializeIdempotentProducerId(long producerId, short epoch) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(Errors.NONE.code())
                .setProducerEpoch(epoch)
                .setProducerId(producerId)
                .setThrottleTimeMs(0);
        client.prepareResponse(body -> {
            InitProducerIdRequest initProducerIdRequest = (InitProducerIdRequest) body;
            assertNull(initProducerIdRequest.data().transactionalId());
            return true;
        }, new InitProducerIdResponse(responseData), false);

        runUntil(transactionManager::hasProducerId);
    }

    private void doInitTransactions() {
        doInitTransactions(producerId, epoch);
    }

    private void doInitTransactions(long producerId, short epoch) {
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        prepareInitPidResponse(Errors.NONE, false, producerId, epoch);
        runUntil(transactionManager::hasProducerId);
        transactionManager.maybeUpdateTransactionV2Enabled(true);

        result.await();
        assertTrue(result.isSuccessful());
        assertTrue(result.isAcked());
    }

    private void doInitTransactionsWith2PCEnabled(boolean keepPrepared) {
        initializeTransactionManager(Optional.of(transactionalId), true, true);
        TransactionalRequestResult result = transactionManager.initializeTransactions(keepPrepared);

        prepareFindCoordinatorResponse(Errors.NONE, false, CoordinatorType.TRANSACTION, transactionalId);
        runUntil(() -> transactionManager.coordinator(CoordinatorType.TRANSACTION) != null);
        assertEquals(brokerNode, transactionManager.coordinator(CoordinatorType.TRANSACTION));

        if (keepPrepared) {
            // Simulate an ongoing prepared transaction (ongoingProducerId != -1).
            short ongoingEpoch = bumpedOngoingEpoch - 1;
            prepareInitPidResponse(
                Errors.NONE,
                false,
                ongoingProducerId,
                bumpedOngoingEpoch,
                true,
                true,
                ongoingProducerId,
                ongoingEpoch
            );
        } else {
            prepareInitPidResponse(
                Errors.NONE,
                false,
                producerId,
                epoch,
                false,
                true,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH
            );
        }

        runUntil(transactionManager::hasProducerId);
        transactionManager.maybeUpdateTransactionV2Enabled(true);

        result.await();
        assertTrue(result.isSuccessful());
        assertTrue(result.isAcked());
    }

    private void assertAbortableError(Class<? extends RuntimeException> cause) {
        try {
            transactionManager.beginCommit();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        assertTrue(transactionManager.hasError());
        transactionManager.beginAbort();
        assertFalse(transactionManager.hasError());
    }

    private void assertFatalError(Class<? extends RuntimeException> cause) {
        assertTrue(transactionManager.hasError());

        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }

        // Transaction abort cannot clear fatal error state
        try {
            transactionManager.beginAbort();
            fail("Should have raised " + cause.getSimpleName());
        } catch (KafkaException e) {
            assertTrue(cause.isAssignableFrom(e.getCause().getClass()));
            assertTrue(transactionManager.hasError());
        }
    }

    private void assertProduceFutureFailed(Future<RecordMetadata> future) throws InterruptedException {
        assertTrue(future.isDone());

        try {
            future.get();
            fail("Expected produce future to throw");
        } catch (ExecutionException e) {
            // expected
        }
    }

    private void runUntil(Supplier<Boolean> condition) {
        ProducerTestUtils.runUntil(sender, condition);
    }

    /**
     * This subclass exists only to optionally change the default behavior related to poisoning the state
     * on invalid state transition attempts.
     */
    private static class TestableTransactionManager extends TransactionManager {

        private Optional<Boolean> shouldPoisonStateOnInvalidTransitionOverride;

        public TestableTransactionManager(LogContext logContext,
                                          String transactionalId,
                                          int transactionTimeoutMs,
                                          long retryBackoffMs,
                                          ApiVersions apiVersions,
                                          boolean enable2Pc) {
            super(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs, apiVersions, enable2Pc);
            this.shouldPoisonStateOnInvalidTransitionOverride = Optional.empty();
        }

        private void setShouldPoisonStateOnInvalidTransitionOverride(boolean override) {
            shouldPoisonStateOnInvalidTransitionOverride = Optional.of(override);
        }

        @Override
        protected boolean shouldPoisonStateOnInvalidTransition() {
            // If there's an override, use it, otherwise invoke the default (i.e. super class) logic.
            return shouldPoisonStateOnInvalidTransitionOverride.orElseGet(super::shouldPoisonStateOnInvalidTransition);
        }
    }
}

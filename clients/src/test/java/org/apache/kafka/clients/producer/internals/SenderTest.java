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
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.LeastLoadedNode;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.errors.TransactionAbortedException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.InOrder;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.clients.producer.internals.ProducerTestUtils.runUntil;
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
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SenderTest {
    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final String CLIENT_ID = "clientId";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 5000;
    private static final long RETRY_BACKOFF_MS = 50;
    private static final int DELIVERY_TIMEOUT_MS = 1500;
    private static final long TOPIC_IDLE_MS = 60 * 1000;

    private static final String TOPIC_NAME = "test";
    private static final Uuid TOPIC_ID = Uuid.fromString("MKXx1fIkQy2J9jXHhK8m1w");
    private static final Map<String, Uuid> TOPIC_IDS = Map.of(
            TOPIC_NAME, TOPIC_ID,
            "testSplitBatchAndSend", Uuid.fromString("2J9hK8m1wHMKjXfIkQyXx1")
    );
    private final TopicPartition tp0 = new TopicPartition(TOPIC_NAME, 0);
    private final TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 1);
    private final TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 2);
    private MockTime time = new MockTime();
    private final int batchSize = 16 * 1024;
    private final ProducerMetadata metadata = new ProducerMetadata(0, 0, Long.MAX_VALUE, TOPIC_IDLE_MS,
            new LogContext(), new ClusterResourceListeners(), time);
    private final ApiVersions apiVersions = new ApiVersions();
    private MockClient client = new MockClient(time, metadata);
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;
    private SenderMetricsRegistry senderMetricsRegistry = null;
    private final LogContext logContext = new LogContext();

    @BeforeEach
    public void setup() {
        setupWithTransactionState(null);
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()));
        this.client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1,
                        Collections.singletonMap(TOPIC_NAME, 3),
                        TOPIC_IDS));
    }

    @AfterEach
    public void tearDown() {
        this.metrics.close();
    }

    private static Map<TopicPartition, MemoryRecords> partitionRecords(ProduceRequest request) {
        Map<TopicPartition, MemoryRecords> partitionRecords = new HashMap<>();
        request.data().topicData().forEach(tpData -> tpData.partitionData().forEach(p -> {
            String topicName = tpData.name();

            if (request.version() >= 13 && tpData.topicId() != Uuid.ZERO_UUID) {
                topicName = TOPIC_IDS.entrySet().stream().filter(e -> e.getValue() == tpData.topicId()).map(Map.Entry::getKey).findFirst().get();
            }

            TopicPartition tp = new TopicPartition(topicName, p.index());
            partitionRecords.put(tp, (MemoryRecords) p.records());
        }));
        return Collections.unmodifiableMap(partitionRecords);
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = appendToAccumulator(tp0, 0L, "key", "value");
        sender.runOnce(); // connect
        sender.runOnce(); // send produce request
        assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");
        assertEquals(1, sender.inFlightBatches(tp0).size());
        assertTrue(client.hasInFlightRequests());
        client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
        sender.runOnce();
        assertEquals(0, client.inFlightRequestCount(), "All requests completed.");
        assertEquals(0, sender.inFlightBatches(tp0).size());
        assertFalse(client.hasInFlightRequests());
        sender.runOnce();
        assertTrue(future.isDone(), "Request should be completed");
        assertEquals(offset, future.get().offset());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() {
        MockSelector selector = new MockSelector(time);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(this.senderMetricsRegistry);
        Cluster cluster = TestUtils.singletonCluster(TOPIC_NAME, 1);
        Node node = cluster.nodes().get(0);
        NetworkClient client = new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                1000, 1000, 64 * 1024, 64 * 1024, 1000, 10 * 1000, 127 * 1000,
                time, true, new ApiVersions(), throttleTimeSensor, logContext,
                MetadataRecoveryStrategy.NONE);

        ApiVersionsResponse apiVersionsResponse = TestUtils.defaultApiVersionsResponse(
            400, ApiMessageType.ListenerType.BROKER);
        ByteBuffer buffer = RequestTestUtils.serializeResponseWithHeader(apiVersionsResponse, ApiKeys.API_VERSIONS.latestVersion(), 0);

        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
        while (!client.ready(node, time.milliseconds())) {
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
        }
        selector.clear();

        for (int i = 1; i <= 3; i++) {
            int throttleTimeMs = 100 * i;
            ProduceRequest.Builder builder = ProduceRequest.builder(new ProduceRequestData()
                    .setTopicData(new ProduceRequestData.TopicProduceDataCollection())
                    .setAcks((short) 1)
                    .setTimeoutMs(1000));
            ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
            client.send(request, time.milliseconds());
            client.poll(1, time.milliseconds());
            ProduceResponse response = produceResponse(tp0, i, Errors.NONE, throttleTimeMs);
            buffer = RequestTestUtils.serializeResponseWithHeader(response, ApiKeys.PRODUCE.latestVersion(), request.correlationId());
            selector.completeReceive(new NetworkReceive(node.idString(), buffer));
            client.poll(1, time.milliseconds());
            // If a throttled response is received, advance the time to ensure progress.
            time.sleep(client.throttleDelayMs(node, time.milliseconds()));
            selector.clear();
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeAvg);
        KafkaMetric maxMetric = allMetrics.get(this.senderMetricsRegistry.produceThrottleTimeMax);
        // Throttle times are ApiVersions=400, Produce=(100, 200, 300)
        assertEquals(250, (Double) avgMetric.metricValue(), EPS);
        assertEquals(400, (Double) maxMetric.metricValue(), EPS);
        client.close();
    }

    @Test
    public void testSenderMetricsTemplates() throws Exception {
        metrics.close();
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        metrics = new Metrics(new MetricConfig().tags(clientTags));
        SenderMetricsRegistry metricsRegistry = new SenderMetricsRegistry(metrics);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                1, metricsRegistry, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);

        // Append a message so that topic metrics are created
        appendToAccumulator(tp0, 0L, "key", "value");
        sender.runOnce(); // connect
        sender.runOnce(); // send produce request
        client.respond(produceResponse(tp0, 0, Errors.NONE, 0));
        sender.runOnce();
        // Create throttle time metrics
        Sender.throttleTimeSensor(metricsRegistry);

        // Verify that all metrics except metrics-count have registered templates
        Set<MetricNameTemplate> allMetrics = new HashSet<>();
        for (MetricName n : metrics.metrics().keySet()) {
            if (!n.group().equals("kafka-metrics-count"))
                allMetrics.add(new MetricNameTemplate(n.name(), n.group(), "", n.tags().keySet()));
        }
        TestUtils.checkEquals(allMetrics, new HashSet<>(metricsRegistry.allTemplates()), "metrics", "templates");
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);
            // do a successful retry
            Future<RecordMetadata> future = appendToAccumulator(tp0, 0L, "key", "value");
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request
            String id = client.requests().peek().destination();
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");
            client.disconnect(id);
            assertEquals(0, client.inFlightRequestCount());
            assertFalse(client.hasInFlightRequests());
            assertFalse(client.isReady(node, time.milliseconds()), "Client ready status should be false");
            // the batch is in accumulator.inFlightBatches until it expires
            assertEquals(1, sender.inFlightBatches(tp0).size());
            sender.runOnce(); // receive error
            sender.runOnce(); // reconnect
            sender.runOnce(); // resend
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp0).size());
            long offset = 0;
            client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
            sender.runOnce();
            assertTrue(future.isDone(), "Request should have retried and completed");
            assertEquals(offset, future.get().offset());
            assertEquals(0, sender.inFlightBatches(tp0).size());

            // do an unsuccessful retry
            future = appendToAccumulator(tp0, 0L, "key", "value");
            sender.runOnce(); // send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            for (int i = 0; i < maxRetries + 1; i++) {
                client.disconnect(client.requests().peek().destination());
                sender.runOnce(); // receive error
                assertEquals(0, sender.inFlightBatches(tp0).size());
                sender.runOnce(); // reconnect
                sender.runOnce(); // resend
                assertEquals(i > 0 ? 0 : 1, sender.inFlightBatches(tp0).size());
            }
            sender.runOnce();
            assertFutureFailure(future, NetworkException.class);
            assertEquals(0, sender.inFlightBatches(tp0).size());
        } finally {
            m.close();
        }
    }

    @Test
    public void testSendInOrder() throws Exception {
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        try {
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            MetadataResponse metadataUpdate1 = RequestTestUtils.metadataUpdateWithIds(2, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS);
            client.prepareMetadataUpdate(metadataUpdate1);

            // Send the first message.
            TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 1);
            appendToAccumulator(tp2, 0L, "key1", "value1");
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");
            assertEquals(1, sender.inFlightBatches(tp2).size());

            time.sleep(900);
            // Now send another message to tp2
            appendToAccumulator(tp2, 0L, "key2", "value2");

            // Update metadata before sender receives response from broker 0. Now partition 2 moves to broker 0
            MetadataResponse metadataUpdate2 = RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS);
            client.prepareMetadataUpdate(metadataUpdate2);
            // Sender should not send the second message to node 0.
            assertEquals(1, sender.inFlightBatches(tp2).size());
            sender.runOnce();  // receive the response for the previous send, and send the new batch
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.hasInFlightRequests());
            assertEquals(1, sender.inFlightBatches(tp2).size());
        } finally {
            m.close();
        }
    }

    @Test
    public void testAppendInExpiryCallback() throws InterruptedException {
        int messagesPerBatch = 10;
        final AtomicInteger expiryCallbackCount = new AtomicInteger(0);
        final AtomicReference<Exception> unexpectedException = new AtomicReference<>();
        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();
        final long maxBlockTimeMs = 1000;
        MetadataSnapshot metadataCache = TestUtils.metadataSnapshotWith(1);
        RecordAccumulator.AppendCallbacks callbacks = new RecordAccumulator.AppendCallbacks() {
            @Override
            public void setPartition(int partition) {
            }

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception instanceof TimeoutException) {
                    expiryCallbackCount.incrementAndGet();
                    try {
                        accumulator.append(tp1.topic(), tp1.partition(), 0L, key, value,
                            Record.EMPTY_HEADERS, null, maxBlockTimeMs, time.milliseconds(), metadataCache.cluster());
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Unexpected interruption", e);
                    }
                } else if (exception != null)
                    unexpectedException.compareAndSet(null, exception);
            }
        };

        final long nowMs = time.milliseconds();
        for (int i = 0; i < messagesPerBatch; i++)
            accumulator.append(tp1.topic(), tp1.partition(), 0L, key, value, null, callbacks, maxBlockTimeMs, nowMs, metadataCache.cluster());

        // Advance the clock to expire the first batch.
        time.sleep(10000);

        Node clusterNode = metadata.fetch().nodes().get(0);
        Map<Integer, List<ProducerBatch>> drainedBatches =
            accumulator.drain(metadataCache, Collections.singleton(clusterNode), Integer.MAX_VALUE, time.milliseconds());
        sender.addToInflightBatches(drainedBatches);

        // Disconnect the target node for the pending produce request. This will ensure that sender will try to
        // expire the batch.
        client.disconnect(clusterNode.idString());
        client.backoff(clusterNode, 100);

        sender.runOnce();  // We should try to flush the batch, but we expire it instead without sending anything.
        assertEquals(messagesPerBatch, expiryCallbackCount.get(), "Callbacks not invoked for expiry");
        assertNull(unexpectedException.get(), "Unexpected exception");
        // Make sure that the records were appended back to the batch.
        assertNotNull(accumulator.getDeque(tp1));
        assertEquals(1, accumulator.getDeque(tp1).size());
        assertEquals(messagesPerBatch, accumulator.getDeque(tp1).peekFirst().recordCount);
    }

    /**
     * Tests that topics are added to the metadata list when messages are available to send
     * and expired if not used during a metadata refresh interval.
     */
    @Test
    public void testMetadataTopicExpiry() throws Exception {
        long offset = 0;
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS));

        Future<RecordMetadata> future = appendToAccumulator(tp0);
        sender.runOnce();
        assertTrue(metadata.containsTopic(tp0.topic()), "Topic not added to metadata");
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS));
        sender.runOnce();  // send produce request
        client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
        sender.runOnce();
        assertEquals(0, client.inFlightRequestCount(), "Request completed.");
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        sender.runOnce();
        assertTrue(future.isDone(), "Request should be completed");

        assertTrue(metadata.containsTopic(tp0.topic()), "Topic not retained in metadata list");
        time.sleep(TOPIC_IDLE_MS);
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS));
        assertFalse(metadata.containsTopic(tp0.topic()), "Unused topic has not been expired");
        future = appendToAccumulator(tp0);
        sender.runOnce();
        assertTrue(metadata.containsTopic(tp0.topic()), "Topic not added to metadata");
        client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS));
        sender.runOnce();  // send produce request
        client.respond(produceResponse(tp0, offset + 1, Errors.NONE, 0));
        sender.runOnce();
        assertEquals(0, client.inFlightRequestCount(), "Request completed.");
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        sender.runOnce();
        assertTrue(future.isDone(), "Request should be completed");
    }

    @Test
    public void senderThreadShouldNotGetStuckWhenThrottledAndAddingPartitionsToTxn() {
        // We want MockClient#poll() to advance time so that eventually the backoff expires.
        try {
            client.advanceTimeDuringPoll(true);

            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
            TransactionManager txnManager = new TransactionManager(logContext, "testUnresolvedSeq", 60000, 100, apiVersions, false);

            setupWithTransactionState(txnManager);
            doInitTransactions(txnManager, producerIdAndEpoch);

            int throttleTimeMs = 1000;
            long startTime = time.milliseconds();
            Node nodeToThrottle = metadata.fetch().nodeById(0);
            client.throttle(nodeToThrottle, throttleTimeMs);

            // Verify node is throttled a little bit. In real-life Apache Kafka, we observe that this can happen
            // as done above by throttling or with a disconnect / backoff.
            long currentPollDelay = client.pollDelayMs(nodeToThrottle, startTime);
            assertEquals(currentPollDelay, throttleTimeMs);

            txnManager.beginTransaction();
            txnManager.maybeAddPartition(tp0);

            assertFalse(txnManager.hasInFlightRequest());
            sender.runOnce();
            assertTrue(txnManager.hasInFlightRequest());

            long totalTimeToRunOnce = time.milliseconds() - startTime;

            // It should have blocked roughly only the backoffTimeMs and some change.
            assertTrue(totalTimeToRunOnce < REQUEST_TIMEOUT);

        } finally {
            client.advanceTimeDuringPoll(false);
        }
    }

    @Test
    public void testNodeLatencyStats() throws Exception {
        try (Metrics m = new Metrics()) {
            // Create a new record accumulator with non-0 partitionAvailabilityTimeoutMs
            // otherwise it wouldn't update the stats.
            RecordAccumulator.PartitionerConfig config = new RecordAccumulator.PartitionerConfig(false, 42);
            long totalSize = 1024 * 1024;
            accumulator = new RecordAccumulator(logContext, batchSize, Compression.NONE, 0, 0L, 0L,
                DELIVERY_TIMEOUT_MS, config, m, "producer-metrics", time, null,
                new BufferPool(totalSize, batchSize, m, time, "producer-internal-metrics"));

            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
            apiVersions.update("0", NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()));

            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL, 1,
                senderMetrics, time, REQUEST_TIMEOUT, 1000L, null);

            // Produce and send batch.
            long time1 = time.milliseconds();
            appendToAccumulator(tp0, 0L, "key", "value");
            sender.runOnce();
            assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");

            // We were able to send the batch out, so both the ready and drain values should be the same.
            RecordAccumulator.NodeLatencyStats stats = accumulator.getNodeLatencyStats(0);
            assertEquals(time1, stats.drainTimeMs);
            assertEquals(time1, stats.readyTimeMs);

            // Make the node 1 not ready.
            client.throttle(metadata.fetch().nodeById(0), 100);

            // Time passes, but we don't have anything to send.
            time.sleep(10);
            sender.runOnce();
            assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");

            // Stats shouldn't change as we didn't have anything ready.
            assertEquals(time1, stats.drainTimeMs);
            assertEquals(time1, stats.readyTimeMs);

            // Produce a new batch, but we won't be able to send it because node is not ready.
            long time2 = time.milliseconds();
            appendToAccumulator(tp0, 0L, "key", "value");
            sender.runOnce();
            assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");

            // The ready time should move forward, but drain time shouldn't change.
            assertEquals(time1, stats.drainTimeMs);
            assertEquals(time2, stats.readyTimeMs);

            // Time passes, we keep trying to send, but the node is not ready.
            time.sleep(10);
            time2 = time.milliseconds();
            sender.runOnce();
            assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");

            // The ready time should move forward, but drain time shouldn't change.
            assertEquals(time1, stats.drainTimeMs);
            assertEquals(time2, stats.readyTimeMs);

            // Finally, time passes beyond the throttle and the node is ready.
            time.sleep(100);
            time2 = time.milliseconds();
            sender.runOnce();
            assertEquals(2, client.inFlightRequestCount(), "We should have 2 produce requests in flight.");

            // Both times should move forward
            assertEquals(time2, stats.drainTimeMs);
            assertEquals(time2, stats.readyTimeMs);
        }
    }

    @Test
    public void testInitProducerIdRequest() {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals((short) 0, transactionManager.producerIdAndEpoch().epoch);
    }

    /**
     * Verifies that InitProducerId of transactional producer succeeds even if metadata requests
     * are pending with only one bootstrap node available and maxInFlight=1, where multiple
     * polls are necessary to send requests.
     */
    @Test
    public void testInitProducerIdWithMaxInFlightOne() {
        final long producerId = 123456L;
        createMockClientWithMaxFlightOneMetadataPending();

        // Initialize transaction manager. InitProducerId will be queued up until metadata response
        // is processed and FindCoordinator can be sent to `leastLoadedNode`.
        TransactionManager transactionManager = new TransactionManager(new LogContext(), "testInitProducerIdWithPendingMetadataRequest",
                60000, 100L, new ApiVersions(), false);
        setupWithTransactionState(transactionManager, false, null, false);
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, (short) 0);
        transactionManager.initializeTransactions(false);
        sender.runOnce();

        // Process metadata response, prepare FindCoordinator and InitProducerId responses.
        // Verify producerId after the sender is run to process responses.
        MetadataResponse metadataUpdate = RequestTestUtils.metadataUpdateWithIds(1, Collections.emptyMap(), Collections.emptyMap());
        client.respond(metadataUpdate);
        prepareFindCoordinatorResponse(Errors.NONE, "testInitProducerIdWithPendingMetadataRequest");
        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        waitForProducerId(transactionManager, producerIdAndEpoch);
    }

    /**
     * Verifies that InitProducerId of idempotent producer succeeds even if metadata requests
     * are pending with only one bootstrap node available and maxInFlight=1, where multiple
     * polls are necessary to send requests.
     */
    @Test
    public void testIdempotentInitProducerIdWithMaxInFlightOne() {
        final long producerId = 123456L;
        createMockClientWithMaxFlightOneMetadataPending();

        // Initialize transaction manager. InitProducerId will be queued up until metadata response
        // is processed.
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager, false, null, false);
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, (short) 0);

        // Process metadata and InitProducerId responses.
        // Verify producerId after the sender is run to process responses.
        MetadataResponse metadataUpdate = RequestTestUtils.metadataUpdateWithIds(1, Collections.emptyMap(), Collections.emptyMap());
        client.respond(metadataUpdate);
        sender.runOnce();
        sender.runOnce();
        client.respond(initProducerIdResponse(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, Errors.NONE));
        waitForProducerId(transactionManager, producerIdAndEpoch);
    }

    /**
     * Tests the code path where the target node to send FindCoordinator or InitProducerId
     * is not ready.
     */
    @Test
    public void testNodeNotReady() {
        final long producerId = 123456L;
        time = new MockTime(10);
        client = new MockClient(time, metadata);

        TransactionManager transactionManager = new TransactionManager(new LogContext(), "testNodeNotReady",
                60000, 100L, new ApiVersions(), false);
        setupWithTransactionState(transactionManager, false, null, true);
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId, (short) 0);
        transactionManager.initializeTransactions(false);
        sender.runOnce();

        Node node = metadata.fetch().nodes().get(0);
        client.delayReady(node, REQUEST_TIMEOUT + 20);
        prepareFindCoordinatorResponse(Errors.NONE, "testNodeNotReady");
        sender.runOnce();
        sender.runOnce();
        assertNotNull(transactionManager.coordinator(CoordinatorType.TRANSACTION), "Coordinator not found");

        client.throttle(node, REQUEST_TIMEOUT + 20);
        prepareFindCoordinatorResponse(Errors.NONE, "Coordinator not found");
        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        waitForProducerId(transactionManager, producerIdAndEpoch);
    }

    @Test
    public void testClusterAuthorizationExceptionInInitProducerIdRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        // cluster authorization failed on initProducerId is retriable
        prepareAndReceiveInitProducerId(producerId, Errors.CLUSTER_AUTHORIZATION_FAILED);
        assertFalse(transactionManager.hasProducerId());
        assertTrue(transactionManager.hasError());
        assertInstanceOf(ClusterAuthorizationException.class, transactionManager.lastError());
        assertEquals(-1, transactionManager.producerIdAndEpoch().epoch);

        assertSendFailure(ClusterAuthorizationException.class);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        // sender retry initProducerId and succeed
        sender.runOnce();
        assertFalse(transactionManager.hasFatalError());
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, transactionManager.producerIdAndEpoch().epoch);
        // subsequent send should be successful
        assertSuccessfulSend();
    }

    @Test
    public void testCanRetryWithoutIdempotence() throws Exception {
        // do a successful retry
        Future<RecordMetadata> future = appendToAccumulator(tp0, 0L, "key", "value");
        sender.runOnce(); // connect
        sender.runOnce(); // send produce request
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue(client.hasInFlightRequests());
        assertEquals(1, sender.inFlightBatches(tp0).size());
        assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");
        assertFalse(future.isDone());

        client.respond(body -> {
            ProduceRequest request = (ProduceRequest) body;
            assertFalse(RequestTestUtils.hasIdempotentRecords(request));
            return true;
        }, produceResponse(tp0, -1L, Errors.TOPIC_AUTHORIZATION_FAILED, 0));
        sender.runOnce();
        assertTrue(future.isDone());
        assertInstanceOf(TopicAuthorizationException.class, assertThrows(Exception.class, future::get).getCause());
    }

    @Test
    public void testIdempotenceWithMultipleInflights() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);

        sender.runOnce(); // receive response 0

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.runOnce(); // receive response 1
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
    }


    @Test
    public void testIdempotenceWithMultipleInflightsRetriedInOrder() throws Exception {
        // Send multiple in flight requests, retry them all one at a time, in the correct order.
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();

         // Send third ProduceRequest
        Future<RecordMetadata> request3 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(3, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.LEADER_NOT_AVAILABLE, -1L);
        sender.runOnce(); // receive response 0

        // Queue the fourth request, it shouldn't be sent until the first 3 complete.
        Future<RecordMetadata> request4 = appendToAccumulator(tp0);

        assertEquals(2, client.inFlightRequestCount());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.runOnce(); // re send request 1, receive response 2

        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);
        sender.runOnce(); // receive response 3

        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(1, client.inFlightRequestCount());

        sender.runOnce(); // Do nothing, we are reduced to one in flight request during retries.

        assertEquals(3, transactionManager.sequenceNumber(tp0));  // the batch for request 4 shouldn't have been drained, and hence the sequence should not have been incremented.
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.runOnce();  // receive response 1
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.runOnce(); // send request 2;
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.runOnce();  // receive response 2
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());

        assertFalse(client.hasInFlightRequests());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.runOnce(); // send request 3
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(2, tp0, Errors.NONE, 2L);
        sender.runOnce();  // receive response 3, send request 4 since we are out of 'retry' mode.
        assertEquals(OptionalInt.of(2), transactionManager.lastAckedSequence(tp0));
        assertTrue(request3.isDone());
        assertEquals(2, request3.get().offset());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(3, tp0, Errors.NONE, 3L);
        sender.runOnce();  // receive response 4
        assertEquals(OptionalInt.of(3), transactionManager.lastAckedSequence(tp0));
        assertTrue(request4.isDone());
        assertEquals(3, request4.get().offset());
    }

    @Test
    public void testIdempotenceWithMultipleInflightsWhereFirstFailsFatallyAndSequenceOfFutureBatchesIsAdjusted() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        sendIdempotentProducerResponse(0, tp0, Errors.MESSAGE_TOO_LARGE, -1L);

        sender.runOnce(); // receive response 0, should adjust sequences of future batches.
        assertFutureFailure(request1, RecordTooLargeException.class);

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.runOnce(); // receive response 1

        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.runOnce(); // resend request 1

        assertEquals(1, client.inFlightRequestCount());

        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.runOnce();  // receive response 1
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        assertTrue(request1.isDone());
        assertEquals(0, request2.get().offset());
    }

    @Test
    public void testEpochBumpOnOutOfOrderSequenceForNextBatch() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest with multiple messages.
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // make sure the next sequence number accounts for multi-message batches.
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0);

        sender.runOnce();

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        // This OutOfOrderSequence triggers an epoch bump since it is returned for the batch succeeding the last acknowledged batch.
        sendIdempotentProducerResponse(2, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1L);

        sender.runOnce();
        sender.runOnce();

        // epoch should be bumped and sequence numbers reset
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(0, transactionManager.firstInFlightSequence(tp0));
    }

    @Test
    public void testEpochBumpOnOutOfOrderSequenceForNextBatchWhenThereIsNoBatchInFlight() throws Exception {
        // Verify that partitions without in-flight batches when the producer epoch
        // is bumped get their sequence number reset correctly.
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);

        // Init producer id/epoch
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(0, transactionManager.producerIdAndEpoch().epoch);

        // Partition 0 - Send first batch
        appendToAccumulator(tp0);
        sender.runOnce();

        // Partition 0 - State is lazily initialized
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 1, OptionalInt.empty());

        // Partition 0 - Successful response
        sendIdempotentProducerResponse(0, 0, tp0, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 0 - Last ack is updated
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 1, OptionalInt.of(0));

        // Partition 1 - Send first batch
        appendToAccumulator(tp1);
        sender.runOnce();

        // Partition 1 - State is lazily initialized
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 1, OptionalInt.empty());

        // Partition 1 - Successful response
        sendIdempotentProducerResponse(0, 0, tp1, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 1 - Last ack is updated
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 1, OptionalInt.of(0));

        // Partition 0 - Send second batch
        appendToAccumulator(tp0);
        sender.runOnce();

        // Partition 0 - Sequence is incremented
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 2, OptionalInt.of(0));

        // Partition 0 - Failed response with OUT_OF_ORDER_SEQUENCE_NUMBER
        sendIdempotentProducerResponse(0, 1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, -1);
        sender.runOnce(); // Receive
        sender.runOnce(); // Bump epoch & Retry

        // Producer epoch is bumped
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);

        // Partition 0 - State is reset to current producer epoch
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 1, OptionalInt.empty());

        // Partition 1 - State is not changed
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 1, OptionalInt.of(0));
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(1, 0, tp0, Errors.NONE, 1, -1);
        sender.runOnce();

        // Partition 0 - Last ack is updated
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 1, OptionalInt.of(0));

        // Partition 1 - Send second batch
        appendToAccumulator(tp1);
        sender.runOnce();

        // Partition 1 - Epoch is bumped, sequence is reset and incremented
        assertPartitionState(transactionManager, tp1, producerId, (short) 1, 1, OptionalInt.empty());
        assertFalse(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 1 - Successful Response
        sendIdempotentProducerResponse(1, 0, tp1, Errors.NONE, 1, -1);
        sender.runOnce();

        // Partition 1 - Last ack is updated
        assertPartitionState(transactionManager, tp1, producerId, (short) 1, 1, OptionalInt.of(0));
    }

    @Test
    public void testEpochBumpOnOutOfOrderSequenceForNextBatchWhenBatchInFlightFails() throws Exception {
        // When a batch failed after the producer epoch is bumped, the sequence number of
        // that partition must be reset for any subsequent batches sent.
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();

        // Retries once
        setupWithTransactionState(transactionManager, false, null, true, 1, 0);

        // Init producer id/epoch
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);
        assertEquals(0, transactionManager.producerIdAndEpoch().epoch);

        // Partition 0 - Send first batch
        appendToAccumulator(tp0);
        sender.runOnce();

        // Partition 0 - State is lazily initialized
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 1, OptionalInt.empty());

        // Partition 0 - Successful response
        sendIdempotentProducerResponse(0, 0, tp0, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 0 - Last ack is updated
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 1, OptionalInt.of(0));

        // Partition 1 - Send first batch
        appendToAccumulator(tp1);
        sender.runOnce();

        // Partition 1 - State is lazily initialized
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 1, OptionalInt.empty());

        // Partition 1 - Successful response
        sendIdempotentProducerResponse(0, 0, tp1, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 1 - Last ack is updated
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 1, OptionalInt.of(0));

        // Partition 0 - Send second batch
        appendToAccumulator(tp0);
        sender.runOnce();

        // Partition 0 - Sequence is incremented
        assertPartitionState(transactionManager, tp0, producerId, (short) 0, 2, OptionalInt.of(0));

        // Partition 1 - Send second batch
        appendToAccumulator(tp1);
        sender.runOnce();

        // Partition 1 - Sequence is incremented
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 2, OptionalInt.of(0));

        // Partition 0 - Failed response with OUT_OF_ORDER_SEQUENCE_NUMBER
        sendIdempotentProducerResponse(0, 1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1, -1);
        sender.runOnce(); // Receive
        sender.runOnce(); // Bump epoch & Retry

        // Producer epoch is bumped
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);

        // Partition 0 - State is reset to current producer epoch
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 1, OptionalInt.empty());

        // Partition 1 - State is not changed. The epoch will be lazily bumped when all in-flight
        // batches are completed
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 2, OptionalInt.of(0));
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 1 - Failed response with NOT_LEADER_OR_FOLLOWER
        sendIdempotentProducerResponse(0, 1, tp1, Errors.NOT_LEADER_OR_FOLLOWER, -1, -1);
        sender.runOnce(); // Receive & Retry

        // Partition 1 - State is not changed.
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 2, OptionalInt.of(0));
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(1, 0, tp0, Errors.NONE, 1, -1);
        sender.runOnce();

        // Partition 0 - Last ack is updated
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 1, OptionalInt.of(0));

        // Partition 1 - Failed response with NOT_LEADER_OR_FOLLOWER
        sendIdempotentProducerResponse(0, 1, tp1, Errors.NOT_LEADER_OR_FOLLOWER, -1, -1);
        sender.runOnce(); // Receive & Fail the batch (retries exhausted)

        // Partition 1 - State is not changed. It will be lazily updated when the next batch is sent.
        assertPartitionState(transactionManager, tp1, producerId, (short) 0, 2, OptionalInt.of(0));
        assertTrue(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 1 - Send third batch
        appendToAccumulator(tp1);
        sender.runOnce();

        // Partition 1 - Epoch is bumped, sequence is reset
        assertPartitionState(transactionManager, tp1, producerId, (short) 1, 1, OptionalInt.empty());
        assertFalse(transactionManager.hasStaleProducerIdAndEpoch(tp1));

        // Partition 1 - Successful Response
        sendIdempotentProducerResponse(1, 0, tp1, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 1 - Last ack is updated
        assertPartitionState(transactionManager, tp1, producerId, (short) 1, 1, OptionalInt.of(0));

        // Partition 0 - Send third batch
        appendToAccumulator(tp0);
        sender.runOnce();

        // Partition 0 - Sequence is incremented
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 2, OptionalInt.of(0));

        // Partition 0 - Successful Response
        sendIdempotentProducerResponse(1, 1, tp0, Errors.NONE, 0, -1);
        sender.runOnce();

        // Partition 0 - Last ack is updated
        assertPartitionState(transactionManager, tp0, producerId, (short) 1, 2, OptionalInt.of(1));
    }

    private void assertPartitionState(
        TransactionManager transactionManager,
        TopicPartition tp,
        long expectedProducerId,
        short expectedProducerEpoch,
        long expectedSequenceValue,
        OptionalInt expectedLastAckedSequence
    ) {
        assertEquals(expectedProducerId, transactionManager.producerIdAndEpoch(tp).producerId, "Producer Id:");
        assertEquals(expectedProducerEpoch, transactionManager.producerIdAndEpoch(tp).epoch, "Producer Epoch:");
        assertEquals(expectedSequenceValue, transactionManager.sequenceNumber(tp), "Seq Number:");
        assertEquals(expectedLastAckedSequence, transactionManager.lastAckedSequence(tp), "Last Acked Seq Number:");
    }

    @Test
    public void testCorrectHandlingOfOutOfOrderResponses() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, -1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1));

        sender.runOnce(); // receive response 1
        Deque<ProducerBatch> queuedBatches = accumulator.getDeque(tp0);

        // Make sure that we are queueing the second batch first.
        assertEquals(1, queuedBatches.size());
        assertEquals(1, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.NOT_LEADER_OR_FOLLOWER, -1));

        sender.runOnce(); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(2, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(1, queuedBatches.peekLast().baseSequence());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());

        sender.runOnce(); // send request 0
        assertEquals(1, client.inFlightRequestCount());
        sender.runOnce(); // don't do anything, only one inflight allowed once we are retrying.

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Make sure that the requests are sent in order, even though the previous responses were not in order.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.runOnce();  // receive response 0
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());

        sender.runOnce(); // send request 1
        assertEquals(1, client.inFlightRequestCount());
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1L);
        sender.runOnce();  // receive response 1

        assertFalse(client.hasInFlightRequests());
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
    }

    @Test
    public void testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, client.inFlightRequestCount());
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1, Errors.NONE, 1));

        sender.runOnce(); // receive response 1
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        assertFalse(request1.isDone());
        Deque<ProducerBatch> queuedBatches = accumulator.getDeque(tp0);

        assertEquals(0, queuedBatches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, -1, Errors.REQUEST_TIMED_OUT, -1));

        sender.runOnce(); // receive response 0

        // Make sure we requeued both batches in the correct order.
        assertEquals(1, queuedBatches.size());
        assertEquals(0, queuedBatches.peekFirst().baseSequence());
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        sender.runOnce(); // resend request 0
        assertEquals(1, client.inFlightRequestCount());

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));

        // Make sure we handle the out of order successful responses correctly.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 0L);
        sender.runOnce();  // receive response 0
        assertEquals(0, queuedBatches.size());
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(0, client.inFlightRequestCount());

        assertFalse(client.hasInFlightRequests());
        assertTrue(request1.isDone());
        assertEquals(0, request1.get().offset());
    }

    @Test
    public void testExpiryOfUnsentBatchesShouldNotCauseUnresolvedSequences() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0, 0L, "key", "value");
        Node node = metadata.fetch().nodes().get(0);
        time.sleep(10000L);
        client.disconnect(node.idString());
        client.backoff(node, 10);

        sender.runOnce();

        assertFutureFailure(request1, TimeoutException.class);
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testExpiryOfFirstBatchShouldNotCauseUnresolvedSequencesIfFutureBatchesSucceed() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager, false, null);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        // We separate the two appends by 1 second so that the two batches
        // don't expire at the same time.
        time.sleep(1000L);

        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, sender.inFlightBatches(tp0).size());

        sendIdempotentProducerResponse(0, tp0, Errors.REQUEST_TIMED_OUT, -1);
        sender.runOnce();  // receive first response
        assertEquals(1, sender.inFlightBatches(tp0).size());

        Node node = metadata.fetch().nodes().get(0);
        // We add 600 millis to expire the first batch but not the second.
        // Note deliveryTimeoutMs is 1500.
        time.sleep(600L);
        client.disconnect(node.idString());
        client.backoff(node, 10);

        sender.runOnce(); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        assertEquals(0, sender.inFlightBatches(tp0).size());

        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        Future<RecordMetadata> request3 = appendToAccumulator(tp0);
        time.sleep(20);
        assertFalse(request2.isDone());

        sender.runOnce();  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1);
        assertEquals(1, sender.inFlightBatches(tp0).size());

        sender.runOnce(); // receive second response, the third request shouldn't be sent since we are in an unresolved state.
        assertTrue(request2.isDone());
        assertEquals(1, request2.get().offset());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        Deque<ProducerBatch> batches = accumulator.getDeque(tp0);
        assertEquals(1, batches.size());
        assertFalse(batches.peekFirst().hasSequence());
        assertFalse(client.hasInFlightRequests());
        assertEquals(2L, transactionManager.sequenceNumber(tp0));
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));

        sender.runOnce();  // clear the unresolved state, send the pending request.
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, batches.size());
        assertEquals(1, client.inFlightRequestCount());
        assertFalse(request3.isDone());
        assertEquals(1, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testExpiryOfFirstBatchShouldCauseEpochBumpIfFutureBatchesFail() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());
        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request

        time.sleep(1000L);
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();  // send request

        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1);
        sender.runOnce();  // receive first response

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(1000L);
        client.disconnect(node.idString());
        client.backoff(node, 10);

        sender.runOnce(); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        // let's enqueue another batch, which should not be dequeued until the unresolved state is clear.
        appendToAccumulator(tp0);

        time.sleep(20);
        assertFalse(request2.isDone());
        sender.runOnce();  // send second request
        sendIdempotentProducerResponse(1, tp0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 1);
        sender.runOnce(); // receive second response, the third request shouldn't be sent since we are in an unresolved state.

        Deque<ProducerBatch> batches = accumulator.getDeque(tp0);

        // The epoch should be bumped and the second request should be requeued
        assertEquals(2, batches.size());

        sender.runOnce();
        assertEquals((short) 1, transactionManager.producerIdAndEpoch().epoch);
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testUnresolvedSequencesAreNotFatal() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
        TransactionManager txnManager = new TransactionManager(logContext, "testUnresolvedSeq", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request

        time.sleep(1000L);
        appendToAccumulator(tp0);
        sender.runOnce();  // send request

        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1);
        sender.runOnce();  // receive first response

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(1000L);
        client.disconnect(node.idString());
        client.backoff(node, 10);

        sender.runOnce(); // now expire the first batch.
        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(txnManager.hasUnresolvedSequence(tp0));

        // Loop once and confirm that the transaction manager does not enter a fatal error state
        sender.runOnce();
        assertTrue(txnManager.hasAbortableError());
    }

    @Test
    public void testExpiryOfAllSentBatchesShouldCauseUnresolvedSequences() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0, 0L, "key", "value");
        sender.runOnce();  // send request
        sendIdempotentProducerResponse(0, tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1);

        sender.runOnce();  // receive response
        assertEquals(1L, transactionManager.sequenceNumber(tp0));

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(15000L);
        client.disconnect(node.idString());
        client.backoff(node, 10);

        sender.runOnce(); // now expire the batch.

        assertFutureFailure(request1, TimeoutException.class);
        assertTrue(transactionManager.hasUnresolvedSequence(tp0));
        assertFalse(client.hasInFlightRequests());
        Deque<ProducerBatch> batches = accumulator.getDeque(tp0);
        assertEquals(0, batches.size());
        assertEquals(producerId, transactionManager.producerIdAndEpoch().producerId);

        // In the next run loop, we bump the epoch and clear the unresolved sequences
        sender.runOnce();
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);
        assertFalse(transactionManager.hasUnresolvedSequence(tp0));
    }

    @Test
    public void testResetOfProducerStateShouldAllowQueuedBatchesToDrain() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        appendToAccumulator(tp0); // failed response
        Future<RecordMetadata> successfulResponse = appendToAccumulator(tp1);
        sender.runOnce();  // connect and send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));

        sender.runOnce(); // trigger epoch bump
        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE); // also send request to tp1
        sender.runOnce(); // reset producer ID because epoch is maxed out
        assertEquals(producerId + 1, transactionManager.producerIdAndEpoch().producerId);

        assertFalse(successfulResponse.isDone());
        client.respond(produceResponse(tp1, 10, Errors.NONE, -1));
        sender.runOnce();

        assertTrue(successfulResponse.isDone());
        assertEquals(10, successfulResponse.get().offset());

        // The epoch and the sequence are updated when the next batch is sent.
        assertEquals(1, transactionManager.sequenceNumber(tp1));
    }

    @Test
    public void testCloseWithProducerIdReset() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, 10,
            senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        appendToAccumulator(tp0); // failed response
        appendToAccumulator(tp1); // success response
        sender.runOnce();  // connect and send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.initiateClose(); // initiate close
        sender.runOnce(); // out of order sequence error triggers producer ID reset because epoch is maxed out

        TestUtils.waitForCondition(() -> {
            prepareInitProducerResponse(Errors.NONE, producerId + 1, (short) 1);
            sender.runOnce();
            return !accumulator.hasUndrained();
        }, 5000, "Failed to drain batches");
    }

    @Test
    public void testForceCloseWithProducerIdReset() throws Exception {
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(1L, Short.MAX_VALUE, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, 10,
            senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        appendToAccumulator(tp0);
        Future<RecordMetadata> successfulResponse = appendToAccumulator(tp1);
        sender.runOnce();  // connect and send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.runOnce(); // out of order sequence error triggers producer ID reset because epoch is maxed out
        sender.forceClose(); // initiate force close
        sender.runOnce(); // this should not block
        sender.run(); // run main loop to test forceClose flag
        assertFalse(accumulator.hasUndrained(), "Pending batches are not aborted.");
        assertTrue(successfulResponse.isDone());
    }

    @Test
    public void testBatchesDrainedWithOldProducerIdShouldSucceedOnSubsequentRetry() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        Future<RecordMetadata> outOfOrderResponse = appendToAccumulator(tp0);
        Future<RecordMetadata> successfulResponse = appendToAccumulator(tp1);
        sender.runOnce();  // connect.
        sender.runOnce();  // send.

        assertEquals(1, client.inFlightRequestCount());

        Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
        responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
        responses.put(tp0, new OffsetAndError(-1, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER));
        client.respond(produceResponse(responses));
        sender.runOnce();
        assertFalse(outOfOrderResponse.isDone());

        sender.runOnce();  // bump epoch send request to tp1 with the old producerId
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);

        assertFalse(successfulResponse.isDone());
        // The response comes back with a retriable error.
        client.respond(produceResponse(tp1, 0, Errors.NOT_LEADER_OR_FOLLOWER, -1));
        sender.runOnce();

        // The response
        assertFalse(successfulResponse.isDone());
        sender.runOnce(); // retry one more time
        client.respond(produceResponse(tp1, 0, Errors.NONE, -1));
        sender.runOnce();
        assertTrue(successfulResponse.isDone());
        // epoch of partition is bumped and sequence is reset when the next batch is sent
        assertEquals(1, transactionManager.sequenceNumber(tp1));
    }

    @Test
    public void testCorrectHandlingOfDuplicateSequenceError() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();
        String nodeId = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(nodeId), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, client.inFlightRequestCount());
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request1.isDone());
        assertFalse(request2.isDone());
        assertTrue(client.isReady(node, time.milliseconds()));

        ClientRequest firstClientRequest = client.requests().peek();
        ClientRequest secondClientRequest = (ClientRequest) client.requests().toArray()[1];

        client.respondToRequest(secondClientRequest, produceResponse(tp0, 1000, Errors.NONE, 0));

        sender.runOnce(); // receive response 1

        assertEquals(OptionalLong.of(1000), transactionManager.lastAckedOffset(tp0));
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));

        client.respondToRequest(firstClientRequest, produceResponse(tp0, ProduceResponse.INVALID_OFFSET, Errors.DUPLICATE_SEQUENCE_NUMBER, 0));

        sender.runOnce(); // receive response 0

        // Make sure that the last ack'd sequence doesn't change.
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000), transactionManager.lastAckedOffset(tp0));
        assertFalse(client.hasInFlightRequests());

        RecordMetadata unknownMetadata = request1.get();
        assertFalse(unknownMetadata.hasOffset());
        assertEquals(-1L, unknownMetadata.offset());
    }

    @Test
    public void testTransactionalUnknownProducerHandlingWhenRetentionLimitReached() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = new TransactionManager(logContext, "testUnresolvedSeq", 60000, 100, apiVersions, false);

        setupWithTransactionState(transactionManager);
        doInitTransactions(transactionManager, new ProducerIdAndEpoch(producerId, (short) 0));
        assertTrue(transactionManager.hasProducerId());

        transactionManager.beginTransaction();
        transactionManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce(); // Receive AddPartitions response

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.runOnce();  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000L), transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest, a single batch with 2 records.
        appendToAccumulator(tp0);
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.runOnce(); // receive response 0, should be retried since the logStartOffset > lastAckedOffset.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.runOnce(); // should retry request 1

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.runOnce(); // receive response 1
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1012L, request2.get().offset());
        assertEquals(OptionalLong.of(1012L), transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testIdempotentUnknownProducerHandlingWhenRetentionLimitReached() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.runOnce();  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000L), transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest, a single batch with 2 records.
        appendToAccumulator(tp0);
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.runOnce(); // receive response 0, should be retried since the logStartOffset > lastAckedOffset.
        sender.runOnce(); // bump epoch and retry request

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(request2.isDone());
        assertTrue(client.hasInFlightRequests());
        assertEquals((short) 1, transactionManager.producerIdAndEpoch().epoch);

        // resend the request. Note that the expected sequence is 0, since we have lost producer state on the broker.
        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.runOnce(); // receive response 1
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1012L, request2.get().offset());
        assertEquals(OptionalLong.of(1012L), transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testUnknownProducerErrorShouldBeRetriedWhenLogStartOffsetIsUnknown() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.runOnce();  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000L), transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, -1L);
        sender.runOnce(); // receive response 0, should be retried without resetting the sequence numbers since the log start offset is unknown.

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(request2.isDone());
        assertFalse(client.hasInFlightRequests());

        sender.runOnce(); // should retry request 1

        // resend the request. Note that the expected sequence is 1, since we never got the logStartOffset in the previous
        // response and hence we didn't reset the sequence numbers.
        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1011L, 1010L);
        sender.runOnce(); // receive response 1
        assertEquals(OptionalInt.of(1), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(client.hasInFlightRequests());
        assertTrue(request2.isDone());
        assertEquals(1011L, request2.get().offset());
        assertEquals(OptionalLong.of(1011L), transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testUnknownProducerErrorShouldBeRetriedForFutureBatchesWhenFirstFails() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.runOnce();  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000L), transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        // Send the third ProduceRequest, in parallel with the second. It should be retried even though the
        // lastAckedOffset > logStartOffset when its UnknownProducerResponse comes back.
        Future<RecordMetadata> request3 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(3, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(2, client.inFlightRequestCount());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 1010L);
        sender.runOnce(); // receive response 2, should reset the sequence numbers and be retried.
        sender.runOnce(); // bump epoch and retry request 2

        // We should have reset the sequence number state of the partition because the state was lost on the broker.
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertFalse(request2.isDone());
        assertFalse(request3.isDone());
        assertEquals(2, client.inFlightRequestCount());
        assertEquals((short) 1, transactionManager.producerIdAndEpoch().epoch);

        // receive the original response 3. note the expected sequence is still the originally assigned sequence.
        sendIdempotentProducerResponse(2, tp0, Errors.UNKNOWN_PRODUCER_ID, -1, 1010L);
        sender.runOnce(); // receive response 3

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertEquals(2, transactionManager.sequenceNumber(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1011L, 1010L);
        sender.runOnce();  // receive response 2, don't send request 3 since we can have at most 1 in flight when retrying

        assertTrue(request2.isDone());
        assertFalse(request3.isDone());
        assertFalse(client.hasInFlightRequests());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(1011L, request2.get().offset());
        assertEquals(OptionalLong.of(1011L), transactionManager.lastAckedOffset(tp0));

        sender.runOnce();  // resend request 3.
        assertEquals(1, client.inFlightRequestCount());

        sendIdempotentProducerResponse(1, tp0, Errors.NONE, 1012L, 1010L);
        sender.runOnce();  // receive response 3.

        assertFalse(client.hasInFlightRequests());
        assertTrue(request3.isDone());
        assertEquals(1012L, request3.get().offset());
        assertEquals(OptionalLong.of(1012L), transactionManager.lastAckedOffset(tp0));
    }

    @Test
    public void testShouldRaiseOutOfOrderSequenceExceptionToUserIfLogWasNotTruncated() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        assertEquals(0, transactionManager.sequenceNumber(tp0));

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));

        sendIdempotentProducerResponse(0, tp0, Errors.NONE, 1000L, 10L);

        sender.runOnce();  // receive the response.

        assertTrue(request1.isDone());
        assertEquals(1000L, request1.get().offset());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(OptionalLong.of(1000L), transactionManager.lastAckedOffset(tp0));

        // Send second ProduceRequest,
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);
        sender.runOnce();
        assertEquals(2, transactionManager.sequenceNumber(tp0));
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));

        assertFalse(request2.isDone());

        sendIdempotentProducerResponse(1, tp0, Errors.UNKNOWN_PRODUCER_ID, -1L, 10L);
        sender.runOnce(); // receive response 0, should request an epoch bump
        sender.runOnce(); // bump epoch
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);
        assertEquals(OptionalInt.empty(), transactionManager.lastAckedSequence(tp0));
        assertFalse(request2.isDone());
    }

    void sendIdempotentProducerResponse(int expectedSequence, TopicPartition tp, Errors responseError, long responseOffset) {
        sendIdempotentProducerResponse(expectedSequence, tp, responseError, responseOffset, -1L);
    }

    void sendIdempotentProducerResponse(int expectedSequence, TopicPartition tp, Errors responseError, long responseOffset, long logStartOffset) {
        sendIdempotentProducerResponse(-1, expectedSequence, tp, responseError, responseOffset, logStartOffset);
    }

    void sendIdempotentProducerResponse(
        int expectedEpoch,
        int expectedSequence,
        TopicPartition tp,
        Errors responseError,
        long responseOffset,
        long logStartOffset
    ) {
        client.respond(body -> {
            ProduceRequest produceRequest = (ProduceRequest) body;
            assertTrue(RequestTestUtils.hasIdempotentRecords(produceRequest));
            MemoryRecords records = partitionRecords(produceRequest).get(tp);
            Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
            RecordBatch firstBatch = batchIterator.next();
            assertFalse(batchIterator.hasNext());
            if (expectedEpoch > -1)
                assertEquals((short) expectedEpoch, firstBatch.producerEpoch());
            assertEquals(expectedSequence, firstBatch.baseSequence());
            return true;
        }, produceResponse(tp, responseOffset, responseError, 0, logStartOffset, null));
    }

    @Test
    public void testClusterAuthorizationExceptionInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future = appendToAccumulator(tp0);
        client.prepareResponse(
            body -> body instanceof ProduceRequest && RequestTestUtils.hasIdempotentRecords((ProduceRequest) body),
            produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.runOnce();
        assertFutureFailure(future, ClusterAuthorizationException.class);

        // cluster authorization errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError());
        assertSendFailure(ClusterAuthorizationException.class);
    }

    @Test
    public void testCancelInFlightRequestAfterFatalError() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        // cluster authorization is a fatal error for the producer
        Future<RecordMetadata> future1 = appendToAccumulator(tp0);
        sender.runOnce();

        Future<RecordMetadata> future2 = appendToAccumulator(tp1);
        sender.runOnce();

        client.respond(
            body -> body instanceof ProduceRequest && RequestTestUtils.hasIdempotentRecords((ProduceRequest) body),
            produceResponse(tp0, -1, Errors.CLUSTER_AUTHORIZATION_FAILED, 0));

        sender.runOnce();
        assertTrue(transactionManager.hasFatalError());
        assertFutureFailure(future1, ClusterAuthorizationException.class);

        sender.runOnce();
        assertFutureFailure(future2, ClusterAuthorizationException.class);

        // Should be fine if the second response eventually returns
        client.respond(
            body -> body instanceof ProduceRequest && RequestTestUtils.hasIdempotentRecords((ProduceRequest) body),
            produceResponse(tp1, 0, Errors.NONE, 0));
        sender.runOnce();
    }

    @Test
    public void testUnsupportedForMessageFormatInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = appendToAccumulator(tp0);
        client.prepareResponse(
            body -> body instanceof ProduceRequest && RequestTestUtils.hasIdempotentRecords((ProduceRequest) body),
            produceResponse(tp0, -1, Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, 0));

        sender.runOnce();
        assertFutureFailure(future, UnsupportedForMessageFormatException.class);

        // unsupported for message format is not a fatal error
        assertFalse(transactionManager.hasError());
    }

    @Test
    public void testUnsupportedVersionInProduceRequest() throws Exception {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);

        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        Future<RecordMetadata> future = appendToAccumulator(tp0);
        client.prepareUnsupportedVersionResponse(
            body -> body instanceof ProduceRequest && RequestTestUtils.hasIdempotentRecords((ProduceRequest) body));

        sender.runOnce();
        assertFutureFailure(future, UnsupportedVersionException.class);

        // unsupported version errors are fatal, so we should continue seeing it on future sends
        assertTrue(transactionManager.hasFatalError());
        assertSendFailure(UnsupportedVersionException.class);
    }

    @Test
    public void testSequenceNumberIncrement() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        client.prepareResponse(body -> {
            if (body instanceof ProduceRequest) {
                ProduceRequest request = (ProduceRequest) body;
                MemoryRecords records = partitionRecords(request).get(tp0);
                Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
                assertTrue(batchIterator.hasNext());
                RecordBatch batch = batchIterator.next();
                assertFalse(batchIterator.hasNext());
                assertEquals(0, batch.baseSequence());
                assertEquals(producerId, batch.producerId());
                assertEquals(0, batch.producerEpoch());
                return true;
            }
            return false;
        }, produceResponse(tp0, 0, Errors.NONE, 0));

        sender.runOnce();  // connect.
        sender.runOnce();  // send.

        sender.runOnce();  // receive response
        assertTrue(responseFuture.isDone());
        assertEquals(OptionalInt.of(0), transactionManager.lastAckedSequence(tp0));
        assertEquals(1L, transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testRetryWhenProducerIdChanges() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Short.MAX_VALUE, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        sender.runOnce();  // connect.
        sender.runOnce();  // send.
        String id = client.requests().peek().destination();
        Node node = new Node(Integer.parseInt(id), "localhost", 0);
        assertEquals(1, client.inFlightRequestCount());
        assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");
        client.disconnect(id);
        assertEquals(0, client.inFlightRequestCount());
        assertFalse(client.isReady(node, time.milliseconds()), "Client ready status should be false");
        sender.runOnce(); // receive error
        sender.runOnce(); // reset producer ID because epoch is maxed out

        prepareAndReceiveInitProducerId(producerId + 1, Errors.NONE);
        sender.runOnce(); // nothing to do, since the pid has changed. We should check the metrics for errors.
        assertEquals(1, client.inFlightRequestCount(), "Expected requests to be retried after pid change");

        assertFalse(responseFuture.isDone());
        assertEquals(1, (long) transactionManager.sequenceNumber(tp0));
    }

    @Test
    public void testBumpEpochWhenOutOfOrderSequenceReceived() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertTrue(transactionManager.hasProducerId());

        int maxRetries = 10;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

        Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        Future<RecordMetadata> responseFuture = appendToAccumulator(tp0);
        sender.runOnce();  // connect.
        sender.runOnce();  // send.

        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        client.respond(produceResponse(tp0, 0, Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, 0));

        sender.runOnce(); // receive the out of order sequence error
        sender.runOnce(); // bump the epoch
        assertFalse(responseFuture.isDone());
        assertEquals(1, sender.inFlightBatches(tp0).size());
        assertEquals(1, transactionManager.producerIdAndEpoch().epoch);
    }

    @Test
    public void testIdempotentSplitBatchAndSend() throws Exception {
        TopicIdPartition tpId = new TopicIdPartition(
                TOPIC_IDS.getOrDefault("testSplitBatchAndSend", Uuid.ZERO_UUID),
                new TopicPartition("testSplitBatchAndSend", 1));
        TransactionManager txnManager = createTransactionManager();
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        setupWithTransactionState(txnManager);
        prepareAndReceiveInitProducerId(123456L, Errors.NONE);
        assertTrue(txnManager.hasProducerId());
        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tpId);
    }

    @Test
    public void testTransactionalSplitBatchAndSend() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        TopicIdPartition tpId = new TopicIdPartition(
                TOPIC_IDS.getOrDefault("testSplitBatchAndSend", Uuid.ZERO_UUID),
                new TopicPartition("testSplitBatchAndSend", 1));

        TransactionManager txnManager = new TransactionManager(logContext, "testSplitBatchAndSend", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tpId.topicPartition());
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()));
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tpId.topicPartition(), Errors.NONE)));
        sender.runOnce();

        testSplitBatchAndSend(txnManager, producerIdAndEpoch, tpId);
    }

    @SuppressWarnings("deprecation")
    private void testSplitBatchAndSend(TransactionManager txnManager,
                                       ProducerIdAndEpoch producerIdAndEpoch,
                                       TopicIdPartition tpId) throws Exception {
        int maxRetries = 1;
        String topic = tpId.topic();
        int deliveryTimeoutMs = 3000;
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";
        // Set a good compression ratio.
        CompressionRatioEstimator.setEstimation(topic, CompressionType.GZIP, 0.2f);
        try (Metrics m = new Metrics()) {
            accumulator = new RecordAccumulator(logContext, batchSize, Compression.gzip().build(),
                0, 0L, 0L, deliveryTimeoutMs, m, metricGrpName, time, txnManager,
                new BufferPool(totalSize, batchSize, metrics, time, "producer-internal-metrics"));
            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, true, MAX_REQUEST_SIZE, ACKS_ALL, maxRetries,
                    senderMetrics, time, REQUEST_TIMEOUT, 1000L, txnManager);
            // Create a two broker cluster, with partition 0 on broker 0 and partition 1 on broker 1
            MetadataResponse metadataUpdate1 = RequestTestUtils.metadataUpdateWithIds(2, Collections.singletonMap(topic, 2), TOPIC_IDS);
            client.prepareMetadataUpdate(metadataUpdate1);
            metadataUpdate1.brokers().forEach(node ->
                    apiVersions.update(node.idString(), NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()))
            );

            // Send the first message.
            long nowMs = time.milliseconds();
            Cluster cluster = TestUtils.singletonCluster();
            Future<RecordMetadata> f1 =
                    accumulator.append(tpId.topic(), tpId.partition(), 0L, "key1".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT, nowMs, cluster).future;
            Future<RecordMetadata> f2 =
                    accumulator.append(tpId.topic(), tpId.partition(), 0L, "key2".getBytes(), new byte[batchSize / 2], null, null, MAX_BLOCK_TIMEOUT, nowMs, cluster).future;
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request

            assertEquals(2, txnManager.sequenceNumber(tpId.topicPartition()), "The next sequence should be 2");
            String id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            Node node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");

            Map<TopicIdPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
            responseMap.put(tpId, new ProduceResponse.PartitionResponse(Errors.MESSAGE_TOO_LARGE));
            client.respond(new ProduceResponse(responseMap));
            sender.runOnce(); // split and reenqueue
            assertEquals(2, txnManager.sequenceNumber(tpId.topicPartition()), "The next sequence should be 2");
            // The compression ratio should have been improved once.
            assertEquals(CompressionType.GZIP.rate - CompressionRatioEstimator.COMPRESSION_RATIO_IMPROVING_STEP,
                    CompressionRatioEstimator.estimation(topic, CompressionType.GZIP), 0.01);
            sender.runOnce(); // send the first produce request
            assertEquals(2, txnManager.sequenceNumber(tpId.topicPartition()), "The next sequence number should be 2");
            assertFalse(f1.isDone(), "The future shouldn't have been done.");
            assertFalse(f2.isDone(), "The future shouldn't have been done.");
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");

            responseMap.put(tpId, new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
            client.respond(produceRequestMatcher(tpId.topicPartition(), producerIdAndEpoch, 0, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.runOnce(); // receive
            assertTrue(f1.isDone(), "The future should have been done.");
            assertEquals(2, txnManager.sequenceNumber(tpId.topicPartition()), "The next sequence number should still be 2");
            assertEquals(OptionalInt.of(0), txnManager.lastAckedSequence(tpId.topicPartition()), "The last ack'd sequence number should be 0");
            assertFalse(f2.isDone(), "The future shouldn't have been done.");
            assertEquals(0L, f1.get().offset(), "Offset of the first message should be 0");
            sender.runOnce(); // send the second produce request
            id = client.requests().peek().destination();
            assertEquals(ApiKeys.PRODUCE, client.requests().peek().requestBuilder().apiKey());
            node = new Node(Integer.parseInt(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue(client.isReady(node, time.milliseconds()), "Client ready status should be true");

            responseMap.put(tpId, new ProduceResponse.PartitionResponse(Errors.NONE, 1L, 0L, 0L));
            client.respond(produceRequestMatcher(tpId.topicPartition(), producerIdAndEpoch, 1, txnManager.isTransactional()),
                    new ProduceResponse(responseMap));

            sender.runOnce(); // receive
            assertTrue(f2.isDone(), "The future should have been done.");
            assertEquals(2, txnManager.sequenceNumber(tpId.topicPartition()), "The next sequence number should be 2");
            assertEquals(OptionalInt.of(1), txnManager.lastAckedSequence(tpId.topicPartition()), "The last ack'd sequence number should be 1");
            assertEquals(1L, f2.get().offset(), "Offset of the first message should be 1");
            assertTrue(accumulator.getDeque(tpId.topicPartition()).isEmpty(), "There should be no batch in the accumulator");
            assertTrue((Double) (m.metrics().get(senderMetrics.batchSplitRate).metricValue()) > 0, "There should be a split");
        }
    }

    @Test
    public void testNoDoubleDeallocation() throws Exception {
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-custom-metrics";
        MatchingBufferPool pool = new MatchingBufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        setupWithTransactionState(null, false, pool);

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(REQUEST_TIMEOUT);
        assertFalse(pool.allMatch());

        sender.runOnce();  // expire the batch
        assertTrue(request1.isDone());
        assertTrue(pool.allMatch(), "The batch should have been de-allocated");
        assertTrue(pool.allMatch());

        sender.runOnce();
        assertTrue(pool.allMatch(), "The batch should have been de-allocated");
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testInflightBatchesExpireOnDeliveryTimeout() throws InterruptedException {
        long deliveryTimeoutMs = 1500L;
        setupWithTransactionState(null, true, null);

        // Send first ProduceRequest
        Future<RecordMetadata> request = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size(), "Expect one in-flight batch in accumulator");

        Map<TopicIdPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        responseMap.put(new TopicIdPartition(TOPIC_ID, tp0), new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
        client.respond(new ProduceResponse(responseMap));

        time.sleep(deliveryTimeoutMs);
        sender.runOnce();  // receive first response
        assertEquals(0, sender.inFlightBatches(tp0).size(), "Expect zero in-flight batch in accumulator");
        assertInstanceOf(
            TimeoutException.class,
            assertThrows(ExecutionException.class, request::get).getCause(),
            "The expired batch should throw a TimeoutException");
    }

    @Test
    public void testRecordErrorPropagatedToApplication() throws InterruptedException {
        int recordCount = 5;

        setup();

        Map<Integer, FutureRecordMetadata> futures = new HashMap<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            futures.put(i, appendToAccumulator(tp0));
        }

        sender.runOnce();  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        OffsetAndError offsetAndError = new OffsetAndError(-1L, Errors.INVALID_RECORD, Arrays.asList(
            new BatchIndexAndErrorMessage().setBatchIndex(0).setBatchIndexErrorMessage("0"),
            new BatchIndexAndErrorMessage().setBatchIndex(2).setBatchIndexErrorMessage("2"),
            new BatchIndexAndErrorMessage().setBatchIndex(3)
        ));

        client.respond(produceResponse(Collections.singletonMap(tp0, offsetAndError)));
        sender.runOnce();

        for (Map.Entry<Integer, FutureRecordMetadata> futureEntry : futures.entrySet()) {
            FutureRecordMetadata future = futureEntry.getValue();
            assertTrue(future.isDone());

            Integer index = futureEntry.getKey();
            if (index == 0 || index == 2) {
                InvalidRecordException exception = TestUtils.assertFutureThrows(InvalidRecordException.class, future);
                assertInstanceOf(InvalidRecordException.class, exception);
                assertEquals(index.toString(), exception.getMessage());
            } else if (index == 3) {
                InvalidRecordException exception = TestUtils.assertFutureThrows(InvalidRecordException.class, future);
                assertInstanceOf(InvalidRecordException.class, exception);
                assertEquals(Errors.INVALID_RECORD.message(), exception.getMessage());
            } else {
                KafkaException exception = TestUtils.assertFutureThrows(KafkaException.class, future);
                assertEquals(KafkaException.class, exception.getClass());
            }
        }
    }

    @Test
    public void testWhenFirstBatchExpireNoSendSecondBatchIfGuaranteeOrder() throws InterruptedException {
        long deliveryTimeoutMs = 1500L;
        setupWithTransactionState(null, true, null);

        // Send first ProduceRequest
        appendToAccumulator(tp0);
        sender.runOnce();  // send request
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(deliveryTimeoutMs / 2);

        // Send second ProduceRequest
        appendToAccumulator(tp0);
        sender.runOnce();  // must not send request because the partition is muted
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());

        time.sleep(deliveryTimeoutMs / 2); // expire the first batch only

        client.respond(produceResponse(tp0, 0L, Errors.NONE, 0, 0L, null));
        sender.runOnce();  // receive response (offset=0)
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.runOnce();  // Drain the second request only this time
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testExpiredBatchDoesNotRetry() throws Exception {
        long deliverTimeoutMs = 1500L;
        setupWithTransactionState(null, false, null);

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        assertEquals(1, client.inFlightRequestCount());
        time.sleep(deliverTimeoutMs);

        client.respond(produceResponse(tp0, -1, Errors.NOT_LEADER_OR_FOLLOWER, -1)); // return a retriable error

        sender.runOnce();  // expire the batch
        assertTrue(request1.isDone());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.runOnce(); // receive first response and do not reenqueue.
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        sender.runOnce(); // run again and must not send anything.
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testExpiredBatchDoesNotSplitOnMessageTooLargeError() throws Exception {
        long deliverTimeoutMs = 1500L;
        // create a producer batch with more than one record so it is eligible for splitting
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        Future<RecordMetadata> request2 = appendToAccumulator(tp0);

        // send request
        sender.runOnce();
        assertEquals(1, client.inFlightRequestCount());
        // return a MESSAGE_TOO_LARGE error
        client.respond(produceResponse(tp0, -1, Errors.MESSAGE_TOO_LARGE, -1));

        time.sleep(deliverTimeoutMs);
        // expire the batch and process the response
        sender.runOnce();
        assertTrue(request1.isDone());
        assertTrue(request2.isDone());
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());

        // run again and must not split big batch and resend anything.
        sender.runOnce();
        assertEquals(0, client.inFlightRequestCount());
        assertEquals(0, sender.inFlightBatches(tp0).size());
    }

    @Test
    public void testResetNextBatchExpiry() throws Exception {
        client = spy(new MockClient(time, metadata));

        setupWithTransactionState(null);

        appendToAccumulator(tp0, 0L, "key", "value");

        sender.runOnce();
        sender.runOnce();
        time.setCurrentTimeMs(time.milliseconds() + accumulator.getDeliveryTimeoutMs() + 1);
        sender.runOnce();

        InOrder inOrder = inOrder(client);
        inOrder.verify(client, atLeastOnce()).ready(any(), anyLong());
        inOrder.verify(client, atLeastOnce()).newClientRequest(anyString(), any(), anyLong(), anyBoolean(), anyInt(), any());
        inOrder.verify(client, atLeastOnce()).send(any(), anyLong());
        inOrder.verify(client).poll(eq(0L), anyLong());
        inOrder.verify(client).poll(eq(accumulator.getDeliveryTimeoutMs()), anyLong());
        inOrder.verify(client).poll(geq(1L), anyLong());

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testExpiredBatchesInMultiplePartitions() throws Exception {
        long deliveryTimeoutMs = 1500L;
        setupWithTransactionState(null, true, null);

        // Send multiple ProduceRequest across multiple partitions.
        Future<RecordMetadata> request1 = appendToAccumulator(tp0, time.milliseconds(), "k1", "v1");
        Future<RecordMetadata> request2 = appendToAccumulator(tp1, time.milliseconds(), "k2", "v2");

        // Send request.
        sender.runOnce();
        assertEquals(1, client.inFlightRequestCount());
        assertEquals(1, sender.inFlightBatches(tp0).size(), "Expect one in-flight batch in accumulator");

        Map<TopicIdPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        responseMap.put(new TopicIdPartition(TOPIC_ID, tp0), new ProduceResponse.PartitionResponse(Errors.NONE, 0L, 0L, 0L));
        client.respond(new ProduceResponse(responseMap));

        // Successfully expire both batches.
        time.sleep(deliveryTimeoutMs);
        sender.runOnce();
        assertEquals(0, sender.inFlightBatches(tp0).size(), "Expect zero in-flight batch in accumulator");

        ExecutionException e = assertThrows(ExecutionException.class, request1::get);
        assertInstanceOf(TimeoutException.class, e.getCause());

        e = assertThrows(ExecutionException.class, request2::get);
        assertInstanceOf(TimeoutException.class, e.getCause());
    }

    @Test
    public void testTransactionalRequestsSentOnShutdown() {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            TransactionManager txnManager = new TransactionManager(logContext, "testTransactionalRequestsSentOnShutdown", 6000, 100, apiVersions, false);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, txnManager);

            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            TopicPartition tp = new TopicPartition("testTransactionalRequestsSentOnShutdown", 1);

            setupWithTransactionState(txnManager);
            doInitTransactions(txnManager, producerIdAndEpoch);

            txnManager.beginTransaction();
            txnManager.maybeAddPartition(tp);
            client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp, Errors.NONE)));
            sender.runOnce();
            sender.initiateClose();
            txnManager.beginCommit();
            AssertEndTxnRequestMatcher endTxnMatcher = new AssertEndTxnRequestMatcher(TransactionResult.COMMIT);
            client.prepareResponse(endTxnMatcher, new EndTxnResponse(new EndTxnResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setThrottleTimeMs(0)));
            sender.run();
            assertTrue(endTxnMatcher.matched, "Response didn't match in test");
        } finally {
            m.close();
        }
    }

    @Test
    public void testRecordsFlushedImmediatelyOnTransactionCompletion() throws Exception {
        try (Metrics m = new Metrics()) {
            int lingerMs = 50;
            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

            TransactionManager txnManager = new TransactionManager(logContext, "txnId", 6000, 100, apiVersions, false);
            setupWithTransactionState(txnManager, lingerMs);

            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                1, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, txnManager);

            // Begin a transaction and successfully add one partition to it.
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            doInitTransactions(txnManager, producerIdAndEpoch);
            txnManager.beginTransaction();
            addPartitionToTxn(sender, txnManager, tp0);

            // Send a couple records and assert that they are not sent immediately (due to linger).
            appendToAccumulator(tp0);
            appendToAccumulator(tp0);
            sender.runOnce();
            assertFalse(client.hasInFlightRequests());

            // Now begin the commit and assert that the Produce request is sent immediately
            // without waiting for the linger.
            TransactionalRequestResult commitResult = txnManager.beginCommit();
            runUntil(sender, client::hasInFlightRequests);

            // Respond to the produce request and wait for the EndTxn request to be sent.
            respondToProduce(tp0, Errors.NONE, 1L);
            runUntil(sender, txnManager::hasInFlightRequest);

            // Respond to the expected EndTxn request.
            respondToEndTxn(Errors.NONE);
            runUntil(sender, txnManager::isReady);

            assertTrue(commitResult.isSuccessful());
            commitResult.await();

            // Finally, we want to assert that the linger time is still effective
            // when the new transaction begins.
            txnManager.beginTransaction();
            addPartitionToTxn(sender, txnManager, tp0);

            appendToAccumulator(tp0);
            appendToAccumulator(tp0);
            time.sleep(lingerMs - 1);
            sender.runOnce();
            assertFalse(client.hasInFlightRequests());
            assertTrue(accumulator.hasUndrained());

            time.sleep(1);
            runUntil(sender, client::hasInFlightRequests);
            assertFalse(accumulator.hasUndrained());
        }
    }

    @Test
    public void testAwaitPendingRecordsBeforeCommittingTransaction() throws Exception {
        try (Metrics m = new Metrics()) {
            SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);

            TransactionManager txnManager = new TransactionManager(logContext, "txnId", 6000, 100, apiVersions, false);
            setupWithTransactionState(txnManager);

            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                1, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, txnManager);

            // Begin a transaction and successfully add one partition to it.
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            doInitTransactions(txnManager, producerIdAndEpoch);
            txnManager.beginTransaction();
            addPartitionToTxn(sender, txnManager, tp0);

            // Send one Produce request.
            appendToAccumulator(tp0);
            runUntil(sender, () -> client.requests().size() == 1);
            assertFalse(accumulator.hasUndrained());
            assertTrue(client.hasInFlightRequests());
            assertTrue(txnManager.hasInflightBatches(tp0));

            // Enqueue another record and then commit the transaction. We expect the unsent record to
            // get sent before the transaction can be completed.
            appendToAccumulator(tp0);
            txnManager.beginCommit();
            runUntil(sender, () -> client.requests().size() == 2);

            assertTrue(txnManager.isCompleting());
            assertFalse(txnManager.hasInFlightRequest());
            assertTrue(txnManager.hasInflightBatches(tp0));

            // Now respond to the pending Produce requests.
            respondToProduce(tp0, Errors.NONE, 0L);
            respondToProduce(tp0, Errors.NONE, 1L);
            runUntil(sender, txnManager::hasInFlightRequest);

            // Finally, respond to the expected EndTxn request.
            respondToEndTxn(Errors.NONE);
            runUntil(sender, txnManager::isReady);
        }
    }

    private void addPartitionToTxn(Sender sender, TransactionManager txnManager, TopicPartition tp) {
        txnManager.maybeAddPartition(tp);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp, Errors.NONE)));
        runUntil(sender, () -> txnManager.transactionContainsPartition(tp));
        assertFalse(txnManager.hasInFlightRequest());
    }

    private void respondToProduce(TopicPartition tp, Errors error, long offset) {
        client.respond(
            request -> request instanceof ProduceRequest,
            produceResponse(tp, offset, error, 0)
        );

    }

    private void respondToEndTxn(Errors error) {
        client.respond(
            request -> request instanceof EndTxnRequest,
            new EndTxnResponse(new EndTxnResponseData()
                .setErrorCode(error.code())
                .setThrottleTimeMs(0))
        );
    }

    @Test
    public void testIncompleteTransactionAbortOnShutdown() {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            TransactionManager txnManager = new TransactionManager(logContext, "testIncompleteTransactionAbortOnShutdown", 6000, 100, apiVersions, false);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, txnManager);

            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            TopicPartition tp = new TopicPartition("testIncompleteTransactionAbortOnShutdown", 1);

            setupWithTransactionState(txnManager);
            doInitTransactions(txnManager, producerIdAndEpoch);

            txnManager.beginTransaction();
            txnManager.maybeAddPartition(tp);
            client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp, Errors.NONE)));
            sender.runOnce();
            sender.initiateClose();
            AssertEndTxnRequestMatcher endTxnMatcher = new AssertEndTxnRequestMatcher(TransactionResult.ABORT);
            client.prepareResponse(endTxnMatcher, new EndTxnResponse(new EndTxnResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setThrottleTimeMs(0)));
            sender.run();
            assertTrue(endTxnMatcher.matched, "Response didn't match in test");
        } finally {
            m.close();
        }
    }

    @Timeout(10L)
    @Test
    public void testForceShutdownWithIncompleteTransaction() {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            TransactionManager txnManager = new TransactionManager(logContext, "testForceShutdownWithIncompleteTransaction", 6000, 100, apiVersions, false);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                    maxRetries, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, txnManager);

            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
            TopicPartition tp = new TopicPartition("testForceShutdownWithIncompleteTransaction", 1);

            setupWithTransactionState(txnManager);
            doInitTransactions(txnManager, producerIdAndEpoch);

            txnManager.beginTransaction();
            txnManager.maybeAddPartition(tp);
            client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp, Errors.NONE)));
            sender.runOnce();

            // Try to commit the transaction but it won't happen as we'll forcefully close the sender
            TransactionalRequestResult commitResult = txnManager.beginCommit();

            sender.forceClose();
            sender.run();
            assertThrows(KafkaException.class, commitResult::await,
                "The test expected to throw a KafkaException for forcefully closing the sender");
        } finally {
            m.close();
        }
    }

    @Test
    public void testTransactionAbortedExceptionOnAbortWithoutError() throws InterruptedException {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        TransactionManager txnManager = new TransactionManager(logContext, "testTransactionAbortedExceptionOnAbortWithoutError", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager, false, null);
        doInitTransactions(txnManager, producerIdAndEpoch);
        // Begin the transaction
        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        // Run it once so that the partition is added to the transaction.
        sender.runOnce();
        // Append a record to the accumulator.
        FutureRecordMetadata metadata = appendToAccumulator(tp0, time.milliseconds(), "key", "value");
        // Now abort the transaction manually.
        txnManager.beginAbort();
        // Try to send.
        // This should abort the existing transaction and
        // drain all the unsent batches with a TransactionAbortedException.
        sender.runOnce();
        // Now attempt to fetch the result for the record.
        TestUtils.assertFutureThrows(TransactionAbortedException.class, metadata);
    }

    @Test
    public void testDoNotPollWhenNoRequestSent() {
        client = spy(new MockClient(time, metadata));

        TransactionManager txnManager = new TransactionManager(logContext, "testDoNotPollWhenNoRequestSent", 6000, 100, apiVersions, false);
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        // doInitTransactions calls sender.doOnce three times, only two requests are sent, so we should only poll twice
        verify(client, times(2)).poll(eq(RETRY_BACKOFF_MS), anyLong());
    }

    @Test
    public void testTooLargeBatchesAreSafelyRemoved() throws InterruptedException {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        TransactionManager txnManager = new TransactionManager(logContext, "testSplitBatchAndSend", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager, false, null);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        // create a producer batch with more than one record so it is eligible for splitting
        appendToAccumulator(tp0, time.milliseconds(), "key1", "value1");
        appendToAccumulator(tp0, time.milliseconds(), "key2", "value2");

        // send request
        sender.runOnce();
        assertEquals(1, sender.inFlightBatches(tp0).size());
        // return a MESSAGE_TOO_LARGE error
        client.respond(produceResponse(tp0, -1, Errors.MESSAGE_TOO_LARGE, -1));
        sender.runOnce();

        // process retried response
        sender.runOnce();
        client.respond(produceResponse(tp0, 0, Errors.NONE, 0));
        sender.runOnce();

        // In-flight batches should be empty. Sleep past the expiration time of the batch and run once, no error should be thrown
        assertEquals(0, sender.inFlightBatches(tp0).size());
        time.sleep(2000);
        sender.runOnce();
    }

    @Test
    public void testDefaultErrorMessage() throws Exception {
        verifyErrorMessage(produceResponse(tp0, 0L, Errors.INVALID_REQUEST, 0), Errors.INVALID_REQUEST.message());
    }

    @Test
    public void testCustomErrorMessage() throws Exception {
        String errorMessage = "testCustomErrorMessage";
        verifyErrorMessage(produceResponse(tp0, 0L, Errors.INVALID_REQUEST, 0, -1, errorMessage), errorMessage);
    }

    @ParameterizedTest
    @EnumSource(value = Errors.class, names = {"COORDINATOR_LOAD_IN_PROGRESS", "INVALID_TXN_STATE"})
    public void testTransactionShouldTransitionToAbortableForSenderAPI(Errors error) throws InterruptedException {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        TransactionManager txnManager = new TransactionManager(
                logContext,
                "testRetriableException",
                60000,
                RETRY_BACKOFF_MS,
                apiVersions,
                false
        );

        // Setup with transaction state and initialize transactions with single retry
        setupWithTransactionState(txnManager, false, null, 1);
        doInitTransactions(txnManager, producerIdAndEpoch);

        // Begin transaction and add partition
        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        // First produce request
        appendToAccumulator(tp0);
        client.prepareResponse(produceResponse(tp0, -1, error, -1));
        sender.runOnce();

        // Sleep for retry backoff
        time.sleep(RETRY_BACKOFF_MS);

        // Second attempt to process record - PREPARE the response before sending
        client.prepareResponse(produceResponse(tp0, -1, error, -1));
        sender.runOnce();

        // Now transaction should be in abortable state after retry is exhausted
        assertTrue(txnManager.hasAbortableError());

        // Second produce request - should fail with TransactionAbortableException
        Future<RecordMetadata> future2 = appendToAccumulator(tp0);
        client.prepareResponse(produceResponse(tp0, -1, Errors.NONE, -1));
        // Sender will try to send and fail with TransactionAbortableException instead of COORDINATOR_LOAD_IN_PROGRESS, because we're in abortable state
        sender.runOnce();
        assertFutureFailure(future2, TransactionAbortableException.class);

        // Verify transaction API requests also fail with TransactionAbortableException
        try {
            txnManager.beginCommit();
            fail("Expected beginCommit() to fail with TransactionAbortableException when in abortable error state");
        } catch (KafkaException e) {
            assertEquals(TransactionAbortableException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testSenderShouldRetryWithBackoffOnRetriableError() throws InterruptedException {
        final long producerId = 343434L;
        TransactionManager transactionManager = createTransactionManager();
        setupWithTransactionState(transactionManager);
        long start = time.milliseconds();

        // first request is sent immediately
        prepareAndReceiveInitProducerId(producerId, (short) -1, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        long request1 = time.milliseconds();
        assertEquals(start, request1);

        // backoff before sending second request
        prepareAndReceiveInitProducerId(producerId, (short) -1, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        long request2 = time.milliseconds();
        assertEquals(RETRY_BACKOFF_MS, request2 - request1);

        // third request should also backoff
        prepareAndReceiveInitProducerId(producerId, Errors.NONE);
        assertEquals(RETRY_BACKOFF_MS, time.milliseconds() - request2);
    }

    @Test
    public void testReceiveFailedBatchTwiceWithTransactions() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
        TransactionManager txnManager = new TransactionManager(logContext, "testFailTwice", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        // Send first ProduceRequest
        Future<RecordMetadata> request1 = appendToAccumulator(tp0);
        sender.runOnce();  // send request

        Node node = metadata.fetch().nodes().get(0);
        time.sleep(2000L);
        client.disconnect(node.idString(), true);
        client.backoff(node, 10);

        sender.runOnce(); // now expire the batch.
        assertFutureFailure(request1, TimeoutException.class);

        time.sleep(20);

        sendIdempotentProducerResponse(0, tp0, Errors.INVALID_TXN_STATE, -1);
        sender.runOnce(); // receive late response

        // Loop once and confirm that the transaction manager does not enter a fatal error state
        sender.runOnce();
        assertTrue(txnManager.hasAbortableError());
        TransactionalRequestResult result = txnManager.beginAbort();
        sender.runOnce();

        respondToEndTxn(Errors.NONE);
        sender.runOnce();
        assertTrue(txnManager::isInitializing);
        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.runOnce();
        assertTrue(txnManager::isReady);

        assertTrue(result.isSuccessful());
        result.await();

        txnManager.beginTransaction();
    }

    @Test
    public void testInvalidTxnStateIsAnAbortableError() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
        TransactionManager txnManager = new TransactionManager(logContext, "testInvalidTxnState", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        Future<RecordMetadata> request = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        sendIdempotentProducerResponse(0, tp0, Errors.INVALID_TXN_STATE, -1);

        // Return InvalidTxnState error. It should be abortable.
        sender.runOnce();
        assertFutureFailure(request, InvalidTxnStateException.class);
        assertTrue(txnManager.hasAbortableError());
        TransactionalRequestResult result = txnManager.beginAbort();
        sender.runOnce();

        // Once the transaction is aborted, we should be able to begin a new one.
        respondToEndTxn(Errors.NONE);
        sender.runOnce();
        assertTrue(txnManager::isInitializing);
        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.runOnce();
        assertTrue(txnManager::isReady);

        assertTrue(result.isSuccessful());
        result.await();

        txnManager.beginTransaction();
    }

    @Test
    public void testTransactionAbortableExceptionIsAnAbortableError() throws Exception {
        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
        apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
        TransactionManager txnManager = new TransactionManager(logContext, "textTransactionAbortableException", 60000, 100, apiVersions, false);

        setupWithTransactionState(txnManager);
        doInitTransactions(txnManager, producerIdAndEpoch);

        txnManager.beginTransaction();
        txnManager.maybeAddPartition(tp0);
        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
        sender.runOnce();

        Future<RecordMetadata> request = appendToAccumulator(tp0);
        sender.runOnce();  // send request
        sendIdempotentProducerResponse(0, tp0, Errors.TRANSACTION_ABORTABLE, -1);

        // Return TransactionAbortableException error. It should be abortable.
        sender.runOnce();
        assertFutureFailure(request, TransactionAbortableException.class);
        assertTrue(txnManager.hasAbortableError());
        TransactionalRequestResult result = txnManager.beginAbort();
        sender.runOnce();

        // Once the transaction is aborted, we should be able to begin a new one.
        respondToEndTxn(Errors.NONE);
        sender.runOnce();
        assertTrue(txnManager::isInitializing);
        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.runOnce();
        assertTrue(txnManager::isReady);

        assertTrue(result.isSuccessful());
        result.await();

        txnManager.beginTransaction();
    }

    @Test
    public void testAbortableErrorIsConvertedToFatalErrorDuringAbort() throws Exception {

        // Initialize and begin transaction
        TransactionManager transactionManager = new TransactionManager(logContext, "testAbortableErrorIsConvertedToFatalErrorDuringAbort", 60000, 100, apiVersions, false);
        setupWithTransactionState(transactionManager);
        doInitTransactions(transactionManager, new ProducerIdAndEpoch(1L, (short) 0));
        transactionManager.beginTransaction();

        // Add partition and send record
        TopicPartition tp = new TopicPartition("test", 0);
        addPartitionToTxn(sender, transactionManager, tp);
        appendToAccumulator(tp);

        // Send record and get response
        sender.runOnce();
        sendIdempotentProducerResponse(0, tp, Errors.NONE, 0);
        sender.runOnce();

        // Commit API with TRANSACTION_ABORTABLE error should set TM to Abortable state
        client.prepareResponse(new EndTxnResponse(new EndTxnResponseData()
                .setErrorCode(Errors.TRANSACTION_ABORTABLE.code())));

        // Attempt to commit transaction
        TransactionalRequestResult commitResult = transactionManager.beginCommit();
        sender.runOnce();
        try {
            commitResult.await(1000, TimeUnit.MILLISECONDS);
            fail("Expected abortable error to be thrown for commit");
        } catch (KafkaException e) {
            assertTrue(transactionManager.hasAbortableError());
            assertEquals(commitResult.error().getClass(), TransactionAbortableException.class);
        }

        // Abort API with TRANSACTION_ABORTABLE error should convert to Fatal error i.e. KafkaException
        client.prepareResponse(new EndTxnResponse(new EndTxnResponseData()
                .setErrorCode(Errors.TRANSACTION_ABORTABLE.code())));

        // Attempt to abort transaction
        TransactionalRequestResult abortResult = transactionManager.beginAbort();
        sender.runOnce();

        // Verify the error is converted to KafkaException (not TransactionAbortableException)
        try {
            abortResult.await(1000, TimeUnit.MILLISECONDS);
            fail("Expected KafkaException to be thrown");
        } catch (KafkaException e) {
            // Verify TM is in FATAL_ERROR state
            assertTrue(transactionManager.hasFatalError());
            assertFalse(e instanceof TransactionAbortableException);
            assertEquals(abortResult.error().getClass(), KafkaException.class);
        }
    }

    @Test
    public void testProducerBatchRetriesWhenPartitionLeaderChanges() throws Exception {
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            // SETUP
            String metricGrpName = "producer-metrics-test-stats-1";
            long totalSize = 1024 * 1024;
            BufferPool pool = new BufferPool(totalSize, batchSize, metrics, time,
                metricGrpName);
            long retryBackoffMaxMs = 100L;
            // lingerMs is 0 to send batch as soon as any records are available on it.
            this.accumulator = new RecordAccumulator(logContext, batchSize,
                Compression.NONE, 0, 10L, retryBackoffMaxMs,
                DELIVERY_TIMEOUT_MS, metrics, metricGrpName, time, null, pool);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL,
                10, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);
            // Update metadata with leader-epochs.
            int tp0LeaderEpoch = 100;
            int epoch = tp0LeaderEpoch;
            this.client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Set.of(new TopicIdPartition(TOPIC_ID, tp0),
                                new TopicIdPartition(TOPIC_ID, tp1)),
                    tp -> {
                        if (tp0.equals(tp)) {
                            return epoch;
                        }  else if (tp1.equals(tp)) {
                            return 0;
                        } else {
                            throw new RuntimeException("unexpected tp " + tp);
                        }
                    }));

            // Produce batch, it returns with a retry-able error like NOT_LEADER_OR_FOLLOWER, scheduled for retry.
            Future<RecordMetadata> futureIsProduced = appendToAccumulator(tp0, 0L, "key", "value");
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request
            assertEquals(1, client.inFlightRequestCount(),
                "We should have a single produce request in flight.");
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertTrue(client.hasInFlightRequests());
            client.respond(produceResponse(tp0, -1, Errors.NOT_LEADER_OR_FOLLOWER, 0));
            sender.runOnce(); // receive produce response, batch scheduled for retry
            assertFalse(futureIsProduced.isDone(), "Produce request should not be done.");

            // TEST that as new-leader(with epochA) is discovered, the batch is retried immediately i.e. skips any backoff period.
            // Update leader epoch for tp0
            int newEpoch = ++tp0LeaderEpoch;
            this.client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Set.of(new TopicIdPartition(TOPIC_ID, tp0),
                                new TopicIdPartition(TOPIC_ID, tp1)),
                    tp -> {
                        if (tp0.equals(tp)) {
                            return newEpoch;
                        } else if (tp1.equals(tp)) {
                            return 0;
                        } else {
                            throw new RuntimeException("unexpected tp " + tp);
                        }
                    }));
            sender.runOnce(); // send produce request, immediately.
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertTrue(client.hasInFlightRequests());
            client.respond(produceResponse(tp0, -1, Errors.NOT_LEADER_OR_FOLLOWER, 0));
            sender.runOnce(); // receive produce response, schedule batch for retry.
            assertFalse(futureIsProduced.isDone(), "Produce request should not be done.");

            // TEST that a subsequent retry to the same leader(epochA) waits the backoff period.
            sender.runOnce(); //send produce request
            // No batches in-flight
            assertEquals(0, sender.inFlightBatches(tp0).size());
            assertFalse(client.hasInFlightRequests());

            // TEST that after waiting for longer than backoff period, batch is retried again.
            time.sleep(2 * retryBackoffMaxMs);
            sender.runOnce(); // send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertTrue(client.hasInFlightRequests());
            long offset = 999;
            client.respond(produceResponse(tp0, offset, Errors.NONE, 0));
            sender.runOnce(); // receive response.
            assertTrue(futureIsProduced.isDone(), "Request to tp0 successfully done");
            assertEquals(offset, futureIsProduced.get().offset());
        } finally {
            m.close();
        }
    }

    // This test is expected to run fast. If timeout, the sender is not able to close properly.
    @Timeout(5)
    @Test
    public void testSenderShouldCloseWhenTransactionManagerInErrorState() {
        metrics.close();
        Map<String, String> clientTags = Collections.singletonMap("client-id", "clientA");
        metrics = new Metrics(new MetricConfig().tags(clientTags));
        TransactionManager transactionManager = mock(TransactionManager.class);
        SenderMetricsRegistry metricsRegistry = new SenderMetricsRegistry(metrics);
        Sender sender = new Sender(logContext, client, metadata, this.accumulator, false, MAX_REQUEST_SIZE, ACKS_ALL,
                1, metricsRegistry, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);
        when(transactionManager.hasOngoingTransaction()).thenReturn(true);
        when(transactionManager.beginAbort()).thenThrow(new IllegalStateException());
        sender.initiateClose();

        // The sender should directly get closed.
        sender.run();
        verify(transactionManager, times(1)).close();
    }

    /**
     * Test the scenario that FetchResponse returns NOT_LEADER_OR_FOLLOWER, indicating change in leadership, but it
     * does not contain new leader info(defined in KIP-951).
     */
    @Test
    public void testWhenProduceResponseReturnsWithALeaderShipChangeErrorButNoNewLeaderInformation()
        throws InterruptedException {
        // Setup 3 partitions, tp0 & tp1 return with NOT_LEADER_OR_FOLLOWER, tp2 doesn't return an error.
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            // SETUP
            String metricGrpName = "producer-metrics-test-stats-1";
            long totalSize = 1024 * 1024;
            BufferPool pool = new BufferPool(totalSize, batchSize, metrics, time,
                metricGrpName);
            long retryBackoffMaxMs = 100L;
            // lingerMs is 0 to send batch as soon as any records are available on it.
            this.accumulator = new RecordAccumulator(logContext, batchSize,
                Compression.NONE, 0, 10L, retryBackoffMaxMs,
                DELIVERY_TIMEOUT_MS, metrics, metricGrpName, time, null, pool);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL,
                10, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);
            // Update metadata with leader-epochs.
            int tp0LeaderEpoch = 100;
            int tp1LeaderEpoch = 200;
            int tp2LeaderEpoch = 300;
            this.client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Set.of(new TopicIdPartition(TOPIC_ID, tp0),
                                new TopicIdPartition(TOPIC_ID, tp1), new TopicIdPartition(TOPIC_ID, tp2)),
                    tp -> {
                        if (tp0.equals(tp)) {
                            return tp0LeaderEpoch;
                        }  else if (tp1.equals(tp)) {
                            return tp1LeaderEpoch;
                        } else if (tp2.equals(tp)) {
                            return tp2LeaderEpoch;
                        } else {
                            throw new RuntimeException("unexpected tp " + tp);
                        }
                    }));
            Cluster startingMetadataCluster = metadata.fetch();

            // Produce to tp0/1/2, where NO_LEADER_OR_FOLLOWER without new leader info is returned for tp0/1, and tp2 is returned without errors.
            Future<RecordMetadata> futureIsProducedTp0 = appendToAccumulator(tp0, 0L, "key", "value");
            Future<RecordMetadata> futureIsProducedTp1 = appendToAccumulator(tp1, 0L, "key", "value");
            Future<RecordMetadata> futureIsProducedTp2 = appendToAccumulator(tp2, 0L, "key", "value");
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertEquals(1, sender.inFlightBatches(tp1).size());
            assertEquals(1, sender.inFlightBatches(tp2).size());
            assertTrue(client.hasInFlightRequests());
            Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
            responses.put(tp0, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
            responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
            responses.put(tp2, new OffsetAndError(100, Errors.NONE));
            client.respond(produceResponse(responses));
            sender.runOnce(); // receive produce response, batch scheduled for retry
            assertFalse(futureIsProducedTp0.isDone(), "Produce request to tp0 should be unfinished.");
            assertFalse(futureIsProducedTp1.isDone(), "Produce request to tp1 should be unfinished.");
            assertTrue(futureIsProducedTp2.isDone(), "Produce request to tp0 should be done.");

            // Validate metadata is unchanged as new leader info wasn't received.
            assertEquals(startingMetadataCluster, metadata.fetch());

            // Validate metadata-refresh is requested as NOT_LEADER_OR_FOLLOWER received earlier
            assertTrue(metadata.updateRequested());

            // TEST that a subsequent retry waits the backoff period as the new leader info is yet not available.
            sender.runOnce(); //send produce request
            assertEquals(0, sender.inFlightBatches(tp0).size());
            assertEquals(0, sender.inFlightBatches(tp1).size());
            assertFalse(client.hasInFlightRequests());
        } finally {
            m.close();
        }
    }

    /**
     * Test the scenario that FetchResponse returns NOT_LEADER_OR_FOLLOWER, indicating change in leadership, along with
     * new leader info(defined in KIP-951).
     */
    @Test
    public void testWhenProduceResponseReturnsWithALeaderShipChangeErrorAndNewLeaderInformation()
        throws InterruptedException {
        // Setup 3 partitions, tp0 & tp1 return with NOT_LEADER_OR_FOLLOWER, tp2 doesn't return an error.
        Metrics m = new Metrics();
        SenderMetricsRegistry senderMetrics = new SenderMetricsRegistry(m);
        try {
            // SETUP
            String metricGrpName = "producer-metrics-test-stats-1";
            long totalSize = 1024 * 1024;
            BufferPool pool = new BufferPool(totalSize, batchSize, metrics, time,
                metricGrpName);
            long retryBackoffMaxMs = 100L;
            // lingerMs is 0 to send batch as soon as any records are available on it.
            this.accumulator = new RecordAccumulator(logContext, batchSize,
                Compression.NONE, 0, 10L, retryBackoffMaxMs,
                DELIVERY_TIMEOUT_MS, metrics, metricGrpName, time, null, pool);
            Sender sender = new Sender(logContext, client, metadata, this.accumulator, false,
                MAX_REQUEST_SIZE, ACKS_ALL,
                10, senderMetrics, time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, null);
            // Update metadata with leader-epochs.
            int tp0LeaderEpoch = 100;
            int tp1LeaderEpoch = 200;
            int tp2LeaderEpoch = 300;
            this.client.updateMetadata(
                RequestTestUtils.metadataUpdateWithIds(1, Set.of(new TopicIdPartition(TOPIC_ID, tp0),
                        new TopicIdPartition(TOPIC_ID, tp1), new TopicIdPartition(TOPIC_ID, tp2)),
                    tp -> {
                        if (tp0.equals(tp)) {
                            return tp0LeaderEpoch;
                        }  else if (tp1.equals(tp)) {
                            return tp1LeaderEpoch;
                        } else if (tp2.equals(tp)) {
                            return tp2LeaderEpoch;
                        } else {
                            throw new RuntimeException("unexpected tp " + tp);
                        }
                    }));
            Cluster startingMetadataCluster = metadata.fetch();
            startingMetadataCluster.nodes().forEach(node ->
                    apiVersions.update(node.idString(), NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()))
            );

            // Produce to tp0/1/2, where NO_LEADER_OR_FOLLOWER with new leader info is returned for tp0/1, and tp2 is returned without errors.
            Future<RecordMetadata> futureIsProducedTp0 = appendToAccumulator(tp0, 0L, "key", "value");
            Future<RecordMetadata> futureIsProducedTp1 = appendToAccumulator(tp1, 0L, "key", "value");
            Future<RecordMetadata> futureIsProducedTp2 = appendToAccumulator(tp2, 0L, "key", "value");
            sender.runOnce(); // connect
            sender.runOnce(); // send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertEquals(1, sender.inFlightBatches(tp1).size());
            assertEquals(1, sender.inFlightBatches(tp2).size());
            assertTrue(client.hasInFlightRequests());

            Node newNodeForTp0 = new Node(9990, "newhost9990", 9990, "newrack9990");
            Node newNodeForTp1 = new Node(9991, "newhost9991", 9991, "newrack9991");
            List<Node> newNodes = Arrays.asList(newNodeForTp0, newNodeForTp1);

            Map<TopicPartition, OffsetAndError> responses = new LinkedHashMap<>();
            responses.put(tp0, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
            responses.put(tp1, new OffsetAndError(-1, Errors.NOT_LEADER_OR_FOLLOWER));
            responses.put(tp2, new OffsetAndError(100, Errors.NONE));
            newNodes.forEach(node ->
                    apiVersions.update(node.idString(), NodeApiVersions.create(ApiKeys.PRODUCE.id, ApiKeys.PRODUCE.oldestVersion(), ApiKeys.PRODUCE.latestVersion()))
            );
            Map<TopicPartition, ProduceResponseData.LeaderIdAndEpoch> partitionLeaderInfo = new HashMap<>();
            ProduceResponseData.LeaderIdAndEpoch tp0LeaderInfo = new ProduceResponseData.LeaderIdAndEpoch();
            tp0LeaderInfo.setLeaderEpoch(tp0LeaderEpoch + 1);
            tp0LeaderInfo.setLeaderId(newNodeForTp0.id());
            partitionLeaderInfo.put(tp0, tp0LeaderInfo);
            ProduceResponseData.LeaderIdAndEpoch tp1LeaderInfo = new ProduceResponseData.LeaderIdAndEpoch();
            tp1LeaderInfo.setLeaderEpoch(tp1LeaderEpoch + 1);
            tp1LeaderInfo.setLeaderId(newNodeForTp1.id());
            partitionLeaderInfo.put(tp1, tp1LeaderInfo);

            client.respond(produceResponse(responses, partitionLeaderInfo, newNodes));
            sender.runOnce(); // receive produce response, batch scheduled for retry
            assertFalse(futureIsProducedTp0.isDone(), "Produce request to tp0 should be unfinished.");
            assertFalse(futureIsProducedTp1.isDone(), "Produce request to tp1 should be unfinished.");
            assertTrue(futureIsProducedTp2.isDone(), "Produce request to tp0 should be done.");

            // Validate metadata is unchanged as new leader info wasn't received.
            assertNotEquals(startingMetadataCluster, metadata.fetch());
            // Validate metadata cached has updated leader info for tp0/1.
            Metadata.LeaderAndEpoch tp0NewLeaderInfo = metadata.currentLeader(tp0);
            assertEquals(newNodeForTp0, tp0NewLeaderInfo.leader.get());
            assertEquals(tp0LeaderEpoch + 1, tp0NewLeaderInfo.epoch.get());
            Metadata.LeaderAndEpoch tp1NewLeaderInfo = metadata.currentLeader(tp1);
            assertEquals(newNodeForTp1, tp1NewLeaderInfo.leader.get());
            assertEquals(tp1LeaderEpoch + 1, tp1NewLeaderInfo.epoch.get());

            // Validate metadata-refresh is requested as NOT_LEADER_OR_FOLLOWER received earlier
            assertTrue(metadata.updateRequested());

            // TEST that a subsequent retry skips the backoff as a new leader information is available.
            sender.runOnce(); //send produce request
            assertEquals(1, sender.inFlightBatches(tp0).size());
            assertEquals(1, sender.inFlightBatches(tp1).size());
            assertTrue(client.hasInFlightRequests());
        } finally {
            m.close();
        }
    }


    private void verifyErrorMessage(ProduceResponse response, String expectedMessage) throws Exception {
        Future<RecordMetadata> future = appendToAccumulator(tp0, 0L, "key", "value");
        sender.runOnce(); // connect
        sender.runOnce(); // send produce request
        client.respond(response);
        sender.runOnce();
        sender.runOnce();
        ExecutionException e1 = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        assertEquals(InvalidRequestException.class, e1.getCause().getClass());
        assertEquals(expectedMessage, e1.getCause().getMessage());
    }

    private static class AssertEndTxnRequestMatcher implements MockClient.RequestMatcher {

        private final TransactionResult requiredResult;
        private boolean matched = false;

        AssertEndTxnRequestMatcher(TransactionResult requiredResult) {
            this.requiredResult = requiredResult;
        }

        @Override
        public boolean matches(AbstractRequest body) {
            if (body instanceof EndTxnRequest) {
                assertSame(requiredResult, ((EndTxnRequest) body).result());
                matched = true;
                return true;
            } else {
                return false;
            }
        }
    }

    private static class MatchingBufferPool extends BufferPool {
        IdentityHashMap<ByteBuffer, Boolean> allocatedBuffers;

        MatchingBufferPool(long totalSize, int batchSize, Metrics metrics, Time time, String metricGrpName) {
            super(totalSize, batchSize, metrics, time, metricGrpName);
            allocatedBuffers = new IdentityHashMap<>();
        }

        @Override
        public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
            ByteBuffer buffer = super.allocate(size, maxTimeToBlockMs);
            allocatedBuffers.put(buffer, Boolean.TRUE);
            return buffer;
        }

        @Override
        public void deallocate(ByteBuffer buffer, int size) {
            if (!allocatedBuffers.containsKey(buffer)) {
                throw new IllegalStateException("Deallocating a buffer that is not allocated");
            }
            allocatedBuffers.remove(buffer);
            super.deallocate(buffer, size);
        }

        public boolean allMatch() {
            return allocatedBuffers.isEmpty();
        }
    }

    private MockClient.RequestMatcher produceRequestMatcher(final TopicPartition tp,
                                                            final ProducerIdAndEpoch producerIdAndEpoch,
                                                            final int sequence,
                                                            final boolean isTransactional) {
        return body -> {
            if (!(body instanceof ProduceRequest))
                return false;

            ProduceRequest request = (ProduceRequest) body;
            Map<TopicPartition, MemoryRecords> recordsMap = partitionRecords(request);
            MemoryRecords records = recordsMap.get(tp);
            if (records == null)
                return false;

            List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
            if (batches.size() != 1)
                return false;

            MutableRecordBatch batch = batches.get(0);
            return batch.baseOffset() == 0L &&
                    batch.baseSequence() == sequence &&
                    batch.producerId() == producerIdAndEpoch.producerId &&
                    batch.producerEpoch() == producerIdAndEpoch.epoch &&
                    batch.isTransactional() == isTransactional;
        };
    }

    private static class OffsetAndError {
        final long offset;
        final Errors error;
        final List<BatchIndexAndErrorMessage> recordErrors;

        OffsetAndError(
            long offset,
            Errors error,
            List<BatchIndexAndErrorMessage> recordErrors
        ) {
            this.offset = offset;
            this.error = error;
            this.recordErrors = recordErrors;
        }

        OffsetAndError(long offset, Errors error) {
            this(offset, error, Collections.emptyList());
        }

    }

    private FutureRecordMetadata appendToAccumulator(TopicPartition tp) throws InterruptedException {
        return appendToAccumulator(tp, time.milliseconds(), "key", "value");
    }

    private FutureRecordMetadata appendToAccumulator(TopicPartition tp, long timestamp, String key, String value) throws InterruptedException {
        return accumulator.append(tp.topic(), tp.partition(), timestamp, key.getBytes(), value.getBytes(), Record.EMPTY_HEADERS,
                null, MAX_BLOCK_TIMEOUT, time.milliseconds(), TestUtils.singletonCluster()).future;
    }

    @SuppressWarnings("deprecation")
    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs, long logStartOffset, String errorMessage) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse(error, offset,
                RecordBatch.NO_TIMESTAMP, logStartOffset, Collections.emptyList(), errorMessage);
        Map<TopicIdPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(new TopicIdPartition(TOPIC_ID, tp), resp);
        return new ProduceResponse(partResp, throttleTimeMs);
    }

    private ProduceResponse produceResponse(Map<TopicPartition, OffsetAndError> responses) {
        return produceResponse(responses, null, null);
    }

    /**
     * It creates response with new leader info and new nodes, as stated in KIP-951
     */
    private ProduceResponse produceResponse(Map<TopicPartition, OffsetAndError> responses, Map<TopicPartition, ProduceResponseData.LeaderIdAndEpoch> partitionLeaderInfo, List<Node> nodes) {
        ProduceResponseData data = new ProduceResponseData();

        for (Map.Entry<TopicPartition, OffsetAndError> entry : responses.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            ProduceResponseData.TopicProduceResponse topicData = data.responses().find(topicPartition.topic(), TOPIC_ID);
            if (topicData == null) {
                topicData = new ProduceResponseData.TopicProduceResponse()
                        .setTopicId(TOPIC_ID)
                        .setName(topicPartition.topic());
                data.responses().add(topicData);
            }

            OffsetAndError offsetAndError = entry.getValue();
            ProduceResponseData.PartitionProduceResponse partitionData =
                new ProduceResponseData.PartitionProduceResponse()
                    .setIndex(topicPartition.partition())
                    .setBaseOffset(offsetAndError.offset)
                    .setErrorCode(offsetAndError.error.code())
                    .setRecordErrors(offsetAndError.recordErrors);

            if (partitionLeaderInfo != null && partitionLeaderInfo.containsKey(topicPartition)) {
                partitionData.setCurrentLeader(partitionLeaderInfo.get(topicPartition));
            }

            topicData.partitionResponses().add(partitionData);
        }

        if (nodes != null) {
            nodes.stream().map(n -> {
                ProduceResponseData.NodeEndpoint nodeEndPt = new ProduceResponseData.NodeEndpoint();
                nodeEndPt.setNodeId(n.id());
                nodeEndPt.setPort(n.port());
                nodeEndPt.setHost(n.host());
                nodeEndPt.setRack(n.rack());
                return nodeEndPt;
            }).forEach(nodeEndpoint -> data.nodeEndpoints().add(nodeEndpoint));
        }

        return new ProduceResponse(data);
    }

    private ProduceResponse produceResponse(TopicPartition tp, long offset, Errors error, int throttleTimeMs) {
        return produceResponse(tp, offset, error, throttleTimeMs, -1L, null);
    }

    private TransactionManager createTransactionManager() {
        return new TransactionManager(new LogContext(), null, 0, RETRY_BACKOFF_MS, new ApiVersions(), false);
    }
    
    private void setupWithTransactionState(TransactionManager transactionManager) {
        setupWithTransactionState(transactionManager, false, null, true, Integer.MAX_VALUE, 0);
    }

    private void setupWithTransactionState(TransactionManager transactionManager, int lingerMs) {
        setupWithTransactionState(transactionManager, false, null, true, Integer.MAX_VALUE, lingerMs);
    }

    private void setupWithTransactionState(TransactionManager transactionManager, boolean guaranteeOrder, BufferPool customPool) {
        setupWithTransactionState(transactionManager, guaranteeOrder, customPool, true, Integer.MAX_VALUE, 0);
    }

    private void setupWithTransactionState(TransactionManager transactionManager, boolean guaranteeOrder, BufferPool customPool, int retries) {
        setupWithTransactionState(transactionManager, guaranteeOrder, customPool, true, retries, 0);
    }

    private void setupWithTransactionState(
        TransactionManager transactionManager,
        boolean guaranteeOrder,
        BufferPool customPool,
        boolean updateMetadata
    ) {
        setupWithTransactionState(transactionManager, guaranteeOrder, customPool, updateMetadata, Integer.MAX_VALUE, 0);
    }

    private void setupWithTransactionState(
        TransactionManager transactionManager,
        boolean guaranteeOrder,
        BufferPool customPool,
        boolean updateMetadata,
        int retries,
        int lingerMs
    ) {
        long totalSize = 1024 * 1024;
        String metricGrpName = "producer-metrics";
        MetricConfig metricConfig = new MetricConfig().tags(Collections.singletonMap("client-id", CLIENT_ID));
        this.metrics = new Metrics(metricConfig, time);
        BufferPool pool = (customPool == null) ? new BufferPool(totalSize, batchSize, metrics, time, metricGrpName) : customPool;

        this.accumulator = new RecordAccumulator(logContext, batchSize, Compression.NONE, lingerMs, 0L, 0L,
            DELIVERY_TIMEOUT_MS, metrics, metricGrpName, time, transactionManager, pool);
        this.senderMetricsRegistry = new SenderMetricsRegistry(this.metrics);
        this.sender = new Sender(logContext, this.client, this.metadata, this.accumulator, guaranteeOrder, MAX_REQUEST_SIZE, ACKS_ALL,
            retries, this.senderMetricsRegistry, this.time, REQUEST_TIMEOUT, RETRY_BACKOFF_MS, transactionManager);

        metadata.add(TOPIC_NAME, time.milliseconds());
        if (updateMetadata)
            this.client.updateMetadata(RequestTestUtils.metadataUpdateWithIds(1, Collections.singletonMap(TOPIC_NAME, 2), TOPIC_IDS));
    }

    private void assertSuccessfulSend() throws InterruptedException {
        Future<RecordMetadata> future = appendToAccumulator(tp0);
        sender.runOnce(); // send request
        assertEquals(1, client.inFlightRequestCount(), "We should have a single produce request in flight.");
        assertEquals(1, sender.inFlightBatches(tp0).size());
        assertTrue(client.hasInFlightRequests());
        client.respond(produceResponse(tp0, 0, Errors.NONE, 0));
        sender.runOnce();
        assertTrue(future.isDone());
        try {
            future.get();
        } catch (ExecutionException e) {
            fail("Future should not have raised an exception: " + e.getCause());
        }
    }

    private void assertSendFailure(Class<? extends RuntimeException> expectedError) throws Exception {
        Future<RecordMetadata> future = appendToAccumulator(tp0);
        sender.runOnce();
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have raised " + expectedError.getSimpleName());
        } catch (ExecutionException e) {
            assertTrue(expectedError.isAssignableFrom(e.getCause().getClass()));
        }
    }

    private void prepareAndReceiveInitProducerId(long producerId, Errors error) {
        prepareAndReceiveInitProducerId(producerId, (short) 0, error);
    }

    private void prepareAndReceiveInitProducerId(long producerId, short producerEpoch, Errors error) {
        if (error != Errors.NONE)
            producerEpoch = RecordBatch.NO_PRODUCER_EPOCH;

        client.prepareResponse(
            body -> body instanceof InitProducerIdRequest &&
                ((InitProducerIdRequest) body).data().transactionalId() == null,
            initProducerIdResponse(producerId, producerEpoch, error));
        sender.runOnce();
    }

    private InitProducerIdResponse initProducerIdResponse(long producerId, short producerEpoch, Errors error) {
        InitProducerIdResponseData responseData = new InitProducerIdResponseData()
                .setErrorCode(error.code())
                .setProducerEpoch(producerEpoch)
                .setProducerId(producerId)
                .setThrottleTimeMs(0);
        return new InitProducerIdResponse(responseData);
    }

    private void doInitTransactions(TransactionManager transactionManager, ProducerIdAndEpoch producerIdAndEpoch) {
        TransactionalRequestResult result = transactionManager.initializeTransactions(false);
        prepareFindCoordinatorResponse(Errors.NONE, transactionManager.transactionalId());
        sender.runOnce();
        sender.runOnce();

        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        sender.runOnce();
        assertTrue(transactionManager.hasProducerId());
        result.await();
    }

    private void prepareFindCoordinatorResponse(Errors error, String txnid) {
        Node node = metadata.fetch().nodes().get(0);
        client.prepareResponse(FindCoordinatorResponse.prepareResponse(error, txnid, node));
    }

    private void prepareInitProducerResponse(Errors error, long producerId, short producerEpoch) {
        client.prepareResponse(initProducerIdResponse(producerId, producerEpoch, error));
    }

    private void assertFutureFailure(Future<?> future, Class<? extends Exception> expectedExceptionType)
            throws InterruptedException {
        assertTrue(future.isDone());
        try {
            future.get();
            fail("Future should have raised " + expectedExceptionType.getName());
        } catch (ExecutionException e) {
            Class<? extends Throwable> causeType = e.getCause().getClass();
            assertTrue(expectedExceptionType.isAssignableFrom(causeType), "Unexpected cause " + causeType.getName());
        }
    }

    private void createMockClientWithMaxFlightOneMetadataPending() {
        client = new MockClient(time, metadata) {
            volatile boolean canSendMore = true;
            @Override
            public LeastLoadedNode leastLoadedNode(long now) {
                for (Node node : metadata.fetch().nodes()) {
                    if (isReady(node, now) && canSendMore)
                        return new LeastLoadedNode(node, true);
                }
                return new LeastLoadedNode(null, false);
            }

            @Override
            public List<ClientResponse> poll(long timeoutMs, long now) {
                canSendMore = inFlightRequestCount() < 1;
                return super.poll(timeoutMs, now);
            }
        };

        // Send metadata request and wait until request is sent. `leastLoadedNode` will be null once
        // request is in progress since no more requests can be sent to the node. Node will be ready
        // on the next poll() after response is processed later on in tests which use this method.
        MetadataRequest.Builder builder = new MetadataRequest.Builder(Collections.emptyList(), false);
        Node node = metadata.fetch().nodes().get(0);
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true);
        while (!client.ready(node, time.milliseconds()))
            client.poll(0, time.milliseconds());
        client.send(request, time.milliseconds());
        while (client.leastLoadedNode(time.milliseconds()).node() != null)
            client.poll(0, time.milliseconds());
    }

    private void waitForProducerId(TransactionManager transactionManager, ProducerIdAndEpoch producerIdAndEpoch) {
        for (int i = 0; i < 5 && !transactionManager.hasProducerId(); i++)
            sender.runOnce();

        assertTrue(transactionManager.hasProducerId());
        assertEquals(producerIdAndEpoch, transactionManager.producerIdAndEpoch());
    }
    
    private AddPartitionsToTxnResponse buildAddPartitionsToTxnResponseData(int throttleMs, Map<TopicPartition, Errors> errors) {
        AddPartitionsToTxnResponseData.AddPartitionsToTxnResult result = AddPartitionsToTxnResponse.resultForTransaction(
                AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID, errors);
        AddPartitionsToTxnResponseData data = new AddPartitionsToTxnResponseData().setResultsByTopicV3AndBelow(result.topicResults()).setThrottleTimeMs(throttleMs);
        return new AddPartitionsToTxnResponse(data);
    }
}

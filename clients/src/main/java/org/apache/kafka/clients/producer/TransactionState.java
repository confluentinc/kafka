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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddOffsetsToTxnRequest;
import org.apache.kafka.common.requests.AddOffsetsToTxnResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnRequest;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.InitPidRequest;
import org.apache.kafka.common.requests.InitPidResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    private static final Logger log = LoggerFactory.getLogger(TransactionState.class);

    private volatile PidAndEpoch pidAndEpoch;
    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final Time time;
    private final String transactionalId;
    private final int transactionTimeoutMs;
    private final PriorityQueue<TransactionalRequest> pendingTransactionalRequests;
    private final Set<TopicPartition> newPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> pendingPartitionsToBeAddedToTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private final Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> pendingTxnOffsetCommits;
    private int inFlightRequestCorrelationId = Integer.MIN_VALUE;

    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;
    private volatile boolean isInTransaction = false;
    private volatile boolean isCompletingTransaction = false;
    private volatile boolean isInitializing = false;

    public TransactionState(Time time, String transactionalId, int transactionTimeoutMs) {
        pidAndEpoch = new PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        sequenceNumbers = new HashMap<>();
        this.time = time;
        this.transactionalId = transactionalId;
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.pendingTransactionalRequests = new PriorityQueue<>(2, new Comparator<TransactionalRequest>() {
            @Override
            public int compare(TransactionalRequest o1, TransactionalRequest o2) {
                return o1.priority() - o2.priority;
            }
        });
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsToBeAddedToTransaction = new HashSet<>();
        this.pendingPartitionsToBeAddedToTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingTxnOffsetCommits = new HashMap<>();
    }

    public static class TransactionalRequest {
        private final AbstractRequest.Builder<?> requestBuilder;

        private final FindCoordinatorRequest.CoordinatorType coordinatorType;
        private final RequestCompletionHandler handler;
        // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
        // a pending FindCoordinator request, that must always go first. Next, If we need a Pid, that must go second.
        // The endTxn request must always go last.
        private final int priority;

        TransactionalRequest(AbstractRequest.Builder<?> requestBuilder, RequestCompletionHandler handler, FindCoordinatorRequest.CoordinatorType coordinatorType, int priority) {
            this.requestBuilder = requestBuilder;
            this.handler = handler;
            this.coordinatorType = coordinatorType;
            this.priority = priority;
        }

        public AbstractRequest.Builder<?> requestBuilder() {
            return requestBuilder;
        }

        public FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return coordinatorType;
        }

        public boolean needsCoordinator() {
            return coordinatorType != FindCoordinatorRequest.CoordinatorType.UNKNOWN;
        }

        public RequestCompletionHandler responseHandler() {
            return handler;
        }

        private int priority() {
            return priority;
        }
    }

    public static class PidAndEpoch {
        public final long producerId;
        public final short epoch;

        PidAndEpoch(long producerId, short epoch) {
            this.producerId = producerId;
            this.epoch = epoch;
        }

        public boolean isValid() {
            return NO_PRODUCER_ID < producerId;
        }
    }

    public boolean hasPendingTransactionalRequests() {
        return !(pendingTransactionalRequests.isEmpty()
                && newPartitionsToBeAddedToTransaction.isEmpty());
    }

    public TransactionalRequest nextTransactionalRequest() {
        if (!hasPendingTransactionalRequests())
            return null;

        if (!newPartitionsToBeAddedToTransaction.isEmpty())
            pendingTransactionalRequests.add(addPartitionsToTransactionRequest());

        return pendingTransactionalRequests.poll();
    }

    public TransactionState(Time time) {
        this(time, "", 0);
    }

    public boolean isTransactional() {
        return !transactionalId.isEmpty();
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasPid() {
        return pidAndEpoch.isValid();
    }

    public void beginTransaction() {
        isInTransaction = true;
    }

    public boolean isCompletingTransaction() {
        return isInTransaction && isCompletingTransaction;
    }

    public synchronized void beginCommittingTransaction() {
        beginCompletingTransaction(true);
    }

    public synchronized void beginAbortingTransaction() {
        beginCompletingTransaction(false);
    }

    private void beginCompletingTransaction(boolean isCommit) {
        if (!isCompletingTransaction) {
            isCompletingTransaction = true;
            if (!pendingTransactionalRequests.isEmpty() || !newPartitionsToBeAddedToTransaction.isEmpty()) {
                pendingTransactionalRequests.add(addPartitionsToTransactionRequest());
            }
            pendingTransactionalRequests.add(endTxnRequest(isCommit));
        }
    }

    public synchronized void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) {
        pendingTransactionalRequests.add(addOffsetsToTxnRequest(offsets, consumerGroupId));
    }

    public boolean isInTransaction() {
        return isTransactional() && isInTransaction;
    }


    public synchronized void maybeAddPartitionToTransaction(TopicPartition topicPartition) {
        if (partitionsInTransaction.contains(topicPartition))
            return;
        newPartitionsToBeAddedToTransaction.add(topicPartition);
    }

    public void needsRetry(TransactionalRequest request) {
        pendingTransactionalRequests.add(request);
    }

    public Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                return null;
        }
    }

    public void needsCoordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                consumerGroupCoordinator = null;
            case TRANSACTION:
                transactionCoordinator = null;
        }
        pendingTransactionalRequests.add(findCoordinatorRequest(type));
    }

    public void setInFlightRequestCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    public void resetInFlightRequestCorrelationId() {
        inFlightRequestCorrelationId = Integer.MIN_VALUE;
    }

    public boolean hasInflightTransactionalRequest() {
        return inFlightRequestCorrelationId != Integer.MIN_VALUE;
    }

    // visible for testing
    public boolean transactionContainsPartition(TopicPartition topicPartition) {
        return isInTransaction && partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    public boolean hasPendingOffsetCommits() {
        return 0 < pendingTxnOffsetCommits.size();
    }

    /**
     * A blocking call to get the pid and epoch for the producer. If the PID and epoch has not been set, this method
     * will block for at most maxWaitTimeMs. It is expected that this method be called from application thread
     * contexts (ie. through Producer.send). The PID it self will be retrieved in the background thread.
     * @param maxWaitTimeMs The maximum time to block.
     * @return a PidAndEpoch object. Callers must call the 'isValid' method fo the returned object to ensure that a
     *         valid Pid and epoch is actually returned.
     */
    public synchronized PidAndEpoch awaitPidAndEpoch(long maxWaitTimeMs) throws InterruptedException {
        if (isInitializing) {
            throw new IllegalStateException("Multiple concurrent calls to initTransactions is not allowed.");
        }
        isInitializing = true;
        if (isTransactional()) {
            if (transactionCoordinator == null)
                pendingTransactionalRequests.add(findCoordinatorRequest(FindCoordinatorRequest.CoordinatorType.TRANSACTION));

            if (!hasPid())
                pendingTransactionalRequests.add(initPidRequest());
        }
        long start = time.milliseconds();
        long elapsed = 0;
        while (!hasPid() && elapsed < maxWaitTimeMs) {
            wait(maxWaitTimeMs);
            elapsed = time.milliseconds() - start;
        }
        return pidAndEpoch;
    }

    /**
     * Get the current pid and epoch without blocking. Callers must use {@link PidAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current PidAndEpoch.
     */
    public PidAndEpoch pidAndEpoch() {
        return pidAndEpoch;
    }

    /**
     * Set the pid and epoch atomically. This method will signal any callers blocked on the `pidAndEpoch` method
     * once the pid is set. This method will be called on the background thread when the broker responds with the pid.
     */
    public synchronized void setPidAndEpoch(long pid, short epoch) {
        this.pidAndEpoch = new PidAndEpoch(pid, epoch);
        if (this.pidAndEpoch.isValid())
            notifyAll();
    }

    /**
     * This method is used when the producer needs to reset it's internal state because of an irrecoverable exception
     * from the broker.
     *
     * We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
     * a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
     * sent to the broker.
     *
     * In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
     * sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
     * messages will return an OutOfOrderSequenceException.
     */
    public synchronized void resetProducerId() {
        if (isTransactional()) {
            // If this is a transactional producer, the user will get an error and should abort the transaction
            // manually.
            return;
        }
        setPidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers.clear();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null) {
            currentSequenceNumber = 0;
            sequenceNumbers.put(topicPartition, currentSequenceNumber);
        }
        return currentSequenceNumber;
    }

    public synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null)
            throw new IllegalStateException("Attempt to increment sequence number for a partition with no current sequence.");

        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }

    private void completeTransaction() {
        partitionsInTransaction.clear();
        isInTransaction = false;
        isCompletingTransaction = false;
    }

    private TransactionalRequest initPidRequest() {
        InitPidRequest.Builder builder = new InitPidRequest.Builder(transactionalId, transactionTimeoutMs);
        return new TransactionalRequest(builder, new InitPidCallback(), FindCoordinatorRequest.CoordinatorType.TRANSACTION, 1);
    }

    private synchronized TransactionalRequest addPartitionsToTransactionRequest() {
        pendingPartitionsToBeAddedToTransaction.addAll(newPartitionsToBeAddedToTransaction);
        newPartitionsToBeAddedToTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder = new AddPartitionsToTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, new ArrayList<>(pendingPartitionsToBeAddedToTransaction));
        return new TransactionalRequest(builder, new AddPartitionsToTransactionCallback(), FindCoordinatorRequest.CoordinatorType.TRANSACTION, 2);
    }

    private TransactionalRequest findCoordinatorRequest(FindCoordinatorRequest.CoordinatorType type) {
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(type, transactionalId);
        return new TransactionalRequest(builder, new FindCoordinatorCallback(type), FindCoordinatorRequest.CoordinatorType.UNKNOWN, 0);
    }

    private TransactionalRequest endTxnRequest(boolean isCommit) {
        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(transactionalId,
                pidAndEpoch.producerId, pidAndEpoch.epoch, isCommit ? TransactionResult.COMMIT : TransactionResult.ABORT);
        return new TransactionalRequest(builder, new EndTxnCallback(), FindCoordinatorRequest.CoordinatorType.TRANSACTION, 3);
    }


    private TransactionalRequest addOffsetsToTxnRequest(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(transactionalId, pidAndEpoch.producerId, pidAndEpoch.epoch, consumerGroupId);
        return new TransactionalRequest(builder, new AddOffsetsToTxnCallback(offsets, consumerGroupId), FindCoordinatorRequest.CoordinatorType.TRANSACTION, 2);
    }

    private TransactionalRequest txnOffsetCommitRequest(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            pendingTxnOffsetCommits.put(entry.getKey(), new TxnOffsetCommitRequest.CommittedOffset(offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }
        return txnOffsetCommitRequest(consumerGroupId);
    }

    private TransactionalRequest txnOffsetCommitRequest(String consumerGroupId) {
        TxnOffsetCommitRequest.Builder builder = new TxnOffsetCommitRequest.Builder(consumerGroupId, pidAndEpoch.producerId, pidAndEpoch.epoch, OffsetCommitRequest.DEFAULT_RETENTION_TIME, pendingTxnOffsetCommits);
        return new TransactionalRequest(builder, new TxnOffsetCommitCallback(consumerGroupId), FindCoordinatorRequest.CoordinatorType.GROUP, 2);
    }

    private abstract class TransactionalRequestCallBack implements RequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId)
                throw new IllegalStateException("Cannot have more than one transactional request in flight.");
            resetInFlightRequestCorrelationId();
            if (response.wasDisconnected()) {
                maybeReenqueue();
            } else if (response.versionMismatch() != null) {
                throw new KafkaException("Could not send " + response + " to node " + response.destination() +
                        " due to a version mismatch. Please ensure that all your brokers have been upgraded before enabling transactions.");
            } else if (response.hasResponse()) {
                handleResponse(response.responseBody());
            } else {
                throw new KafkaException("Failed to send " + response + " for unknown reasons. Please check your broker logs or your network. Cannot use the transactional producer at the moment.");
            }
        }

        protected abstract void handleResponse(AbstractResponse responseBody);

        protected abstract void maybeReenqueue();
    }

    private class InitPidCallback extends TransactionalRequestCallBack {
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            InitPidResponse initPidResponse = (InitPidResponse) responseBody;
            if (initPidResponse.error() == Errors.NONE) {
                setPidAndEpoch(initPidResponse.producerId(), initPidResponse.epoch());
                isInitializing = false;
            } else {
                throw new KafkaException("Need to handle error: " + initPidResponse.error());
            }
        }

        @Override
        protected  void maybeReenqueue() {
            pendingTransactionalRequests.add(initPidRequest());
        }

    }

    private class AddPartitionsToTransactionCallback extends TransactionalRequestCallBack {
        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            if (addPartitionsToTxnResponse.error() == Errors.NONE) {
                partitionsInTransaction.addAll(pendingPartitionsToBeAddedToTransaction);
                pendingPartitionsToBeAddedToTransaction.clear();
            }
        }

        @Override
        protected void maybeReenqueue() {
            // no need to reenqueue since the pending partitions will automatically be added back in the next loop.
        }
    }

    private class FindCoordinatorCallback extends TransactionalRequestCallBack {
        private final FindCoordinatorRequest.CoordinatorType type;

        FindCoordinatorCallback(FindCoordinatorRequest.CoordinatorType type) {
            this.type = type;
        }
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) responseBody;
            if (findCoordinatorResponse.error() == Errors.NONE) {
                Node node = findCoordinatorResponse.node();
                switch (type) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                    case TRANSACTION:
                        transactionCoordinator = node;
                }
                log.debug("Found transaction node {} for transactionalId {}", node, transactionalId);
            } else {
                throw new KafkaException("Need to handle error: " + findCoordinatorResponse.error());
            }
        }

        @Override
        protected void maybeReenqueue() {
            pendingTransactionalRequests.add(findCoordinatorRequest(type));
        }
    }

    private class EndTxnCallback extends TransactionalRequestCallBack {
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) responseBody;
            if (endTxnResponse.error() == Errors.NONE) {
                completeTransaction();
            }
        }

        @Override
        protected void maybeReenqueue() {

        }
    }

    private class AddOffsetsToTxnCallback extends TransactionalRequestCallBack {
        String consumerGroupId;
        Map<TopicPartition, OffsetAndMetadata> offsets;

        public AddOffsetsToTxnCallback(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId) {
            this.offsets = offsets;
            this.consumerGroupId = consumerGroupId;
        }
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) responseBody;
            if (addOffsetsToTxnResponse.error() == Errors.NONE) {
                consumerGroupCoordinator = addOffsetsToTxnResponse.consumerGroupCoordinator();
                pendingTransactionalRequests.add(txnOffsetCommitRequest(offsets, consumerGroupId));
            }
        }

        @Override
        protected void maybeReenqueue() {

        }
    }

    private class TxnOffsetCommitCallback extends TransactionalRequestCallBack {
        private final String consumerGroupId;
        public TxnOffsetCommitCallback(String consumerGroupId) {
            this.consumerGroupId = consumerGroupId;
        }
        @Override
        protected void handleResponse(AbstractResponse responseBody) {
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) responseBody;
            for (Map.Entry<TopicPartition, Errors> entry : txnOffsetCommitResponse.errors().entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                }
            }
            if (0 < pendingTxnOffsetCommits.size()) {
                pendingTransactionalRequests.add(txnOffsetCommitRequest(consumerGroupId));
            }
        }

        @Override
        protected void maybeReenqueue() {

        }
    }

}

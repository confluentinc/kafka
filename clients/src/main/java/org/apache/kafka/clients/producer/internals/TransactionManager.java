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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TransactionAbortableException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
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
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest.CommittedOffset;
import org.apache.kafka.common.requests.TxnOffsetCommitResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionManager {
    private static final int NO_INFLIGHT_REQUEST_CORRELATION_ID = -1;

    private final Logger log;
    private final String transactionalId;
    private final int transactionTimeoutMs;
    private final ApiVersions apiVersions;

    private final TxnPartitionMap txnPartitionMap;

    private final Map<TopicPartition, CommittedOffset> pendingTxnOffsetCommits;

    // If a batch bound for a partition expired locally after being sent at least once, the partition is considered
    // to have an unresolved state. We keep track of such partitions here, and cannot assign any more sequence numbers
    // for this partition until the unresolved state gets cleared. This may happen if other inflight batches returned
    // successfully (indicating that the expired batch actually made it to the broker). If we don't get any successful
    // responses for the partition once the inflight request count falls to zero, we reset the producer id and
    // consequently clear this data structure as well.
    // The value of the map is the sequence number of the batch following the expired one, computed by adding its
    // record count to its sequence number. This is used to tell if a subsequent batch is the one immediately following
    // the expired one.
    private final Map<TopicPartition, Integer> partitionsWithUnresolvedSequences;

    // The partitions that have received an error that triggers an epoch bump. When the epoch is bumped, these
    // partitions will have the sequences of their in-flight batches rewritten
    private final Set<TopicPartition> partitionsToRewriteSequences;

    private final PriorityQueue<TxnRequestHandler> pendingRequests;
    private final Set<TopicPartition> newPartitionsInTransaction;
    private final Set<TopicPartition> pendingPartitionsInTransaction;
    private final Set<TopicPartition> partitionsInTransaction;
    private PendingStateTransition pendingTransition;

    // This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
    // For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
    // error for the first AddPartitionsRequest in a transaction.
    private final long retryBackoffMs;

    // The retryBackoff is overridden to the following value if the first AddPartitions receives a
    // CONCURRENT_TRANSACTIONS error.
    private static final long ADD_PARTITIONS_RETRY_BACKOFF_MS = 20L;

    private int inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    private Node transactionCoordinator;
    private Node consumerGroupCoordinator;
    private boolean coordinatorSupportsBumpingEpoch;

    private volatile State currentState = State.UNINITIALIZED;
    private volatile RuntimeException lastError = null;
    private volatile ProducerIdAndEpoch producerIdAndEpoch;
    private volatile boolean transactionStarted = false;
    private volatile boolean clientSideEpochBumpRequired = false;
    private volatile long latestFinalizedFeaturesEpoch = -1;
    private volatile boolean isTransactionV2Enabled = false;
    private final boolean enable2PC;
    private volatile ProducerIdAndEpoch preparedTxnState = ProducerIdAndEpoch.NONE;

    private enum State {
        UNINITIALIZED,
        INITIALIZING,
        READY,
        IN_TRANSACTION,
        PREPARED_TRANSACTION,
        COMMITTING_TRANSACTION,
        ABORTING_TRANSACTION,
        ABORTABLE_ERROR,
        FATAL_ERROR;

        private boolean isTransitionValid(State source, State target) {
            switch (target) {
                case UNINITIALIZED:
                    return source == READY || source == ABORTABLE_ERROR;
                case INITIALIZING:
                    return source == UNINITIALIZED || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case READY:
                    return source == INITIALIZING || source == COMMITTING_TRANSACTION || source == ABORTING_TRANSACTION;
                case IN_TRANSACTION:
                    return source == READY;
                case PREPARED_TRANSACTION:
                    return source == IN_TRANSACTION || source == INITIALIZING;
                case COMMITTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == PREPARED_TRANSACTION;
                case ABORTING_TRANSACTION:
                    return source == IN_TRANSACTION || source == PREPARED_TRANSACTION || source == ABORTABLE_ERROR;
                case ABORTABLE_ERROR:
                    return source == IN_TRANSACTION || source == COMMITTING_TRANSACTION || source == ABORTABLE_ERROR
                            || source == INITIALIZING;
                case FATAL_ERROR:
                default:
                    // We can transition to FATAL_ERROR unconditionally.
                    // FATAL_ERROR is never a valid starting state for any transition. So the only option is to close the
                    // producer or do purely non transactional requests.
                    return true;
            }
        }
    }

    // We use the priority to determine the order in which requests need to be sent out. For instance, if we have
    // a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
    // The endTxn request must always go last, unless we are bumping the epoch (a special case of InitProducerId) as
    // part of ending the transaction.
    private enum Priority {
        FIND_COORDINATOR(0),
        INIT_PRODUCER_ID(1),
        ADD_PARTITIONS_OR_OFFSETS(2),
        END_TXN(3),
        EPOCH_BUMP(4);

        final int priority;

        Priority(int priority) {
            this.priority = priority;
        }
    }

    public TransactionManager(final LogContext logContext,
                              final String transactionalId,
                              final int transactionTimeoutMs,
                              final long retryBackoffMs,
                              final ApiVersions apiVersions,
                              final boolean enable2PC) {
        this.producerIdAndEpoch = ProducerIdAndEpoch.NONE;
        this.transactionalId = transactionalId;
        this.log = logContext.logger(TransactionManager.class);
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.transactionCoordinator = null;
        this.consumerGroupCoordinator = null;
        this.newPartitionsInTransaction = new HashSet<>();
        this.pendingPartitionsInTransaction = new HashSet<>();
        this.partitionsInTransaction = new HashSet<>();
        this.pendingRequests = new PriorityQueue<>(10, Comparator.comparingInt(o -> o.priority().priority));
        this.pendingTxnOffsetCommits = new HashMap<>();
        this.partitionsWithUnresolvedSequences = new HashMap<>();
        this.partitionsToRewriteSequences = new HashSet<>();
        this.retryBackoffMs = retryBackoffMs;
        this.txnPartitionMap = new TxnPartitionMap(logContext);
        this.apiVersions = apiVersions;
        this.enable2PC = enable2PC;
    }

    /**
     * During its normal course of operations, the transaction manager transitions through different internal
     * states (i.e. by updating {@link #currentState}) to one of those defined in {@link State}. These state transitions
     * result from actions on one of the following classes of threads:
     *
     * <ul>
     *     <li><em>Application</em> threads that invokes {@link Producer} API calls</li>
     *     <li><em>{@link Sender}</em> thread operations</li>
     * </ul>
     *
     * When an invalid state transition is detected during execution on an <em>application</em> thread, the
     * {@link #currentState} is <em>not updated</em> and an {@link IllegalStateException} is thrown. This gives the
     * application the opportunity to fix the issue without permanently poisoning the state of the
     * transaction manager. The {@link Producer} API calls that perform a state transition include:
     *
     * <ul>
     *     <li>{@link Producer#initTransactions()} calls {@link #initializeTransactions(boolean)}</li>
     *     <li>{@link Producer#beginTransaction()} calls {@link #beginTransaction()}</li>
     *     <li>{@link Producer#commitTransaction()}} calls {@link #beginCommit()}</li>
     *     <li>{@link Producer#abortTransaction()} calls {@link #beginAbort()}
     *     </li>
     *     <li>{@link Producer#sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} calls
     *         {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}
     *     </li>
     *     <li>{@link Producer#send(ProducerRecord)} (and its variants) calls
     *         {@link #maybeAddPartition(TopicPartition)} and
     *         {@link #maybeTransitionToErrorState(RuntimeException)}
     *     </li>
     * </ul>
     *
     * <p/>
     *
     * The {@link Producer} is implemented such that much of its work delegated to and performed asynchronously on the
     * <em>{@link Sender}</em> thread. This includes record batching, network I/O, broker response handlers, etc. If an
     * invalid state transition is detected in the <em>{@link Sender}</em> thread, in addition to throwing an
     * {@link IllegalStateException}, the transaction manager intentionally "poisons" itself by setting its
     * {@link #currentState} to {@link State#FATAL_ERROR}, a state from which it cannot recover.
     *
     * <p/>
     *
     * It's important to prevent possible corruption when the transaction manager has determined that it is in a
     * fatal state. Subsequent transaction operations attempted via either the <em>application</em> or the
     * <em>{@link Sender}</em> thread should fail. This is achieved when these operations invoke the
     * {@link #maybeFailWithError()} method, as it causes a {@link KafkaException} to be thrown, ensuring the stated
     * transactional guarantees are not violated.
     *
     * <p/>
     *
     * See KAFKA-14831 for more detail.
     *
     * @return {@code true} to set state to {@link State#FATAL_ERROR} before throwing an exception,
     *         {@code false} to throw an exception without first changing the state
     */
    protected boolean shouldPoisonStateOnInvalidTransition() {
        return Thread.currentThread() instanceof Sender.SenderThread;
    }

    synchronized TransactionalRequestResult initializeTransactions(ProducerIdAndEpoch producerIdAndEpoch) {
        return initializeTransactions(producerIdAndEpoch, false);
    }

    public synchronized TransactionalRequestResult initializeTransactions(boolean keepPreparedTxn) {
        return initializeTransactions(ProducerIdAndEpoch.NONE, keepPreparedTxn);
    }

    synchronized TransactionalRequestResult initializeTransactions(
        ProducerIdAndEpoch producerIdAndEpoch,
        boolean keepPreparedTxn
    ) {
        maybeFailWithError();

        boolean isEpochBump = producerIdAndEpoch != ProducerIdAndEpoch.NONE;
        return handleCachedTransactionRequestResult(() -> {
            // If this is an epoch bump, we will transition the state as part of handling the EndTxnRequest
            if (!isEpochBump) {
                transitionTo(State.INITIALIZING);
                log.info("Invoking InitProducerId for the first time in order to acquire a producer ID");
                if (keepPreparedTxn) {
                    log.info("Invoking InitProducerId with keepPreparedTxn set to true for 2PC transactions");
                }
            } else {
                log.info("Invoking InitProducerId with current producer ID and epoch {} in order to bump the epoch", producerIdAndEpoch);
            }
            InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                    .setTransactionalId(transactionalId)
                    .setTransactionTimeoutMs(transactionTimeoutMs)
                    .setProducerId(producerIdAndEpoch.producerId)
                    .setProducerEpoch(producerIdAndEpoch.epoch)
                    .setEnable2Pc(enable2PC)
                    .setKeepPreparedTxn(keepPreparedTxn);

            InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData),
                    isEpochBump);
            enqueueRequest(handler);
            return handler.result;
        }, State.INITIALIZING, "initTransactions");
    }

    public synchronized void beginTransaction() {
        ensureTransactional();
        throwIfPendingState("beginTransaction");
        maybeFailWithError();
        transitionTo(State.IN_TRANSACTION);
    }

    /**
     * Prepare a transaction for a two-phase commit.
     * This transitions the transaction to the PREPARED_TRANSACTION state.
     * The preparedTxnState is set with the current producer ID and epoch.
     */
    public synchronized void prepareTransaction() {
        ensureTransactional();
        throwIfPendingState("prepareTransaction");
        maybeFailWithError();
        transitionTo(State.PREPARED_TRANSACTION);
        this.preparedTxnState = new ProducerIdAndEpoch(
            this.producerIdAndEpoch.producerId,
            this.producerIdAndEpoch.epoch
        );
    }

    public synchronized TransactionalRequestResult beginCommit() {
        return handleCachedTransactionRequestResult(() -> {
            maybeFailWithError();
            transitionTo(State.COMMITTING_TRANSACTION);
            return beginCompletingTransaction(TransactionResult.COMMIT);
        }, State.COMMITTING_TRANSACTION, "commitTransaction");
    }

    public synchronized TransactionalRequestResult beginAbort() {
        return handleCachedTransactionRequestResult(() -> {
            if (currentState != State.ABORTABLE_ERROR)
                maybeFailWithError();
            transitionTo(State.ABORTING_TRANSACTION);

            // We're aborting the transaction, so there should be no need to add new partitions
            newPartitionsInTransaction.clear();
            return beginCompletingTransaction(TransactionResult.ABORT);
        }, State.ABORTING_TRANSACTION, "abortTransaction");
    }

    private TransactionalRequestResult beginCompletingTransaction(TransactionResult transactionResult) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        EndTxnRequest.Builder builder = new EndTxnRequest.Builder(
            new EndTxnRequestData()
                .setTransactionalId(transactionalId)
                .setProducerId(producerIdAndEpoch.producerId)
                .setProducerEpoch(producerIdAndEpoch.epoch)
                .setCommitted(transactionResult.id),
            isTransactionV2Enabled
        );

        // Maybe update the transaction version here before we enqueue the EndTxn request so there are no races with
        // completion of the EndTxn request. Since this method may update clientSideEpochBumpRequired, we want to update
        // before the check below, but we also want to call it after the EndTxnRequest.Builder so we complete the transaction
        // with the same version as it started.
        maybeUpdateTransactionV2Enabled(false);

        EndTxnHandler handler = new EndTxnHandler(builder);
        enqueueRequest(handler);

        // If an epoch bump is required for recovery, initialize the transaction after completing the EndTxn request.
        // If we are upgrading to TV2 transactions on the next transaction, also bump the epoch.
        if (clientSideEpochBumpRequired) {
            return initializeTransactions(this.producerIdAndEpoch);
        }

        return handler.result;
    }

    public synchronized TransactionalRequestResult sendOffsetsToTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                            final ConsumerGroupMetadata groupMetadata) {
        ensureTransactional();
        throwIfPendingState("sendOffsetsToTransaction");
        maybeFailWithError();

        if (currentState != State.IN_TRANSACTION) {
            throw new IllegalStateException("Cannot send offsets if a transaction is not in progress " +
                "(currentState= " + currentState + ")");
        }

        // In transaction V2, the client will skip sending AddOffsetsToTxn before sending txnOffsetCommit.
        TxnRequestHandler handler;
        if (isTransactionV2Enabled()) {
            log.debug("Begin adding offsets {} for consumer group {} to transaction with transaction protocol V2", offsets, groupMetadata);
            handler = txnOffsetCommitHandler(null, offsets, groupMetadata);
            transactionStarted = true;
        } else {
            log.debug("Begin adding offsets {} for consumer group {} to transaction", offsets, groupMetadata);
            AddOffsetsToTxnRequest.Builder builder = new AddOffsetsToTxnRequest.Builder(
                    new AddOffsetsToTxnRequestData()
                            .setTransactionalId(transactionalId)
                            .setProducerId(producerIdAndEpoch.producerId)
                            .setProducerEpoch(producerIdAndEpoch.epoch)
                            .setGroupId(groupMetadata.groupId())
            );
            handler = new AddOffsetsToTxnHandler(builder, offsets, groupMetadata);
        }

        enqueueRequest(handler);
        return handler.result;
    }

    public synchronized void maybeAddPartition(TopicPartition topicPartition) {
        maybeFailWithError();
        throwIfPendingState("send");

        if (isTransactional()) {
            if (!hasProducerId()) {
                throw new IllegalStateException("Cannot add partition " + topicPartition +
                    " to transaction before completing a call to initTransactions");
            } else if (currentState != State.IN_TRANSACTION) {
                throw new IllegalStateException("Cannot add partition " + topicPartition +
                    " to transaction while in state  " + currentState);
            } else if (isTransactionV2Enabled()) {
                txnPartitionMap.getOrCreate(topicPartition);
                partitionsInTransaction.add(topicPartition);
                transactionStarted = true;
            } else if (transactionContainsPartition(topicPartition) || isPartitionPendingAdd(topicPartition)) {
                return;
            } else {
                log.debug("Begin adding new partition {} to transaction", topicPartition);
                txnPartitionMap.getOrCreate(topicPartition);
                newPartitionsInTransaction.add(topicPartition);
            }
        }
    }

    RuntimeException lastError() {
        return lastError;
    }

    synchronized boolean isSendToPartitionAllowed(TopicPartition tp) {
        if (hasFatalError())
            return false;
        return !isTransactional() || partitionsInTransaction.contains(tp);
    }

    public String transactionalId() {
        return transactionalId;
    }

    public boolean hasProducerId() {
        return producerIdAndEpoch.isValid();
    }

    public boolean isTransactional() {
        return transactionalId != null;
    }

    /**
     *  Check all the finalized features from apiVersions to verify whether the transaction V2 is enabled.
     *  Sets clientSideEpochBumpRequired if upgrading to V2 since we need to bump the epoch.
     *  This is because V2 no longer adds partitions explicitly and there are some edge cases on upgrade
     *  that can be avoided by fencing the old V1 transaction epoch. For example, we won't consider
     *  partitions from the previous transaction as already added to the new V2 transaction if the epoch is fenced.
     */

    public synchronized void maybeUpdateTransactionV2Enabled(boolean onInitiatialization) {
        if (latestFinalizedFeaturesEpoch >= apiVersions.getMaxFinalizedFeaturesEpoch()) {
            return;
        }
        ApiVersions.FinalizedFeaturesInfo info = apiVersions.getFinalizedFeaturesInfo();
        latestFinalizedFeaturesEpoch = info.finalizedFeaturesEpoch;
        Short transactionVersion = info.finalizedFeatures.get("transaction.version");
        boolean wasTransactionV2Enabled = isTransactionV2Enabled;
        isTransactionV2Enabled = transactionVersion != null && transactionVersion >= 2;
        log.debug("Updating isTV2 enabled to {} with FinalizedFeaturesEpoch {}", isTransactionV2Enabled, latestFinalizedFeaturesEpoch);
        if (!onInitiatialization && !wasTransactionV2Enabled && isTransactionV2Enabled)
            clientSideEpochBumpRequired = true;
    }

    public boolean isTransactionV2Enabled() {
        return isTransactionV2Enabled;
    }

    public boolean is2PCEnabled() {
        return enable2PC;
    }

    synchronized boolean hasPartitionsToAdd() {
        return !newPartitionsInTransaction.isEmpty() || !pendingPartitionsInTransaction.isEmpty();
    }

    synchronized boolean isCompleting() {
        return currentState == State.COMMITTING_TRANSACTION || currentState == State.ABORTING_TRANSACTION;
    }

    synchronized boolean hasError() {
        return currentState == State.ABORTABLE_ERROR || currentState == State.FATAL_ERROR;
    }

    synchronized boolean isAborting() {
        return currentState == State.ABORTING_TRANSACTION;
    }

    synchronized void transitionToAbortableError(RuntimeException exception) {
        if (currentState == State.ABORTING_TRANSACTION) {
            log.debug("Skipping transition to abortable error state since the transaction is already being " +
                    "aborted. Underlying exception: ", exception);
            return;
        }

        log.info("Transiting to abortable error state due to {}", exception.toString());
        transitionTo(State.ABORTABLE_ERROR, exception);
    }

    synchronized void transitionToFatalError(RuntimeException exception) {
        log.info("Transiting to fatal error state due to {}", exception.toString());
        transitionTo(State.FATAL_ERROR, exception);

        if (pendingTransition != null) {
            pendingTransition.result.fail(exception);
        }
    }

    /**
     * Transitions to an abortable error state if the coordinator can handle an abortable error or
     * to a fatal error if not.
     *
     * @param abortableException    The exception in case of an abortable error.
     * @param fatalException        The exception in case of a fatal error.
     */
    private void transitionToAbortableErrorOrFatalError(
        RuntimeException abortableException,
        RuntimeException fatalException
    ) {
        if (canHandleAbortableError()) {
            if (needToTriggerEpochBumpFromClient())
                clientSideEpochBumpRequired = true;
            transitionToAbortableError(abortableException);
        } else {
            transitionToFatalError(fatalException);
        }
    }

    // visible for testing
    synchronized boolean isPartitionPendingAdd(TopicPartition partition) {
        return newPartitionsInTransaction.contains(partition) || pendingPartitionsInTransaction.contains(partition);
    }

    /**
     * Get the current producer id and epoch without blocking. Callers must use {@link ProducerIdAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current ProducerIdAndEpoch.
     */
    ProducerIdAndEpoch producerIdAndEpoch() {
        return producerIdAndEpoch;
    }

    public synchronized void maybeUpdateProducerIdAndEpoch(TopicPartition topicPartition) {
        if (hasFatalError()) {
            log.debug("Ignoring producer ID and epoch update request since the producer is in fatal error state");
            return;
        }

        if (hasStaleProducerIdAndEpoch(topicPartition) && !hasInflightBatches(topicPartition)) {
            // If the batch was on a different ID and/or epoch (due to an epoch bump) and all its in-flight batches
            // have completed, reset the partition sequence so that the next batch (with the new epoch) starts from 0
            txnPartitionMap.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            log.debug("ProducerId of partition {} set to {} with epoch {}. Reinitialize sequence at beginning.",
                      topicPartition, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        }
    }

    /**
     * Set the producer id and epoch atomically.
     */
    private void setProducerIdAndEpoch(ProducerIdAndEpoch producerIdAndEpoch) {
        // With TV2, the epoch bump is common and frequent. Only log if it is at debug level or the producer ID is changed.
        if (!isTransactional() || !isTransactionV2Enabled || producerIdAndEpoch.producerId != this.producerIdAndEpoch.producerId) {
            log.info("ProducerId set to {} with epoch {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        } else {
            log.debug("ProducerId set to {} with epoch {}", producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
        }
        this.producerIdAndEpoch = producerIdAndEpoch;
    }

    /**
     * This method resets the producer ID and epoch and sets the state to UNINITIALIZED, which will trigger a new
     * InitProducerId request. This method is only called when the producer epoch is exhausted; we will bump the epoch
     * instead.
     */
    private void resetIdempotentProducerId() {
        if (isTransactional())
            throw new IllegalStateException("Cannot reset producer state for a transactional producer. " +
                    "You must either abort the ongoing transaction or reinitialize the transactional producer instead");
        log.debug("Resetting idempotent producer ID. ID and epoch before reset are {}", this.producerIdAndEpoch);
        setProducerIdAndEpoch(ProducerIdAndEpoch.NONE);
        transitionTo(State.UNINITIALIZED);
    }

    private void resetSequenceForPartition(TopicPartition topicPartition) {
        txnPartitionMap.remove(topicPartition);
        this.partitionsWithUnresolvedSequences.remove(topicPartition);
    }

    private void resetSequenceNumbers() {
        txnPartitionMap.reset();
        this.partitionsWithUnresolvedSequences.clear();
    }

    /**
     * This method is used to trigger an epoch bump for non-transactional idempotent producers.
     */
    synchronized void requestIdempotentEpochBumpForPartition(TopicPartition tp) {
        clientSideEpochBumpRequired = true;
        this.partitionsToRewriteSequences.add(tp);
    }

    private void bumpIdempotentProducerEpoch() {
        if (this.producerIdAndEpoch.epoch == Short.MAX_VALUE) {
            resetIdempotentProducerId();
        } else {
            setProducerIdAndEpoch(new ProducerIdAndEpoch(this.producerIdAndEpoch.producerId, (short) (this.producerIdAndEpoch.epoch + 1)));
            log.debug("Incremented producer epoch, current producer ID and epoch are now {}", this.producerIdAndEpoch);
        }

        // When the epoch is bumped, rewrite all in-flight sequences for the partition(s) that triggered the epoch bump
        for (TopicPartition topicPartition : this.partitionsToRewriteSequences) {
            this.txnPartitionMap.startSequencesAtBeginning(topicPartition, this.producerIdAndEpoch);
            this.partitionsWithUnresolvedSequences.remove(topicPartition);
        }
        this.partitionsToRewriteSequences.clear();

        clientSideEpochBumpRequired = false;
    }

    synchronized void bumpIdempotentEpochAndResetIdIfNeeded() {
        if (!isTransactional()) {
            if (clientSideEpochBumpRequired) {
                bumpIdempotentProducerEpoch();
            }
            if (currentState != State.INITIALIZING && !hasProducerId()) {
                transitionTo(State.INITIALIZING);
                InitProducerIdRequestData requestData = new InitProducerIdRequestData()
                        .setTransactionalId(null)
                        .setTransactionTimeoutMs(Integer.MAX_VALUE);
                InitProducerIdHandler handler = new InitProducerIdHandler(new InitProducerIdRequest.Builder(requestData), false);
                enqueueRequest(handler);
            }
        }
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    synchronized int sequenceNumber(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).nextSequence();
    }

    /**
     * Returns the current producer id/epoch of the given TopicPartition.
     */
    synchronized ProducerIdAndEpoch producerIdAndEpoch(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch();
    }

    synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        txnPartitionMap.get(topicPartition).incrementSequence(increment);
    }

    synchronized void addInFlightBatch(ProducerBatch batch) {
        if (!batch.hasSequence())
            throw new IllegalStateException("Can't track batch for partition " + batch.topicPartition + " when sequence is not set.");
        txnPartitionMap.get(batch.topicPartition).addInflightBatch(batch);
    }

    /**
     * Returns the first inflight sequence for a given partition. This is the base sequence of an inflight batch with
     * the lowest sequence number.
     * @return the lowest inflight sequence if the transaction manager is tracking inflight requests for this partition.
     *         If there are no inflight requests being tracked for this partition, this method will return
     *         RecordBatch.NO_SEQUENCE.
     */
    synchronized int firstInFlightSequence(TopicPartition topicPartition) {
        if (!hasInflightBatches(topicPartition))
            return RecordBatch.NO_SEQUENCE;
        ProducerBatch batch = nextBatchBySequence(topicPartition);
        return batch == null ? RecordBatch.NO_SEQUENCE : batch.baseSequence();
    }

    synchronized ProducerBatch nextBatchBySequence(TopicPartition topicPartition) {
        return txnPartitionMap.nextBatchBySequence(topicPartition);
    }

    synchronized void removeInFlightBatch(ProducerBatch batch) {
        if (hasInflightBatches(batch.topicPartition))
            txnPartitionMap.removeInFlightBatch(batch);
    }

    private int maybeUpdateLastAckedSequence(TopicPartition topicPartition, int sequence) {
        return txnPartitionMap.maybeUpdateLastAckedSequence(topicPartition, sequence);
    }

    synchronized OptionalInt lastAckedSequence(TopicPartition topicPartition) {
        return txnPartitionMap.lastAckedSequence(topicPartition);
    }

    synchronized OptionalLong lastAckedOffset(TopicPartition topicPartition) {
        return txnPartitionMap.lastAckedOffset(topicPartition);
    }

    private void updateLastAckedOffset(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        if (response.baseOffset == ProduceResponse.INVALID_OFFSET)
            return;
        long lastOffset = response.baseOffset + batch.recordCount - 1;
        txnPartitionMap.updateLastAckedOffset(batch.topicPartition, isTransactional(), lastOffset);
    }

    public synchronized void handleCompletedBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        int lastAckedSequence = maybeUpdateLastAckedSequence(batch.topicPartition, batch.lastSequence());
        log.trace("ProducerId: {}; Set last ack'd sequence number for topic-partition {} to {}",
                batch.producerId(),
                batch.topicPartition,
                lastAckedSequence);

        updateLastAckedOffset(response, batch);
        removeInFlightBatch(batch);
    }

    public synchronized void transitionToUninitialized(RuntimeException exception) {
        transitionTo(State.UNINITIALIZED);
        if (pendingTransition != null) {
            pendingTransition.result.fail(exception);
        }
        lastError = null;
    }

    public synchronized void maybeTransitionToErrorState(RuntimeException exception) {
        if (exception instanceof ClusterAuthorizationException
                || exception instanceof TransactionalIdAuthorizationException
                || exception instanceof ProducerFencedException
                || exception instanceof UnsupportedVersionException
                || exception instanceof InvalidPidMappingException) {
            transitionToFatalError(exception);
        } else if (isTransactional()) {
            // RetriableExceptions from the Sender thread are converted to Abortable errors
            // because they indicate that the transaction cannot be completed after all retry attempts.
            // This conversion ensures the application layer treats these errors as abortable,
            // preventing duplicate message delivery.
            if (exception instanceof RetriableException ||
                    exception instanceof InvalidTxnStateException) {
                exception = new TransactionAbortableException("Transaction Request was aborted after exhausting retries.", exception);
            }

            if (needToTriggerEpochBumpFromClient() && !isCompleting()) {
                clientSideEpochBumpRequired = true;
            }
            transitionToAbortableError(exception);
        }
    }

    synchronized void handleFailedBatch(ProducerBatch batch, RuntimeException exception, boolean adjustSequenceNumbers) {
        maybeTransitionToErrorState(exception);
        removeInFlightBatch(batch);

        if (hasFatalError()) {
            log.debug("Ignoring batch {} with producer id {}, epoch {}, and sequence number {} " +
                            "since the producer is already in fatal error state", batch, batch.producerId(),
                    batch.producerEpoch(), batch.baseSequence(), exception);
            return;
        }

        if (exception instanceof OutOfOrderSequenceException && !isTransactional()) {
            log.error("The broker returned {} for topic-partition {} with producerId {}, epoch {}, and sequence number {}",
                    exception, batch.topicPartition, batch.producerId(), batch.producerEpoch(), batch.baseSequence());

            // If we fail with an OutOfOrderSequenceException, we have a gap in the log. Bump the epoch for this
            // partition, which will reset the sequence number to 0 and allow us to continue
            requestIdempotentEpochBumpForPartition(batch.topicPartition);
        } else if (exception instanceof UnknownProducerIdException) {
            // If we get an UnknownProducerId for a partition, then the broker has no state for that producer. It will
            // therefore accept a write with sequence number 0. We reset the sequence number for the partition here so
            // that the producer can continue after aborting the transaction. All inflight-requests to this partition
            // will also fail with an UnknownProducerId error, so the sequence will remain at 0. Note that if the
            // broker supports bumping the epoch, we will later reset all sequence numbers after calling InitProducerId
            resetSequenceForPartition(batch.topicPartition);
        } else {
            if (adjustSequenceNumbers) {
                if (!isTransactional()) {
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                } else {
                    txnPartitionMap.adjustSequencesDueToFailedBatch(batch);
                }
            }
        }
    }

    synchronized boolean hasInflightBatches(TopicPartition topicPartition) {
        return txnPartitionMap.getOrCreate(topicPartition).hasInflightBatches();
    }

    synchronized boolean hasStaleProducerIdAndEpoch(TopicPartition topicPartition) {
        return !producerIdAndEpoch.equals(txnPartitionMap.getOrCreate(topicPartition).producerIdAndEpoch());
    }

    synchronized boolean hasUnresolvedSequences() {
        return !partitionsWithUnresolvedSequences.isEmpty();
    }

    synchronized boolean hasUnresolvedSequence(TopicPartition topicPartition) {
        return partitionsWithUnresolvedSequences.containsKey(topicPartition);
    }

    synchronized void markSequenceUnresolved(ProducerBatch batch) {
        int nextSequence = batch.lastSequence() + 1;
        partitionsWithUnresolvedSequences.compute(batch.topicPartition,
            (k, v) -> v == null ? nextSequence : Math.max(v, nextSequence));
        log.debug("Marking partition {} unresolved with next sequence number {}", batch.topicPartition,
                partitionsWithUnresolvedSequences.get(batch.topicPartition));
    }

    // Attempts to resolve unresolved sequences. If all in-flight requests are complete and some partitions are still
    // unresolved, either bump the epoch if possible, or transition to a fatal error
    synchronized void maybeResolveSequences() {
        for (Iterator<TopicPartition> iter = partitionsWithUnresolvedSequences.keySet().iterator(); iter.hasNext(); ) {
            TopicPartition topicPartition = iter.next();
            if (!hasInflightBatches(topicPartition)) {
                // The partition has been fully drained. At this point, the last ack'd sequence should be one less than
                // next sequence destined for the partition. If so, the partition is fully resolved. If not, we should
                // reset the sequence number if necessary.
                if (isNextSequence(topicPartition, sequenceNumber(topicPartition))) {
                    // This would happen when a batch was expired, but subsequent batches succeeded.
                    iter.remove();
                } else {
                    // We would enter this branch if all in flight batches were ultimately expired in the producer.
                    if (isTransactional()) {
                        // For the transactional producer, we bump the epoch if possible, otherwise we transition to a fatal error
                        String unackedMessagesErr = "The client hasn't received acknowledgment for some previously " +
                                "sent messages and can no longer retry them. ";
                        KafkaException abortableException = new KafkaException(unackedMessagesErr + "It is safe to abort " +
                                "the transaction and continue.");
                        KafkaException fatalException = new KafkaException(unackedMessagesErr + "It isn't safe to continue.");

                        transitionToAbortableErrorOrFatalError(abortableException, fatalException);
                    } else {
                        // For the idempotent producer, bump the epoch
                        log.info("No inflight batches remaining for {}, last ack'd sequence for partition is {}, next sequence is {}. " +
                                        "Going to bump epoch and reset sequence numbers.", topicPartition,
                                lastAckedSequence(topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER), sequenceNumber(topicPartition));
                        requestIdempotentEpochBumpForPartition(topicPartition);
                    }

                    iter.remove();
                }
            }
        }
    }

    private boolean isNextSequence(TopicPartition topicPartition, int sequence) {
        return sequence - lastAckedSequence(topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER) == 1;
    }

    private boolean isNextSequenceForUnresolvedPartition(TopicPartition topicPartition, int sequence) {
        return this.hasUnresolvedSequence(topicPartition) &&
                sequence == this.partitionsWithUnresolvedSequences.get(topicPartition);
    }

    synchronized TxnRequestHandler nextRequest(boolean hasIncompleteBatches) {
        if (!newPartitionsInTransaction.isEmpty())
            enqueueRequest(addPartitionsToTransactionHandler());

        TxnRequestHandler nextRequestHandler = pendingRequests.peek();
        if (nextRequestHandler == null)
            return null;

        // Do not send the EndTxn until all batches have been flushed
        if (nextRequestHandler.isEndTxn() && hasIncompleteBatches)
            return null;

        pendingRequests.poll();
        if (maybeTerminateRequestWithError(nextRequestHandler)) {
            log.trace("Not sending transactional request {} because we are in an error state",
                    nextRequestHandler.requestBuilder());
            return null;
        }

        if (nextRequestHandler.isEndTxn() && !transactionStarted) {
            nextRequestHandler.result.done();
            if (currentState != State.FATAL_ERROR) {
                if (isTransactionV2Enabled) {
                    log.debug("Not sending EndTxn for completed transaction since no send " +
                            "or sendOffsetsToTransaction were triggered");
                } else {
                    log.debug("Not sending EndTxn for completed transaction since no partitions " +
                            "or offsets were successfully added");
                }
                resetTransactionState();
            }
            nextRequestHandler = pendingRequests.poll();
        }

        if (nextRequestHandler != null)
            log.trace("Request {} dequeued for sending", nextRequestHandler.requestBuilder());

        return nextRequestHandler;
    }

    synchronized void retry(TxnRequestHandler request) {
        request.setRetry();
        enqueueRequest(request);
    }

    synchronized void authenticationFailed(AuthenticationException e) {
        for (TxnRequestHandler request : pendingRequests)
            request.fatalError(e);
    }

    synchronized void failPendingRequests(RuntimeException exception) {
        pendingRequests.forEach(handler ->
                handler.abortableError(exception));
    }

    synchronized void close() {
        KafkaException shutdownException = new KafkaException("The producer closed forcefully");
        pendingRequests.forEach(handler ->
                handler.fatalError(shutdownException));
        if (pendingTransition != null) {
            pendingTransition.result.fail(shutdownException);
        }
    }

    Node coordinator(FindCoordinatorRequest.CoordinatorType type) {
        switch (type) {
            case GROUP:
                return consumerGroupCoordinator;
            case TRANSACTION:
                return transactionCoordinator;
            default:
                throw new IllegalStateException("Received an invalid coordinator type: " + type);
        }
    }

    void lookupCoordinator(TxnRequestHandler request) {
        lookupCoordinator(request.coordinatorType(), request.coordinatorKey());
    }

    void setInFlightCorrelationId(int correlationId) {
        inFlightRequestCorrelationId = correlationId;
    }

    private void clearInFlightCorrelationId() {
        inFlightRequestCorrelationId = NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    boolean hasInFlightRequest() {
        return inFlightRequestCorrelationId != NO_INFLIGHT_REQUEST_CORRELATION_ID;
    }

    // visible for testing.
    boolean hasFatalError() {
        return currentState == State.FATAL_ERROR;
    }

    // visible for testing.
    boolean hasAbortableError() {
        return currentState == State.ABORTABLE_ERROR;
    }

    // visible for testing
    public synchronized boolean transactionContainsPartition(TopicPartition topicPartition) {
        return partitionsInTransaction.contains(topicPartition);
    }

    // visible for testing
    synchronized boolean hasPendingOffsetCommits() {
        return !pendingTxnOffsetCommits.isEmpty();
    }

    synchronized boolean hasPendingRequests() {
        return !pendingRequests.isEmpty();
    }

    // visible for testing
    synchronized boolean hasOngoingTransaction() {
        // transactions are considered ongoing once started until completion or a fatal error
        return currentState == State.IN_TRANSACTION || isCompleting() || hasAbortableError();
    }

    synchronized boolean canRetry(ProduceResponse.PartitionResponse response, ProducerBatch batch) {
        Errors error = response.error;

        // An UNKNOWN_PRODUCER_ID means that we have lost the producer state on the broker. Depending on the log start
        // offset, we may want to retry these, as described for each case below. If none of those apply, then for the
        // idempotent producer, we will locally bump the epoch and reset the sequence numbers of in-flight batches from
        // sequence 0, then retry the failed batch, which should now succeed. For the transactional producer, allow the
        // batch to fail. When processing the failed batch, we will transition to an abortable error and set a flag
        // indicating that we need to bump the epoch (if supported by the broker).
        if (error == Errors.UNKNOWN_PRODUCER_ID) {
            if (response.logStartOffset == -1) {
                // We don't know the log start offset with this response. We should just retry the request until we get it.
                // The UNKNOWN_PRODUCER_ID error code was added along with the new ProduceResponse which includes the
                // logStartOffset. So the '-1' sentinel is not for backward compatibility. Instead, it is possible for
                // a broker to not know the logStartOffset at when it is returning the response because the partition
                // may have moved away from the broker from the time the error was initially raised to the time the
                // response was being constructed. In these cases, we should just retry the request: we are guaranteed
                // to eventually get a logStartOffset once things settle down.
                return true;
            }

            if (batch.sequenceHasBeenReset()) {
                // When the first inflight batch fails due to the truncation case, then the sequences of all the other
                // in flight batches would have been restarted from the beginning. However, when those responses
                // come back from the broker, they would also come with an UNKNOWN_PRODUCER_ID error. In this case, we should not
                // reset the sequence numbers to the beginning.
                return true;
            } else if (lastAckedOffset(batch.topicPartition).orElse(TxnPartitionEntry.NO_LAST_ACKED_SEQUENCE_NUMBER) < response.logStartOffset) {
                // The head of the log has been removed, probably due to the retention time elapsing. In this case,
                // we expect to lose the producer state. For the transactional producer, reset the sequences of all
                // inflight batches to be from the beginning and retry them, so that the transaction does not need to
                // be aborted. For the idempotent producer, bump the epoch to avoid reusing (sequence, epoch) pairs
                if (isTransactional()) {
                    txnPartitionMap.startSequencesAtBeginning(batch.topicPartition, this.producerIdAndEpoch);
                } else {
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }

            if (!isTransactional()) {
                // For the idempotent producer, always retry UNKNOWN_PRODUCER_ID errors. If the batch has the current
                // producer ID and epoch, request a bump of the epoch. Otherwise just retry the produce.
                requestIdempotentEpochBumpForPartition(batch.topicPartition);
                return true;
            }
        } else if (error == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER) {
            if (!hasUnresolvedSequence(batch.topicPartition) &&
                    (batch.sequenceHasBeenReset() || !isNextSequence(batch.topicPartition, batch.baseSequence()))) {
                // We should retry the OutOfOrderSequenceException if the batch is _not_ the next batch, ie. its base
                // sequence isn't the lastAckedSequence + 1.
                return true;
            } else if (!isTransactional()) {
                // For the idempotent producer, retry all OUT_OF_ORDER_SEQUENCE_NUMBER errors. If there are no
                // unresolved sequences, or this batch is the one immediately following an unresolved sequence, we know
                // there is actually a gap in the sequences, and we bump the epoch. Otherwise, retry without bumping
                // and wait to see if the sequence resolves
                if (!hasUnresolvedSequence(batch.topicPartition) ||
                        isNextSequenceForUnresolvedPartition(batch.topicPartition, batch.baseSequence())) {
                    requestIdempotentEpochBumpForPartition(batch.topicPartition);
                }
                return true;
            }
        }

        // If neither of the above cases are true, retry if the exception is retriable
        return error.exception() instanceof RetriableException;
    }

    // visible for testing
    synchronized boolean isReady() {
        return isTransactional() && currentState == State.READY;
    }

    // visible for testing
    synchronized boolean isInitializing() {
        return isTransactional() && currentState == State.INITIALIZING;
    }

    /**
     * Check if the transaction is in the prepared state.
     *
     * @return true if the current state is PREPARED_TRANSACTION
     */
    public synchronized boolean isPrepared() {
        return currentState == State.PREPARED_TRANSACTION;
    }

    void handleCoordinatorReady() {
        NodeApiVersions nodeApiVersions = transactionCoordinator != null ?
                apiVersions.get(transactionCoordinator.idString()) :
                null;
        ApiVersion initProducerIdVersion = nodeApiVersions != null ?
                nodeApiVersions.apiVersion(ApiKeys.INIT_PRODUCER_ID) :
                null;
        this.coordinatorSupportsBumpingEpoch = initProducerIdVersion != null &&
                initProducerIdVersion.maxVersion() >= 3;
    }

    private void transitionTo(State target) {
        transitionTo(target, null);
    }

    private void transitionTo(State target, RuntimeException error) {
        if (!currentState.isTransitionValid(currentState, target)) {
            String idString = transactionalId == null ?  "" : "TransactionalId " + transactionalId + ": ";
            String message = idString + "Invalid transition attempted from state "
                    + currentState.name() + " to state " + target.name();

            if (shouldPoisonStateOnInvalidTransition()) {
                currentState = State.FATAL_ERROR;
                lastError = new IllegalStateException(message);
                throw lastError;
            } else {
                throw new IllegalStateException(message);
            }
        } else if (target == State.FATAL_ERROR || target == State.ABORTABLE_ERROR) {
            if (error == null)
                throw new IllegalArgumentException("Cannot transition to " + target + " with a null exception");
            lastError = error;
        } else {
            lastError = null;
        }

        if (lastError != null)
            log.debug("Transition from state {} to error state {}", currentState, target, lastError);
        else
            log.debug("Transition from state {} to {}", currentState, target);

        currentState = target;
    }

    private void ensureTransactional() {
        if (!isTransactional())
            throw new IllegalStateException("Transactional method invoked on a non-transactional producer.");
    }

    private void maybeFailWithError() {
        if (!hasError()) {
            return;
        }
        // for ProducerFencedException, do not wrap it as a KafkaException
        // but create a new instance without the call trace since it was not thrown because of the current call
        if (lastError instanceof ProducerFencedException) {
            throw new ProducerFencedException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " has been fenced by another producer " +
                    "with the same transactionalId");
        }
        if (lastError instanceof InvalidProducerEpochException) {
            throw new InvalidProducerEpochException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " attempted to produce with an old epoch");
        }
        if (lastError instanceof IllegalStateException) {
            throw new IllegalStateException("Producer with transactionalId '" + transactionalId
                    + "' and " + producerIdAndEpoch + " cannot execute transactional method because of previous invalid state transition attempt", lastError);
        }
        throw new KafkaException("Cannot execute transactional method because we are in an error state", lastError);
    }

    private boolean maybeTerminateRequestWithError(TxnRequestHandler requestHandler) {
        if (hasError()) {
            if (hasAbortableError() && requestHandler instanceof FindCoordinatorHandler)
                // No harm letting the FindCoordinator request go through if we're expecting to abort
                return false;

            requestHandler.fail(lastError);
            return true;
        }
        return false;
    }

    private void enqueueRequest(TxnRequestHandler requestHandler) {
        log.debug("Enqueuing transactional request {}", requestHandler.requestBuilder());
        pendingRequests.add(requestHandler);
    }

    private void lookupCoordinator(FindCoordinatorRequest.CoordinatorType type, String coordinatorKey) {
        switch (type) {
            case GROUP:
                consumerGroupCoordinator = null;
                break;
            case TRANSACTION:
                transactionCoordinator = null;
                break;
            default:
                throw new IllegalStateException("Invalid coordinator type: " + type);
        }

        FindCoordinatorRequestData data = new FindCoordinatorRequestData()
                .setKeyType(type.id())
                .setKey(coordinatorKey);
        FindCoordinatorRequest.Builder builder = new FindCoordinatorRequest.Builder(data);
        enqueueRequest(new FindCoordinatorHandler(builder));
    }

    private TxnRequestHandler addPartitionsToTransactionHandler() {
        pendingPartitionsInTransaction.addAll(newPartitionsInTransaction);
        newPartitionsInTransaction.clear();
        AddPartitionsToTxnRequest.Builder builder =
            AddPartitionsToTxnRequest.Builder.forClient(transactionalId,
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                new ArrayList<>(pendingPartitionsInTransaction));
        return new AddPartitionsToTxnHandler(builder);
    }

    private TxnOffsetCommitHandler txnOffsetCommitHandler(TransactionalRequestResult result,
                                                          Map<TopicPartition, OffsetAndMetadata> offsets,
                                                          ConsumerGroupMetadata groupMetadata) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            CommittedOffset committedOffset = new CommittedOffset(offsetAndMetadata.offset(),
                    offsetAndMetadata.metadata(), offsetAndMetadata.leaderEpoch());
            pendingTxnOffsetCommits.put(entry.getKey(), committedOffset);
        }

        final TxnOffsetCommitRequest.Builder builder =
            new TxnOffsetCommitRequest.Builder(transactionalId,
                groupMetadata.groupId(),
                producerIdAndEpoch.producerId,
                producerIdAndEpoch.epoch,
                pendingTxnOffsetCommits,
                groupMetadata.memberId(),
                groupMetadata.generationId(),
                groupMetadata.groupInstanceId(),
                isTransactionV2Enabled()
            );
        if (result == null) {
            // In this case, transaction V2 is in use.
            return new TxnOffsetCommitHandler(builder);
        }
        return new TxnOffsetCommitHandler(result, builder);
    }

    private void throwIfPendingState(String operation) {
        if (pendingTransition != null) {
            if (pendingTransition.result.isAcked()) {
                pendingTransition = null;
            } else {
                throw new IllegalStateException("Cannot attempt operation `" + operation + "` "
                    + "because the previous call to `" + pendingTransition.operation + "` "
                    + "timed out and must be retried");
            }
        }
    }

    private TransactionalRequestResult handleCachedTransactionRequestResult(
        Supplier<TransactionalRequestResult> transactionalRequestResultSupplier,
        State nextState,
        String operation
    ) {
        ensureTransactional();

        if (pendingTransition != null) {
            if (pendingTransition.result.isAcked()) {
                pendingTransition = null;
            } else if (nextState != pendingTransition.state) {
                throw new IllegalStateException("Cannot attempt operation `" + operation + "` "
                    + "because the previous call to `" + pendingTransition.operation + "` "
                    + "timed out and must be retried");
            } else {
                return pendingTransition.result;
            }
        }

        TransactionalRequestResult result = transactionalRequestResultSupplier.get();
        pendingTransition = new PendingStateTransition(result, nextState, operation);
        return result;
    }

    /**
     * Determines if an epoch bump can be triggered manually based on the api versions.
     *
     * <b>NOTE:</b>
     * This method should only be used for transactional producers.
     * For non-transactional producers epoch bumping is always allowed.
     *
     * <ol>
     *   <li><b>Client-Triggered Epoch Bump</b>:
     *          If the coordinator supports epoch bumping (initProducerIdVersion.maxVersion() >= 3),
     *          client-triggered epoch bumping is allowed, returns true.
     *          <code>clientSideEpochBumpTriggerRequired</code> must be set to true in this case.</li>
     *
     *   <li><b>No Epoch Bump Allowed</b>:
     *          If the coordinator does not support epoch bumping, returns false.</li>
     *
     *   <li><b>Server-Triggered Only</b>:
     *          When TransactionV2 is enabled, epoch bumping is handled automatically
     *          by the server in EndTxn, so manual epoch bumping is not required, returns false.</li>
     * </ol>
     *
     * @return true if a client-triggered epoch bump is allowed, otherwise false.
     */
    // package-private for testing
    boolean needToTriggerEpochBumpFromClient() {
        return coordinatorSupportsBumpingEpoch && !isTransactionV2Enabled;
    }

    /**
     * Determines if the coordinator can handle an abortable error.
     * Recovering from an abortable error requires an epoch bump which can be triggered by the client
     * or automatically taken care of at the end of every transaction (Transaction V2).
     * Use <code>needToTriggerEpochBumpFromClient</code> to check whether the epoch bump needs to be triggered
     * manually.
     *
     * <b>NOTE:</b>
     * This method should only be used for transactional producers.
     * There is no concept of abortable errors for idempotent producers.
     *
     * @return true if an abortable error can be handled, otherwise false.
     */
    boolean canHandleAbortableError() {
        return coordinatorSupportsBumpingEpoch || isTransactionV2Enabled;
    }

    private void resetTransactionState() {
        if (clientSideEpochBumpRequired) {
            transitionTo(State.INITIALIZING);
        } else {
            transitionTo(State.READY);
        }
        lastError = null;
        clientSideEpochBumpRequired = false;
        transactionStarted = false;
        newPartitionsInTransaction.clear();
        pendingPartitionsInTransaction.clear();
        partitionsInTransaction.clear();
        preparedTxnState = ProducerIdAndEpoch.NONE;
    }

    abstract class TxnRequestHandler implements RequestCompletionHandler {
        protected final TransactionalRequestResult result;
        private boolean isRetry = false;

        TxnRequestHandler(TransactionalRequestResult result) {
            this.result = result;
        }

        TxnRequestHandler(String operation) {
            this(new TransactionalRequestResult(operation));
        }

        void fatalError(RuntimeException e) {
            result.fail(e);
            transitionToFatalError(e);
        }

        void abortableError(RuntimeException e) {
            result.fail(e);
            transitionToAbortableError(e);
        }

        /**
         * Determines if an error should be treated as abortable or fatal, based on transaction state and configuration.
         * <ol><l> NOTE: Only use this method for transactional producers </l></ol>
         *
         * - <b>Abortable Error</b>:
         *     An abortable error can be handled effectively, if epoch bumping is supported.
         *     1) If transactionV2 is enabled, automatic epoch bumping happens at the end of every transaction.
         *     2) If the client can trigger an epoch bump, the abortable error can be handled.
         *
         *- <b>Fatal Error</b>:
         *      If epoch bumping is not supported, the system cannot recover and the error must be treated as fatal.
         * @param e the error to determine as either abortable or fatal.
         */
        void abortableErrorIfPossible(RuntimeException e) {
            if (canHandleAbortableError()) {
                if (needToTriggerEpochBumpFromClient())
                    clientSideEpochBumpRequired = true;
                abortableError(e);
            } else {
                fatalError(e);
            }
        }

        void fail(RuntimeException e) {
            result.fail(e);
        }

        void reenqueue() {
            synchronized (TransactionManager.this) {
                this.isRetry = true;
                enqueueRequest(this);
            }
        }

        long retryBackoffMs() {
            return retryBackoffMs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            if (response.requestHeader().correlationId() != inFlightRequestCorrelationId) {
                fatalError(new RuntimeException("Detected more than one in-flight transactional request."));
            } else {
                clearInFlightCorrelationId();
                if (response.wasDisconnected()) {
                    log.debug("Disconnected from {}. Will retry.", response.destination());
                    if (this.needsCoordinator())
                        lookupCoordinator(this.coordinatorType(), this.coordinatorKey());
                    reenqueue();
                } else if (response.versionMismatch() != null) {
                    fatalError(response.versionMismatch());
                } else if (response.hasResponse()) {
                    log.trace("Received transactional response {} for request {}", response.responseBody(),
                            requestBuilder());
                    synchronized (TransactionManager.this) {
                        handleResponse(response.responseBody());
                    }
                } else {
                    fatalError(new KafkaException("Could not execute transactional request for unknown reasons"));
                }
            }
        }

        boolean needsCoordinator() {
            return coordinatorType() != null;
        }

        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
        }

        String coordinatorKey() {
            return transactionalId;
        }

        void setRetry() {
            this.isRetry = true;
        }

        boolean isRetry() {
            return isRetry;
        }

        boolean isEndTxn() {
            return false;
        }

        abstract AbstractRequest.Builder<?> requestBuilder();

        abstract void handleResponse(AbstractResponse responseBody);

        abstract Priority priority();
    }

    private class InitProducerIdHandler extends TxnRequestHandler {
        private final InitProducerIdRequest.Builder builder;
        private final boolean isEpochBump;

        private InitProducerIdHandler(InitProducerIdRequest.Builder builder, boolean isEpochBump) {
            super("InitProducerId");
            this.builder = builder;
            this.isEpochBump = isEpochBump;
        }

        @Override
        InitProducerIdRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return this.isEpochBump ? Priority.EPOCH_BUMP : Priority.INIT_PRODUCER_ID;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            if (isTransactional()) {
                return FindCoordinatorRequest.CoordinatorType.TRANSACTION;
            } else {
                return null;
            }
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            InitProducerIdResponse initProducerIdResponse = (InitProducerIdResponse) response;
            Errors error = initProducerIdResponse.error();

            if (error == Errors.NONE) {
                ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(initProducerIdResponse.data().producerId(),
                        initProducerIdResponse.data().producerEpoch());
                setProducerIdAndEpoch(producerIdAndEpoch);
                // If this is a transaction with keepPreparedTxn=true, transition directly
                // to PREPARED_TRANSACTION state IFF there is an ongoing transaction.
                if (builder.data.keepPreparedTxn() &&
                    initProducerIdResponse.data().ongoingTxnProducerId() != RecordBatch.NO_PRODUCER_ID
                ) {
                    transitionTo(State.PREPARED_TRANSACTION);
                    // Update the preparedTxnState with the ongoing pid and epoch from the response.
                    // This will be used to complete the transaction later.
                    TransactionManager.this.preparedTxnState = new ProducerIdAndEpoch(
                        initProducerIdResponse.data().ongoingTxnProducerId(),
                        initProducerIdResponse.data().ongoingTxnProducerEpoch()
                    );
                } else {
                    transitionTo(State.READY);
                }
                lastError = null;
                if (this.isEpochBump) {
                    resetSequenceNumbers();
                }
                result.done();
            } else if (error == Errors.NOT_COORDINATOR || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.CLUSTER_AUTHORIZATION_FAILED) {
                log.info("Abortable authorization error: {}.  Transition the producer state to {}", error.message(), State.ABORTABLE_ERROR);
                lastError = error.exception();
                abortableError(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                abortableError(error.exception());
            } else {
                fatalError(new KafkaException("Unexpected error in InitProducerIdResponse; " + error.message()));
            }
        }
    }

    private class AddPartitionsToTxnHandler extends TxnRequestHandler {
        private final AddPartitionsToTxnRequest.Builder builder;
        private long retryBackoffMs;

        private AddPartitionsToTxnHandler(AddPartitionsToTxnRequest.Builder builder) {
            super("AddPartitionsToTxn");
            this.builder = builder;
            this.retryBackoffMs = TransactionManager.this.retryBackoffMs;
        }

        @Override
        AddPartitionsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;
            Map<TopicPartition, Errors> errors = addPartitionsToTxnResponse.errors().get(AddPartitionsToTxnResponse.V3_AND_BELOW_TXN_ID);
            boolean hasPartitionErrors = false;
            Set<String> unauthorizedTopics = new HashSet<>();
            retryBackoffMs = TransactionManager.this.retryBackoffMs;

            for (Map.Entry<TopicPartition, Errors> topicPartitionErrorEntry : errors.entrySet()) {
                TopicPartition topicPartition = topicPartitionErrorEntry.getKey();
                Errors error = topicPartitionErrorEntry.getValue();

                if (error == Errors.NONE) {
                    continue;
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                    lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                    reenqueue();
                    return;
                } else if (error == Errors.CONCURRENT_TRANSACTIONS) {
                    maybeOverrideRetryBackoffMs();
                    reenqueue();
                    return;
                } else if (error.exception() instanceof RetriableException) {
                    reenqueue();
                    return;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                    // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                    // just treat it the same as PRODUCE_FENCED.
                    fatalError(Errors.PRODUCER_FENCED.exception());
                    return;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                        error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                    fatalError(error.exception());
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(topicPartition.topic());
                } else if (error == Errors.OPERATION_NOT_ATTEMPTED) {
                    log.debug("Did not attempt to add partition {} to transaction because other partitions in the " +
                            "batch had errors.", topicPartition);
                    hasPartitionErrors = true;
                } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                    abortableErrorIfPossible(error.exception());
                    return;
                } else if (error == Errors.TRANSACTION_ABORTABLE) {
                    abortableError(error.exception());
                    return;
                } else {
                    log.error("Could not add partition {} due to unexpected error {}", topicPartition, error);
                    hasPartitionErrors = true;
                }
            }

            Set<TopicPartition> partitions = errors.keySet();

            // Remove the partitions from the pending set regardless of the result. We use the presence
            // of partitions in the pending set to know when it is not safe to send batches. However, if
            // the partitions failed to be added and we enter an error state, we expect the batches to be
            // aborted anyway. In this case, we must be able to continue sending the batches which are in
            // retry for partitions that were successfully added.
            pendingPartitionsInTransaction.removeAll(partitions);

            if (!unauthorizedTopics.isEmpty()) {
                abortableError(new TopicAuthorizationException(unauthorizedTopics));
            } else if (hasPartitionErrors) {
                abortableError(new KafkaException("Could not add partitions to transaction due to errors: " + errors));
            } else {
                log.debug("Successfully added partitions {} to transaction", partitions);
                partitionsInTransaction.addAll(partitions);
                transactionStarted = true;
                result.done();
            }
        }

        @Override
        public long retryBackoffMs() {
            return Math.min(TransactionManager.this.retryBackoffMs, this.retryBackoffMs);
        }

        private void maybeOverrideRetryBackoffMs() {
            // We only want to reduce the backoff when retrying the first AddPartition which errored out due to a
            // CONCURRENT_TRANSACTIONS error since this means that the previous transaction is still completing and
            // we don't want to wait too long before trying to start the new one.
            //
            // This is only a temporary fix, the long term solution is being tracked in
            // https://issues.apache.org/jira/browse/KAFKA-5482
            if (partitionsInTransaction.isEmpty())
                this.retryBackoffMs = ADD_PARTITIONS_RETRY_BACKOFF_MS;
        }
    }

    private class FindCoordinatorHandler extends TxnRequestHandler {
        private final FindCoordinatorRequest.Builder builder;

        private FindCoordinatorHandler(FindCoordinatorRequest.Builder builder) {
            super("FindCoordinator");
            this.builder = builder;
        }

        @Override
        FindCoordinatorRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.FIND_COORDINATOR;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return null;
        }

        @Override
        String coordinatorKey() {
            return null;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            CoordinatorType coordinatorType = CoordinatorType.forId(builder.data().keyType());

            List<Coordinator> coordinators = ((FindCoordinatorResponse) response).coordinators();
            if (coordinators.size() != 1) {
                log.error("Group coordinator lookup failed: Invalid response containing more than a single coordinator");
                fatalError(new IllegalStateException("Group coordinator lookup failed: Invalid response containing more than a single coordinator"));
            }
            Coordinator coordinatorData = coordinators.get(0);
            // For older versions without batching, obtain key from request data since it is not included in response
            String key = coordinatorData.key() == null ? builder.data().key() : coordinatorData.key();
            Errors error = Errors.forCode(coordinatorData.errorCode());
            if (error == Errors.NONE) {
                Node node = new Node(coordinatorData.nodeId(), coordinatorData.host(), coordinatorData.port());
                switch (coordinatorType) {
                    case GROUP:
                        consumerGroupCoordinator = node;
                        break;
                    case TRANSACTION:
                        transactionCoordinator = node;
                        break;
                    default:
                        log.error("Group coordinator lookup failed: Unexpected coordinator type in response");
                        fatalError(new IllegalStateException("Group coordinator lookup failed: Unexpected coordinator type in response"));
                }
                result.done();
                log.info("Discovered {} coordinator {}", coordinatorType.toString().toLowerCase(Locale.ROOT), node);
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(key));
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                abortableError(error.exception());
            } else {
                fatalError(new KafkaException(String.format("Could not find a coordinator with type %s with key %s due to " +
                        "unexpected error: %s", coordinatorType, key,
                        coordinatorData.errorMessage())));
            }
        }
    }

    private class EndTxnHandler extends TxnRequestHandler {
        private final EndTxnRequest.Builder builder;

        private EndTxnHandler(EndTxnRequest.Builder builder) {
            super("EndTxn(" + builder.data.committed() + ")");
            this.builder = builder;
        }

        @Override
        EndTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.END_TXN;
        }

        @Override
        boolean isEndTxn() {
            return true;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            EndTxnResponse endTxnResponse = (EndTxnResponse) response;
            Errors error = endTxnResponse.error();
            boolean isAbort = !builder.data.committed();
            if (error == Errors.NONE) {
                // For End Txn version 5+, the broker includes the producerId and producerEpoch in the EndTxnResponse.
                // For versions lower than 5, the producer Id and epoch are set to -1 by default.
                // When Transaction Version 2 is enabled, the end txn request 5+ is used,
                // it mandates bumping the epoch after every transaction.
                // If the epoch overflows, a new producerId is returned with epoch set to 0.
                // Note, we still may see EndTxn TV1 (< 5) responses when the producer has upgraded to TV2 due to the upgrade
                // occurring at the end of beginCompletingTransaction. The next transaction started should be TV2.
                if (endTxnResponse.data().producerId() != -1) {
                    ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(
                        endTxnResponse.data().producerId(),
                        endTxnResponse.data().producerEpoch()
                    );
                    setProducerIdAndEpoch(producerIdAndEpoch);
                    resetSequenceNumbers();
                }
                resetTransactionState();
                result.done();
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                fatalError(error.exception());
            } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                abortableErrorIfPossible(error.exception());
            } else if (isAbort && error.exception() instanceof TransactionAbortableException) {
                // When aborting a transaction, we must convert TRANSACTION_ABORTABLE errors to KafkaException
                // because if an abort operation itself encounters an abortable error, retrying the abort would create a cycle.
                // Instead, we treat this as fatal error at the application layer to ensure the transaction can be cleanly terminated.
                fatalError(new KafkaException("Failed to abort transaction", error.exception()));
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                abortableError(error.exception());
            } else {
                fatalError(new KafkaException("Unhandled error in EndTxnResponse: " + error.message()));
            }
        }
    }

    private class AddOffsetsToTxnHandler extends TxnRequestHandler {
        private final AddOffsetsToTxnRequest.Builder builder;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final ConsumerGroupMetadata groupMetadata;

        private AddOffsetsToTxnHandler(AddOffsetsToTxnRequest.Builder builder,
                                       Map<TopicPartition, OffsetAndMetadata> offsets,
                                       ConsumerGroupMetadata groupMetadata) {
            super("AddOffsetsToTxn");
            this.builder = builder;
            this.offsets = offsets;
            this.groupMetadata = groupMetadata;
        }

        @Override
        AddOffsetsToTxnRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            AddOffsetsToTxnResponse addOffsetsToTxnResponse = (AddOffsetsToTxnResponse) response;
            Errors error = Errors.forCode(addOffsetsToTxnResponse.data().errorCode());

            if (error == Errors.NONE) {
                log.debug("Successfully added partition for consumer group {} to transaction", builder.data.groupId());

                // note the result is not completed until the TxnOffsetCommit returns
                pendingRequests.add(txnOffsetCommitHandler(result, offsets, groupMetadata));

                transactionStarted = true;
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR) {
                lookupCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
                reenqueue();
            } else if (error.exception() instanceof RetriableException) {
                reenqueue();
            } else if (error == Errors.UNKNOWN_PRODUCER_ID) {
                abortableErrorIfPossible(error.exception());
            } else if (error == Errors.INVALID_PRODUCER_EPOCH || error == Errors.PRODUCER_FENCED) {
                // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                // just treat it the same as PRODUCE_FENCED.
                fatalError(Errors.PRODUCER_FENCED.exception());
            } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED ||
                    error == Errors.INVALID_TXN_STATE || error == Errors.INVALID_PRODUCER_ID_MAPPING) {
                fatalError(error.exception());
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
            } else if (error == Errors.TRANSACTION_ABORTABLE) {
                abortableError(error.exception());
            } else {
                fatalError(new KafkaException("Unexpected error in AddOffsetsToTxnResponse: " + error.message()));
            }
        }
    }

    private class TxnOffsetCommitHandler extends TxnRequestHandler {
        private final TxnOffsetCommitRequest.Builder builder;

        private TxnOffsetCommitHandler(TransactionalRequestResult result,
                                       TxnOffsetCommitRequest.Builder builder) {
            super(result);
            this.builder = builder;
        }

        private TxnOffsetCommitHandler(TxnOffsetCommitRequest.Builder builder) {
            super("TxnOffsetCommitHandler");
            this.builder = builder;
        }

        @Override
        TxnOffsetCommitRequest.Builder requestBuilder() {
            return builder;
        }

        @Override
        Priority priority() {
            return Priority.ADD_PARTITIONS_OR_OFFSETS;
        }

        @Override
        FindCoordinatorRequest.CoordinatorType coordinatorType() {
            return FindCoordinatorRequest.CoordinatorType.GROUP;
        }

        @Override
        String coordinatorKey() {
            return builder.data.groupId();
        }

        @Override
        public void handleResponse(AbstractResponse response) {
            TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;
            boolean coordinatorReloaded = false;
            Map<TopicPartition, Errors> errors = txnOffsetCommitResponse.errors();

            log.debug("Received TxnOffsetCommit response for consumer group {}: {}", builder.data.groupId(),
                    errors);

            for (Map.Entry<TopicPartition, Errors> entry : errors.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                Errors error = entry.getValue();
                if (error == Errors.NONE) {
                    pendingTxnOffsetCommits.remove(topicPartition);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR
                        || error == Errors.REQUEST_TIMED_OUT) {
                    if (!coordinatorReloaded) {
                        coordinatorReloaded = true;
                        lookupCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, builder.data.groupId());
                    }
                } else if (error.exception() instanceof RetriableException) {
                    // If the topic is unknown, the coordinator is loading, or is another retriable error, retry with the current coordinator
                    continue;
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    abortableError(GroupAuthorizationException.forGroupId(builder.data.groupId()));
                    break;
                } else if (error == Errors.FENCED_INSTANCE_ID ||
                        error == Errors.TRANSACTION_ABORTABLE) {
                    abortableError(error.exception());
                    break;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    abortableError(new CommitFailedException("Transaction offset Commit failed " +
                        "due to consumer group metadata mismatch: " + error.exception().getMessage()));
                    break;
                } else if (error == Errors.INVALID_PRODUCER_EPOCH
                        || error == Errors.PRODUCER_FENCED) {
                    // We could still receive INVALID_PRODUCER_EPOCH from old versioned transaction coordinator,
                    // just treat it the same as PRODUCE_FENCED.
                    fatalError(Errors.PRODUCER_FENCED.exception());
                    break;
                } else if (error == Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                        || error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                    fatalError(error.exception());
                    break;
                } else {
                    fatalError(new KafkaException("Unexpected error in TxnOffsetCommitResponse: " + error.message()));
                    break;
                }
            }

            if (result.isCompleted()) {
                pendingTxnOffsetCommits.clear();
            } else if (pendingTxnOffsetCommits.isEmpty()) {
                result.done();
            } else {
                // Retry the commits which failed with a retriable error
                reenqueue();
            }
        }
    }

    private static final class PendingStateTransition {
        private final TransactionalRequestResult result;
        private final State state;
        private final String operation;

        private PendingStateTransition(
            TransactionalRequestResult result,
            State state,
            String operation
        ) {
            this.result = result;
            this.state = state;
            this.operation = operation;
        }
    }

    /**
     * Returns a ProducerIdAndEpoch object containing the producer ID and epoch
     * of the ongoing transaction.
     * This is used when preparing a transaction for a two-phase commit.
     *
     * @return a ProducerIdAndEpoch with the current producer ID and epoch.
     */
    public ProducerIdAndEpoch preparedTransactionState() {
        return this.preparedTxnState;
    }
}

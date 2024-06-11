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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareAcknowledgeRequest;
import org.apache.kafka.common.requests.ShareAcknowledgeResponse;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * {@code ShareConsumeRequestManager} is responsible for generating {@link ShareFetchRequest} and
 * {@link ShareAcknowledgeRequest} to fetch and acknowledge records being delivered for a consumer
 * in a share group.
 */
public class ShareConsumeRequestManager implements RequestManager, MemberStateListener, Closeable {

    private final Logger log;
    private final LogContext logContext;
    private final String groupId;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    protected final ShareFetchBuffer shareFetchBuffer;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingRequests;
    private final ShareFetchMetricsManager metricsManager;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    private Uuid memberId;
    private boolean fetchMoreRecords = false;
    private final Map<TopicIdPartition, Acknowledgements> fetchAcknowledgementsMap;
    private final Queue<AcknowledgeRequestState> acknowledgeRequestStates;
    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;

    ShareConsumeRequestManager(final LogContext logContext,
                               final String groupId,
                               final ConsumerMetadata metadata,
                               final SubscriptionState subscriptions,
                               final FetchConfig fetchConfig,
                               final ShareFetchBuffer shareFetchBuffer,
                               final ShareFetchMetricsManager metricsManager,
                               final long retryBackoffMs,
                               final long retryBackoffMaxMs) {
        this.log = logContext.logger(ShareConsumeRequestManager.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.metricsManager = metricsManager;
        this.retryBackoffMs = retryBackoffMs;
        this.retryBackoffMaxMs = retryBackoffMaxMs;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingRequests = new HashSet<>();
        this.acknowledgeRequestStates = new LinkedList<>();
        this.fetchAcknowledgementsMap = new HashMap<>();
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        if (memberId == null) {
            return PollResult.EMPTY;
        }

        PollResult pollResult = processAcknowledgements(currentTimeMs, false);
        if (pollResult != null) {
            return pollResult;
        }

        if (!fetchMoreRecords) {
            return PollResult.EMPTY;
        }

        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();
        Map<String, Uuid> topicIds = metadata.topicIds();
        for (TopicPartition partition : partitionsToFetch()) {
            Optional<Node> leaderOpt = metadata.currentLeader(partition).leader;

            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since current leader node is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            Uuid topicId = topicIds.get(partition.topic());
            if (topicId == null) {
                log.debug("Requesting metadata update for partition {} since topic ID is missing", partition);
                metadata.requestUpdate(false);
                continue;
            }

            Node node = leaderOpt.get();
            if (nodesWithPendingRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                ShareSessionHandler handler = handlerMap.computeIfAbsent(node, k -> sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n, memberId)));

                TopicIdPartition tip = new TopicIdPartition(topicId, partition);
                Acknowledgements acknowledgementsToSend = fetchAcknowledgementsMap.get(tip);
                if (acknowledgementsToSend != null)
                    metricsManager.recordAcknowledgementSent(acknowledgementsToSend.size());
                handler.addPartitionToFetch(tip, acknowledgementsToSend);

                log.debug("Added fetch request for partition {} to node {}", partition, node);
            }
        }

        Map<Node, ShareFetchRequest.Builder> builderMap = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler> entry : handlerMap.entrySet()) {
            builderMap.put(entry.getKey(), entry.getValue().newShareFetchBuilder(groupId, fetchConfig));
        }

        List<UnsentRequest> requests = builderMap.entrySet().stream().map(entry -> {
            Node target = entry.getKey();
            ShareFetchRequest.Builder requestBuilder = entry.getValue();

            nodesWithPendingRequests.add(target.id());

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareFetchFailure(target, requestBuilder.data(), error);
                } else {
                    handleShareFetchSuccess(target, requestBuilder.data(), clientResponse);
                }
            };
            return new UnsentRequest(requestBuilder, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
    }

    @Override
    public PollResult pollOnClose() {
        if (memberId == null) {
            return PollResult.EMPTY;
        }
        long currentTimeMs = Time.SYSTEM.milliseconds();

        PollResult pollResult = processAcknowledgements(currentTimeMs, true);
        if (pollResult != null) {
            return pollResult;
        }

        if (fetchAcknowledgementsMap.isEmpty()) {
            return PollResult.EMPTY;
        }

        final Cluster cluster = metadata.fetch();
        List<UnsentRequest> requests = new LinkedList<>();

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            sessionHandler.notifyClose();
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = fetchAcknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);
                        sessionHandler.addPartitionToFetch(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), node);
                    }
                }

                ShareAcknowledgeRequest.Builder requestBuilder = sessionHandler.newShareAcknowledgeBuilder(groupId, fetchConfig);
                if (requestBuilder != null) {
                    AcknowledgeRequestState requestState = new AcknowledgeRequestState(logContext,
                            ShareConsumeRequestManager.class.getSimpleName(),
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            Optional.empty(),
                            node.id(),
                            acknowledgementsMapForNode,
                            Optional.empty());

                    BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                        if (error != null) {
                            handleShareAcknowledgeCloseFailure(node, requestBuilder.data(), requestState, error, currentTimeMs);
                        } else {
                            handleShareAcknowledgeCloseSuccess(node, requestBuilder.data(), requestState, clientResponse, currentTimeMs);
                        }
                    };
                    requests.add(new UnsentRequest(requestBuilder, Optional.of(node)).whenComplete(responseHandler));
                }
            }
        });

        return new PollResult(requests);
    }

    public void fetch(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        if (!fetchMoreRecords) {
            log.debug("Fetch more data");
            fetchMoreRecords = true;
        }
        acknowledgementsMap.forEach((tip, acks) -> fetchAcknowledgementsMap.merge(tip, acks, Acknowledgements::merge));
    }

    public void acknowledgeOnClose(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        acknowledgementsMap.forEach((tip, acks) -> fetchAcknowledgementsMap.merge(tip, acks, Acknowledgements::merge));
    }

    private PollResult processAcknowledgements(long currentTimeMs, boolean onClose) {
        List<UnsentRequest> unsentRequests = new ArrayList<>();
        for (AcknowledgeRequestState acknowledgeRequestState : acknowledgeRequestStates) {
            if (acknowledgeRequestState.isProcessed()) {
                acknowledgeRequestStates.remove(acknowledgeRequestState);
            } else if (!acknowledgeRequestState.retryTimeoutExpired(currentTimeMs)) {
                if (acknowledgeRequestState.canSendRequest(currentTimeMs)) {
                    acknowledgeRequestState.onSendAttempt(currentTimeMs);
                    if (onClose) {
                        unsentRequests.add(acknowledgeRequestState.buildRequest(
                                this::handleShareAcknowledgeCloseSuccess,
                                this::handleShareAcknowledgeCloseFailure,
                                currentTimeMs));
                    } else {
                        unsentRequests.add(acknowledgeRequestState.buildRequest(
                                this::handleShareAcknowledgeSuccess,
                                this::handleShareAcknowledgeFailure,
                                currentTimeMs));
                    }
                }
            } else {
                // Fill in TimeoutException
                for (TopicIdPartition tip : acknowledgeRequestState.acknowledgementsMap.keySet()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getAcknowledgementsCount(tip));
                    acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.REQUEST_TIMED_OUT);
                }
                acknowledgeRequestStates.remove(acknowledgeRequestState);
            }
        }

        PollResult pollResult = null;
        if (!unsentRequests.isEmpty()) {
            pollResult = new PollResult(unsentRequests);
        } else if (!acknowledgeRequestStates.isEmpty()) {
            // Return empty result until all the acknowledgement request states are processed
            pollResult = PollResult.EMPTY;
        }
        return pollResult;
    }

    /**
     * Enqueue an AcknowledgeRequestState to be picked up on the next poll
     *
     * @param retryExpirationTimeMs The timeout for the operation
     * @param acknowledgementsMap The acknowledgements to commit
     *
     * @return The future which completes when the acknowledgements finished
     */
    public CompletableFuture<Map<TopicIdPartition, Acknowledgements>> commitSync(
            final long retryExpirationTimeMs,
            final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        final AtomicInteger resultCount = new AtomicInteger();
        final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> commitSyncFuture = new CompletableFuture<>();
        final CommitSyncResultHandler resultHandler = new CommitSyncResultHandler(resultCount, commitSyncFuture);

        final Cluster cluster = metadata.fetch();

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                Map<TopicIdPartition, Acknowledgements> acknowledgementsMapForNode = new HashMap<>();
                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = acknowledgementsMap.get(tip);
                    if (acknowledgements != null) {
                        acknowledgementsMapForNode.put(tip, acknowledgements);

                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        log.debug("Added acknowledge request for partition {} to node {}", tip.topicPartition(), node);
                    }
                    resultCount.incrementAndGet();
                }
                acknowledgeRequestStates.add(new AcknowledgeRequestState(logContext,
                        ShareConsumeRequestManager.class.getSimpleName(),
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        Optional.of(retryExpirationTimeMs),
                        nodeId,
                        acknowledgementsMapForNode,
                        Optional.of(resultHandler)
                ));
            }
        });

        return commitSyncFuture;
    }

    private void handleShareFetchSuccess(Node fetchTarget,
                                         ShareFetchRequestData requestData,
                                         ClientResponse resp) {
        try {
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring ShareFetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.UNKNOWN_TOPIC_ID) {
                    metadata.requestUpdate(false);
                }
                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();

            response.data().responses().forEach(topicResponse ->
                    topicResponse.partitions().forEach(partition ->
                            responseData.put(new TopicIdPartition(topicResponse.topicId(),
                                    partition.partitionIndex(),
                                    metadata.topicNames().get(topicResponse.topicId())), partition)));

            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final ShareFetchMetricsAggregator shareFetchMetricsAggregator = new ShareFetchMetricsAggregator(metricsManager, partitions);

            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition tip = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", tip, partitionData);

                Acknowledgements acks = fetchAcknowledgementsMap.remove(tip);
                if (acks != null) {
                    if (partitionData.acknowledgeErrorCode() != Errors.NONE.code()) {
                        metricsManager.recordFailedAcknowledgements(acks.size());
                    }
                    acks.setAcknowledgeErrorCode(Errors.forCode(partitionData.acknowledgeErrorCode()));
                    shareFetchBuffer.addCompletedAcknowledgements(Collections.singletonMap(tip, acks));
                }

                ShareCompletedFetch completedFetch = new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        tip,
                        partitionData,
                        shareFetchMetricsAggregator,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);

                if (!partitionData.acquiredRecords().isEmpty()) {
                    fetchMoreRecords = false;
                }
            }

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareFetchFailure(Node fetchTarget,
                                         ShareFetchRequestData requestData,
                                         Throwable error) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));

                Acknowledgements acks = fetchAcknowledgementsMap.remove(tip);
                if (acks != null) {
                    metricsManager.recordFailedAcknowledgements(acks.size());
                    acks.setAcknowledgeErrorCode(Errors.forException(error));
                    shareFetchBuffer.addCompletedAcknowledgements(Collections.singletonMap(tip, acks));
                }
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeSuccess(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               ClientResponse resp,
                                               long currentTimeMs) {
        try {
            final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring ShareAcknowledge response.",
                        fetchTarget.id());
                acknowledgeRequestState.onFailedAttempt(currentTimeMs);
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();
            if (!handler.handleResponse(response, requestVersion)) {
                acknowledgeRequestState.onFailedAttempt(currentTimeMs);
                if (response.error() != Errors.INVALID_SHARE_SESSION_EPOCH) {
                    return;
                } else {
                    // We pop this request off the queue as the error was not retriable.
                    acknowledgeRequestState.isProcessed = true;
                }
            }

            response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                if (partition.errorCode() != Errors.NONE.code()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getAcknowledgementsCount(tip));
                }
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partition.errorCode()));
            }));

            metricsManager.recordLatency(resp.requestLatencyMs());
            acknowledgeRequestState.isProcessed = true;
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeFailure(Node fetchTarget,
                                               ShareAcknowledgeRequestData requestData,
                                               AcknowledgeRequestState acknowledgeRequestState,
                                               Throwable error,
                                               long currentTimeMs) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getAcknowledgementsCount(tip));
                    acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseSuccess(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    AcknowledgeRequestState acknowledgeRequestState,
                                                    ClientResponse resp,
                                                    long currentTimeMs) {
        try {
            final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();

            response.data().responses().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                if (partition.errorCode() != Errors.NONE.code()) {
                    metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getAcknowledgementsCount(tip));
                }
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forCode(partition.errorCode()));
            }));

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
            sessionHandlers.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseFailure(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    AcknowledgeRequestState acknowledgeRequestState,
                                                    Throwable error,
                                                    long currentTimeMs) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> topic.partitions().forEach(partition -> {
                TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                        partition.partitionIndex(),
                        metadata.topicNames().get(topic.topicId()));
                metricsManager.recordFailedAcknowledgements(acknowledgeRequestState.getAcknowledgementsCount(tip));
                acknowledgeRequestState.handleAcknowledgeErrorCode(tip, Errors.forException(error));
            }));
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
            sessionHandlers.remove(fetchTarget.id());
        }
    }

    private List<TopicPartition> partitionsToFetch() {
        return subscriptions.fetchablePartitions(tp -> true);
    }

    public ShareSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    boolean hasCompletedFetches() {
        return !shareFetchBuffer.isEmpty();
    }

    protected void closeInternal() {
        Utils.closeQuietly(shareFetchBuffer, "shareFetchBuffer");
    }

    public void close() {
        idempotentCloser.close(this::closeInternal);
    }

    @Override
    public void onMemberEpochUpdated(Optional<Integer> memberEpochOpt, Optional<String> memberIdOpt) {
        memberIdOpt.ifPresent(s -> memberId = Uuid.fromString(s));
    }

    /**
     * Represents a request to acknowledge delivery that can be retried or aborted.
     */
    class AcknowledgeRequestState extends RequestState {

        /**
         * The node to send the request to.
         */
        private final int nodeId;

        /**
         * The map of acknowledgements to send
         */
        private final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap;

        /**
         * Time until which the request should be retried if it fails with retriable
         * errors. If not present, the request is triggered without waiting for a response or
         * retrying.
         */
        private final Optional<Long> expirationTimeMs;

        /**
         * Whether the request has been processed.
         */
        private boolean isProcessed = false;

        /**
         * For a synchronous request, this handles completing a future when all results are known.
         */
        private final Optional<CommitSyncResultHandler> resultHandler;

        AcknowledgeRequestState(LogContext logContext, String owner,
                                long retryBackoffMs, long retryBackoffMaxMs,
                                Optional<Long> expirationTimeMs,
                                int nodeId,
                                Map<TopicIdPartition, Acknowledgements> acknowledgementsMap,
                                Optional<CommitSyncResultHandler> resultHandler) {
            super(logContext, owner, retryBackoffMs, retryBackoffMaxMs);
            this.expirationTimeMs = expirationTimeMs;
            this.nodeId = nodeId;
            this.acknowledgementsMap = acknowledgementsMap;
            this.resultHandler = resultHandler;
        }

        UnsentRequest buildRequest(ResponseHandler<ClientResponse> successHandler,
                                   ResponseHandler<Throwable> errorHandler,
                                   long currentTimeMs) {
            ShareSessionHandler handler = sessionHandlers.get(nodeId);
            if (handler == null) {
                return null;
            }

            for (Map.Entry<TopicIdPartition, Acknowledgements> entry : acknowledgementsMap.entrySet()) {
                handler.addPartitionToFetch(entry.getKey(), entry.getValue());
            }
            ShareAcknowledgeRequest.Builder requestBuilder = handler.newShareAcknowledgeBuilder(groupId, fetchConfig);
            Node nodeToSend = metadata.fetch().nodeById(nodeId);

            nodesWithPendingRequests.add(nodeId);

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    onFailedAttempt(currentTimeMs);
                    errorHandler.handle(nodeToSend, requestBuilder.data(), this, error, currentTimeMs);
                    isProcessed = true;
                } else {
                    successHandler.handle(nodeToSend, requestBuilder.data(), this, clientResponse, currentTimeMs);
                }
            };

            if (requestBuilder == null) {
                return null;
            } else {
                return new UnsentRequest(requestBuilder, Optional.of(nodeToSend)).whenComplete(responseHandler);
            }
        }

        int getAcknowledgementsCount(TopicIdPartition tip) {
            Acknowledgements acks = acknowledgementsMap.get(tip);
            if (acks == null) {
                return 0;
            } else {
                return acks.size();
            }
        }

        void handleAcknowledgeErrorCode(TopicIdPartition tip, Errors acknowledgeErrorCode) {
            Acknowledgements acks = acknowledgementsMap.get(tip);
            if (acks != null) {
                acks.setAcknowledgeErrorCode(acknowledgeErrorCode);
                shareFetchBuffer.addCompletedAcknowledgements(Collections.singletonMap(tip, acks));
                resultHandler.ifPresent(handler -> handler.complete(tip, acks));
            }
        }

        boolean retryTimeoutExpired(long currentTimeMs) {
            return expirationTimeMs.isPresent() && expirationTimeMs.get() <= currentTimeMs;
        }

        boolean isProcessed() {
            return isProcessed;
        }
    }

    /**
     * Defines the contract for handling responses from brokers.
     * @param <T> Type of response, usually either {@link ClientResponse} or {@link Throwable}
     */
    @FunctionalInterface
    protected interface ResponseHandler<T> {

        /**
         * Handle the response from the given {@link Node target}
         */
        void handle(Node target, ShareAcknowledgeRequestData request, AcknowledgeRequestState requestState, T response, long currentTimeMs);
    }

    /**
     * Manages completing the future for a synchronous acknowledgement commit by counting
     * down the result as they are known and completing the future at the end.
     */
    static class CommitSyncResultHandler {
        private final Map<TopicIdPartition, Acknowledgements> result;
        private final AtomicInteger remainingResults;
        private final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future;

        CommitSyncResultHandler(final AtomicInteger remainingResults,
                                final CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future) {
            this.result = new HashMap<>();
            this.remainingResults = remainingResults;
            this.future = future;
        }

        void complete(TopicIdPartition partition, Acknowledgements acknowledgements) {
            result.put(partition, acknowledgements);
            if (remainingResults.decrementAndGet() == 0) {
                future.complete(result);
            }
        }
    }
}

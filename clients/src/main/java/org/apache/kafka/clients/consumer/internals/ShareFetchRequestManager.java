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
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code ShareFetchRequestManager} is responsible for generating {@link ShareFetchRequest} that
 * represent the {@link SubscriptionState#fetchablePartitions(Predicate)} based on the share group
 * consumer's assignment. It also uses {@link ShareAcknowledgeRequest} to close the share session.
 */
public class ShareFetchRequestManager implements RequestManager, MemberStateListener {

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

    ShareFetchRequestManager(final LogContext logContext,
                             final String groupId,
                             final ConsumerMetadata metadata,
                             final SubscriptionState subscriptions,
                             final FetchConfig fetchConfig,
                             final ShareFetchBuffer shareFetchBuffer,
                             final ShareFetchMetricsManager metricsManager) {
        this.log = logContext.logger(ShareFetchRequestManager.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.metricsManager = metricsManager;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingRequests = new HashSet<>();
    }

    @Override
    public PollResult poll(long currentTimeMs) {
        if (memberId == null) {
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
                Acknowledgements acknowledgementsToSend = shareFetchBuffer.getAcknowledgementsToSend(tip);
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

        final Cluster cluster = metadata.fetch();

        Map<Node, ShareSessionHandler> handlerMap = new HashMap<>();

        sessionHandlers.forEach((nodeId, sessionHandler) -> {
            sessionHandler.notifyClose();
            Node node = cluster.nodeById(nodeId);
            if (node != null) {
                handlerMap.put(node, sessionHandler);

                for (TopicIdPartition tip : sessionHandler.sessionPartitions()) {
                    Acknowledgements acknowledgements = shareFetchBuffer.getAcknowledgementsToSend(tip);
                    if (acknowledgements != null) {
                        metricsManager.recordAcknowledgementSent(acknowledgements.size());
                        sessionHandler.addPartitionToFetch(tip, acknowledgements);

                        log.debug("Added closing acknowledge request for partition {} to node {}", tip.topicPartition(), node);
                    }
                }
            }
        });

        Map<Node, ShareAcknowledgeRequest.Builder> builderMap = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler> entry : handlerMap.entrySet()) {
            Node target = entry.getKey();
            ShareSessionHandler handler = entry.getValue();
            ShareAcknowledgeRequest.Builder builder = handler.newShareAcknowledgeBuilder(groupId, fetchConfig);
            if (builder != null) {
                builderMap.put(target, builder);
            }
        }

        List<UnsentRequest> requests = builderMap.entrySet().stream().map(entry -> {
            Node target = entry.getKey();
            ShareAcknowledgeRequest.Builder requestBuilder = entry.getValue();

            nodesWithPendingRequests.add(target.id());

            BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareAcknowledgeCloseFailure(target, requestBuilder.data(), error);
                } else {
                    handleShareAcknowledgeCloseSuccess(target, requestBuilder.data(), clientResponse);
                }
            };
            return new UnsentRequest(requestBuilder, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        return new PollResult(requests);
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
                TopicIdPartition partition = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();

                log.debug("ShareFetch for partition {} returned fetch data {}", partition, partitionData);

                if (partitionData.acknowledgeErrorCode() != 0) {
                    metricsManager.recordFailedAcknowledgements(shareFetchBuffer.getPendingAcknowledgementsCount(partition));
                }
                shareFetchBuffer.handleAcknowledgementResponses(partition, Errors.forCode(partitionData.acknowledgeErrorCode()));

                ShareCompletedFetch completedFetch = new ShareCompletedFetch(
                        logContext,
                        BufferSupplier.create(),
                        partition,
                        partitionData,
                        shareFetchMetricsAggregator,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);
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

            requestData.topics().forEach(topic -> {
                topic.partitions().forEach(partition -> {
                    TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                            partition.partitionIndex(),
                            metadata.topicNames().get(topic.topicId()));
                    metricsManager.recordFailedAcknowledgements(shareFetchBuffer.getPendingAcknowledgementsCount(tip));
                    shareFetchBuffer.handleAcknowledgementResponses(tip, Errors.forException(error));
                });
            });
        } finally {
            log.debug("Removing pending request for node {} - failed", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseSuccess(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    ClientResponse resp) {
        try {
            final ShareAcknowledgeResponse response = (ShareAcknowledgeResponse) resp.responseBody();

            response.data().responses().forEach(topic -> {
                topic.partitions().forEach(partition -> {
                    TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                            partition.partitionIndex(),
                            metadata.topicNames().get(topic.topicId()));
                    if (partition.errorCode() != 0) {
                        metricsManager.recordFailedAcknowledgements(shareFetchBuffer.getPendingAcknowledgementsCount(tip));
                    }
                    shareFetchBuffer.handleAcknowledgementResponses(tip, Errors.forCode(partition.errorCode()));
                });
            });

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {} - success", fetchTarget);
            nodesWithPendingRequests.remove(fetchTarget.id());
            sessionHandlers.remove(fetchTarget.id());
        }
    }

    private void handleShareAcknowledgeCloseFailure(Node fetchTarget,
                                                    ShareAcknowledgeRequestData requestData,
                                                    Throwable error) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());
            if (handler != null) {
                handler.handleError(error);
            }

            requestData.topics().forEach(topic -> {
                topic.partitions().forEach(partition -> {
                    TopicIdPartition tip = new TopicIdPartition(topic.topicId(),
                            partition.partitionIndex(),
                            metadata.topicNames().get(topic.topicId()));
                    metricsManager.recordAcknowledgementSent(shareFetchBuffer.getPendingAcknowledgementsCount(tip));
                    shareFetchBuffer.handleAcknowledgementResponses(tip, Errors.forException(error));
                });
            });
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
}
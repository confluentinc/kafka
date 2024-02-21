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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
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

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * {@code ShareFetchRequestManager} is responsible for generating {@link ShareFetchRequest} that
 * represent the {@link SubscriptionState#fetchablePartitions(Predicate)} based on the share group
 * consumer's assignment.
 */
public class ShareFetchRequestManager implements RequestManager {

    private final Logger log;
    private final LogContext logContext;
    private final String groupId;
    private final Time time;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final FetchConfig fetchConfig;
    private final ShareFetchBuffer shareFetchBuffer;
    private final NetworkClientDelegate networkClientDelegate;
    private final Map<Integer, ShareSessionHandler> sessionHandlers;
    private final Set<Integer> nodesWithPendingFetchRequests;
    private final FetchMetricsManager metricsManager;

    ShareFetchRequestManager(final LogContext logContext,
                             final Time time,
                             final String groupId,
                             final ConsumerMetadata metadata,
                             final SubscriptionState subscriptions,
                             final FetchConfig fetchConfig,
                             final ShareFetchBuffer shareFetchBuffer,
                             final NetworkClientDelegate networkClientDelegate,
                             final FetchMetricsManager metricsManager) {
        this.log = logContext.logger(AbstractFetch.class);
        this.logContext = logContext;
        this.groupId = groupId;
        this.time = time;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.fetchConfig = fetchConfig;
        this.shareFetchBuffer = shareFetchBuffer;
        this.networkClientDelegate = networkClientDelegate;
        this.metricsManager = metricsManager;
        this.sessionHandlers = new HashMap<>();
        this.nodesWithPendingFetchRequests = new HashSet<>();
    }

    @Override
    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        List<NetworkClientDelegate.UnsentRequest> requests;
        Map<Node, ShareSessionHandler.ShareFetchRequestData> shareFetchRequests = prepareShareFetchRequests();

        requests = shareFetchRequests.entrySet().stream().map(entry -> {
            final Node target = entry.getKey();
            final ShareSessionHandler.ShareFetchRequestData data = entry.getValue();
            final ShareFetchRequest.Builder request = createShareFetchRequest(target, data);
            final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                if (error != null) {
                    handleShareFetchFailure(target, data, error);
                } else {
                    handleShareFetchSuccess(target, data, clientResponse);
                }
            };
            return new NetworkClientDelegate.UnsentRequest(request, Optional.of(target)).whenComplete(responseHandler);
        }).collect(Collectors.toList());

        return new NetworkClientDelegate.PollResult(requests);
    }

    private void handleShareFetchSuccess(Node fetchTarget, ShareSessionHandler.ShareFetchRequestData data, ClientResponse resp) {
        try {
            final ShareFetchResponse response = (ShareFetchResponse) resp.responseBody();
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler == null) {
                log.error("Unable to find ShareSessionHandler for node {}. Ignoring fetch response.",
                        fetchTarget.id());
                return;
            }

            final short requestVersion = resp.requestHeader().apiVersion();

            if (!handler.handleResponse(response, requestVersion)) {
                if (response.error() == Errors.FETCH_SESSION_TOPIC_ID_ERROR) {
                    metadata.requestUpdate(false);
                }

                return;
            }

            final Map<TopicIdPartition, ShareFetchResponseData.PartitionData> responseData = new LinkedHashMap<>();
            Map<Uuid, String> topicNames = handler.sessionTopicNames();

            response.data().responses().forEach(topicResponse -> {
                String name = topicNames.get(topicResponse.topicId());
                if (name != null) {
                    topicResponse.partitions().forEach(partition ->
                            responseData.put(new TopicIdPartition(topicResponse.topicId(), partition.partitionIndex(), name), partition));
                }
            });
            final Set<TopicPartition> partitions = responseData.keySet().stream().map(TopicIdPartition::topicPartition).collect(Collectors.toSet());
            final FetchMetricsAggregator metricAggregator = new FetchMetricsAggregator(metricsManager, partitions);


            for (Map.Entry<TopicIdPartition, ShareFetchResponseData.PartitionData> entry : responseData.entrySet()) {
                TopicIdPartition partition = entry.getKey();

                ShareFetchResponseData.PartitionData partitionData = entry.getValue();
                long fetchOffset = -1L;

                log.debug("Share fetch {} for partition {} returned fetch data {}",
                        fetchConfig.isolationLevel, partition, partitionData);

                // Making a FetchResponseData.PartitionData from ShareResponseData.PartitionData for now.
                // This can be removed once we have a separate CompletedShareFetch which uses ShareResponseData.PartitionData
                FetchResponseData.PartitionData partitionDataFetch = new FetchResponseData.PartitionData();
                partitionDataFetch.setPartitionIndex(partitionData.partitionIndex())
                        .setErrorCode(partitionData.errorCode())
                        .setRecords(partitionData.records());


                CompletedFetch completedFetch = new CompletedFetch(
                        logContext,
                        subscriptions,
                        BufferSupplier.create(),
                        partition.topicPartition(),
                        partitionDataFetch,
                        metricAggregator,
                        0L,
                        requestVersion);
                shareFetchBuffer.add(completedFetch);
            }

            metricsManager.recordLatency(resp.requestLatencyMs());
        } finally {
            log.debug("Removing pending request for node {}", fetchTarget);
            nodesWithPendingFetchRequests.remove(fetchTarget.id());
        }
    }

    private ShareFetchRequest.Builder createShareFetchRequest(Node fetchTarget, ShareSessionHandler.ShareFetchRequestData requestData) {
        final ShareFetchRequest.Builder request = ShareFetchRequest.Builder
                .forConsumer(fetchConfig.maxWaitMs, fetchConfig.minBytes, requestData.toSend())
                .forShareSession(groupId, requestData.metadata())
                .setMaxBytes(fetchConfig.maxBytes);

        nodesWithPendingFetchRequests.add(fetchTarget.id());

        return request;
    }

    private Map<Node, ShareSessionHandler.ShareFetchRequestData> prepareShareFetchRequests() {
        Map<Node, ShareSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        for (TopicPartition partition : fetchablePartitions()) {
            SubscriptionState.FetchPosition position = subscriptions.position(partition);

            if (position == null)
                throw new IllegalStateException("Missing position for fetchable partition " + partition);

            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (!leaderOpt.isPresent()) {
                log.debug("Requesting metadata update for partition {} since the position {} is missing the current leader node", partition, position);
                metadata.requestUpdate(false);
                continue;
            }

            Node node = leaderOpt.get();
            if (nodesWithPendingFetchRequests.contains(node.id())) {
                log.trace("Skipping fetch for partition {} because previous fetch request to {} has not been processed", partition, node);
            } else {
                // if there is a leader and no in-flight requests, issue a new fetch
                ShareSessionHandler.Builder builder = fetchable.computeIfAbsent(node, k -> {
                    ShareSessionHandler shareSessionHandler = sessionHandlers.computeIfAbsent(node.id(), n -> new ShareSessionHandler(logContext, n, Uuid.randomUuid()));
                    return shareSessionHandler.newBuilder();
                });
                Uuid topicId = topicIds.getOrDefault(partition.topic(), Uuid.ZERO_UUID);
                builder.add(new TopicIdPartition(topicId, partition));

                log.debug("Added {} fetch request for partition {} at position {} to node {}", fetchConfig.isolationLevel,
                        partition, position, node);
            }
        }

        Map<Node, ShareSessionHandler.ShareFetchRequestData> reqs = new LinkedHashMap<>();
        for (Map.Entry<Node, ShareSessionHandler.Builder> entry : fetchable.entrySet()) {
            reqs.put(entry.getKey(), entry.getValue().build());
        }

        // Because ShareFetcher doesn't have the concept of a validated position, we need to
        // do some fakery here to keep the underlying fetching state management happy. This
        // is a sign of a future refactor.
        if (reqs.isEmpty()) {
            for (TopicPartition tp : subscriptions.initializingPartitions()) {
                Optional<Node> leader = metadata.currentLeader(tp).leader;
                if (leader.isPresent()) {
                    subscriptions.seekValidated(tp, new SubscriptionState.FetchPosition(0, Optional.empty(), metadata.currentLeader(tp)));
                    subscriptions.maybeValidatePositionForCurrentLeader(new ApiVersions(), tp, metadata.currentLeader(tp));
                }
            }
        }

        return reqs;
    }

    private List<TopicPartition> fetchablePartitions() {
        return subscriptions.fetchablePartitions(tp -> true);
    }

    private void handleShareFetchFailure(Node fetchTarget,
                                         ShareSessionHandler.ShareFetchRequestData data,
                                         Throwable error) {
        try {
            final ShareSessionHandler handler = sessionHandler(fetchTarget.id());

            if (handler != null) {
                handler.handleError(error);
            }
        } finally {
            nodesWithPendingFetchRequests.remove(fetchTarget.id());
        }
    }

    public ShareSessionHandler sessionHandler(int node) {
        return sessionHandlers.get(node);
    }

    @Override
    public NetworkClientDelegate.PollResult pollOnClose() {
        return EMPTY;
    }
}
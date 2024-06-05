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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The default implementation of the {@link Persister} interface which is used by the
 * group coordinator and share-partition leaders to manage the durable share-partition state.
 * This implementation uses inter-broker RPCs to make requests with the share coordinator
 * which is responsible for persisting the share-partition state.
 */
@InterfaceStability.Evolving
public class DefaultStatePersister implements Persister {
    private PersisterConfig persisterConfig;
    private PersisterStateManager stateManager;

    private static final Logger log = LoggerFactory.getLogger(DefaultStatePersister.class);

    /**
     * needs to be public as its instance will be
     * created in BrokerServer from class name.
     */
    public DefaultStatePersister() {
    }

    // avoid double check locking - safer, neater
    private static final class InstanceHolder {
        static final Persister INSTANCE = new DefaultStatePersister();
    }

    public static Persister getInstance() {
        return InstanceHolder.INSTANCE;
    }

    @Override
    public void configure(PersisterConfig config) {
        this.persisterConfig = Objects.requireNonNull(config);
        this.stateManager = Objects.requireNonNull(config.stateManager);
        this.stateManager.start();
    }

    @Override
    public void stop() {
        try {
            this.stateManager.stop();
        } catch (Exception e) {
            log.error("Unable to stop state manager", e);
        }
    }

    /**
     * Used by the group coordinator to initialize the share-partition state.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request InitializeShareGroupStateParameters
     * @return InitializeShareGroupStateResult
     */
    public CompletableFuture<InitializeShareGroupStateResult> initializeState(InitializeShareGroupStateParameters request) {
        throw new RuntimeException("not implemented");
    }

    /**
     * Used by share-partition leaders to read share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request ReadShareGroupStateParameters
     * @return ReadShareGroupStateResult
     */
    public CompletableFuture<ReadShareGroupStateResult> readState(ReadShareGroupStateParameters request) {
        log.info("Shard readState request received - {}", request);
        GroupTopicPartitionData<PartitionIdLeaderEpochData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();
        HashMap<Uuid, HashMap<Integer, CompletableFuture<ReadShareGroupStateResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.ReadStateHandler> handlers = gtp.topicsData().stream()
                .map(topicData -> topicData.partitions().stream()
                        .map(partitionData -> {

                            Map<Integer, CompletableFuture<ReadShareGroupStateResponse>> partMap = futureMap.computeIfAbsent(topicData.topicId(), k -> new HashMap<>());
                            if (!partMap.containsKey(partitionData.partition())) {
                                partMap.put(partitionData.partition(), new CompletableFuture<>());
                            }
                            return stateManager.new ReadStateHandler(
                                    groupId,
                                    topicData.topicId(),
                                    partitionData.partition(),
                                    partitionData.leaderEpoch(),
                                    partMap.get(partitionData.partition()));
                        })
                        .collect(Collectors.toList()))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
                .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResult>
        return combinedFuture.thenApply(v -> {
            List<TopicData<PartitionAllData>> topicsData = futureMap.entrySet().stream()
                    .map(topicId -> {
                        List<PartitionAllData> partitionAllData = futureMap.get(topicId).values().stream()
                                .map(future -> {
                                    try {
                                        ReadShareGroupStateResponse partitionResponse = future.get();
                                        return partitionResponse.data().results().get(0).partitions().stream()
                                                .map(partitionResult -> PartitionFactory.newPartitionAllData(
                                                        partitionResult.partition(),
                                                        partitionResult.stateEpoch(),
                                                        partitionResult.startOffset(),
                                                        partitionResult.errorCode(),
                                                        partitionResult.errorMessage(),
                                                        partitionResult.stateBatches().stream().map(PersisterStateBatch::from).collect(Collectors.toList())
                                                ))
                                                .collect(Collectors.toList());
                                    } catch (InterruptedException | ExecutionException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .flatMap(List::stream)
                                .collect(Collectors.toList());
                        return new TopicData<>((Uuid) topicId, partitionAllData);
                    })
                    .collect(Collectors.toList());
            return new ReadShareGroupStateResult.Builder()
                    .setTopicsData(topicsData)
                    .build();
        });
    }

    /**
     * Used by share-partition leaders to write share-partition state to a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request WriteShareGroupStateParameters
     * @return WriteShareGroupStateResult
     */
    public CompletableFuture<WriteShareGroupStateResult> writeState(WriteShareGroupStateParameters request) {
        throw new RuntimeException("not implemented");
    }

    /**
     * Used by the group coordinator to delete share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request DeleteShareGroupStateParameters
     * @return DeleteShareGroupStateResult
     */
    public CompletableFuture<DeleteShareGroupStateResult> deleteState(DeleteShareGroupStateParameters request) {
        throw new RuntimeException("not implemented");
    }

    /**
     * Used by the group coordinator to read the offset information from share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request ReadShareGroupStateSummaryParameters
     * @return ReadShareGroupStateSummaryResult
     */
    public CompletableFuture<ReadShareGroupStateSummaryResult> readSummary(ReadShareGroupStateSummaryParameters request) {
        throw new RuntimeException("not implemented");
    }
}

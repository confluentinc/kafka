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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeleteShareGroupStateResponse;
import org.apache.kafka.common.requests.InitializeShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.ReadShareGroupStateSummaryResponse;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The default implementation of the {@link Persister} interface which is used by the
 * group coordinator and share-partition leaders to manage the durable share-partition state.
 * This implementation uses inter-broker RPCs to make requests to the share coordinator
 * which is responsible for persisting the share-partition state.
 */
public class DefaultStatePersister implements Persister {
    private final PersisterStateManager stateManager;

    private static final Logger log = LoggerFactory.getLogger(DefaultStatePersister.class);

    public DefaultStatePersister(PersisterStateManager stateManager) {
        this.stateManager = stateManager;
        this.stateManager.start();
    }

    @Override
    public void stop() {
        try {
            if (stateManager != null) {
                stateManager.stop();
            }
        } catch (Exception e) {
            log.error("Unable to stop state manager", e);
        }
    }

    /**
     * Used by the group coordinator to initialize the share-partition state.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request InitializeShareGroupStateParameters
     * @return A completable future of InitializeShareGroupStateResult
     */
    public CompletableFuture<InitializeShareGroupStateResult> initializeState(InitializeShareGroupStateParameters request) {
        try {
            validate(request);
        } catch (Exception e) {
            log.error("Unable to validate initialize state request", e);
            return CompletableFuture.failedFuture(e);
        }
        GroupTopicPartitionData<PartitionStateData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();

        Map<Uuid, Map<Integer, CompletableFuture<InitializeShareGroupStateResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.InitializeStateHandler> handlers = new ArrayList<>();

        gtp.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                CompletableFuture<InitializeShareGroupStateResponse> future = futureMap
                    .computeIfAbsent(topicData.topicId(), k -> new HashMap<>())
                    .computeIfAbsent(partitionData.partition(), k -> new CompletableFuture<>());

                handlers.add(
                    stateManager.new InitializeStateHandler(
                        groupId,
                        topicData.topicId(),
                        partitionData.partition(),
                        partitionData.stateEpoch(),
                        partitionData.startOffset(),
                        future,
                        null
                    )
                );
            });
        });

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            handlers.stream()
                .map(PersisterStateManager.InitializeStateHandler::result)
                .toArray(CompletableFuture[]::new));

        return combinedFuture.thenApply(v -> initializeResponsesToResult(futureMap));
    }

    /**
     * Used by share-partition leaders to write share-partition state to a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request WriteShareGroupStateParameters
     * @return A completable future of WriteShareGroupStateResult
     */
    public CompletableFuture<WriteShareGroupStateResult> writeState(WriteShareGroupStateParameters request) {
        try {
            validate(request);
        } catch (Exception e) {
            log.error("Unable to validate write state request", e);
            return CompletableFuture.failedFuture(e);
        }
        GroupTopicPartitionData<PartitionStateBatchData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();

        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.WriteStateHandler> handlers = new ArrayList<>();

        gtp.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                CompletableFuture<WriteShareGroupStateResponse> future = futureMap
                    .computeIfAbsent(topicData.topicId(), k -> new HashMap<>())
                    .computeIfAbsent(partitionData.partition(), k -> new CompletableFuture<>());

                log.debug("{}-{}-{}: stateEpoch - {}, leaderEpoch - {}.",
                    groupId, topicData.topicId(), partitionData.partition(), partitionData.stateEpoch(), partitionData.leaderEpoch());

                handlers.add(
                    stateManager.new WriteStateHandler(
                        groupId,
                        topicData.topicId(),
                        partitionData.partition(),
                        partitionData.stateEpoch(),
                        partitionData.leaderEpoch(),
                        partitionData.startOffset(),
                        partitionData.stateBatches(),
                        future, null)
                );
            });
        });

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            handlers.stream()
                .map(PersisterStateManager.WriteStateHandler::result)
                .toArray(CompletableFuture[]::new));

        return combinedFuture.thenApply(v -> writeResponsesToResult(futureMap));
    }

    /**
     * Takes in a list of COMPLETED futures and combines the results,
     * taking care of errors if any, into a single InitializeShareGroupStateResult
     *
     * @param futureMap - HashMap of {topic -> {partition -> future}}
     * @return Object representing combined result of type InitializeShareGroupStateResult
     */
    // visible for testing
    InitializeShareGroupStateResult initializeResponsesToResult(
        Map<Uuid, Map<Integer, CompletableFuture<InitializeShareGroupStateResponse>>> futureMap
    ) {
        List<TopicData<PartitionErrorData>> topicsData = futureMap.keySet().stream()
            .map(topicId -> {
                List<PartitionErrorData> partitionErrData = futureMap.get(topicId).entrySet().stream()
                    .map(partitionFuture -> {
                        int partition = partitionFuture.getKey();
                        CompletableFuture<InitializeShareGroupStateResponse> future = partitionFuture.getValue();
                        try {
                            // already completed because of allOf application in the caller
                            InitializeShareGroupStateResponse partitionResponse = future.join();
                            return partitionResponse.data().results().get(0).partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionErrorData(
                                    partitionResult.partition(),
                                    partitionResult.errorCode(),
                                    partitionResult.errorMessage()))
                                .toList();
                        } catch (Exception e) {
                            log.error("Unexpected exception while initializing data in share coordinator", e);
                            return List.of(PartitionFactory.newPartitionErrorData(
                                partition,
                                Errors.UNKNOWN_SERVER_ERROR.code(),   // No specific public error code exists for InterruptedException / ExecutionException
                                "Error initializing state in share coordinator: " + e.getMessage())
                            );
                        }
                    })
                    .flatMap(List::stream)
                    .toList();
                return new TopicData<>(topicId, partitionErrData);
            })
            .toList();
        return new InitializeShareGroupStateResult.Builder()
            .setTopicsData(topicsData)
            .build();
    }

    /**
     * Takes in a list of COMPLETED futures and combines the results,
     * taking care of errors if any, into a single WriteShareGroupStateResult
     *
     * @param futureMap - HashMap of {topic -> {partition -> future}}
     * @return Object representing combined result of type WriteShareGroupStateResult
     */
    // visible for testing
    WriteShareGroupStateResult writeResponsesToResult(
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponse>>> futureMap
    ) {
        List<TopicData<PartitionErrorData>> topicsData = futureMap.keySet().stream()
            .map(topicId -> {
                List<PartitionErrorData> partitionErrData = futureMap.get(topicId).entrySet().stream()
                    .map(partitionFuture -> {
                        int partition = partitionFuture.getKey();
                        CompletableFuture<WriteShareGroupStateResponse> future = partitionFuture.getValue();
                        try {
                            // already completed because of allOf application in the caller
                            WriteShareGroupStateResponse partitionResponse = future.join();
                            return partitionResponse.data().results().get(0).partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionErrorData(
                                    partitionResult.partition(),
                                    partitionResult.errorCode(),
                                    partitionResult.errorMessage()))
                                .toList();
                        } catch (Exception e) {
                            log.error("Unexpected exception while writing data to share coordinator", e);
                            return List.of(PartitionFactory.newPartitionErrorData(
                                partition,
                                Errors.UNKNOWN_SERVER_ERROR.code(),   // No specific public error code exists for InterruptedException / ExecutionException
                                "Error writing state to share coordinator: " + e.getMessage())
                            );
                        }
                    })
                    .flatMap(List::stream)
                    .toList();
                return new TopicData<>(topicId, partitionErrData);
            })
            .toList();
        return new WriteShareGroupStateResult.Builder()
            .setTopicsData(topicsData)
            .build();
    }

    /**
     * Used by share-partition leaders to read share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request ReadShareGroupStateParameters
     * @return A completable future of ReadShareGroupStateResult
     */
    public CompletableFuture<ReadShareGroupStateResult> readState(ReadShareGroupStateParameters request) {
        try {
            validate(request);
        } catch (Exception e) {
            log.error("Unable to validate read state request", e);
            return CompletableFuture.failedFuture(e);
        }
        GroupTopicPartitionData<PartitionIdLeaderEpochData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.ReadStateHandler> handlers = new ArrayList<>();

        gtp.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                CompletableFuture<ReadShareGroupStateResponse> future = futureMap
                    .computeIfAbsent(topicData.topicId(), k -> new HashMap<>())
                    .computeIfAbsent(partitionData.partition(), k -> new CompletableFuture<>());

                handlers.add(
                    stateManager.new ReadStateHandler(
                        groupId,
                        topicData.topicId(),
                        partitionData.partition(),
                        partitionData.leaderEpoch(),
                        future,
                        null)
                );
            });
        });

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            handlers.stream()
                .map(PersisterStateManager.ReadStateHandler::result)
                .toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResult>
        return combinedFuture.thenApply(v -> readResponsesToResult(futureMap));
    }

    /**
     * Takes in a list of COMPLETED futures and combines the results,
     * taking care of errors if any, into a single ReadShareGroupStateResult
     *
     * @param futureMap - HashMap of {topic -> {partition -> future}}
     * @return Object representing combined result of type ReadShareGroupStateResult
     */
    // visible for testing
    ReadShareGroupStateResult readResponsesToResult(
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateResponse>>> futureMap
    ) {
        List<TopicData<PartitionAllData>> topicsData = futureMap.keySet().stream()
            .map(topicId -> {
                List<PartitionAllData> partitionAllData = futureMap.get(topicId).entrySet().stream()
                    .map(partitionFuture -> {
                        int partition = partitionFuture.getKey();
                        CompletableFuture<ReadShareGroupStateResponse> future = partitionFuture.getValue();
                        try {
                            // already completed because of allOf call in the caller
                            ReadShareGroupStateResponse partitionResponse = future.join();
                            return partitionResponse.data().results().get(0).partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionAllData(
                                    partitionResult.partition(),
                                    partitionResult.stateEpoch(),
                                    partitionResult.startOffset(),
                                    partitionResult.errorCode(),
                                    partitionResult.errorMessage(),
                                    partitionResult.stateBatches().stream().map(PersisterStateBatch::from).toList()
                                ))
                                .toList();
                        } catch (Exception e) {
                            log.error("Unexpected exception while getting data from share coordinator", e);
                            return List.of(PartitionFactory.newPartitionAllData(
                                partition,
                                -1,
                                -1,
                                Errors.UNKNOWN_SERVER_ERROR.code(),   // No specific public error code exists for InterruptedException / ExecutionException
                                "Error reading state from share coordinator: " + e.getMessage(),
                                List.of())
                            );
                        }
                    })
                    .flatMap(List::stream)
                    .toList();
                return new TopicData<>(topicId, partitionAllData);
            })
            .toList();
        return new ReadShareGroupStateResult.Builder()
            .setTopicsData(topicsData)
            .build();
    }

    /**
     * Used by the group coordinator to delete share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request DeleteShareGroupStateParameters
     * @return A completable future of DeleteShareGroupStateResult
     */
    public CompletableFuture<DeleteShareGroupStateResult> deleteState(DeleteShareGroupStateParameters request) {
        try {
            validate(request);
        } catch (Exception e) {
            log.error("Unable to validate delete state request", e);
            return CompletableFuture.failedFuture(e);
        }
        GroupTopicPartitionData<PartitionIdData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();

        Map<Uuid, Map<Integer, CompletableFuture<DeleteShareGroupStateResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.DeleteStateHandler> handlers = new ArrayList<>();

        gtp.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                CompletableFuture<DeleteShareGroupStateResponse> future = futureMap
                    .computeIfAbsent(topicData.topicId(), k -> new HashMap<>())
                    .computeIfAbsent(partitionData.partition(), k -> new CompletableFuture<>());

                handlers.add(
                    stateManager.new DeleteStateHandler(
                        groupId,
                        topicData.topicId(),
                        partitionData.partition(),
                        future,
                        null
                    )
                );
            });
        });

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            handlers.stream()
                .map(PersisterStateManager.DeleteStateHandler::result)
                .toArray(CompletableFuture[]::new));

        return combinedFuture.thenApply(v -> deleteResponsesToResult(futureMap));
    }

    /**
     * Used by the group coordinator to read the offset information from share-partition state from a share coordinator.
     * This is an inter-broker RPC authorized as a cluster action.
     *
     * @param request ReadShareGroupStateSummaryParameters
     * @return A completable future of  ReadShareGroupStateSummaryResult
     */
    public CompletableFuture<ReadShareGroupStateSummaryResult> readSummary(ReadShareGroupStateSummaryParameters request) {
        try {
            validate(request);
        } catch (Exception e) {
            log.error("Unable to validate read state summary request", e);
            return CompletableFuture.failedFuture(e);
        }

        GroupTopicPartitionData<PartitionIdLeaderEpochData> gtp = request.groupTopicPartitionData();
        String groupId = gtp.groupId();
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateSummaryResponse>>> futureMap = new HashMap<>();
        List<PersisterStateManager.ReadStateSummaryHandler> handlers = new ArrayList<>();

        gtp.topicsData().forEach(topicData -> {
            topicData.partitions().forEach(partitionData -> {
                CompletableFuture<ReadShareGroupStateSummaryResponse> future = futureMap
                    .computeIfAbsent(topicData.topicId(), k -> new HashMap<>())
                    .computeIfAbsent(partitionData.partition(), k -> new CompletableFuture<>());

                handlers.add(
                    stateManager.new ReadStateSummaryHandler(
                        groupId,
                        topicData.topicId(),
                        partitionData.partition(),
                        partitionData.leaderEpoch(),
                        future,
                        null
                    )
                );
            });
        });

        for (PersisterStateManager.PersisterStateManagerHandler handler : handlers) {
            stateManager.enqueue(handler);
        }

        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
            handlers.stream()
                .map(PersisterStateManager.ReadStateSummaryHandler::result)
                .toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResult>
        return combinedFuture.thenApply(v -> readSummaryResponsesToResult(futureMap));
    }

    /**
     * Takes in a list of COMPLETED futures and combines the results,
     * taking care of errors if any, into a single ReadShareGroupStateSummaryResult
     *
     * @param futureMap - HashMap of {topic -> {partition -> future}}
     * @return Object representing combined result of type ReadShareGroupStateSummaryResult
     */
    // visible for testing
    ReadShareGroupStateSummaryResult readSummaryResponsesToResult(
        Map<Uuid, Map<Integer, CompletableFuture<ReadShareGroupStateSummaryResponse>>> futureMap
    ) {
        List<TopicData<PartitionStateSummaryData>> topicsData = futureMap.keySet().stream()
            .map(topicId -> {
                List<PartitionStateSummaryData> partitionStateErrorData = futureMap.get(topicId).entrySet().stream()
                    .map(partitionFuture -> {
                        int partition = partitionFuture.getKey();
                        CompletableFuture<ReadShareGroupStateSummaryResponse> future = partitionFuture.getValue();
                        try {
                            // already completed because of allOf call in the caller
                            ReadShareGroupStateSummaryResponse partitionResponse = future.join();
                            return partitionResponse.data().results().get(0).partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionStateSummaryData(
                                    partitionResult.partition(),
                                    partitionResult.stateEpoch(),
                                    partitionResult.startOffset(),
                                    partitionResult.leaderEpoch(),
                                    partitionResult.errorCode(),
                                    partitionResult.errorMessage()))
                                .toList();
                        } catch (Exception e) {
                            log.error("Unexpected exception while getting data from share coordinator", e);
                            return List.of(PartitionFactory.newPartitionStateSummaryData(
                                partition,
                                -1,
                                -1,
                                -1,
                                Errors.UNKNOWN_SERVER_ERROR.code(),   // No specific public error code exists for InterruptedException / ExecutionException
                                "Error reading state from share coordinator: " + e.getMessage()));
                        }
                    })
                    .flatMap(List::stream)
                    .toList();
                return new TopicData<>(topicId, partitionStateErrorData);
            })
            .toList();
        return new ReadShareGroupStateSummaryResult.Builder()
            .setTopicsData(topicsData)
            .build();
    }

    /**
     * Takes in a list of COMPLETED futures and combines the results,
     * taking care of errors if any, into a single DeleteShareGroupStateResult
     *
     * @param futureMap - HashMap of {topic -> {partition -> future}}
     * @return Object representing combined result of type DeleteShareGroupStateResult
     */
    // visible for testing
    DeleteShareGroupStateResult deleteResponsesToResult(
        Map<Uuid, Map<Integer, CompletableFuture<DeleteShareGroupStateResponse>>> futureMap
    ) {
        List<TopicData<PartitionErrorData>> topicsData = futureMap.keySet().stream()
            .map(topicId -> {
                List<PartitionErrorData> partitionErrorData = futureMap.get(topicId).entrySet().stream()
                    .map(partitionFuture -> {
                        int partition = partitionFuture.getKey();
                        CompletableFuture<DeleteShareGroupStateResponse> future = partitionFuture.getValue();
                        try {
                            // already completed because of allOf call in the caller
                            DeleteShareGroupStateResponse partitionResponse = future.join();
                            return partitionResponse.data().results().get(0).partitions().stream()
                                .map(partitionResult -> PartitionFactory.newPartitionErrorData(
                                        partitionResult.partition(),
                                        partitionResult.errorCode(),
                                        partitionResult.errorMessage()
                                    )
                                )
                                .toList();
                        } catch (Exception e) {
                            log.error("Unexpected exception while getting data from share coordinator", e);
                            return List.of(
                                PartitionFactory.newPartitionErrorData(
                                    partition,
                                    Errors.UNKNOWN_SERVER_ERROR.code(),   // No specific public error code exists for InterruptedException / ExecutionException
                                    "Error deleting state from share coordinator: " + e.getMessage()
                                )
                            );
                        }
                    })
                    .flatMap(List::stream)
                    .toList();
                return new TopicData<>(topicId, partitionErrorData);
            })
            .toList();
        return new DeleteShareGroupStateResult.Builder()
            .setTopicsData(topicsData)
            .build();
    }

    private static void validate(InitializeShareGroupStateParameters params) {
        String prefix = "Initialize share group parameters";
        if (params == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (params.groupTopicPartitionData() == null) {
            throw new IllegalArgumentException(prefix + " data cannot be null.");
        }

        validateGroupTopicPartitionData(prefix, params.groupTopicPartitionData());
    }
    
    private static void validate(WriteShareGroupStateParameters params) {
        String prefix = "Write share group parameters";
        if (params == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (params.groupTopicPartitionData() == null) {
            throw new IllegalArgumentException(prefix + " data cannot be null.");
        }

        validateGroupTopicPartitionData(prefix, params.groupTopicPartitionData());
    }

    private static void validate(ReadShareGroupStateParameters params) {
        String prefix = "Read share group parameters";
        if (params == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (params.groupTopicPartitionData() == null) {
            throw new IllegalArgumentException(prefix + " data cannot be null.");
        }

        validateGroupTopicPartitionData(prefix, params.groupTopicPartitionData());
    }

    private static void validate(ReadShareGroupStateSummaryParameters params) {
        String prefix = "Read share group summary parameters";
        if (params == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (params.groupTopicPartitionData() == null) {
            throw new IllegalArgumentException(prefix + " data cannot be null.");
        }

        validateGroupTopicPartitionData(prefix, params.groupTopicPartitionData());
    }

    private static void validate(DeleteShareGroupStateParameters params) {
        String prefix = "Delete share group parameters";
        if (params == null) {
            throw new IllegalArgumentException(prefix + " cannot be null.");
        }
        if (params.groupTopicPartitionData() == null) {
            throw new IllegalArgumentException(prefix + " data cannot be null.");
        }

        validateGroupTopicPartitionData(prefix, params.groupTopicPartitionData());
    }

    private static void validateGroupTopicPartitionData(String prefix, GroupTopicPartitionData<? extends PartitionIdData> data) {
        String groupId = data.groupId();
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException(prefix + " groupId cannot be null or empty.");
        }

        List<? extends TopicData<? extends PartitionIdData>> topicsData = data.topicsData();
        if (isEmpty(topicsData)) {
            throw new IllegalArgumentException(prefix + " topics data cannot be null or empty.");
        }

        for (TopicData<? extends PartitionIdData> topicData : topicsData) {
            if (topicData.topicId() == null) {
                throw new IllegalArgumentException(prefix + " topicId cannot be null.");
            }
            if (isEmpty(topicData.partitions())) {
                throw new IllegalArgumentException(prefix + " partitions cannot be null or empty.");
            }
            for (PartitionIdData partitionData : topicData.partitions()) {
                if (partitionData.partition() < 0) {
                    throw new IllegalArgumentException(
                        String.format("%s has invalid partitionId - %s %s %d", prefix, groupId, topicData.topicId(), partitionData.partition()));
                }
            }
        }
    }

    private static boolean isEmpty(List<?> list) {
        return list == null || list.isEmpty();
    }
}

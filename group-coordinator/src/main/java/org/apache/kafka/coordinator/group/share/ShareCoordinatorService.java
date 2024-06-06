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

package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.metrics.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.group.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.group.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilderSupplier;
import org.apache.kafka.coordinator.group.runtime.MultiThreadedEventProcessor;
import org.apache.kafka.coordinator.group.runtime.PartitionWriter;
import org.apache.kafka.server.group.share.ShareGroupHelper;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.timer.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

public class ShareCoordinatorService implements ShareCoordinator {
    private final ShareCoordinatorConfig config;
    private final Logger log;
    private final AtomicBoolean isActive = new AtomicBoolean(false);  // for controlling start and stop
    private final CoordinatorRuntime<ShareCoordinatorShard, Record> runtime;
    private final ShareCoordinatorMetrics shareCoordinatorMetrics;
    private volatile int numPartitions = -1; // Number of partitions for __share_group_state. Provided when component is started.

    public static class Builder {
        private final int nodeId;
        private final ShareCoordinatorConfig config;
        private PartitionWriter<Record> writer;
        private CoordinatorLoader<Record> loader;
        private Time time;
        private Timer timer;

        private ShareCoordinatorMetrics coordinatorMetrics;
        private CoordinatorRuntimeMetrics coordinatorRuntimeMetrics;

        public Builder(int nodeId, ShareCoordinatorConfig config) {
            this.nodeId = nodeId;
            this.config = config;
        }

        public Builder withWriter(PartitionWriter<Record> writer) {
            this.writer = writer;
            return this;
        }

        public Builder withLoader(CoordinatorLoader<Record> loader) {
            this.loader = loader;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder withCoordinatorMetrics(ShareCoordinatorMetrics coordinatorMetrics) {
            this.coordinatorMetrics = coordinatorMetrics;
            return this;
        }

        public Builder withCoordinatorRuntimeMetrics(CoordinatorRuntimeMetrics coordinatorRuntimeMetrics) {
            this.coordinatorRuntimeMetrics = coordinatorRuntimeMetrics;
            return this;
        }

        public ShareCoordinatorService build() {
            //todo maybe move to common class as similar to GroupCoordinatorService
            if (config == null) {
                throw new IllegalArgumentException("Config must be set.");
            }
            if (writer == null) {
                throw new IllegalArgumentException("Writer must be set.");
            }
            if (loader == null) {
                throw new IllegalArgumentException("Loader must be set.");
            }
            if (time == null) {
                throw new IllegalArgumentException("Time must be set.");
            }
            if (timer == null) {
                throw new IllegalArgumentException("Timer must be set.");
            }
            if (coordinatorMetrics == null) {
                throw new IllegalArgumentException("Share Coordinator metrics must be set.");
            }
            if (coordinatorRuntimeMetrics == null) {
                throw new IllegalArgumentException("Coordinator runtime metrics must be set.");
            }

            String logPrefix = String.format("ShareCoordinator id=%d", nodeId);
            LogContext logContext = new LogContext(String.format("[%s] ", logPrefix));

            CoordinatorShardBuilderSupplier<ShareCoordinatorShard, Record> supplier = () ->
                    new ShareCoordinatorShard.Builder(config);

            CoordinatorEventProcessor processor = new MultiThreadedEventProcessor(
                    logContext,
                    "share-coordinator-event-processor-",
                    config.numThreads,
                    time,
                    coordinatorRuntimeMetrics
            );

            CoordinatorRuntime<ShareCoordinatorShard, Record> runtime =
                    new CoordinatorRuntime.Builder<ShareCoordinatorShard, Record>()
                            .withTime(time)
                            .withTimer(timer)
                            .withLogPrefix(logPrefix)
                            .withLogContext(logContext)
                            .withEventProcessor(processor)
                            .withPartitionWriter(writer)
                            .withLoader(loader)
                            .withCoordinatorShardBuilderSupplier(supplier)
                            .withTime(time)
                            .withDefaultWriteTimeOut(Duration.ofMillis(config.writeTimeoutMs))
                            .withCoordinatorRuntimeMetrics(coordinatorRuntimeMetrics)
                            .withCoordinatorMetrics(coordinatorMetrics)
                            .build();

            return new ShareCoordinatorService(
                    logContext,
                    config,
                    runtime,
                    coordinatorMetrics
            );
        }
    }

    public ShareCoordinatorService(
            LogContext logContext,
            ShareCoordinatorConfig config,
            CoordinatorRuntime<ShareCoordinatorShard, Record> runtime,
            ShareCoordinatorMetrics shareCoordinatorMetrics) {
        this.log = logContext.logger(ShareCoordinatorService.class);
        this.config = config;
        this.runtime = runtime;
        this.shareCoordinatorMetrics = shareCoordinatorMetrics;
    }

    @Override
    public int partitionFor(String key) {
        return Utils.abs(key.hashCode()) % numPartitions;
    }

    @Override
    public Properties shareGroupStateTopicConfigs() {
        Properties properties = new Properties();
        properties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE); // as defined in KIP-932
        properties.put(TopicConfig.COMPRESSION_TYPE_CONFIG, BrokerCompressionType.PRODUCER.name);
        properties.put(TopicConfig.SEGMENT_BYTES_CONFIG, config.shareCoordinatorStateTopicSegmentBytes);
        return properties;
    }

    @Override
    public void startup(
            IntSupplier shareGroupTopicPartitionCount
    ) {
        if (!isActive.compareAndSet(false, true)) {
            log.warn("Share coordinator is already running.");
            return;
        }

        log.info("Starting up.");
        numPartitions = shareGroupTopicPartitionCount.getAsInt();
        isActive.set(true);
        log.info("Startup complete.");
    }

    @Override
    public void shutdown() {
        if (!isActive.compareAndSet(true, false)) {
            log.warn("Share coordinator is already shutting down.");
            return;
        }

        log.info("Shutting down.");
        isActive.set(false);
        Utils.closeQuietly(runtime, "coordinator runtime");
        Utils.closeQuietly(shareCoordinatorMetrics, "share coordinator metrics");
        log.info("Shutdown complete.");
    }

    private static boolean isGroupIdNotEmpty(String groupId) {
        return groupId != null && !groupId.isEmpty();
    }

    private ReadShareGroupStateResponseData generateErrorReadStateResponse(ReadShareGroupStateRequestData request, short errorCode, String errorMessage) {
        return new ReadShareGroupStateResponseData()
                .setResults(request.topics().stream().map(topic -> {
                    Uuid topicId = topic.topicId();
                    return new ReadShareGroupStateResponseData.ReadStateResult()
                            .setTopicId(topicId)
                            .setPartitions(topic.partitions().stream().map(partition ->
                                            new ReadShareGroupStateResponseData.PartitionResult()
                                                    .setPartition(partition.partition())
                                                    .setErrorCode(errorCode)
                                                    .setErrorMessage(errorMessage))
                                    .collect(Collectors.toList())
                            );
                }).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<WriteShareGroupStateResponseData> writeState(RequestContext context, WriteShareGroupStateRequestData request) {
        log.info("ShareCoordinatorService writeState request received - {}", request);
        String groupId = request.groupId();
        Map<Uuid, Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>>> futureMap = new HashMap<>();

        // The request received here could have multiple keys of structure group:topic:partition. However,
        // the writeState method in ShareCoordinatorShard expects a single key in the request. Hence, we will
        // be looping over the keys below and constructing new WriteShareGroupStateRequestData objects to pass
        // onto the shard method.

        // validate groupId
        if (groupId == null || groupId.isEmpty()) {
            if (!isEmpty(request.topics())) {
                return CompletableFuture.completedFuture(new WriteShareGroupStateResponseData()
                        .setResults(request.topics().stream()
                                .map(topicData -> {
                                    WriteShareGroupStateResponseData.WriteStateResult resultData = new WriteShareGroupStateResponseData.WriteStateResult();
                                    resultData.setTopicId(topicData.topicId());
                                    if (!isEmpty(topicData.partitions())) {
                                        resultData.setPartitions(topicData.partitions().stream()
                                                .map(partitionData -> WriteShareGroupStateResponse.getErrorResponsePartitionResult(
                                                        partitionData.partition(), Errors.INVALID_GROUP_ID, "Group id must be specified and non-empty."))
                                                .collect(Collectors.toList()));
                                    } else {
                                        resultData.setPartitions(Collections.singletonList(WriteShareGroupStateResponse.getErrorResponsePartitionResult(
                                                -1, Errors.INVALID_GROUP_ID, "Group id must be specified and non-empty.")));
                                    }
                                    return resultData;
                                })
                                .collect(Collectors.toList())));
            } else {
                return CompletableFuture.completedFuture(WriteShareGroupStateResponse.getErrorResponseData(
                        Uuid.ZERO_UUID, -1, Errors.INVALID_GROUP_ID, "Group id must be specified and non-empty."));
            }
        }

        // validate topicsData
        if (isEmpty(request.topics())) {
            return CompletableFuture.completedFuture(WriteShareGroupStateResponse.getErrorResponseData(
                    Uuid.ZERO_UUID, -1, Errors.INVALID_REQUEST, "Topic data must be specified."));
        }

        // validate partitionsData
        request.topics().forEach(topicData -> {
            if (isEmpty(topicData.partitions())) {
                WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData();
                responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.getErrorResponseResult(
                        topicData.topicId(), Collections.singletonList(WriteShareGroupStateResponse.getErrorResponsePartitionResult(
                                -1, Errors.INVALID_REQUEST, "Partition data must be specified.")))));

                Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>> partMap = new HashMap<>();
                partMap.put(-1, CompletableFuture.completedFuture(responseData));
                futureMap.put(topicData.topicId(), partMap);
            } else {
                Map<Integer, CompletableFuture<WriteShareGroupStateResponseData>> partitionFut =
                        futureMap.computeIfAbsent(topicData.topicId(), k -> new HashMap<>());
                topicData.partitions().forEach(
                        partitionData -> partitionFut.put(partitionData.partition(), runtime.scheduleWriteOperation(
                                "write-share-group-state",
                                topicPartitionFor(ShareGroupHelper.coordinatorKey(groupId, topicData.topicId(), partitionData.partition())),
                                Duration.ofMillis(config.writeTimeoutMs),
                                coordinator -> coordinator.writeState(context, new WriteShareGroupStateRequestData()
                                        .setGroupId(groupId)
                                        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
                                                .setTopicId(topicData.topicId())
                                                .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                                                        .setPartition(partitionData.partition())
                                                        .setStartOffset(partitionData.startOffset())
                                                        .setLeaderEpoch(partitionData.leaderEpoch())
                                                        .setStateEpoch(partitionData.stateEpoch())
                                                        .setStateBatches(partitionData.stateBatches()))))))))
                );
            }
        });

        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futureMap.values().stream()
                .flatMap(partMap -> partMap.values().stream()).toArray(CompletableFuture[]::new));

        return combinedFuture.thenApply(v -> {
            List<WriteShareGroupStateResponseData.WriteStateResult> writeStateResults = futureMap.keySet().stream()
                    .map(topicId -> {
                        List<WriteShareGroupStateResponseData.PartitionResult> partitionResults = futureMap.get(topicId).values().stream()
                                .map(future -> {
                                    try {
                                        WriteShareGroupStateResponseData partitionData = future.get();
                                        // error check if the partitionData results contains only 1 row (corresponding to topicId)
                                        return partitionData.results().get(0).partitions();
                                    } catch (InterruptedException | ExecutionException e) {
                                        throw new RuntimeException(e);
                                    }
                                })
                                .flatMap(List::stream)
                                .collect(Collectors.toList());

                        return WriteShareGroupStateResponse.getErrorResponseResult(topicId, partitionResults);
                    })
                    .collect(Collectors.toList());
            return new WriteShareGroupStateResponseData()
                    .setResults(writeStateResults);
        });
    }

    @Override
    public CompletableFuture<ReadShareGroupStateResponseData> readState(RequestContext context, ReadShareGroupStateRequestData request) {
        String groupId = request.groupId();
        // A map to store the futures for each topicId and partition.
        Map<Uuid, HashMap<Integer, CompletableFuture<ReadShareGroupStateResponseData>>> futureMap = new HashMap<>();

        if (!isActive.get()) {
            return CompletableFuture.completedFuture(
                    generateErrorReadStateResponse(
                            request,
                            Errors.COORDINATOR_NOT_AVAILABLE.code(),
                            "Share coordinator is not available."
                    )
            );
        }

        // validate group Id
        if (!isGroupIdNotEmpty(groupId)) {
            return CompletableFuture.completedFuture(
                    generateErrorReadStateResponse(
                            request,
                            Errors.INVALID_GROUP_ID.code(),
                            "Invalid Group Id in the request."
                    )
            );
        }

        // validate topicsData
        if (isEmpty(request.topics())) {
            ReadShareGroupStateResponseData responseData = new ReadShareGroupStateResponseData();
            responseData.setResults(Collections.singletonList(new ReadShareGroupStateResponseData.ReadStateResult()
                    .setTopicId(Uuid.ZERO_UUID)
                    .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                            .setPartition(-1)
                            .setErrorCode(Errors.INVALID_REQUEST.code())
                            .setErrorMessage("Topic data must be specified.")))));
            return CompletableFuture.completedFuture(responseData);
        }

        request.topics().forEach(topicData -> {
            Uuid topicId = topicData.topicId();
            // validate partitionsData
            if (!isEmpty(topicData.partitions())) {
                ReadShareGroupStateResponseData responseData = new ReadShareGroupStateResponseData();
                responseData.setResults(Collections.singletonList(new ReadShareGroupStateResponseData.ReadStateResult()
                        .setTopicId(topicId)
                        .setPartitions(Collections.singletonList(new ReadShareGroupStateResponseData.PartitionResult()
                                .setPartition(-1)
                                .setErrorCode(Errors.INVALID_REQUEST.code())
                                .setErrorMessage("Partition data must be specified.")))));
                HashMap<Integer, CompletableFuture<ReadShareGroupStateResponseData>> partMap = new HashMap<>();
                partMap.put(-1, CompletableFuture.completedFuture(responseData));
                futureMap.put(topicData.topicId(), partMap);
            } else {
                topicData.partitions().forEach(partitionData -> {
                    // Request object containing information of a single topic partition
                    ReadShareGroupStateRequestData requestForCurrentPartition = new ReadShareGroupStateRequestData()
                            .setGroupId(groupId)
                            .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
                                    .setTopicId(topicId)
                                    .setPartitions(Collections.singletonList(partitionData))));
                    String coordinatorKey = request.groupId() + ":" + topicId + ":" + partitionData.partition();
                    // Scheduling a runtime read operation to read share partition state from the coordinator in memory state
                    CompletableFuture<ReadShareGroupStateResponseData> future = runtime.scheduleReadOperation(
                            "fetch-offsets",
                            topicPartitionFor(coordinatorKey),
                            (coordinator, offset) -> coordinator.readState(requestForCurrentPartition, offset)
                    );
                    if (futureMap.containsKey(topicId)) {
                        futureMap.get(topicId).put(partitionData.partition(), future);
                    } else {
                        HashMap<Integer, CompletableFuture<ReadShareGroupStateResponseData>> map = new HashMap<>();
                        map.put(partitionData.partition(), future);
                        futureMap.put(topicId, map);
                    }
                });
            }
        });


        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futureMap.values().stream()
                .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

        // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResponseData>
        return allOf.thenApply(v -> {
            List<ReadShareGroupStateResponseData.ReadStateResult> readStateResult = futureMap.entrySet().stream()
                    .map(topicEntry -> {
                        Uuid topicId = topicEntry.getKey();
                        List<ReadShareGroupStateResponseData.PartitionResult> partitionDataList =
                                topicEntry.getValue().values().stream()
                                        .map(future -> {
                                            try {
                                                return future.get().results().get(0).partitions().get(0);
                                            } catch (InterruptedException | ExecutionException e) {
                                                throw new RuntimeException(e);
                                            }
                                        })
                                        .collect(Collectors.toList());
                        return new ReadShareGroupStateResponseData.ReadStateResult().setTopicId(topicId).setPartitions(partitionDataList);
                    })
                    .collect(Collectors.toList());
            return new ReadShareGroupStateResponseData().setResults(readStateResult);
        });
    }

    @Override
    public void onElection(int partitionIndex, int partitionLeaderEpoch) {
        runtime.scheduleLoadOperation(
                new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex),
                partitionLeaderEpoch
        );
    }

    @Override
    public void onResignation(int partitionIndex, OptionalInt partitionLeaderEpoch) {
        runtime.scheduleUnloadOperation(
                new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionIndex),
                partitionLeaderEpoch
        );
    }

    private TopicPartition topicPartitionFor(String key) {
        return new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionFor(key));
    }

    private static <P> boolean isEmpty(List<P> list) {
        return list == null || list.isEmpty() || list.get(0) == null;
    }
}

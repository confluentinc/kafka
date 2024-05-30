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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.RequestContext;
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
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.util.timer.Timer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
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

  /**
   * @return The topic partition for the given group.
   */
  private TopicPartition topicPartitionFor(
          String coordinatorKey
  ) {
    return new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, partitionFor(coordinatorKey));
  }

  @Override
  public int partitionFor(String key) {
    return Utils.abs(key.hashCode()) % numPartitions;
  }

  @Override
  public CompletableFuture<ReadShareGroupStateResponseData> readShareGroupStates(
          RequestContext context,
          ReadShareGroupStateRequestData requestData
  ) {
    String groupId = requestData.groupId();

    // A map to store the futures for each topicId and partition.
    HashMap<String, HashMap<Integer, CompletableFuture<ReadShareGroupStateResponseData.PartitionResult>>> futures = new HashMap<>();

    requestData.topics().forEach(topicData -> {
      String topicId = topicData.topicId().toString();
      topicData.partitions().forEach(partitionData -> {
        // Getting the response for Read Share State pr topic partition
        CompletableFuture<ReadShareGroupStateResponseData.PartitionResult> future =
                readShareGroupState(context, groupId, topicId, partitionData);
        if (futures.containsKey(topicId)) {
          futures.get(topicId).put(partitionData.partition(), future);
        } else {
          HashMap<Integer, CompletableFuture<ReadShareGroupStateResponseData.PartitionResult>> map = new HashMap<>();
          map.put(partitionData.partition(), future);
          futures.put(topicId, map);
        }
      });
    });


    // Combine all futures into a single CompletableFuture<Void>
    CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.values().stream()
            .flatMap(map -> map.values().stream()).toArray(CompletableFuture[]::new));

    // Transform the combined CompletableFuture<Void> into CompletableFuture<ReadShareGroupStateResponseData>
    return allOf.thenApply(v -> {
      List<ReadShareGroupStateResponseData.ReadStateResult> readStateResult = futures.entrySet().stream()
              .map(topicEntry -> {
                Uuid topicId = Uuid.fromString(topicEntry.getKey());
                List<ReadShareGroupStateResponseData.PartitionResult> partitionDataList =
                        topicEntry.getValue().values().stream()
                        .map(future -> {
                          try {
                            return future.get();
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
  public CompletableFuture<ReadShareGroupStateResponseData.PartitionResult> readShareGroupState(
          RequestContext context,
          String groupId,
          String topicId,
          ReadShareGroupStateRequestData.PartitionData requestPartition
  ) {
    if (!isActive.get()) {
      return CompletableFuture.completedFuture(
              new ReadShareGroupStateResponseData.PartitionResult()
                      .setPartition(requestPartition.partition())
                      .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                      .setErrorMessage("Share coordinator is not available.")
      );
    }

    if (!isGroupIdNotEmpty(groupId)) {
      return CompletableFuture.completedFuture(
              new ReadShareGroupStateResponseData.PartitionResult()
                      .setPartition(requestPartition.partition())
                      .setErrorCode(Errors.INVALID_GROUP_ID.code())
                      .setErrorMessage("Invalid Group Id in the request.")
      );
    }

    String coordinatorKey = groupId + ":" + topicId + ":" + requestPartition.partition();

    return runtime.scheduleReadOperation(
            "fetch-offsets",
            topicPartitionFor(coordinatorKey),
            (coordinator, offset) -> coordinator.readShareGroupState(groupId, topicId, requestPartition.partition(), offset)
    );
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

}

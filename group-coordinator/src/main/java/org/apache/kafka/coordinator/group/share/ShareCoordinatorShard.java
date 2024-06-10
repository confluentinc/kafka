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
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.RecordHelpers;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.group.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.group.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorShardBuilder;
import org.apache.kafka.coordinator.group.runtime.CoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.group.share.ShareGroupHelper;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ShareCoordinatorShard implements CoordinatorShard<Record> {
  private final Logger log;
  private final Time time;
  private final CoordinatorTimer<Void, Record> timer;
  private final ShareCoordinatorConfig config;
  private final CoordinatorMetrics coordinatorMetrics;
  private final CoordinatorMetricsShard metricsShard;
  private final TimelineHashMap<String, ShareSnapshotValue> shareStateMap;  // coord key -> ShareSnapshotValue
  private final Map<String, Integer> leaderMap;
  private MetadataImage metadataImage;

  public static class Builder implements CoordinatorShardBuilder<ShareCoordinatorShard, Record> {
    private ShareCoordinatorConfig config;
    private LogContext logContext;
    private SnapshotRegistry snapshotRegistry;
    private Time time;
    private CoordinatorTimer<Void, Record> timer;
    private CoordinatorMetrics coordinatorMetrics;
    private TopicPartition topicPartition;

    public Builder(ShareCoordinatorConfig config) {
      this.config = config;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
      this.snapshotRegistry = snapshotRegistry;
      return this;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withLogContext(LogContext logContext) {
      this.logContext = logContext;
      return this;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withTime(Time time) {
      this.time = time;
      return this;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withTimer(CoordinatorTimer<Void, Record> timer) {
      this.timer = timer;
      return this;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withCoordinatorMetrics(CoordinatorMetrics coordinatorMetrics) {
      this.coordinatorMetrics = coordinatorMetrics;
      return this;
    }

    @Override
    public CoordinatorShardBuilder<ShareCoordinatorShard, Record> withTopicPartition(TopicPartition topicPartition) {
      this.topicPartition = topicPartition;
      return this;
    }

    @Override
    public ShareCoordinatorShard build() {
      //todo smjn - maybe move since same as GroupCoordinator
      if (logContext == null) logContext = new LogContext();
      if (config == null)
        throw new IllegalArgumentException("Config must be set.");
      if (snapshotRegistry == null)
        throw new IllegalArgumentException("SnapshotRegistry must be set.");
      if (time == null)
        throw new IllegalArgumentException("Time must be set.");
      if (timer == null)
        throw new IllegalArgumentException("Timer must be set.");
      if (coordinatorMetrics == null || !(coordinatorMetrics instanceof ShareCoordinatorMetrics))
        throw new IllegalArgumentException("CoordinatorMetrics must be set and be of type ShareCoordinatorMetrics.");
      if (topicPartition == null)
        throw new IllegalArgumentException("TopicPartition must be set.");

      ShareCoordinatorMetricsShard metricsShard = ((ShareCoordinatorMetrics) coordinatorMetrics)
          .newMetricsShard(snapshotRegistry, topicPartition);

      return new ShareCoordinatorShard(
          logContext,
          time,
          timer,
          config,
          coordinatorMetrics,
          metricsShard,
          snapshotRegistry
      );
    }
  }

  ShareCoordinatorShard(
      LogContext logContext,
      Time time,
      CoordinatorTimer<Void, Record> timer,
      ShareCoordinatorConfig config,
      CoordinatorMetrics coordinatorMetrics,
      CoordinatorMetricsShard metricsShard,
      SnapshotRegistry snapshotRegistry
  ) {
    this.log = logContext.logger(ShareCoordinatorShard.class);
    this.time = time;
    this.timer = timer;
    this.config = config;
    this.coordinatorMetrics = coordinatorMetrics;
    this.metricsShard = metricsShard;
    this.shareStateMap = new TimelineHashMap<>(snapshotRegistry, 0);
    this.leaderMap = new HashMap<>();
  }

  @Override
  public void onLoaded(MetadataImage newImage) {
    CoordinatorShard.super.onLoaded(newImage);
  }

  @Override
  public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
    this.metadataImage = newImage;
  }

  @Override
  public void onUnloaded() {
    CoordinatorShard.super.onUnloaded();
  }

  @Override
  public void replay(long offset, long producerId, short producerEpoch, Record record) throws RuntimeException {
    ApiMessageAndVersion key = record.key();
    ApiMessageAndVersion value = record.value();

    switch (key.version()) {
      case ShareCoordinator.SHARE_SNAPSHOT_RECORD_KEY_VERSION: // ShareSnapshot
        handleShareSnapshot((ShareSnapshotKey) key.message(), (ShareSnapshotValue) Utils.messageOrNull(value));
        break;
      case ShareCoordinator.SHARE_UPDATE_RECORD_KEY_VERSION: // ShareUpdate
        handleShareUpdate((ShareUpdateKey) key.message(), (ShareUpdateValue) Utils.messageOrNull(value));
        break;
      default:
        // noop
    }
  }

  private void handleShareSnapshot(ShareSnapshotKey key, ShareSnapshotValue value) {
    String mapKey = ShareGroupHelper.coordinatorKey(key.groupId(), key.topicId(), key.partition());
    Integer oldValue = leaderMap.get(mapKey);
    if (oldValue == null) {
      leaderMap.put(mapKey, value.leaderEpoch());
    } else if (oldValue < value.leaderEpoch()) {
      leaderMap.put(mapKey, value.leaderEpoch());
    }
    shareStateMap.put(mapKey, value);
  }

  private void handleShareUpdate(ShareUpdateKey key, ShareUpdateValue value) {
    // update internal hashmaps
  }

  @Override
  public void replayEndTransactionMarker(long producerId, short producerEpoch, TransactionResult result) throws RuntimeException {
    CoordinatorShard.super.replayEndTransactionMarker(producerId, producerEpoch, result);
  }

  /**
   * This method as called by the ShareCoordinatorService will be provided with
   * the request data which covers only key i.e. group1:topic1:partition1. The implementation
   * below was done keeping this in mind.
   *
   * @param context - RequestContext
   * @param request - WriteShareGroupStateRequestData for a single key
   * @return CoordinatorResult(records, response)
   */
  public CoordinatorResult<WriteShareGroupStateResponseData, Record> writeState(RequestContext context, WriteShareGroupStateRequestData request) {
    log.info("Shard writeState request received - {}", request);
    // records to write (with both key and value of snapshot type), response to caller
    // only one key will be there in the request by design
    Optional<CoordinatorResult<WriteShareGroupStateResponseData, Record>> error = maybeGetWriteStateError(request);
    if (error.isPresent()) {
      return error.get();
    }

    String groupId = request.groupId();
    WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
    WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

    List<Record> recordList = Collections.singletonList(RecordHelpers.newShareSnapshotRecord(
        groupId, topicData.topicId(), partitionData.partition(), ShareGroupOffset.fromRequest(partitionData)
    ));
    List<Record> validRecords = new ArrayList<>();

    WriteShareGroupStateResponseData responseData = new WriteShareGroupStateResponseData();
    for (Record record : recordList) {  // should be single record
      if (record.key().message() instanceof ShareSnapshotKey && record.value().message() instanceof ShareSnapshotValue) {
        ShareSnapshotKey newKey = (ShareSnapshotKey) record.key().message();
        ShareSnapshotValue newValue = (ShareSnapshotValue) record.value().message();
        responseData.setResults(Collections.singletonList(WriteShareGroupStateResponse.toResponseWriteStateResult(
            newKey.topicId(), Collections.singletonList(WriteShareGroupStateResponse.toErrorResponsePartitionResult(
                newKey.partition(), Errors.NONE, Errors.NONE.message())))));

        String mapKey = ShareGroupHelper.coordinatorKey(newKey.groupId(), newKey.topicId(), newKey.partition());

        if (shareStateMap.containsKey(mapKey)) {
          ShareSnapshotValue oldValue = shareStateMap.get(mapKey);
          newValue.setSnapshotEpoch(oldValue.snapshotEpoch() + 1);  // increment the snapshot epoch
        }
        validRecords.add(record); // this will have updated snapshot epoch
        shareStateMap.put(mapKey, newValue);
      }
    }

    return new CoordinatorResult<>(validRecords, responseData);
  }

  /**
   * This method as called by the ShareCoordinatorService will be provided with
   * the request data which covers only key i.e. group1:topic1:partition1. The implementation
   * below was done keeping this in mind.
   *
   * @param request - WriteShareGroupStateRequestData for a single key
   * @param offset  - offset to read from the __share_group_state topic partition
   * @return CoordinatorResult(records, response)
   */
  public ReadShareGroupStateResponseData readState(ReadShareGroupStateRequestData request, Long offset) {
    log.info("Shard readState request received - {}", request);
    // records to read (with the key of snapshot type), response to caller
    // only one key will be there in the request by design
    Optional<ReadShareGroupStateResponseData> error = maybeGetReadStateError(request);
    if (error.isPresent()) {
      return error.get();
    }

    Uuid topicId = request.topics().get(0).topicId();
    int partition = request.topics().get(0).partitions().get(0).partition();

    String coordinatorKey = ShareGroupHelper.coordinatorKey(request.groupId(), topicId, partition);

    ShareSnapshotValue snapshotValue = shareStateMap.get(coordinatorKey, offset);

    if (snapshotValue == null) {
      // Returning an error response as the snapshot value was not found
      return ReadShareGroupStateResponse.toErrorResponseData(
          topicId,
          partition,
          Errors.UNKNOWN_SERVER_ERROR,
          "Data not found for topic {}, partition {} for group {}, in the in-memory state of share coordinator"
      );
    }

    List<ReadShareGroupStateResponseData.StateBatch> stateBatches = (snapshotValue.stateBatches() != null && !snapshotValue.stateBatches().isEmpty()) ?
        snapshotValue.stateBatches().stream().map(
            stateBatch -> new ReadShareGroupStateResponseData.StateBatch()
                .setFirstOffset(stateBatch.firstOffset())
                .setLastOffset(stateBatch.lastOffset())
                .setDeliveryState(stateBatch.deliveryState())
                .setDeliveryCount(stateBatch.deliveryCount())
        ).collect(java.util.stream.Collectors.toList()) : Collections.emptyList();

    // Returning the successfully retrieved snapshot value
    return ReadShareGroupStateResponse.toResponseData(topicId, partition, snapshotValue.startOffset(), snapshotValue.stateEpoch(), stateBatches);
  }

  private Optional<CoordinatorResult<WriteShareGroupStateResponseData, Record>> maybeGetWriteStateError(WriteShareGroupStateRequestData request) {
    String groupId = request.groupId();
    WriteShareGroupStateRequestData.WriteStateData topicData = request.topics().get(0);
    WriteShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

    Uuid topicId = topicData.topicId();
    int partitionId = partitionData.partition();

    if (topicId == null || partitionId < 0) {
      return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicId, partitionId));
    }

    String mapKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partitionId);
    if (leaderMap.containsKey(mapKey) && leaderMap.get(mapKey) > partitionData.leaderEpoch()) {
      return Optional.of(getWriteErrorResponse(Errors.FENCED_LEADER_EPOCH, topicId, partitionId));
    }
    if (metadataImage != null && (metadataImage.topics().getTopic(topicId) == null ||
        metadataImage.topics().getPartition(topicId, partitionId) == null)) {
      return Optional.of(getWriteErrorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicId, partitionId));
    }

    return Optional.empty();
  }

  private Optional<ReadShareGroupStateResponseData> maybeGetReadStateError(ReadShareGroupStateRequestData request) {
    String groupId = request.groupId();
    ReadShareGroupStateRequestData.ReadStateData topicData = request.topics().get(0);
    ReadShareGroupStateRequestData.PartitionData partitionData = topicData.partitions().get(0);

    Uuid topicId = topicData.topicId();
    int partitionId = partitionData.partition();

    if (topicId == null || partitionId < 0) {
      return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
    }

    String mapKey = ShareGroupHelper.coordinatorKey(groupId, topicId, partitionId);
    if (leaderMap.containsKey(mapKey) && leaderMap.get(mapKey) > partitionData.leaderEpoch()) {
      return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message()));
    }

    // Updating the leader map with the new leader epoch
    leaderMap.put(mapKey, partitionData.leaderEpoch());

    if (metadataImage != null && (metadataImage.topics().getTopic(topicId) == null ||
        metadataImage.topics().getPartition(topicId, partitionId) == null)) {
      return Optional.of(ReadShareGroupStateResponse.toErrorResponseData(topicId, partitionId, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message()));
    }

    return Optional.empty();
  }

  private CoordinatorResult<WriteShareGroupStateResponseData, Record> getWriteErrorResponse(Errors error, Uuid topicId, int partitionId) {
    WriteShareGroupStateResponseData responseData = WriteShareGroupStateResponse.toErrorResponseData(topicId, partitionId, error, error.message());
    return new CoordinatorResult<>(Collections.emptyList(), responseData);
  }
}

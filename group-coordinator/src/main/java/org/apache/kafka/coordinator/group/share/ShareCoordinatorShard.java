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
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.group.generated.ShareUpdateKey;
import org.apache.kafka.coordinator.group.generated.ShareUpdateValue;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
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

import java.util.HashMap;
import java.util.Map;

public class ShareCoordinatorShard implements CoordinatorShard<Record> {
  private final Logger log;
  private final Time time;
  private final CoordinatorTimer<Void, Record> timer;
  private final ShareCoordinatorConfig config;
  private final CoordinatorMetrics coordinatorMetrics;
  private final CoordinatorMetricsShard metricsShard;
  private final TimelineHashMap<String, ShareSnapshotValue> shareStateMap;  // coord key -> ShareSnapshotValue
  private final Map<String, Integer> leaderMap;

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
    CoordinatorShard.super.onNewMetadataImage(newImage, delta);
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
}

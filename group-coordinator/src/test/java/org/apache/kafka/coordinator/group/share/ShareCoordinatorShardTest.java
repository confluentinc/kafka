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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ReadShareGroupStateRequestData;
import org.apache.kafka.common.message.ReadShareGroupStateResponseData;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.common.message.WriteShareGroupStateResponseData;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ReadShareGroupStateResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.WriteShareGroupStateResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.group.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.Record;
import org.apache.kafka.coordinator.group.RecordHelpers;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.group.generated.ShareSnapshotValue;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.group.share.ShareGroupHelper;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class ShareCoordinatorShardTest {

  private static final String GROUP_ID = "group1";
  private static final Uuid TOPIC_ID = Uuid.randomUuid();
  private static final int PARTITION = 0;

  public static class ShareCoordinatorShardBuilder {
    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();
    private final MockCoordinatorTimer<Void, Record> timer = new MockCoordinatorTimer<>(time);
    private ShareCoordinatorConfig config = null;
    private CoordinatorMetrics coordinatorMetrics = mock(CoordinatorMetrics.class);
    private CoordinatorMetricsShard metricsShard = mock(CoordinatorMetricsShard.class);
    private final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
    private MetadataImage metadataImage = null;

    ShareCoordinatorShard build() {
      if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
      if (config == null) {
        config = ShareCoordinatorConfigTest.createShareCoordinatorConfig(1000, 1, 5000);
      }

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

  private RequestContext getDefaultWriteStateContext() {
    return new RequestContext(
        new RequestHeader(
            ApiKeys.WRITE_SHARE_GROUP_STATE,
            ApiKeys.WRITE_SHARE_GROUP_STATE.latestVersion(),
            "client",
            0
        ),
        "1",
        InetAddress.getLoopbackAddress(),
        KafkaPrincipal.ANONYMOUS,
        ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
        SecurityProtocol.PLAINTEXT,
        ClientInformation.EMPTY,
        false
    );
  }

  private void writeAndReplayDefaultRecord(ShareCoordinatorShard shard) {
    writeAndReplayRecord(shard, 0);
  }

  private void writeAndReplayRecord(ShareCoordinatorShard shard, int leaderEpoch) {

    WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(leaderEpoch)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    RequestContext context = getDefaultWriteStateContext();

    CoordinatorResult<WriteShareGroupStateResponseData, Record> result = shard.writeState(context, request);

    shard.replay(0L, 0L, (short) 0, result.records().get(0));
  }

  @Test
  public void testReplayWithShareSnapshot() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    long offset = 0;
    long producerId = 0;
    short producerEpoch = 0;

    int leaderEpoch = 1;

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    Record record1 = new Record(
        new ApiMessageAndVersion(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(PARTITION),
            (short) 0
        ),
        new ApiMessageAndVersion(
            new ShareSnapshotValue()
                .setSnapshotEpoch(0)
                .setStateEpoch(0)
                .setLeaderEpoch(leaderEpoch)
                .setStateBatches(Collections.singletonList(
                    new ShareSnapshotValue.StateBatch()
                        .setFirstOffset(0)
                        .setLastOffset(10)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0))),
            (short) 0
        )
    );

    Record record2 = new Record(
        new ApiMessageAndVersion(
            new ShareSnapshotKey()
                .setGroupId(GROUP_ID)
                .setTopicId(TOPIC_ID)
                .setPartition(PARTITION),
            (short) 0
        ),
        new ApiMessageAndVersion(
            new ShareSnapshotValue()
                .setSnapshotEpoch(1)
                .setStateEpoch(1)
                .setLeaderEpoch(leaderEpoch + 1)
                .setStateBatches(Collections.singletonList(
                    new ShareSnapshotValue.StateBatch()
                        .setFirstOffset(11)
                        .setLastOffset(12)
                        .setDeliveryCount((short) 1)
                        .setDeliveryState((byte) 0))),
            (short) 0
        )
    );

    // First replay should populate values in otherwise empty shareStateMap and leaderMap
    shard.replay(offset, producerId, producerEpoch, record1);

    assertEquals(record1.value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(leaderEpoch, shard.getLeaderMapValue(shareCoordinatorKey));


    // Second replay should update the existing values in shareStateMap and leaderMap
    shard.replay(offset + 1, producerId, producerEpoch, record2);

    assertEquals(record2.value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(leaderEpoch + 1, shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testWriteStateSuccess() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(0)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    RequestContext context = getDefaultWriteStateContext();

    CoordinatorResult<WriteShareGroupStateResponseData, Record> result = shard.writeState(context, request);

    shard.replay(0L, 0L, (short) 0, result.records().get(0));

    WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
    List<Record> expectedRecords = Collections.singletonList(RecordHelpers.newShareSnapshotRecord(
        GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
    ));

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    assertEquals(RecordHelpers.newShareSnapshotRecord(
        GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request.topics().get(0).partitions().get(0))
    ).value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testSubsequentWriteStateSnapshotEpochUpdatesSuccessfully() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(0)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(0)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(11)
                    .setLastOffset(20)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    RequestContext context = getDefaultWriteStateContext();

    CoordinatorResult<WriteShareGroupStateResponseData, Record> result = shard.writeState(context, request1);

    shard.replay(0L, 0L, (short) 0, result.records().get(0));

    WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
    List<Record> expectedRecords = Collections.singletonList(RecordHelpers.newShareSnapshotRecord(
        GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0))
    ));

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    assertEquals(expectedRecords.get(0).value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));

    result = shard.writeState(context, request2);

    shard.replay(0L, 0L, (short) 0, result.records().get(0));

    expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
    expectedRecords = Collections.singletonList(RecordHelpers.newShareSnapshotRecord(
        GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request2.topics().get(0).partitions().get(0), 1)
    ));

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    assertEquals(expectedRecords.get(0).value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testWriteStateInvalidRequestData() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    int partition = -1;

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, partition);

    WriteShareGroupStateRequestData request = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(partition)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(0)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    RequestContext context = getDefaultWriteStateContext();

    CoordinatorResult<WriteShareGroupStateResponseData, Record> result = shard.writeState(context, request);

    WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toErrorResponseData(
        TOPIC_ID, partition, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
    List<Record> expectedRecords = Collections.emptyList();

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    assertNull(shard.getShareStateMapValue(shareCoordinatorKey));
    assertNull(shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testWriteStateFencedLeaderEpochError() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    WriteShareGroupStateRequestData request1 = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(5)
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(0)
                    .setLastOffset(10)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    WriteShareGroupStateRequestData request2 = new WriteShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new WriteShareGroupStateRequestData.WriteStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new WriteShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setStartOffset(0)
                .setStateEpoch(0)
                .setLeaderEpoch(3) // lower leader epoch in the second request
                .setStateBatches(Collections.singletonList(new WriteShareGroupStateRequestData.StateBatch()
                    .setFirstOffset(11)
                    .setLastOffset(20)
                    .setDeliveryCount((short) 1)
                    .setDeliveryState((byte) 0)))))));

    RequestContext context = getDefaultWriteStateContext();

    CoordinatorResult<WriteShareGroupStateResponseData, Record> result = shard.writeState(context, request1);

    shard.replay(0L, 0L, (short) 0, result.records().get(0));

    WriteShareGroupStateResponseData expectedData = WriteShareGroupStateResponse.toResponseData(TOPIC_ID, PARTITION);
    List<Record> expectedRecords = Collections.singletonList(RecordHelpers.newShareSnapshotRecord(
        GROUP_ID, TOPIC_ID, PARTITION, ShareGroupOffset.fromRequest(request1.topics().get(0).partitions().get(0))
    ));

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    assertEquals(expectedRecords.get(0).value().message(), shard.getShareStateMapValue(shareCoordinatorKey));
    assertEquals(5, shard.getLeaderMapValue(shareCoordinatorKey));

    result = shard.writeState(context, request2);

    // Since the leader epoch in the second request was lower than the one in the first request, FENCED_LEADER_EPOCH error is expected
    expectedData = WriteShareGroupStateResponse.toErrorResponseData(
        TOPIC_ID, PARTITION, Errors.FENCED_LEADER_EPOCH, Errors.FENCED_LEADER_EPOCH.message());
    expectedRecords = Collections.emptyList();

    assertEquals(expectedData, result.response());
    assertEquals(expectedRecords, result.records());

    // No changes to th3 leaderMap
    assertEquals(5, shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testReadStateSuccess() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    String coordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    writeAndReplayDefaultRecord(shard);

    ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setLeaderEpoch(1)))));

    ReadShareGroupStateResponseData result = shard.readState(request, 0L);

    assertEquals(ReadShareGroupStateResponse.toResponseData(
        TOPIC_ID,
        PARTITION,
        0,
        0,
        Collections.singletonList(new ReadShareGroupStateResponseData.StateBatch()
            .setFirstOffset(0)
            .setLastOffset(10)
            .setDeliveryCount((short) 1)
            .setDeliveryState((byte) 0)
        )
    ), result);

    assertEquals(1, shard.getLeaderMapValue(coordinatorKey));
  }

  @Test
  public void testReadStateShareStateMapEmpty() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    String coordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setLeaderEpoch(1)))));

    ReadShareGroupStateResponseData result = shard.readState(request, 0L);

    assertEquals(ReadShareGroupStateResponse.toErrorResponseData(
        TOPIC_ID,
        PARTITION,
        Errors.UNKNOWN_SERVER_ERROR,
        "Data not found for topic {}, partition {} for group {}, in the in-memory state of share coordinator"
    ), result);

    assertNull(shard.getLeaderMapValue(coordinatorKey));
  }

  @Test
  public void testReadStateInvalidRequestData() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    int partition = -1;

    writeAndReplayDefaultRecord(shard);

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                .setPartition(partition)
                .setLeaderEpoch(5)))));

    ReadShareGroupStateResponseData result = shard.readState(request, 0L);

    ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
        TOPIC_ID, partition, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.UNKNOWN_TOPIC_OR_PARTITION.message());

    assertEquals(expectedData, result);

    // Leader epoch should not be changed because the request failed
    assertEquals(0, shard.getLeaderMapValue(shareCoordinatorKey));
  }

  @Test
  public void testReadStateFencedLeaderEpochError() {
    ShareCoordinatorShard shard = new ShareCoordinatorShardBuilder().build();

    int leaderEpoch = 5;

    writeAndReplayRecord(shard, leaderEpoch); // leaderEpoch in the leaderMap will be 5

    String shareCoordinatorKey = ShareGroupHelper.coordinatorKey(GROUP_ID, TOPIC_ID, PARTITION);

    ReadShareGroupStateRequestData request = new ReadShareGroupStateRequestData()
        .setGroupId(GROUP_ID)
        .setTopics(Collections.singletonList(new ReadShareGroupStateRequestData.ReadStateData()
            .setTopicId(TOPIC_ID)
            .setPartitions(Collections.singletonList(new ReadShareGroupStateRequestData.PartitionData()
                .setPartition(PARTITION)
                .setLeaderEpoch(3))))); // lower leaderEpoch than the one stored in leaderMap

    ReadShareGroupStateResponseData result = shard.readState(request, 0L);

    ReadShareGroupStateResponseData expectedData = ReadShareGroupStateResponse.toErrorResponseData(
        TOPIC_ID,
        PARTITION,
        Errors.FENCED_LEADER_EPOCH,
        Errors.FENCED_LEADER_EPOCH.message());

    assertEquals(expectedData, result);

    assertEquals(leaderEpoch, shard.getLeaderMapValue(shareCoordinatorKey));
  }
}
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.message.SyncGroupRequestData
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.group.classic.ClassicGroupState

import java.util.Collections
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

@ClusterTestDefaults(
  types = Array(Type.KRAFT),
  serverProperties = Array(
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
    new ClusterConfigProperty(key = GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "1000")
  )
)
class SyncGroupRequestTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {
  @ClusterTest
  def testSyncGroupWithOldConsumerGroupProtocol(): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    for (version <- ApiKeys.SYNC_GROUP.oldestVersion() to ApiKeys.SYNC_GROUP.latestVersion(isUnstableApiEnabled)) {
      // Sync with unknown group id.
      verifySyncGroupWithOldProtocol(
        groupId = "grp-unknown",
        memberId = "member-id",
        generationId = -1,
        expectedProtocolType = null,
        expectedProtocolName = null,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Sync with empty group id.
      verifySyncGroupWithOldProtocol(
        groupId = "",
        memberId = "member-id",
        generationId = -1,
        expectedProtocolType = null,
        expectedProtocolName = null,
        expectedError = Errors.INVALID_GROUP_ID,
        version = version.toShort
      )

      val metadata = ConsumerProtocol.serializeSubscription(
        new ConsumerPartitionAssignor.Subscription(Collections.singletonList("foo"))
      ).array

      // Join a dynamic member without member id.
      // Prior to JoinGroup version 4, a new member is immediately added if it sends a join group request with UNKNOWN_MEMBER_ID.
      val joinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        metadata = metadata
      )
      val leaderMemberId = joinLeaderResponseData.memberId

      // Rejoin the group with the member id.
      sendJoinRequest(
        groupId = "grp",
        memberId = leaderMemberId,
        metadata = metadata
      )

      if (version >= 5) {
        // Sync the leader with unmatched protocolName.
        verifySyncGroupWithOldProtocol(
          groupId = "grp",
          memberId = leaderMemberId,
          generationId = 1,
          protocolName = "unmatched",
          assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1))
          ),
          expectedProtocolType = null,
          expectedProtocolName = null,
          expectedError = Errors.INCONSISTENT_GROUP_PROTOCOL,
          version = version.toShort
        )

        // Sync the leader with unmatched protocolType.
        verifySyncGroupWithOldProtocol(
          groupId = "grp",
          memberId = leaderMemberId,
          generationId = 1,
          protocolType = "unmatched",
          assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1))
          ),
          expectedProtocolType = null,
          expectedProtocolName = null,
          expectedError = Errors.INCONSISTENT_GROUP_PROTOCOL,
          version = version.toShort
        )
      }

      // Sync with unknown member id.
      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = "member-id-unknown",
        generationId = -1,
        expectedProtocolType = null,
        expectedProtocolName = null,
        expectedError = Errors.UNKNOWN_MEMBER_ID,
        version = version.toShort
      )

      // Sync with illegal generation id.
      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = 2,
        expectedProtocolType = null,
        expectedProtocolName = null,
        expectedError = Errors.ILLEGAL_GENERATION,
        version = version.toShort
      )

      // Sync the leader with empty protocolType and protocolName if version < 5.
      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = 1,
        protocolType = if (version < 5) null else "consumer",
        protocolName = if (version < 5) null else "consumer-range",
        assignments = List(new SyncGroupRequestData.SyncGroupRequestAssignment()
          .setMemberId(leaderMemberId)
          .setAssignment(Array[Byte](1))
        ),
        expectedProtocolType = if (version < 5) null else "consumer",
        expectedProtocolName = if (version < 5) null else "consumer-range",
        expectedAssignment = Array[Byte](1),
        version = version.toShort
      )

      // Join a second member.
      val joinFollowerFuture = Future {
        sendJoinRequest(
          groupId = "grp",
          groupInstanceId = "group-instance-id",
          metadata = metadata
        )
      }

      TestUtils.waitUntilTrue(() => {
        val described = describeGroups(groupIds = List("grp"))
        ClassicGroupState.PREPARING_REBALANCE.toString == described.head.groupState
      }, msg = s"The group is not in PREPARING_REBALANCE state.")

      // The leader rejoins.
      val rejoinLeaderResponseData = sendJoinRequest(
        groupId = "grp",
        memberId = leaderMemberId,
        metadata = metadata
      )

      val joinFollowerFutureResponseData = Await.result(joinFollowerFuture, Duration.Inf)
      val followerMemberId = joinFollowerFutureResponseData.memberId

      // Sync the leader ahead of the follower.
      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = rejoinLeaderResponseData.generationId,
        assignments = List(
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1)),
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(followerMemberId)
            .setAssignment(Array[Byte](2))
        ),
        expectedAssignment = Array[Byte](1),
        version = version.toShort
      )

      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = followerMemberId,
        generationId = joinFollowerFutureResponseData.generationId,
        expectedAssignment = Array[Byte](2),
        version = version.toShort
      )

      // Sync the follower ahead of the leader.
      val syncFollowerFuture = Future {
        verifySyncGroupWithOldProtocol(
          groupId = "grp",
          memberId = followerMemberId,
          generationId = 2,
          expectedAssignment = Array[Byte](2),
          version = version.toShort
        )
      }

      verifySyncGroupWithOldProtocol(
        groupId = "grp",
        memberId = leaderMemberId,
        generationId = 2,
        assignments = List(
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(leaderMemberId)
            .setAssignment(Array[Byte](1)),
          new SyncGroupRequestData.SyncGroupRequestAssignment()
            .setMemberId(followerMemberId)
            .setAssignment(Array[Byte](2))
        ),
        expectedAssignment = Array[Byte](1),
        version = version.toShort
      )

      Await.result(syncFollowerFuture, Duration.Inf)

      leaveGroup(
        groupId = "grp",
        memberId = leaderMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )
      leaveGroup(
        groupId = "grp",
        memberId = followerMemberId,
        useNewProtocol = false,
        version = ApiKeys.LEAVE_GROUP.latestVersion(isUnstableApiEnabled)
      )

      deleteGroups(
        groupIds = List("grp"),
        expectedErrors = List(Errors.NONE),
        version = ApiKeys.DELETE_GROUPS.latestVersion(isUnstableApiEnabled)
      )
    }
  }
}

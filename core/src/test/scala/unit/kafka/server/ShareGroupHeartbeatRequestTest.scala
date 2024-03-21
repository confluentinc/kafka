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
package kafka.server

import kafka.test.ClusterInstance
import kafka.test.annotation.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import kafka.test.junit.ClusterTestExtensions
import kafka.test.junit.RaftClusterInvocationContext.RaftClusterInstance
import kafka.utils.TestUtils
import org.apache.kafka.common.message.{ShareGroupHeartbeatRequestData, ShareGroupHeartbeatResponseData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNotNull}
import org.junit.jupiter.api.{Disabled, Tag, Timeout}
import org.junit.jupiter.api.extension.ExtendWith

import java.util.stream.Collectors
import scala.jdk.CollectionConverters._

@Timeout(120)
@ExtendWith(value = Array(classOf[ClusterTestExtensions]))
@ClusterTestDefaults(clusterType = Type.KRAFT, brokers = 1)
@Tag("integration")
class ShareGroupHeartbeatRequestTest(cluster: ClusterInstance) {

  @ClusterTest()
  def testShareGroupHeartbeatIsAccessibleWhenEnabled(): Unit = {
    val shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData(), true
    ).build()

    val shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
    val expectedResponse = new ShareGroupHeartbeatResponseData().setErrorCode(Errors.UNSUPPORTED_VERSION.code)
    assertEquals(expectedResponse, shareGroupHeartbeatResponse.data)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testShareGroupHeartbeatIsAccessibleWhenShareGroupIsEnabled(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertNotNull(shareGroupHeartbeatResponse.data.memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // Prepare the next heartbeat.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(shareGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(shareGroupHeartbeatResponse.data.memberEpoch), true
    ).build()

    // This is the expected assignment.
    val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Heartbeats until the partitions are assigned.
    shareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(expectedAssignment, shareGroupHeartbeatResponse.data.assignment)

    // Leave the group.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(shareGroupHeartbeatResponse.data.memberId)
        .setMemberEpoch(-1), true
    ).build()

    shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)

    // Verify the response.
    assertEquals(-1, shareGroupHeartbeatResponse.data.memberEpoch)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testShareGroupHeartbeatWithMultipleMembers(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response for member 1.
    val memberId1 = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId1)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

    // Send the second member request until receiving a successful response.
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response for member 2.
    val memberId2 = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId2)
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
    // Verify the member id is different.
    assertNotEquals(memberId1, memberId2)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 3
    )

    // This is the expected assignment.
    val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)

    // Prepare the next heartbeat for member 1.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId1)
        .setMemberEpoch(1), true
    ).build()

    // Heartbeats until the partitions are assigned for member 1.
    shareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)

    // Prepare the next heartbeat for member 2.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId2)
        .setMemberEpoch(2), true
    ).build()

    // Heartbeats until the partitions are assigned for member 2.
    shareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)

    // Verify the assignments are not changed for member 1.
    // Prepare another heartbeat for member 1 with latest received epoch 3 for member 1.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId1)
        .setMemberEpoch(3), true
    ).build()

    // Heartbeats until the response for no change of assignment occurs for member 1 with same epoch.
    shareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == null
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testMemberLeavingAndRejoining(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response for member.
    val memberId = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val topicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 2
    )

    // This is the expected assignment.
    val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(topicId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(1), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)

    // Member leaves the group.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(-1)
        .setMemberId(memberId), true
    ).build()

    // Send the member request until receiving a successful response.
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not leave the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response for member.
    assertEquals(-1, shareGroupHeartbeatResponse.data.memberEpoch)

    // Member sends request to rejoin the group.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setMemberId(memberId)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)

    // Verify the response for member 1.
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(memberId, shareGroupHeartbeatResponse.data.memberId)
    // Partition assignment remains intact on rejoining.
    assertEquals(expectedAssignment, shareGroupHeartbeatResponse.data.assignment)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testPartitionAssignmentWithChangingTopics(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )
    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(5 * 60 * 1000)
        .setSubscribedTopicNames(List("foo", "bar", "baz").asJava), true
    ).build()
    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")
    // Verify the response for member.
    val memberId = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
    // Create the topic foo.
    val fooTopicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 2
    )
    // Create the topic bar.
    val barTopicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "bar",
      numPartitions = 3
    )

    var expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(fooTopicId)
          .setPartitions(List[Integer](0, 1).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(barTopicId)
          .setPartitions(List[Integer](0, 1, 2).asJava)).asJava)
    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(1), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
      shareGroupHeartbeatResponse.data.assignment != null &&
      expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
      shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not get partitions for topic foo and bar assigned. Last response $shareGroupHeartbeatResponse.")
    // Verify the response.
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
    // Create the topic baz.
    val bazTopicId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "baz",
      numPartitions = 4
    )

    expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(fooTopicId)
          .setPartitions(List[Integer](0, 1).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(barTopicId)
          .setPartitions(List[Integer](0, 1, 2).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(bazTopicId)
          .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(3), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment != null &&
        expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
        shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not get partitions for topic baz assigned. Last response $shareGroupHeartbeatResponse.")
    // Verify the response.
    assertEquals(4, shareGroupHeartbeatResponse.data.memberEpoch)
    // Increasing the partitions of topic bar which is already being consumed in the share group.
    TestUtils.increasePartitions(admin, "bar", 6, Seq.empty)

    expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(fooTopicId)
          .setPartitions(List[Integer](0, 1).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(barTopicId)
          .setPartitions(List[Integer](0, 1, 2, 3, 4, 5).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(bazTopicId)
          .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(4), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment != null &&
        expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
        shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not update partitions assignment for topic bar. Last response $shareGroupHeartbeatResponse.")
    // Verify the response.
    assertEquals(5, shareGroupHeartbeatResponse.data.memberEpoch)
    // Delete the topic foo.
    TestUtils.deleteTopicWithAdmin(
      admin = admin,
      topic = "foo",
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(barTopicId)
          .setPartitions(List[Integer](0, 1, 2, 3, 4, 5).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(bazTopicId)
          .setPartitions(List[Integer](0, 1, 2, 3).asJava)).asJava)
    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(5), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment != null &&
        expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
        shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not update partitions assignment for topic foo. Last response $shareGroupHeartbeatResponse.")
    // Verify the response.
    assertEquals(6, shareGroupHeartbeatResponse.data.memberEpoch)
  }

  @Disabled
  //TODO: The heartbeat interval and session timeout should be for share group and not consumer group.
  // Working with these configs until we have the share group configs available
  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    new ClusterConfigProperty(key = "group.consumer.heartbeat.interval.ms", value = "500"),
    new ClusterConfigProperty(key = "group.consumer.min.heartbeat.interval.ms", value = "500"),
    new ClusterConfigProperty(key = "group.consumer.session.timeout.ms", value = "500"),
    new ClusterConfigProperty(key = "group.consumer.min.session.timeout.ms", value = "500")
  ))
  def testMemberJoiningAndExpiring(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()

    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )

    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(500)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")

    // Verify the response for member.
    val memberId = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)

    // Create the topic.
    val fooId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 2
    )

    val barId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "bar",
      numPartitions = 1
    )

    // This is the expected assignment.
    var expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(fooId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)

    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(1), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get foo partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)

    // Prepare the next heartbeat with a new subscribed topic.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(2)
        .setSubscribedTopicNames(List("foo", "bar").asJava), true
    ).build()

    expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(
        new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(fooId)
        .setPartitions(List[Integer](0, 1).asJava),
        new ShareGroupHeartbeatResponseData.TopicPartitions()
          .setTopicId(barId)
          .setPartitions(List[Integer](0).asJava)).asJava)

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
      shareGroupHeartbeatResponse.data.assignment != null &&
      expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
      shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not get bar partitions assigned. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(4, shareGroupHeartbeatResponse.data.memberEpoch)

    // Prepare the next heartbeat which is empty to verify no assignment changes.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(4), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not get empty heartbeat response. Last response $shareGroupHeartbeatResponse.")

    // Verify the response.
    assertEquals(4, shareGroupHeartbeatResponse.data.memberEpoch)

    // Blocking the thread for 1 sec so that the session times out and the member needs to rejoin.
    Thread.sleep(1000)

    // Prepare the next heartbeat which is empty to verify no assignment changes.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(4), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.UNKNOWN_MEMBER_ID.code
    }, msg = s"Member should have been expired because of the timeout . Last response $shareGroupHeartbeatResponse.")

    // Member sends a request again to join the share group
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(500)
        .setSubscribedTopicNames(List("foo", "bar").asJava), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment != null &&
        expectedAssignment.assignedTopicPartitions.containsAll(shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions) &&
        shareGroupHeartbeatResponse.data.assignment.assignedTopicPartitions.containsAll(expectedAssignment.assignedTopicPartitions)
    }, msg = s"Could not get bar partitions assigned upon rejoining. Last response $shareGroupHeartbeatResponse.")

    assertEquals(6, shareGroupHeartbeatResponse.data.memberEpoch)
  }

  @ClusterTest(serverProperties = Array(
    new ClusterConfigProperty(key = "group.coordinator.new.enable", value = "true"),
    new ClusterConfigProperty(key = "group.share.enable", value = "true"),
    new ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
    new ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
  ))
  def testGroupCoordinatorChange(): Unit = {
    val raftCluster = cluster.asInstanceOf[RaftClusterInstance]
    val admin = cluster.createAdminClient()
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    TestUtils.createOffsetsTopicWithAdmin(
      admin = admin,
      brokers = raftCluster.brokers.collect(Collectors.toList[BrokerServer]).asScala,
      controllers = raftCluster.controllerServers().asScala.toSeq
    )
    // Heartbeat request to join the group. Note that the member subscribes
    // to an nonexistent topic.
    var shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(500)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()
    // Send the request until receiving a successful response. There is a delay
    // here because the group coordinator is loaded in the background.
    var shareGroupHeartbeatResponse: ShareGroupHeartbeatResponse = null
    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code
    }, msg = s"Could not join the group successfully. Last response $shareGroupHeartbeatResponse.")
    // Verify the response for member.
    val memberId = shareGroupHeartbeatResponse.data.memberId
    assertNotNull(memberId)
    assertEquals(1, shareGroupHeartbeatResponse.data.memberEpoch)
    assertEquals(new ShareGroupHeartbeatResponseData.Assignment(), shareGroupHeartbeatResponse.data.assignment)
    // Create the topic.
    val fooId = TestUtils.createTopicWithAdminRaw(
      admin = admin,
      topic = "foo",
      numPartitions = 2
    )
    // This is the expected assignment.
    val expectedAssignment = new ShareGroupHeartbeatResponseData.Assignment()
      .setAssignedTopicPartitions(List(new ShareGroupHeartbeatResponseData.TopicPartitions()
        .setTopicId(fooId)
        .setPartitions(List[Integer](0, 1).asJava)).asJava)
    // Prepare the next heartbeat for member.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(1), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned. Last response $shareGroupHeartbeatResponse.")
    // Verify the response.
    assertEquals(2, shareGroupHeartbeatResponse.data.memberEpoch)

    // Restart the only running broker.
    val broker = raftCluster.brokers().iterator().next()
    raftCluster.shutdownBroker(broker.config.brokerId)
    raftCluster.startBroker(broker.config.brokerId)

    // Prepare the next heartbeat for member with no updates.
    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberId(memberId)
        .setMemberEpoch(2), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.UNKNOWN_MEMBER_ID.code
    }, msg = s"Could not change the group coordinator. Last response $shareGroupHeartbeatResponse.")

    // Verify the response
    assertEquals(0, shareGroupHeartbeatResponse.data.memberEpoch)

    shareGroupHeartbeatRequest = new ShareGroupHeartbeatRequest.Builder(
      new ShareGroupHeartbeatRequestData()
        .setGroupId("grp")
        .setMemberEpoch(0)
        .setRebalanceTimeoutMs(500)
        .setSubscribedTopicNames(List("foo").asJava), true
    ).build()

    TestUtils.waitUntilTrue(() => {
      shareGroupHeartbeatResponse = connectAndReceive(shareGroupHeartbeatRequest)
      shareGroupHeartbeatResponse.data.errorCode == Errors.NONE.code &&
        shareGroupHeartbeatResponse.data.assignment == expectedAssignment
    }, msg = s"Could not get partitions assigned after rejoin. Last response $shareGroupHeartbeatResponse.")

    // Verify the response
    assertEquals(3, shareGroupHeartbeatResponse.data.memberEpoch)
    assertNotEquals(memberId, shareGroupHeartbeatResponse.data.memberId)
  }

  private def connectAndReceive(request: ShareGroupHeartbeatRequest): ShareGroupHeartbeatResponse = {
    IntegrationTestUtils.connectAndReceive[ShareGroupHeartbeatResponse](
      request,
      cluster.anyBrokerSocketServer(),
      cluster.clientListener()
    )
  }
}

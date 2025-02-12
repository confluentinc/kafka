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

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.junit.jupiter.api.Timeout

import java.util.Collections
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

@Timeout(120)
@ClusterTestDefaults(types = Array(Type.KRAFT))
class ConsumerProtocolMigrationTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  @ClusterTest(
    serverProperties = Array(
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
      new ClusterConfigProperty(key = GroupCoordinatorConfig.CONSUMER_GROUP_MIGRATION_POLICY_CONFIG, value = "bidirectional")
    )
  )
  def testOnlineMigrationWithEagerAssignmentStrategyAndDynamicMembers(): Unit = {
    testOnlineMigrationWithEagerAssignmentStrategy(useStaticMembers = false)
  }

  /**
   * The test method checks the following scenario:
   * 1. Creating a classic group with member 1.
   * 2. Member 2 using consumer protocol joins. The group is upgraded and a rebalance is triggered.
   * 3. Member 1 performs different operations. (Heartbeat, OffsetCommit, OffsetFetch)
   * 4. Member 1 reconciles with the cooperative strategy. It revokes all its partitions and rejoin.
   * 5. Member 2 rejoins. The groups is stabilized.
   * 6. Member 2 leaves. The group is downgraded.
   *
   * @param useStaticMembers A boolean indicating whether member 1 and member 2 are static members.
   */
  private def testOnlineMigrationWithEagerAssignmentStrategy(useStaticMembers: Boolean): Unit = {
    // Creates the __consumer_offsets topics because it won't be created automatically
    // in this test because it does not use FindCoordinator API.
    createOffsetsTopic()

    // Create the topic.
    createTopic(
      topic = "foo",
      numPartitions = 3
    )

    val groupId = "grp"
    val numMembers = 1000

    val threadPool = Executors.newFixedThreadPool(2000)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
    (0 until numMembers).map { i =>
      Future {
        joinDynamicConsumerGroupWithOldProtocol(
          groupId = groupId,
          metadata = metadata(List.empty),
          assignment = assignment(List.empty),
          completeRebalance = false,
        )
      }
    }

    TestUtils.waitUntilTrue(() => {
      val described = describeGroups(groupIds = List(groupId))
      described.head.members().size() >= numMembers
    }, msg = s"The members have not all joined.", waitTimeMs = 6000000)

    threadPool.shutdown()
    try {
      if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        threadPool.shutdownNow()
      }
    } catch {
      case e: InterruptedException =>
        threadPool.shutdownNow()
        Thread.currentThread().interrupt()
    }
    System.gc()

    // Upgrade triggered.
    consumerGroupHeartbeat(
      groupId = groupId,
      memberId = Uuid.randomUuid.toString,
      instanceId = null,
      rebalanceTimeoutMs = 5 * 60 * 1000,
      subscribedTopicNames = List("foo"),
      topicPartitions = List.empty,
      expectedError = Errors.NONE
    ).memberId
  }

  private def metadata(ownedPartitions: List[Int]): Array[Byte] = {
    ConsumerProtocol.serializeSubscription(
      new ConsumerPartitionAssignor.Subscription(
        Collections.singletonList("foo"),
        null,
        ownedPartitions.map(new TopicPartition("foo", _)).asJava
      )
    ).array
  }

  private def assignment(assignedPartitions: List[Int]): Array[Byte] = {
    ConsumerProtocol.serializeAssignment(
      new ConsumerPartitionAssignor.Assignment(
        assignedPartitions.map(new TopicPartition("foo", _)).asJava,
        null
      )
    ).array
  }
}

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
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.requests.{FetchRequest, FetchResponse, FetchMetadata => JFetchMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.{IsolationLevel, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.server.record.BrokerCompressionType
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util
import java.util.Optional
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.util.Random

/**
  * Subclasses of `BaseConsumerTest` exercise the consumer and fetch request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class FetchRequestTest extends BaseFetchRequestTest {

  @Test
  def testBrokerRespectsPartitionsOrderAndSizeLimits(): Unit = {
    initProducer()

    val messagesPerPartition = 9
    val maxResponseBytes = 800
    val maxPartitionBytes = 190

    def createConsumerFetchRequest(topicPartitions: Seq[TopicPartition], offsetMap: Map[TopicPartition, Long] = Map.empty,
                           version: Short = ApiKeys.FETCH.latestVersion()): FetchRequest =
      this.createConsumerFetchRequest(maxResponseBytes, maxPartitionBytes, topicPartitions, offsetMap, version)

    val topicPartitionToLeader = createTopics(numTopics = 5, numPartitions = 6)
    val random = new Random(0)
    val topicPartitions = topicPartitionToLeader.keySet
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    produceData(topicPartitions, messagesPerPartition)

    val leaderId = brokers.head.config.brokerId
    val partitionsForLeader = topicPartitionToLeader.toVector.collect {
      case (tp, partitionLeaderId) if partitionLeaderId == leaderId => tp
    }

    val partitionsWithLargeMessages = partitionsForLeader.takeRight(2)
    val partitionWithLargeMessage1 = partitionsWithLargeMessages.head
    val partitionWithLargeMessage2 = partitionsWithLargeMessages(1)
    producer.send(new ProducerRecord(partitionWithLargeMessage1.topic, partitionWithLargeMessage1.partition,
      "larger than partition limit", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    producer.send(new ProducerRecord(partitionWithLargeMessage2.topic, partitionWithLargeMessage2.partition,
      "larger than response limit", new String(new Array[Byte](maxResponseBytes + 1)))).get

    val partitionsWithoutLargeMessages = partitionsForLeader.filterNot(partitionsWithLargeMessages.contains)

    // 1. Partitions with large messages at the end
    val shuffledTopicPartitions1 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest1 = createConsumerFetchRequest(shuffledTopicPartitions1)
    val fetchResponse1 = sendFetchRequest(leaderId, fetchRequest1)
    checkFetchResponse(shuffledTopicPartitions1, fetchResponse1, maxPartitionBytes, maxResponseBytes, messagesPerPartition)
    val fetchRequest1V12 = createConsumerFetchRequest(shuffledTopicPartitions1, version = 12)
    val fetchResponse1V12 = sendFetchRequest(leaderId, fetchRequest1V12)
    checkFetchResponse(shuffledTopicPartitions1, fetchResponse1V12, maxPartitionBytes, maxResponseBytes, messagesPerPartition, 12)

    // 2. Same as 1, but shuffled again
    val shuffledTopicPartitions2 = random.shuffle(partitionsWithoutLargeMessages) ++ partitionsWithLargeMessages
    val fetchRequest2 = createConsumerFetchRequest(shuffledTopicPartitions2)
    val fetchResponse2 = sendFetchRequest(leaderId, fetchRequest2)
    checkFetchResponse(shuffledTopicPartitions2, fetchResponse2, maxPartitionBytes, maxResponseBytes, messagesPerPartition)
    val fetchRequest2V12 = createConsumerFetchRequest(shuffledTopicPartitions2, version = 12)
    val fetchResponse2V12 = sendFetchRequest(leaderId, fetchRequest2V12)
    checkFetchResponse(shuffledTopicPartitions2, fetchResponse2V12, maxPartitionBytes, maxResponseBytes, messagesPerPartition, 12)

    // 3. Partition with message larger than the partition limit at the start of the list
    val shuffledTopicPartitions3 = Seq(partitionWithLargeMessage1, partitionWithLargeMessage2) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest3 = createConsumerFetchRequest(shuffledTopicPartitions3, Map(partitionWithLargeMessage1 -> messagesPerPartition))
    val fetchResponse3 = sendFetchRequest(leaderId, fetchRequest3)
    val fetchRequest3V12 = createConsumerFetchRequest(shuffledTopicPartitions3, Map(partitionWithLargeMessage1 -> messagesPerPartition), 12)
    val fetchResponse3V12 = sendFetchRequest(leaderId, fetchRequest3V12)
    def evaluateResponse3(response: FetchResponse, version: Short = ApiKeys.FETCH.latestVersion()): Unit = {
      val responseData = response.responseData(topicNames, version)
      assertEquals(shuffledTopicPartitions3, responseData.keySet.asScala.toSeq)
      val responseSize = responseData.asScala.values.map { partitionData =>
        records(partitionData).map(_.sizeInBytes).sum
      }.sum
      assertTrue(responseSize <= maxResponseBytes)
      val partitionData = responseData.get(partitionWithLargeMessage1)
      assertEquals(Errors.NONE.code, partitionData.errorCode)
      assertTrue(partitionData.highWatermark > 0)
      val size3 = records(partitionData).map(_.sizeInBytes).sum
      assertTrue(size3 <= maxResponseBytes, s"Expected $size3 to be smaller than $maxResponseBytes")
      assertTrue(size3 > maxPartitionBytes, s"Expected $size3 to be larger than $maxPartitionBytes")
      assertTrue(maxPartitionBytes < partitionData.records.sizeInBytes)
    }
    evaluateResponse3(fetchResponse3)
    evaluateResponse3(fetchResponse3V12, 12)

    // 4. Partition with message larger than the response limit at the start of the list
    val shuffledTopicPartitions4 = Seq(partitionWithLargeMessage2, partitionWithLargeMessage1) ++
      random.shuffle(partitionsWithoutLargeMessages)
    val fetchRequest4 = createConsumerFetchRequest(shuffledTopicPartitions4, Map(partitionWithLargeMessage2 -> messagesPerPartition))
    val fetchResponse4 = sendFetchRequest(leaderId, fetchRequest4)
    val fetchRequest4V12 = createConsumerFetchRequest(shuffledTopicPartitions4, Map(partitionWithLargeMessage2 -> messagesPerPartition), 12)
    val fetchResponse4V12 = sendFetchRequest(leaderId, fetchRequest4V12)
    def evaluateResponse4(response: FetchResponse, version: Short = ApiKeys.FETCH.latestVersion()): Unit = {
      val responseData = response.responseData(topicNames, version)
      assertEquals(shuffledTopicPartitions4, responseData.keySet.asScala.toSeq)
      val nonEmptyPartitions = responseData.asScala.toSeq.collect {
        case (tp, partitionData) if records(partitionData).map(_.sizeInBytes).sum > 0 => tp
      }
      assertEquals(Seq(partitionWithLargeMessage2), nonEmptyPartitions)
      val partitionData = responseData.get(partitionWithLargeMessage2)
      assertEquals(Errors.NONE.code, partitionData.errorCode)
      assertTrue(partitionData.highWatermark > 0)
      val size4 = records(partitionData).map(_.sizeInBytes).sum
      assertTrue(size4 > maxResponseBytes, s"Expected $size4 to be larger than $maxResponseBytes")
      assertTrue(maxResponseBytes < partitionData.records.sizeInBytes)
    }
    evaluateResponse4(fetchResponse4)
    evaluateResponse4(fetchResponse4V12, 12)
  }

  @Test
  def testFetchRequestV4WithReadCommitted(): Unit = {
    initProducer()
    val maxPartitionBytes = 200
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key", new String(new Array[Byte](maxPartitionBytes + 1)))).get
    val fetchRequest = FetchRequest.Builder.forConsumer(4, Int.MaxValue, 0, createPartitionMap(maxPartitionBytes,
      Seq(topicPartition))).isolationLevel(IsolationLevel.READ_COMMITTED).build(4)
    val fetchResponse = sendFetchRequest(leaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, 4).get(topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertTrue(partitionData.lastStableOffset > 0)
    assertTrue(records(partitionData).map(_.sizeInBytes).sum > 0)
  }

  @Test
  def testFetchRequestToNonReplica(): Unit = {
    val topic = "topic"
    val partition = 0
    val topicPartition = new TopicPartition(topic, partition)

    // Create a single-partition topic and find a broker which is not the leader
    val partitionToLeader = createTopic(topic)
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    val leader = partitionToLeader(partition)
    val nonReplicaOpt = brokers.find(_.config.brokerId != leader)
    assertTrue(nonReplicaOpt.isDefined)
    val nonReplicaId =  nonReplicaOpt.get.config.brokerId

    // Send the fetch request to the non-replica and verify the error code
    val fetchRequest = FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion, Int.MaxValue, 0, createPartitionMap(1024,
      Seq(topicPartition))).build()
    val fetchResponse = sendFetchRequest(nonReplicaId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, ApiKeys.FETCH.latestVersion).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, partitionData.errorCode)

    // Repeat with request that does not use topic IDs
    val oldFetchRequest = FetchRequest.Builder.forConsumer(12, Int.MaxValue, 0, createPartitionMap(1024,
      Seq(topicPartition))).build()
    val oldFetchResponse = sendFetchRequest(nonReplicaId, oldFetchRequest)
    val oldPartitionData = oldFetchResponse.responseData(topicNames, 12).get(topicPartition)
    assertEquals(Errors.NOT_LEADER_OR_FOLLOWER.code, oldPartitionData.errorCode)
  }

  @Test
  def testLastFetchedEpochValidation(): Unit = {
    checkLastFetchedEpochValidation(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testLastFetchedEpochValidationV12(): Unit = {
    checkLastFetchedEpochValidation(12)
  }

  private def checkLastFetchedEpochValidation(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(topic, replicationFactor = 3)
    TestUtils.waitUntilLeaderIsKnown(brokers, topicPartition)
    val firstLeaderId = partitionToLeader(topicPartition.partition)
    val firstLeaderEpoch = TestUtils.findLeaderEpoch(firstLeaderId, topicPartition, brokers)

    initProducer()

    // Write some data in epoch 0
    val firstEpochResponses = produceData(Seq(topicPartition), 100)
    val firstEpochEndOffset = firstEpochResponses.lastOption.get.offset + 1
    // Force a leader change
    killBroker(firstLeaderId)
    // Write some more data in epoch 1
    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, topicPartition, oldLeaderOpt = Some(firstLeaderId))
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, brokers)
    val secondEpochResponses = produceData(Seq(topicPartition), 100)
    val secondEpochEndOffset = secondEpochResponses.lastOption.get.offset + 1
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Build a fetch request in the middle of the second epoch, but with the first epoch
    val fetchOffset = secondEpochEndOffset + (secondEpochEndOffset - firstEpochEndOffset) / 2
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    partitionMap.put(topicPartition,
      new FetchRequest.PartitionData(topicIds.getOrDefault(topic, Uuid.ZERO_UUID), fetchOffset, 0L, 1024,
      Optional.of(secondLeaderEpoch), Optional.of(firstLeaderEpoch)))
    val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap).build()

    // Validate the expected truncation
    val fetchResponse = sendFetchRequest(secondLeaderId, fetchRequest)
    val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
    assertEquals(Errors.NONE.code, partitionData.errorCode)
    assertEquals(0L, FetchResponse.recordsSize(partitionData))
    assertTrue(FetchResponse.isDivergingEpoch(partitionData))

    val divergingEpoch = partitionData.divergingEpoch
    assertEquals(firstLeaderEpoch, divergingEpoch.epoch)
    assertEquals(firstEpochEndOffset, divergingEpoch.endOffset)
  }

  @Test
  def testCurrentEpochValidation(): Unit = {
    checkCurrentEpochValidation(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testCurrentEpochValidationV12(): Unit = {
    checkCurrentEpochValidation(12)
  }

  private def checkCurrentEpochValidation(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(topic, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    def assertResponseErrorForEpoch(error: Errors, brokerId: Int, leaderEpoch: Optional[Integer]): Unit = {
      val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      val topicIds = getTopicIds().asJava
      val topicNames = topicIds.asScala.map(_.swap).asJava
      partitionMap.put(topicPartition,
        new FetchRequest.PartitionData(topicIds.getOrDefault(topic, Uuid.ZERO_UUID), 0L, 0L, 1024, leaderEpoch))
      val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap).build()
      val fetchResponse = sendFetchRequest(brokerId, fetchRequest)
      val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
      assertEquals(error.code, partitionData.errorCode)
    }

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    // Check leader error codes
    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, topicPartition, oldLeaderOpt = Some(firstLeaderId))
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, secondLeaderId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, secondLeaderId, Optional.of(secondLeaderEpoch + 1))

    // Check follower error codes
    val followerId = TestUtils.findFollowerId(topicPartition, brokers)
    assertResponseErrorForEpoch(Errors.NONE, followerId, Optional.empty())
    assertResponseErrorForEpoch(Errors.NONE, followerId, Optional.of(secondLeaderEpoch))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch + 1))
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, followerId, Optional.of(secondLeaderEpoch - 1))
  }

  @Test
  def testEpochValidationWithinFetchSession(): Unit = {
    checkEpochValidationWithinFetchSession(ApiKeys.FETCH.latestVersion())
  }

  @Test
  def testEpochValidationWithinFetchSessionV12(): Unit = {
    checkEpochValidationWithinFetchSession(12)
  }

  private def checkEpochValidationWithinFetchSession(version: Short): Unit = {
    val topic = "topic"
    val topicPartition = new TopicPartition(topic, 0)
    val partitionToLeader = createTopic(topic, replicationFactor = 3)
    val firstLeaderId = partitionToLeader(topicPartition.partition)

    // We need a leader change in order to check epoch fencing since the first epoch is 0 and
    // -1 is treated as having no epoch at all
    killBroker(firstLeaderId)

    val secondLeaderId = TestUtils.awaitLeaderChange(brokers, topicPartition, oldLeaderOpt = Some(firstLeaderId))
    val secondLeaderEpoch = TestUtils.findLeaderEpoch(secondLeaderId, topicPartition, brokers)
    verifyFetchSessionErrors(topicPartition, secondLeaderEpoch, secondLeaderId, version)

    val followerId = TestUtils.findFollowerId(topicPartition, brokers)
    verifyFetchSessionErrors(topicPartition, secondLeaderEpoch, followerId, version)
  }

  private def verifyFetchSessionErrors(topicPartition: TopicPartition,
                                       leaderEpoch: Int,
                                       destinationBrokerId: Int,
                                       version: Short): Unit = {
    val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava
    partitionMap.put(topicPartition, new FetchRequest.PartitionData(topicIds.getOrDefault(topicPartition.topic, Uuid.ZERO_UUID),
      0L, 0L, 1024, Optional.of(leaderEpoch)))
    val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap)
      .metadata(JFetchMetadata.INITIAL)
      .build()
    val fetchResponse = sendFetchRequest(destinationBrokerId, fetchRequest)
    val sessionId = fetchResponse.sessionId

    def assertResponseErrorForEpoch(expectedError: Errors,
                                    sessionFetchEpoch: Int,
                                    leaderEpoch: Optional[Integer]): Unit = {
      val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      partitionMap.put(topicPartition, new FetchRequest.PartitionData(topicIds.getOrDefault(topicPartition.topic, Uuid.ZERO_UUID), 0L, 0L, 1024, leaderEpoch))
      val fetchRequest = FetchRequest.Builder.forConsumer(version, 0, 1, partitionMap)
        .metadata(new JFetchMetadata(sessionId, sessionFetchEpoch))
        .build()
      val fetchResponse = sendFetchRequest(destinationBrokerId, fetchRequest)
      val partitionData = fetchResponse.responseData(topicNames, version).get(topicPartition)
      assertEquals(expectedError.code, partitionData.errorCode)
    }

    // We only check errors because we do not expect the partition in the response otherwise
    assertResponseErrorForEpoch(Errors.FENCED_LEADER_EPOCH, 1, Optional.of(leaderEpoch - 1))
    assertResponseErrorForEpoch(Errors.UNKNOWN_LEADER_EPOCH, 2, Optional.of(leaderEpoch + 1))
  }

  /**
   * Test that when an incremental fetch session contains partitions with an error,
   * those partitions are returned in all incremental fetch requests.
   * This tests using FetchRequests that don't use topic IDs
   */
  @Test
  def testCreateIncrementalFetchWithPartitionsInErrorV12(): Unit = {
    def createConsumerFetchRequest(topicPartitions: Seq[TopicPartition],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicIdPartition]): FetchRequest =
      FetchRequest.Builder.forConsumer(12, Int.MaxValue, 0,
        createPartitionMap(Integer.MAX_VALUE, topicPartitions, Map.empty))
        .removed(toForget.asJava)
        .metadata(metadata)
        .build()
    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    // topicNames can be empty because we are using old requests
    val topicNames = Map[Uuid, String]().asJava
    createTopicWithAssignment("foo", Map(0 -> List(0, 1), 1 -> List(0, 2)))
    TestUtils.waitUntilLeaderIsKnown(brokers, foo0)
    TestUtils.waitUntilLeaderIsKnown(brokers, foo1)
    val bar0 = new TopicPartition("bar", 0)
    val req1 = createConsumerFetchRequest(List(foo0, foo1, bar0), JFetchMetadata.INITIAL, Nil)
    val resp1 = sendFetchRequest(0, req1)
    assertEquals(Errors.NONE, resp1.error())
    assertTrue(resp1.sessionId() > 0, "Expected the broker to create a new incremental fetch session")
    debug(s"Test created an incremental fetch session ${resp1.sessionId}")
    val responseData1 = resp1.responseData(topicNames, 12)
    assertTrue(responseData1.containsKey(foo0))
    assertTrue(responseData1.containsKey(foo1))
    assertTrue(responseData1.containsKey(bar0))
    assertEquals(Errors.NONE.code, responseData1.get(foo0).errorCode)
    assertEquals(Errors.NONE.code, responseData1.get(foo1).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, responseData1.get(bar0).errorCode)
    val req2 = createConsumerFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 1), Nil)
    val resp2 = sendFetchRequest(0, req2)
    assertEquals(Errors.NONE, resp2.error())
    assertEquals(resp1.sessionId(),
      resp2.sessionId(), "Expected the broker to continue the incremental fetch session")
    val responseData2 = resp2.responseData(topicNames, 12)
    assertFalse(responseData2.containsKey(foo0))
    assertFalse(responseData2.containsKey(foo1))
    assertTrue(responseData2.containsKey(bar0))
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code, responseData2.get(bar0).errorCode)
    createTopicWithAssignment("bar", Map(0 -> List(0, 1)))
    TestUtils.waitUntilLeaderIsKnown(brokers, bar0)
    val req3 = createConsumerFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 2), Nil)
    val resp3 = sendFetchRequest(0, req3)
    assertEquals(Errors.NONE, resp3.error())
    val responseData3 = resp3.responseData(topicNames, 12)
    assertFalse(responseData3.containsKey(foo0))
    assertFalse(responseData3.containsKey(foo1))
    assertTrue(responseData3.containsKey(bar0))
    assertEquals(Errors.NONE.code, responseData3.get(bar0).errorCode)
    val req4 = createConsumerFetchRequest(Nil, new JFetchMetadata(resp1.sessionId(), 3), Nil)
    val resp4 = sendFetchRequest(0, req4)
    assertEquals(Errors.NONE, resp4.error())
    val responseData4 = resp4.responseData(topicNames, 12)
    assertFalse(responseData4.containsKey(foo0))
    assertFalse(responseData4.containsKey(foo1))
    assertFalse(responseData4.containsKey(bar0))
  }

  /**
   * Test that when a Fetch Request receives an unknown topic ID, it returns a top level error.
   */
  @Test
  def testFetchWithPartitionsWithIdError(): Unit = {
    def createConsumerFetchRequest(fetchData: util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData],
                           metadata: JFetchMetadata,
                           toForget: Seq[TopicIdPartition]): FetchRequest = {
      FetchRequest.Builder.forConsumer(ApiKeys.FETCH.latestVersion(), Int.MaxValue, 0, fetchData)
        .removed(toForget.asJava)
        .metadata(metadata)
        .build()
    }

    val foo0 = new TopicPartition("foo", 0)
    val foo1 = new TopicPartition("foo", 1)
    createTopicWithAssignment("foo", Map(0 -> List(0, 1), 1 -> List(0, 2)))
    TestUtils.waitUntilLeaderIsKnown(brokers, foo0)
    TestUtils.waitUntilLeaderIsKnown(brokers, foo1)
    val topicIds = getTopicIds()
    val topicIdsWithUnknown = topicIds ++ Map("bar" -> Uuid.randomUuid())
    val bar0 = new TopicPartition("bar", 0)

    def createPartitionMap(maxPartitionBytes: Int, topicPartitions: Seq[TopicPartition],
                           offsetMap: Map[TopicPartition, Long]): util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData] = {
      val partitionMap = new util.LinkedHashMap[TopicPartition, FetchRequest.PartitionData]
      topicPartitions.foreach { tp =>
        partitionMap.put(tp,
          new FetchRequest.PartitionData(topicIdsWithUnknown.getOrElse(tp.topic, Uuid.ZERO_UUID), offsetMap.getOrElse(tp, 0),
            0L, maxPartitionBytes, Optional.empty()))
      }
      partitionMap
    }

    val req1 = createConsumerFetchRequest( createPartitionMap(Integer.MAX_VALUE, List(foo0, foo1, bar0), Map.empty), JFetchMetadata.INITIAL, Nil)
    val resp1 = sendFetchRequest(0, req1)
    assertEquals(Errors.NONE, resp1.error())
    val topicNames1 = topicIdsWithUnknown.map(_.swap).asJava
    val responseData1 = resp1.responseData(topicNames1, ApiKeys.FETCH.latestVersion())
    assertTrue(responseData1.containsKey(foo0))
    assertTrue(responseData1.containsKey(foo1))
    assertTrue(responseData1.containsKey(bar0))
    assertEquals(Errors.NONE.code, responseData1.get(foo0).errorCode)
    assertEquals(Errors.NONE.code, responseData1.get(foo1).errorCode)
    assertEquals(Errors.UNKNOWN_TOPIC_ID.code, responseData1.get(bar0).errorCode)
  }

  @Test
  def testZStdCompressedTopic(): Unit = {
    // ZSTD compressed topic
    val topicConfig = Map(TopicConfig.COMPRESSION_TYPE_CONFIG -> BrokerCompressionType.ZSTD.name)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, configs = topicConfig).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Produce messages (v2)
    producer = TestUtils.createProducer(bootstrapServers(),
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key1", "value1")).get
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key2", "value2")).get
    producer.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key3", "value3")).get
    producer.close()

    // fetch request with version below v10: UNSUPPORTED_COMPRESSION_TYPE error occurs
    val req0 = new FetchRequest.Builder(0, 9, -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()

    val res0 = sendFetchRequest(leaderId, req0)
    val data0 = res0.responseData(topicNames, 9).get(topicPartition)
    assertEquals(Errors.NONE.code, data0.errorCode)

    // fetch request with version 10: works fine!
    val req1= new FetchRequest.Builder(0, 10, -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()
    val res1 = sendFetchRequest(leaderId, req1)
    val data1 = res1.responseData(topicNames, 10).get(topicPartition)
    assertEquals(Errors.NONE.code, data1.errorCode)
    assertEquals(3, records(data1).size)

    val req2 = new FetchRequest.Builder(ApiKeys.FETCH.latestVersion(), ApiKeys.FETCH.latestVersion(), -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()
    val res2 = sendFetchRequest(leaderId, req2)
    val data2 = res2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).get(topicPartition)
    assertEquals(Errors.NONE.code, data2.errorCode)
    assertEquals(3, records(data2).size)
  }

  @Test
  def testZStdCompressedRecords(): Unit = {
    // Producer compressed topic
    val topicConfig = Map(TopicConfig.COMPRESSION_TYPE_CONFIG -> BrokerCompressionType.PRODUCER.name)
    val (topicPartition, leaderId) = createTopics(numTopics = 1, numPartitions = 1, configs = topicConfig).head
    val topicIds = getTopicIds().asJava
    val topicNames = topicIds.asScala.map(_.swap).asJava

    // Produce GZIP compressed messages (v2)
    val producer1 = TestUtils.createProducer(bootstrapServers(),
      compressionType = CompressionType.GZIP.name,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer1.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key1", "value1")).get
    producer1.close()
    // Produce ZSTD compressed messages (v2)
    val producer2 = TestUtils.createProducer(bootstrapServers(),
      compressionType = CompressionType.ZSTD.name,
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer)
    producer2.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key2", "value2")).get
    producer2.send(new ProducerRecord(topicPartition.topic, topicPartition.partition,
      "key3", "value3")).get
    producer2.close()

    // fetch request with version 4: even though zstd is officially only supported from v10, this actually succeeds
    // since the server validation is only active when zstd is configured via a topic config, the server doesn't
    // check the record batches and hence has no mechanism to detect the case where the producer sent record batches
    // compressed with zstd and the topic config for compression is the default
    val req0 = new FetchRequest.Builder(0, 4, -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()
    val res0 = sendFetchRequest(leaderId, req0)
    val data0 = res0.responseData(topicNames, 10).get(topicPartition)
    assertEquals(Errors.NONE.code, data0.errorCode)
    assertEquals(3, records(data0).size)

    // fetch request with version 10: works fine!
    val req1 = new FetchRequest.Builder(0, 10, -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()
    val res1 = sendFetchRequest(leaderId, req1)
    val data1 = res1.responseData(topicNames, 10).get(topicPartition)
    assertEquals(Errors.NONE.code, data1.errorCode)
    assertEquals(3, records(data1).size)

    val req2 = new FetchRequest.Builder(0, ApiKeys.FETCH.latestVersion(), -1, -1, Int.MaxValue, 0,
      createPartitionMap(300, Seq(topicPartition), Map.empty))
      .setMaxBytes(800).build()
    val res2 = sendFetchRequest(leaderId, req2)
    val data2 = res2.responseData(topicNames, ApiKeys.FETCH.latestVersion()).get(topicPartition)
    assertEquals(Errors.NONE.code, data2.errorCode)
    assertEquals(3, records(data2).size)
  }

  private def checkFetchResponse(expectedPartitions: Seq[TopicPartition], fetchResponse: FetchResponse,
                                 maxPartitionBytes: Int, maxResponseBytes: Int, numMessagesPerPartition: Int,
                                 responseVersion: Short = ApiKeys.FETCH.latestVersion()): Unit = {
    val topicNames = getTopicIds().map(_.swap).asJava
    val responseData = fetchResponse.responseData(topicNames, responseVersion)
    assertEquals(expectedPartitions, responseData.keySet.asScala.toSeq)
    var emptyResponseSeen = false
    var responseSize = 0
    var responseBufferSize = 0

    expectedPartitions.foreach { tp =>
      val partitionData = responseData.get(tp)
      assertEquals(Errors.NONE.code, partitionData.errorCode)
      assertTrue(partitionData.highWatermark > 0)

      val records = FetchResponse.recordsOrFail(partitionData)
      responseBufferSize += records.sizeInBytes

      val batches = records.batches.asScala.toBuffer
      assertTrue(batches.size < numMessagesPerPartition)
      val batchesSize = batches.map(_.sizeInBytes).sum
      responseSize += batchesSize
      if (batchesSize == 0 && !emptyResponseSeen) {
        assertEquals(0, records.sizeInBytes)
        emptyResponseSeen = true
      }
      else if (batchesSize != 0 && !emptyResponseSeen) {
        assertTrue(batchesSize <= maxPartitionBytes)
        assertEquals(maxPartitionBytes, records.sizeInBytes)
      }
      else if (batchesSize != 0 && emptyResponseSeen)
        fail(s"Expected partition with size 0, but found $tp with size $batchesSize")
      else if (records.sizeInBytes != 0 && emptyResponseSeen)
        fail(s"Expected partition buffer with size 0, but found $tp with size ${records.sizeInBytes}")
    }

    assertEquals(maxResponseBytes - maxResponseBytes % maxPartitionBytes, responseBufferSize)
    assertTrue(responseSize <= maxResponseBytes)
  }

}

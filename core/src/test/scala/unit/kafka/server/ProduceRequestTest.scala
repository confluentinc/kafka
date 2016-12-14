/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
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

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.{CompressionType, MemoryRecords, Record}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Subclasses of `BaseProduceSendRequestTest` exercise the producer and produce request/response. This class
  * complements those classes with tests that require lower-level access to the protocol.
  */
class ProduceRequestTest extends BaseRequestTest {

  @Test
  def testSimpleProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")

    def sendAndCheck(records: MemoryRecords, expectedOffset: Long): ProduceResponse.PartitionResponse = {
      val topicPartition = new TopicPartition("topic", partition)
      val partitionRecords = Map(topicPartition -> records)
      val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
      assertEquals(1, produceResponse.responses.size)
      val (tp, partitionResponse) = produceResponse.responses.asScala.head
      assertEquals(topicPartition, tp)
      assertEquals(Errors.NONE.code, partitionResponse.errorCode)
      assertEquals(expectedOffset, partitionResponse.baseOffset)
      assertEquals(-1, partitionResponse.timestamp)
      partitionResponse
    }

    sendAndCheck(TestUtils.records(Record.CURRENT_MAGIC_VALUE, CompressionType.NONE,
      ("key".getBytes, "value".getBytes, System.currentTimeMillis())), 0)

    sendAndCheck(TestUtils.records(Record.CURRENT_MAGIC_VALUE, CompressionType.GZIP,
      ("key1".getBytes, "value1".getBytes, System.currentTimeMillis()),
      ("key2".getBytes, "value2".getBytes, System.currentTimeMillis())), 1)
  }

  /* returns a pair of partition id and leader id */
  private def createTopicAndFindPartitionWithLeader(topic: String): (Int, Int) = {
    val partitionToLeader = TestUtils.createTopic(zkUtils, topic, 3, 2, servers)
    partitionToLeader.collectFirst {
      case (partition, Some(leader)) if leader != -1 => (partition, leader)
    }.getOrElse(fail(s"No leader elected for topic $topic"))
  }

  @Test
  def testCorruptLz4ProduceRequest() {
    val (partition, leader) = createTopicAndFindPartitionWithLeader("topic")
    val timestamp = 1000000
    val recordBuffer = TestUtils.singletonRecords(codec = CompressionType.LZ4, timestamp = timestamp,
      key = "key".getBytes, value = "value".getBytes).buffer()
    // Change the lz4 checksum value so that it doesn't match the contents
    recordBuffer.array.update(40, 3) // FIXME: Is this index significant? For older magic, the CRC should be at offset 13
    val topicPartition = new TopicPartition("topic", partition)
    val partitionRecords = Map(topicPartition -> MemoryRecords.readableRecords(recordBuffer))
    val produceResponse = sendProduceRequest(leader, new ProduceRequest(-1, 3000, partitionRecords.asJava))
    assertEquals(1, produceResponse.responses.size)
    val (tp, partitionResponse) = produceResponse.responses.asScala.head
    assertEquals(topicPartition, tp)
    assertEquals(Errors.CORRUPT_MESSAGE.code, partitionResponse.errorCode)
    assertEquals(-1, partitionResponse.baseOffset)
    assertEquals(-1, partitionResponse.timestamp)
  }

  private def sendProduceRequest(leaderId: Int, request: ProduceRequest): ProduceResponse = {
    val response = send(request, ApiKeys.PRODUCE, destination = brokerSocketServer(leaderId))
    ProduceResponse.parse(response)
  }

}

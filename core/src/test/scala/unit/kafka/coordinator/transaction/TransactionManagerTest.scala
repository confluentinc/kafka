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
package kafka.coordinator.transaction

import java.nio.ByteBuffer

import kafka.common.Topic.TransactionStateTopicName
import kafka.log.Log
import kafka.server.{FetchDataInfo, LogOffsetMetadata, ReplicaManager}
import kafka.utils.ZkUtils
import kafka.utils.TestUtils.fail

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, FileRecords, KafkaRecord, MemoryRecords}
import org.apache.kafka.common.utils.MockTime

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import org.easymock.EasyMock

import scala.collection.mutable
import scala.collection.JavaConverters._

class TransactionManagerTest {

  val partitionId = 0

  val transactionTimeoutMs: Int = 1000

  val topicPartition = new TopicPartition(TransactionStateTopicName, partitionId)

  val txnRecords: mutable.ArrayBuffer[KafkaRecord] = mutable.ArrayBuffer[KafkaRecord]()

  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])

  EasyMock.expect(zkUtils.getTopicPartitionCount(TransactionStateTopicName))
    .andReturn(Some(2))
    .once()

  EasyMock.replay(zkUtils)

  val replicaManager: ReplicaManager = EasyMock.createNiceMock(classOf[ReplicaManager])

  val time = new MockTime()

  val transactionManager: TransactionManager = new TransactionManager(0, zkUtils, replicaManager, time)

  val txnId1: String = "one"
  val txnId2: String = "two"
  val txnMessageKeyBytes1: Array[Byte] = TransactionLog.keyToBytes(txnId1)
  val txnMessageKeyBytes2: Array[Byte] = TransactionLog.keyToBytes(txnId2)
  val pidMappings: Map[String, Long] = Map[String, Long](txnId1 -> 1L, txnId2 -> 2L)
  val txnMetadata1: TransactionMetadata = TransactionMetadata.stateToTxnMetadata(NotExist)
  val txnMetadata2: TransactionMetadata = TransactionMetadata.stateToTxnMetadata(NotExist)
  var pidMetadata1: PidMetadata = new PidMetadata(pidMappings(txnId1), 1, transactionTimeoutMs, txnMetadata1)
  var pidMetadata2: PidMetadata = new PidMetadata(pidMappings(txnId2), 1, transactionTimeoutMs, txnMetadata2)

  @Before
  def setUp(): Unit = {
    // generate transaction log messages for two pids traces:

    // pid1's transaction started with two partitions
    txnMetadata1.state = Ongoing
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic1", 0),
      new TopicPartition("topic1", 1)))

    txnRecords += new KafkaRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(pidMetadata1))

    // pid1's transaction adds three more partitions
    txnMetadata1.addPartitions(Set[TopicPartition](new TopicPartition("topic2", 0),
      new TopicPartition("topic2", 1),
      new TopicPartition("topic2", 2)))

    txnRecords += new KafkaRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(pidMetadata1))

    // pid1's transaction is preparing to commit
    txnMetadata1.state = PrepareCommit

    txnRecords += new KafkaRecord(txnMessageKeyBytes1, TransactionLog.valueToBytes(pidMetadata1))

    // pid2's transaction started with three partitions
    txnMetadata2.state = Ongoing
    txnMetadata2.addPartitions(Set[TopicPartition](new TopicPartition("topic3", 0),
      new TopicPartition("topic3", 1),
      new TopicPartition("topic3", 2)))

    txnRecords += new KafkaRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(pidMetadata2))

    // pid2's transaction is preparing to abort
    txnMetadata2.state = PrepareAbort

    txnRecords += new KafkaRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(pidMetadata2))

    // pid2's transaction has aborted
    txnMetadata2.state = CompleteAbort

    txnRecords += new KafkaRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(pidMetadata2))

    // pid2's epoch has advanced, with no ongoing transaction yet
    txnMetadata2.state = NotExist
    txnMetadata2.topicPartitions.clear()

    txnRecords += new KafkaRecord(txnMessageKeyBytes2, TransactionLog.valueToBytes(pidMetadata2))
  }

  @Test
  def testLoadOffsetsAndGroup() {
    val startOffset = 15L   // just check it should work for any start offset
    val records = MemoryRecords.withRecords(startOffset, CompressionType.NONE, txnRecords: _*)

    prepareTxnLog(topicPartition, startOffset, records)

    EasyMock.replay(replicaManager)

    transactionManager.loadPidMetadata(topicPartition)

    val cachedPidMetadata1 = transactionManager.getPid(txnId1).getOrElse(fail(txnId1 + "'s transaction state was not loaded into the cache"))
    val cachedPidMetadata2 = transactionManager.getPid(txnId2).getOrElse(fail(txnId2 + "'s transaction state was not loaded into the cache"))

    // they should be equal to the latest status of the transaction
    assertEquals(pidMetadata1, cachedPidMetadata1)
    assertEquals(pidMetadata2, cachedPidMetadata2)
  }

  private def prepareTxnLog(topicPartition: TopicPartition,
                            startOffset: Long,
                            records: MemoryRecords): Unit = {
    val logMock =  EasyMock.mock(classOf[Log])
    val fileRecordsMock = EasyMock.mock(classOf[FileRecords])

    val endOffset = startOffset + records.records.asScala.size

    EasyMock.expect(replicaManager.getLog(topicPartition)).andStubReturn(Some(logMock))
    EasyMock.expect(replicaManager.getHighWatermark(topicPartition)).andStubReturn(Some(endOffset))

    EasyMock.expect(logMock.logStartOffset).andStubReturn(Some(startOffset))
    EasyMock.expect(logMock.read(EasyMock.eq(startOffset), EasyMock.anyInt(), EasyMock.eq(None), EasyMock.eq(true)))
      .andReturn(FetchDataInfo(LogOffsetMetadata(startOffset), fileRecordsMock))
    EasyMock.expect(fileRecordsMock.readInto(EasyMock.anyObject(classOf[ByteBuffer]), EasyMock.anyInt()))
      .andReturn(records.buffer)

    EasyMock.replay(logMock, fileRecordsMock)
  }
}

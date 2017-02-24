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
package kafka.coordinator

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{CompressionType, KafkaRecord, MemoryRecords}

import scala.collection.JavaConverters._

import org.junit.Test
import org.junit.Assert.{assertEquals, fail}

class TransactionLogManagerTest {

  val epoch: Short = 0
  val transactionTimeoutMs: Int = 1000

  @Test
  def shouldReadWritePidMessages() {
    // generate pid mapping messages
    val pidMappings = Map[String, Long]("one" -> 1L, "two" -> 2L, "three" -> 3L)

    val pidRecords = pidMappings.map { case (transactionalId, pid) =>
      val pidMetadata = new PidMetadata(pid, epoch, transactionTimeoutMs)
      val pidKey = TransactionLogManager.pidKey(transactionalId)
      val pidValue = TransactionLogManager.pidValue(pidMetadata)
      new KafkaRecord(pidKey, pidValue)
    }.toSeq

    val records = MemoryRecords.withRecords(0, CompressionType.NONE, pidRecords: _*)

    for (record <- records.records.asScala) {
      val key = TransactionLogManager.readMessageKey(record.key())

      key match {
        case pidKey: PidKey =>
          val transactionalId = pidKey.key
          val pidMetadata = TransactionLogManager.readPidValue(record.value())

          assertEquals(pidMappings(transactionalId), pidMetadata.pid)
          assertEquals(epoch, pidMetadata.epoch)
          assertEquals(transactionTimeoutMs, pidMetadata.transactionTimeoutMs)

        case _ => fail(s"Unexpected transaction topic message key $key")
      }
    }
  }

  @Test
  def shouldReadWriteTxnStatusMessages() {
    // generate pid mapping messages
    val topicPartitions = Set[TopicPartition](new TopicPartition("topic1", 0),
                                              new TopicPartition("topic1", 1),
                                              new TopicPartition("topic2", 0),
                                              new TopicPartition("topic2", 1),
                                              new TopicPartition("topic2", 2))

    val transactionStates = Map[Long, TransactionState](1L ->  Ongoing,
                                                        2L ->  PrepareCommit,
                                                        3L ->  PrepareAbort,
                                                        4L ->  CompleteCommit,
                                                        5L ->  CompleteAbort)

    val txnRecords = transactionStates.map { case (pid, state) =>
      val txnMetadata = new TransactionMetadata(state)
      txnMetadata.addPartitions(topicPartitions)
      val txnKey = TransactionLogManager.txnKey(pid)
      val txnValue = TransactionLogManager.txnValue(txnMetadata)
      new KafkaRecord(txnKey, txnValue)
    }.toSeq

    val records = MemoryRecords.withRecords(0, CompressionType.NONE, txnRecords: _*)

    for (record <- records.records.asScala) {
      val key = TransactionLogManager.readMessageKey(record.key())

      key match {
        case txnKey: TxnKey =>
          val pid = txnKey.key
          val txnMetadata = TransactionLogManager.readTxnStatusValue(record.value())

          assertEquals(transactionStates(pid), txnMetadata.state)
          assertEquals(topicPartitions, txnMetadata.topicPartitions)

        case _ => fail(s"Unexpected transaction topic message key $key")
      }
    }
  }
}

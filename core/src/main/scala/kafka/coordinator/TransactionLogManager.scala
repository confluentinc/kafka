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

package kafka.coordinator

import kafka.common.{KafkaException, MessageFormatter, Topic}
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, ZkUtils}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import java.util.concurrent.locks.ReentrantLock
import java.io.PrintStream
import java.nio.ByteBuffer

import scala.collection.mutable

/**
  * Messages stored for the transaction topic has versions for both the key and value fields. Key
  * version is used to indicate the type of the message (also to differentiate different types
  * of messages from being compacted together if they have the same field values); and value
  * version is used to evolve the messages within their data types:
  *
  * key version 0 (producer pid mapping):               [transactionalId]
  *    -> value version 0:                              [pid, epoch, expire_timestamp]
  *
  * key version 1 (producer transaction status):        [pid]
  *    -> value version 0:                              [status, [topic [partition] ]
  */
object TransactionLogManager {

  private val CURRENT_PID_KEY_SCHEMA_VERSION = 0.toShort
  private val CURRENT_TXN_KEY_SCHEMA_VERSION = 1.toShort

  private val PID_KEY = "pid"
  private val EPOCH_KEY = "epoch"
  private val STATUS_KEY = "status"
  private val TXN_ID_KEY = "transactional_id"
  private val TXN_TIMEOUT_KEY = "transaction_timeout"
  private val TOPIC_KEY = "topic"
  private val PARTITIONS_KEY = "partitions"
  private val PARITION_IDS_KEY = "partition_ids"

  private val PID_KEY_SCHEMA_V0 = new Schema(new Field(TXN_ID_KEY, STRING))
  private val PID_KEY_SCHEMA_TXNID_FIELD = PID_KEY_SCHEMA_V0.get(TXN_ID_KEY)

  private val PID_VALUE_SCHEMA_V0 = new Schema(new Field(PID_KEY, INT64),
                                               new Field(EPOCH_KEY, INT16),
                                               new Field(TXN_TIMEOUT_KEY, INT32))
  private val PID_VALUE_SCHEMA_PID_FIELD = PID_VALUE_SCHEMA_V0.get(PID_KEY)
  private val PID_VALUE_SCHEMA_EPOCH_FIELD = PID_VALUE_SCHEMA_V0.get(EPOCH_KEY)
  private val PID_VALUE_SCHEMA_TXN_TIMEOUT_FIELD = PID_VALUE_SCHEMA_V0.get(TXN_TIMEOUT_KEY)

  private val TXN_KEY_SCHEMA_V0 = new Schema(new Field(PID_KEY, INT64))
  private val TXN_KEY_SCHEMA_PID_FIELD = TXN_KEY_SCHEMA_V0.get(PID_KEY)

  private val PARTITION_SCHEMA_V0 = new Schema(new Field(TOPIC_KEY, STRING),
                                               new Field(PARITION_IDS_KEY, new ArrayOf(INT32)))
  private val PARTITION_SCHEMA_TOPIC_FIELD = PARTITION_SCHEMA_V0.get(TOPIC_KEY)
  private val PARTITION_SCHEMA_PARTITIONS_FIELD = PARTITION_SCHEMA_V0.get(PARITION_IDS_KEY)

  private val TXN_VALUE_SCHEMA_V0 = new Schema(new Field(STATUS_KEY, INT8),
                                               new Field(PARTITIONS_KEY, new ArrayOf(PARTITION_SCHEMA_V0)))
  private val TXN_VALUE_STATUS_FIELD = TXN_VALUE_SCHEMA_V0.get(STATUS_KEY)
  private val TXN_VALUE_PARTITIONS_FIELD = TXN_VALUE_SCHEMA_V0.get(PARTITIONS_KEY)

  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> PID_KEY_SCHEMA_V0,
    1 -> TXN_KEY_SCHEMA_V0)

  // map of version of offset value schemas
  private val PID_VALUE_SCHEMAS = Map(
    0 -> PID_VALUE_SCHEMA_V0)
  private val CURRENT_PID_VALUE_SCHEMA_VERSION = 0.toShort

  // map of version of group metadata value schemas
  private val TXN_VALUE_SCHEMAS = Map(
    0 -> TXN_VALUE_SCHEMA_V0)
  private val CURRENT_TXN_VALUE_SCHEMA_VERSION = 0.toShort

  private val CURRENT_PID_KEY_SCHEMA = schemaForKey(CURRENT_PID_KEY_SCHEMA_VERSION)
  private val CURRENT_TXN_KEY_SCHEMA = schemaForKey(CURRENT_TXN_KEY_SCHEMA_VERSION)

  private val CURRENT_PID_VALUE_SCHEMA = schemaForPid(CURRENT_PID_VALUE_SCHEMA_VERSION)
  private val CURRENT_TXN_VALUE_SCHEMA = schemaForTxn(CURRENT_TXN_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction log key schema version $version")
    }
  }

  private def schemaForPid(version: Int) = {
    val schemaOpt = PID_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown pid mapping message value schema version $version")
    }
  }

  private def schemaForTxn(version: Int) = {
    val schemaOpt = TXN_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction status value schema version $version")
    }
  }

  /**
    * Generates the key for transactionalid to pid mapping message
    *
    * @return key bytes
    */
  private[coordinator] def pidKey(transactionalId: String): Array[Byte] = {
    val key = new Struct(CURRENT_PID_KEY_SCHEMA)
    key.set(PID_KEY_SCHEMA_TXNID_FIELD, transactionalId)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_PID_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the key for transaction status message
    *
    * @return key bytes
    */
  private[coordinator] def txnKey(pid: Long): Array[Byte] = {
    val key = new Struct(CURRENT_TXN_KEY_SCHEMA)
    key.set(TXN_KEY_SCHEMA_PID_FIELD, pid)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_TXN_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload for transactional id to pid mapping message
    *
    * @return value payload bytes
    */
  private[coordinator] def pidValue(pidMetadata: PidMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_PID_VALUE_SCHEMA)
    value.set(PID_VALUE_SCHEMA_PID_FIELD, pidMetadata.pid)
    value.set(PID_VALUE_SCHEMA_EPOCH_FIELD, pidMetadata.epoch)
    value.set(PID_VALUE_SCHEMA_TXN_TIMEOUT_FIELD, pidMetadata.transactionTimeoutMs)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_PID_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload for transaction status message
    *
    * @return value payload bytes
    */
  private[coordinator] def txnValue(txnMetadata: TransactionMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_TXN_VALUE_SCHEMA)

    value.set(TXN_VALUE_STATUS_FIELD, txnMetadata.state.byte)

    // first group the topic partitions by their topic names
    val topicAndPartitions = txnMetadata.topicPartitions.groupBy(_.topic())

    val partitionArray = topicAndPartitions.map { topicAndPartitionIds =>
      val topicPartitionsStruct = value.instance(TXN_VALUE_PARTITIONS_FIELD)
      val topic: String = topicAndPartitionIds._1
      val partitionIds: Array[Integer] = topicAndPartitionIds._2.map(topicPartition => Integer.valueOf(topicPartition.partition())).toArray

      topicPartitionsStruct.set(PARTITION_SCHEMA_TOPIC_FIELD, topic)
      topicPartitionsStruct.set(PARTITION_SCHEMA_PARTITIONS_FIELD, partitionIds)

      topicPartitionsStruct
    }

    value.set(TXN_VALUE_PARTITIONS_FIELD, partitionArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_TXN_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Decodes the transaction log messages' key
    *
    * @return the key
    */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version == CURRENT_PID_KEY_SCHEMA_VERSION) {
      // version 0 refer to pid messages
      val transactionalId = key.get(PID_KEY_SCHEMA_TXNID_FIELD).asInstanceOf[String]

      PidKey(version, transactionalId)
    } else if (version == CURRENT_TXN_KEY_SCHEMA_VERSION) {
      // version 1 refers to txn status messages
      val pid = key.get(TXN_KEY_SCHEMA_PID_FIELD).asInstanceOf[Long]

      TxnKey(version, pid)
    } else {
      throw new IllegalStateException(s"Unknown version $version from the transaction log message")
    }
  }

  /**
    * Decodes the pid messages' payload and retrieves pid metadata from it
    *
    * @return a pid metadata object from the message
    */
  def readPidValue(buffer: ByteBuffer): PidMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForPid(version)
      val value = valueSchema.read(buffer)

      if (version == CURRENT_PID_VALUE_SCHEMA_VERSION) {
        val pid = value.get(PID_VALUE_SCHEMA_PID_FIELD).asInstanceOf[Long]
        val epoch = value.get(PID_VALUE_SCHEMA_EPOCH_FIELD).asInstanceOf[Short]
        val timeout = value.get(PID_VALUE_SCHEMA_TXN_TIMEOUT_FIELD).asInstanceOf[Int]

        new PidMetadata(pid, epoch, timeout)
      } else {
        throw new IllegalStateException(s"Unknown version $version from the pid mapping message value")
      }
    }
  }

  /**
    * Decodes the txn status messages' payload and retrieves transaction metadata from it
    *
    * @return a transaction metadata object from the message
    */
  def readTxnStatusValue(buffer: ByteBuffer): TransactionMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForTxn(version)
      val value = valueSchema.read(buffer)

      if (version == CURRENT_TXN_VALUE_SCHEMA_VERSION) {
        val stateByte = value.get(TXN_VALUE_STATUS_FIELD).asInstanceOf[Byte]
        val state = TransactionMetadata.byteToState(stateByte)

        val transactionMetadata = new TransactionMetadata(state)

        val topicPartitionArray = value.getArray(TXN_VALUE_PARTITIONS_FIELD)

        topicPartitionArray.foreach { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val topic = memberMetadata.get(PARTITION_SCHEMA_TOPIC_FIELD).asInstanceOf[String]
          val partitionIdArray = memberMetadata.getArray(PARTITION_SCHEMA_PARTITIONS_FIELD)

          val topicPartitions = partitionIdArray.map { partitionIdObj =>
            val partitionId = partitionIdObj.asInstanceOf[Integer]
            new TopicPartition(topic, partitionId)
          }

          transactionMetadata.addPartitions(topicPartitions.toSet)
        }

        transactionMetadata
      } else {
        throw new IllegalStateException(s"Unknown version $version from the transaction status message value")
      }
    }
  }

  // Formatter for use with tools to read transactional id to pid mapping history
  class PidMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => readMessageKey(ByteBuffer.wrap(key))).foreach {
        case pidKey: PidKey =>
          val transactionalId = pidKey.key
          val value = consumerRecord.value
          val pidMetadata =
            if (value == null) "NULL"
            else readPidValue(ByteBuffer.wrap(value))
          output.write(transactionalId.getBytes)
          output.write("::".getBytes)
          output.write(pidMetadata.toString.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read transaction status update history
  class TxnStatusMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => readMessageKey(ByteBuffer.wrap(key))).foreach {
        case txnKey: TxnKey =>
          val pid = txnKey.key
          val value = consumerRecord.value
          val transactionMetadata =
            if (value == null) "NULL"
            else readTxnStatusValue(ByteBuffer.wrap(value))
          output.write(pid.toString.getBytes)
          output.write("::".getBytes)
          output.write(transactionMetadata.toString.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }
}

case class PidKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key.toString
}

case class TxnKey(version: Short, key: Long) extends BaseKey {

  override def toString: String = key.toString
}

/**
  * Transaction log manager is part of the transaction coordinator that manages the transaction log, which is
  * a special internal topic.
  */
class TransactionLogManager(val brokerId: Int,
                            val zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[Transaction Log Manager " + brokerId + "]: "

  /* number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of transaction topics that are assigned to this manager */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) { ownedPartitions.contains(partitionFor(transactionalId)) }

  /**
    * Gets the partition count of the transaction log topic from ZooKeeper.
    * If the topic does not exist, the default partition count is returned.
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(50)  // TODO: need a config for this
  }

  /**
    * Add the partition into the owned list
    *
    * TODO: this is for test only and should be augmented with txn log bootstrapping
    */
  def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }

  /**
    * Remove the partition from the owned list
    *
    * TODO: this is for test only and should be augmented with cache cleaning
    */
  def removePartitionOwnership(offsetsPartition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(offsetsPartition)
    }
  }
}

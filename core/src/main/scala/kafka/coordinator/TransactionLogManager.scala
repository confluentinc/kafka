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

import java.nio.ByteBuffer

import kafka.common.{KafkaException, Topic}
import kafka.utils.CoreUtils.inLock
import kafka.utils.{Logging, ZkUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.utils.Utils
import java.util.concurrent.locks.ReentrantLock

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
  *    -> value version 0:                              [epoch, status, [topic [partition] ]
  */
object TransactionLogManager {

  private val CURRENT_PID_KEY_SCHEMA_VERSION = 0.toShort
  private val CURRENT_TXN_KEY_SCHEMA_VERSION = 1.toShort

  private val TXN_ID_KEY = "transactional_id"
  private val PID_KEY = "pid"
  private val EPOCH_KEY = "epoch"
  private val STATUS_KEY = "status"
  private val TXN_TIMEOUT_KEY = "transaction_timeout"
  private val TIMESTAMP_KEY = "timestamp"
  private val PARTITIONS_KEY = "partitions"
  private val TOPIC_KEY = "topic"
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

  private val TXN_VALUE_SCHEMA_V0 = new Schema(new Field(EPOCH_KEY, INT16),
                                               new Field(STATUS_KEY, INT8),
                                               new Field(PARTITIONS_KEY, new ArrayOf(PARTITION_SCHEMA_V0)))
  private val TXN_VALUE_EPOCH_FIELD = TXN_VALUE_SCHEMA_V0.get(EPOCH_KEY)
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
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForPid(version: Int) = {
    val schemaOpt = PID_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForTxn(version: Int) = {
    val schemaOpt = TXN_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
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
    val key = new Struct(CURRENT_TXN_VALUE_SCHEMA)
    key.set(TXN_KEY_SCHEMA_PID_FIELD, pid)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_TXN_VALUE_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload for transactionalid to pid mapping message
    *
    * @return value payload bytes
    */
  private[coordinator] def pidValue(pidMetadata: PidMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_PID_VALUE_SCHEMA)
    value.set(PID_VALUE_SCHEMA_PID_FIELD, pidMetadata.pid)
    value.set(PID_VALUE_SCHEMA_EPOCH_FIELD, pidMetadata.epoch)
    value.set(PID_VALUE_SCHEMA_TXN_TIMEOUT_FIELD, 5000L) // FIXME

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
  private[coordinator] def groupMetadataValue(txnMetadata: TransactionMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_TXN_VALUE_SCHEMA)

    value.set(TXN_VALUE_EPOCH_FIELD, txnMetadata.pidMetadata.epoch)
    value.set(TXN_VALUE_STATUS_FIELD, txnMetadata.state)

    val partitionArray = txnMetadata.topicPartitions.map { topcPartition =>
      val memberStruct = value.instance(TXN_VALUE_PARTITIONS_FIELD)
      memberStruct.set(TOPIC_KEY, topcPartition.topic())
      memberStruct.set(CLIENT_ID_KEY, topcPartition.partition())
      memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost)
      memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs)

      if (version > 0)
        memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs)

      val metadata = memberMetadata.metadata(groupMetadata.protocol)
      memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata))

      val memberAssignment = assignment(memberMetadata.memberId)
      assert(memberAssignment != null)

      memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment))

      memberStruct
    }

    value.set(MEMBERS_KEY, memberArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_TXN_VALUE_SCHEMA)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Decodes the offset messages' key
    *
    * @param buffer input byte-buffer
    * @return an GroupTopicPartition object
    */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException("Unknown version " + version + " for group metadata message")
    }
  }

  /**
    * Decodes the offset messages' payload and retrieves offset and metadata from it
    *
    * @param buffer input byte-buffer
    * @return an offset-metadata object from the message
    */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffset(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version")
      }
    }
  }

  /**
    * Decodes the group metadata messages' payload and retrieves its member metadatafrom it
    *
    * @param buffer input byte-buffer
    * @return a group metadata object from the message
    */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer): GroupMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroup(version)
      val value = valueSchema.read(buffer)

      if (version == 0 || version == 1) {
        val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]

        val memberMetadataArray = value.getArray(MEMBERS_KEY)
        val initialState = if (memberMetadataArray.isEmpty) Empty else Stable

        val group = new GroupMetadata(groupId, initialState)

        group.generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
        group.leaderId = value.get(LEADER_KEY).asInstanceOf[String]
        group.protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]

        memberMetadataArray.foreach { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
          val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
          val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
          val sessionTimeout = memberMetadata.get(SESSION_TIMEOUT_KEY).asInstanceOf[Int]
          val rebalanceTimeout = if (version == 0) sessionTimeout else memberMetadata.get(REBALANCE_TIMEOUT_KEY).asInstanceOf[Int]

          val subscription = Utils.toArray(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])

          val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
            protocolType, List((group.protocol, subscription)))

          member.assignment = Utils.toArray(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])

          group.add(member)
        }

        group
      } else {
        throw new IllegalStateException("Unknown group metadata message version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value)).toString
          output.write(groupId.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

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

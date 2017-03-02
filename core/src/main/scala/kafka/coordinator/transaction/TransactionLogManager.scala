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

import kafka.common.{KafkaException, MessageFormatter, Topic}
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils.{KafkaScheduler, Logging, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.{Time, Utils}

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable
import scala.collection.JavaConverters._

/*
 * Messages stored for the transaction topic represent the pid and transactional status of the corresponding
 * transactional id, which have versions for both the key and value fields. Key and value
 * versions are used to evolve the message formats:
 *
 * key version 0:               [transactionalId]
 *    -> value version 0:       [pid, epoch, expire_timestamp, status, [topic [partition] ]
 */
object TransactionLogManager {

  private val TXN_ID_KEY = "transactional_id"

  private val PID_KEY = "pid"
  private val EPOCH_KEY = "epoch"
  private val TXN_TIMEOUT_KEY = "transaction_timeout"
  private val TXN_STATUS_KEY = "transaction_status"
  private val TXN_PARTITIONS_KEY = "transaction_partitions"
  private val TOPIC_KEY = "topic"
  private val PARTITION_IDS_KEY = "partition_ids"

  private val KEY_SCHEMA_V0 = new Schema(new Field(TXN_ID_KEY, STRING))
  private val KEY_SCHEMA_TXN_ID_FIELD = KEY_SCHEMA_V0.get(TXN_ID_KEY)

  private val VALUE_PARTITIONS_SCHEMA = new Schema(new Field(TOPIC_KEY, STRING),
                                                   new Field(PARTITION_IDS_KEY, new ArrayOf(INT32)))
  private val PARTITIONS_SCHEMA_TOPIC_FIELD = VALUE_PARTITIONS_SCHEMA.get(TOPIC_KEY)
  private val PARTITIONS_SCHEMA_PARTITION_IDS_FIELD = VALUE_PARTITIONS_SCHEMA.get(PARTITION_IDS_KEY)

  private val VALUE_SCHEMA_V0 = new Schema(new Field(PID_KEY, INT64),
                                           new Field(EPOCH_KEY, INT16),
                                           new Field(TXN_TIMEOUT_KEY, INT32),
                                           new Field(TXN_STATUS_KEY, INT8),
                                           new Field(TXN_PARTITIONS_KEY, ArrayOf.nullable(VALUE_PARTITIONS_SCHEMA)) )
  private val VALUE_SCHEMA_PID_FIELD = VALUE_SCHEMA_V0.get(PID_KEY)
  private val VALUE_SCHEMA_EPOCH_FIELD = VALUE_SCHEMA_V0.get(EPOCH_KEY)
  private val VALUE_SCHEMA_TXN_TIMEOUT_FIELD = VALUE_SCHEMA_V0.get(TXN_TIMEOUT_KEY)
  private val VALUE_SCHEMA_TXN_STATUS_FIELD = VALUE_SCHEMA_V0.get(TXN_STATUS_KEY)
  private val VALUE_SCHEMA_TXN_PARTITIONS_FIELD = VALUE_SCHEMA_V0.get(TXN_PARTITIONS_KEY)

  private val KEY_SCHEMAS = Map(
    0 -> KEY_SCHEMA_V0)

  private val VALUE_SCHEMAS = Map(
    0 -> VALUE_SCHEMA_V0)

  private val CURRENT_KEY_SCHEMA_VERSION = 0.toShort
  private val CURRENT_VALUE_SCHEMA_VERSION = 0.toShort

  private val CURRENT_KEY_SCHEMA = schemaForKey(CURRENT_KEY_SCHEMA_VERSION)

  private val CURRENT_VALUE_SCHEMA = schemaForValue(CURRENT_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = KEY_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction log message key schema version $version")
    }
  }

  private def schemaForValue(version: Int) = {
    val schemaOpt = VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException(s"Unknown transaction log message value schema version $version")
    }
  }

  /**
    * Generates the bytes for transaction log message key
    *
    * @return key bytes
    */
  private[coordinator] def keyToBytes(transactionalId: String): Array[Byte] = {
    val key = new Struct(CURRENT_KEY_SCHEMA)
    key.set(KEY_SCHEMA_TXN_ID_FIELD, transactionalId)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
    * Generates the payload bytes for transaction log message value
    *
    * @return value payload bytes
    */
  private[coordinator] def valueToBytes(pidMetadata: PidMetadata): Array[Byte] = {
    val value = new Struct(CURRENT_VALUE_SCHEMA)
    value.set(VALUE_SCHEMA_PID_FIELD, pidMetadata.pid)
    value.set(VALUE_SCHEMA_EPOCH_FIELD, pidMetadata.epoch)
    value.set(VALUE_SCHEMA_TXN_TIMEOUT_FIELD, pidMetadata.txnTimeoutMs)
    value.set(VALUE_SCHEMA_TXN_STATUS_FIELD, pidMetadata.txnMetadata.state.byte)

    if (pidMetadata.txnMetadata.state.equals(NotExist)) {
      if (pidMetadata.txnMetadata.topicPartitions.nonEmpty)
        throw new IllegalStateException(s"Transaction is not expected to have any partitions since its state is ${pidMetadata.txnMetadata.state}: ${pidMetadata.txnMetadata}")

      value.set(VALUE_SCHEMA_TXN_PARTITIONS_FIELD, null)
    } else {
      // first group the topic partitions by their topic names
      val topicAndPartitions = pidMetadata.txnMetadata.topicPartitions.groupBy(_.topic())

      val partitionArray = topicAndPartitions.map { topicAndPartitionIds =>
        val topicPartitionsStruct = value.instance(VALUE_SCHEMA_TXN_PARTITIONS_FIELD)
        val topic: String = topicAndPartitionIds._1
        val partitionIds: Array[Integer] = topicAndPartitionIds._2.map(topicPartition => Integer.valueOf(topicPartition.partition())).toArray

        topicPartitionsStruct.set(PARTITIONS_SCHEMA_TOPIC_FIELD, topic)
        topicPartitionsStruct.set(PARTITIONS_SCHEMA_PARTITION_IDS_FIELD, partitionIds)

        topicPartitionsStruct
      }
      value.set(VALUE_SCHEMA_TXN_PARTITIONS_FIELD, partitionArray.toArray)
    }

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_VALUE_SCHEMA_VERSION)
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

    if (version == CURRENT_KEY_SCHEMA_VERSION) {
      val transactionalId = key.getString(KEY_SCHEMA_TXN_ID_FIELD)

      TxnKey(version, transactionalId)
    } else {
      throw new IllegalStateException(s"Unknown version $version from the transaction log message")
    }
  }

  /**
    * Decodes the transaction log messages' payload and retrieves pid metadata from it
    *
    * @return a pid metadata object from the message
    */
  def readMessageValue(buffer: ByteBuffer): PidMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForValue(version)
      val value = valueSchema.read(buffer)

      if (version == CURRENT_VALUE_SCHEMA_VERSION) {
        val pid = value.get(VALUE_SCHEMA_PID_FIELD).asInstanceOf[Long]
        val epoch = value.get(VALUE_SCHEMA_EPOCH_FIELD).asInstanceOf[Short]
        val timeout = value.get(VALUE_SCHEMA_TXN_TIMEOUT_FIELD).asInstanceOf[Int]

        val stateByte = value.getByte(VALUE_SCHEMA_TXN_STATUS_FIELD)
        val state = TransactionMetadata.byteToState(stateByte)

        val transactionMetadata = new TransactionMetadata(state)

        if (!state.equals(NotExist)) {
          val topicPartitionArray = value.getArray(VALUE_SCHEMA_TXN_PARTITIONS_FIELD)

          topicPartitionArray.foreach { memberMetadataObj =>
            val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
            val topic = memberMetadata.get(PARTITIONS_SCHEMA_TOPIC_FIELD).asInstanceOf[String]
            val partitionIdArray = memberMetadata.getArray(PARTITIONS_SCHEMA_PARTITION_IDS_FIELD)

            val topicPartitions = partitionIdArray.map { partitionIdObj =>
              val partitionId = partitionIdObj.asInstanceOf[Integer]
              new TopicPartition(topic, partitionId)
            }

            transactionMetadata.addPartitions(topicPartitions.toSet)
          }
        }

        new PidMetadata(pid, epoch, timeout, transactionMetadata)
      } else {
        throw new IllegalStateException(s"Unknown version $version from the pid mapping message value")
      }
    }
  }

  // Formatter for use with tools to read transaction log messages
  class PidMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => readMessageKey(ByteBuffer.wrap(key))).foreach {
        case txnKey: TxnKey =>
          val transactionalId = txnKey.key
          val value = consumerRecord.value
          val pidMetadata =
            if (value == null) "NULL"
            else readMessageValue(ByteBuffer.wrap(value))
          output.write(transactionalId.getBytes)
          output.write("::".getBytes)
          output.write(pidMetadata.toString.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }
}

trait BaseKey{
  def version: Short
  def key: Any
}

case class TxnKey(version: Short, key: String) extends BaseKey {

  override def toString: String = key.toString
}

/*
 * Transaction log manager is part of the transaction coordinator that manages the transaction log, which is
 * a special internal topic.
 */
class TransactionLogManager(brokerId: Int,
                            replicaManager: ReplicaManager,
                            zkUtils: ZkUtils,
                            time: Time) extends Logging {

  this.logIdent = "[Transaction Log Manager " + brokerId + "]: "

  /* number of partitions for the transaction log topic */
  private val transactionTopicPartitionCount = getTransactionTopicPartitionCount

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of transaction topic that are assigned to this manager, partition lock should be called BEFORE accessing this set */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of transaction topic that are being loaded, partition lock should be called BEFORE accessing this set */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-")

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  def enablePidExpiration() {
    scheduler.startup()

    // TODO: add transaction and pid expiration logic
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) { ownedPartitions.contains(partitionFor(transactionalId)) }

  /**
    * Gets the partition count of the transaction log topic from ZooKeeper.
    * If the topic does not exist, the default partition count is returned.
    */
  private def getTransactionTopicPartitionCount: Int = {
    zkUtils.getTopicPartitionCount(Topic.TransactionStateTopicName).getOrElse(50)  // TODO: need a config for this
  }

  private[coordinator] def loadPidMetadata(topicPartition: TopicPartition) {
    def highWaterMark = replicaManager.getHighWatermark(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        var currOffset = log.logStartOffset.getOrElse(throw new IllegalStateException(s"Could not find log start offset for $topicPartition"))
        val buffer = ByteBuffer.allocate(5 * 1024 * 1024)   // TODO: need a config for this
        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        val loadedPids = mutable.Map.empty[String, PidMetadata]
        val removedPids = mutable.Set.empty[String]

        while (currOffset < highWaterMark && !shuttingDown.get()) {
          buffer.clear()
          val fileRecords = log.read(currOffset, 5 * 1024 * 1024, maxOffset = None, minOneMessage = true)
            .records.asInstanceOf[FileRecords]
          val bufferRead = fileRecords.readInto(buffer, 0)

          MemoryRecords.readableRecords(bufferRead).entries.asScala.foreach { entry =>
            for (record <- entry.asScala) {
              require(record.hasKey, "Group metadata/offset entry key should not be null")
              TransactionLogManager.readMessageKey(record.key) match {

                case txnKey: TxnKey =>
                  // load offset
                  val transactionalId: String = txnKey.key
                  if (record.hasNullValue) {
                    loadedPids.remove(transactionalId)
                    removedPids.add(transactionalId)
                  } else {
                    val pidMetadata = TransactionLogManager.readMessageValue(record.value)
                    loadedPids.put(transactionalId, pidMetadata)
                    removedPids.remove(transactionalId)
                  }

                case unknownKey =>
                  throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
              }

              currOffset = entry.nextOffset
            }
          }

          val (groupOffsets, emptyGroupOffsets) = loadedPids
            .groupBy(_._1.group)
            .mapValues(_.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset) })
            .partition { case (group, _) => loadedGroups.contains(group) }

          loadedGroups.values.foreach { group =>
            val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, OffsetAndMetadata])
            loadGroup(group, offsets)
            onGroupLoaded(group)
          }

          // load groups which store offsets in kafka, but which have no active members and thus no group
          // metadata stored in the log
          emptyGroupOffsets.foreach { case (groupId, offsets) =>
            val group = new GroupMetadata(groupId)
            loadGroup(group, offsets)
            onGroupLoaded(group)
          }

          removedGroups.foreach { groupId =>
            // if the cache already contains a group which should be removed, raise an error. Note that it
            // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
            // offset storage (i.e. by "simple" consumers)
            if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
              throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
                s"loading partition $topicPartition")
          }

          if (!shuttingDown.get())
            info("Finished loading offsets from %s in %d milliseconds."
              .format(topicPartition, time.milliseconds() - startMs))
        }
    }
  }

  /**
    * Add the partition into the owned list
    */
  def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }

    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    def doLoadGroupsAndOffsets() {
      info(s"Loading pid metadata from $topicPartition")

      inLock(partitionLock) {
        if (loadingPartitions.contains(partition)) {
          info(s"Offset load from $topicPartition already in progress.")
          return
        } else {
          loadingPartitions.add(partition)
        }
      }

      try {
        loadPidMetadata(topicPartition)
      } catch {
        case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
      } finally {
        inLock(partitionLock) {
          ownedPartitions.add(partition)
          loadingPartitions.remove(partition)
        }
      }
    }

    scheduler.schedule(topicPartition.toString, doLoadGroupsAndOffsets)
  }

  /**
    * Remove the partition from the owned list
    */
  def removePartitionOwnership(offsetsPartition: Int): Unit = {
    inLock(partitionLock) {
      ownedPartitions.remove(offsetsPartition)
    }
  }

  def shutdown() {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()

    ownedPartitions.clear()

    info("Shutdown complete")
  }
}

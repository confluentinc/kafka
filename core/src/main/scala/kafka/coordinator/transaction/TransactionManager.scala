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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import kafka.common.Topic
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils.{KafkaScheduler, Logging, Pool, ZkUtils}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.{FileRecords, MemoryRecords}
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.mutable
import scala.collection.JavaConverters._

/*
 * Transaction manager is part of the transaction coordinator, it manages:
 *
 * 1. the transaction log, which is a special internal topic.
 * 2. the pid metadata including its ongoing transaction status.
 * 3. the background expiration of the transaction as well as the transactional id to pid mapping.
 *
 */
class TransactionManager(brokerId: Int,
                         zkUtils: ZkUtils,
                         replicaManager: ReplicaManager,
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

  /* partitions of transaction topic that are corrupted; should always be an empty set under normal operations */
  private val corruptedPartitions: mutable.Set[Int] = mutable.Set()

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-")

  /* Pid metadata cache indexed by transactional id, including ongoing transaction status */
  private val pidMetadataCache = new Pool[String, PidMetadata]

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  def enablePidExpiration() {
    scheduler.startup()

    // TODO: add transaction and pid expiration logic
  }

  /**
    * Get the pid metadata associated with the given groupId, or null if not found
    */
  def getPid(groupId: String): Option[PidMetadata] = {
    Option(pidMetadataCache.get(groupId))
  }

  /**
    * Add a new pid metadata, or retrieve the metadata if it already exists with the associated transactional id
    */
  def addPid(transactionalId: String, pidMetadata: PidMetadata): PidMetadata = {
    val currentPidMetadata = pidMetadataCache.putIfNotExists(transactionalId, pidMetadata)
    if (currentPidMetadata != null) {
      currentPidMetadata
    } else {
      pidMetadata
    }
  }

  def partitionFor(transactionalId: String): Int = Utils.abs(transactionalId.hashCode) % transactionTopicPartitionCount

  def isCoordinatorFor(transactionalId: String): Boolean = inLock(partitionLock) {
    val partitionId = partitionFor(transactionalId)

    // partition id should be within the owned list and NOT in the corrupted list
    ownedPartitions.contains(partitionId) && !corruptedPartitions.contains(partitionId)
  }

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
        val buffer = ByteBuffer.allocate(5 * 1024 * 1024)   // TODO: need a config for this

        val loadedPids = mutable.Map.empty[String, PidMetadata]
        val removedPids = mutable.Set.empty[String]

        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        var currOffset = log.logStartOffset.getOrElse(throw new IllegalStateException(s"Could not find log start offset for $topicPartition"))
        while (currOffset < highWaterMark && !shuttingDown.get()) {
          buffer.clear()
          val fileRecords = log.read(currOffset, 5 * 1024 * 1024, maxOffset = None, minOneMessage = true)
            .records.asInstanceOf[FileRecords]
          val bufferRead = fileRecords.readInto(buffer, 0)

          MemoryRecords.readableRecords(bufferRead).entries.asScala.foreach { entry =>
            for (record <- entry.asScala) {
              require(record.hasKey, "Group metadata/offset entry key should not be null")
              TransactionLog.readMessageKey(record.key) match {

                case txnKey: TxnKey =>
                  // load offset
                  val transactionalId: String = txnKey.key
                  if (record.hasNullValue) {
                    loadedPids.remove(transactionalId)
                    removedPids.add(transactionalId)
                  } else {
                    val pidMetadata = TransactionLog.readMessageValue(record.value)
                    loadedPids.put(transactionalId, pidMetadata)
                    removedPids.remove(transactionalId)
                  }

                case unknownKey =>
                  throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
              }

              currOffset = entry.nextOffset
            }
          }

          loadedPids.foreach {
            case (transactionalId, pidMetadata) =>
              val currentPidMetadata = addPid(transactionalId, pidMetadata)
              if (!pidMetadata.equals(currentPidMetadata)) {
                // treat this as a fatal failure as this should never happen
                fatal(s"Attempt to load $transactionalId's pid metadata $pidMetadata failed " +
                  s"because there is already a different cached pid metadata $currentPidMetadata; " +
                  s"all future client requests with transactional ids related to this partition will result in an error code, " +
                  s"and this partition of the log will be effectively disabled.")

                corruptedPartitions.add(topicPartition.partition)
              }
          }

          removedPids.foreach { transactionalId =>
            // if the cache already contains a pid which should be removed, raise an error.
            if (pidMetadataCache.contains(transactionalId))
              throw new IllegalStateException(s"Unexpected unload of $transactionalId's pid metadata while " +
                s"loading partition $topicPartition")
          }

          info(s"Finished loading ${loadedPids.size} pid metadata from $topicPartition in ${time.milliseconds() - startMs} milliseconds")
        }
    }
  }

  /**
    * When this broker becomes a leader for a transaction log partition, load this partition and
    * populate the pid metadata cache with the transactional ids.
    */
  def loadTransactionsForPartition(partition: Int) {
    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    def loadPidAndTransactions() {
      info(s"Loading pid metadata from $topicPartition")

      inLock(partitionLock) {
        if (loadingPartitions.contains(partition)) {
          // with background scheduler containing one thread, this should never happen,
          // but just in case we change it in the future.
          info(s"Pid and transaction status loading from $topicPartition already in progress.")
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

    scheduler.schedule(topicPartition.toString, loadPidAndTransactions)
  }

  /**
    * When this broker becomes a follower for a transaction log partition, clear out the cache for corresponding transactional ids
    * that belong to that partition.
    */
  def removePartitionOwnership(partition: Int) {
    val topicPartition = new TopicPartition(Topic.TransactionStateTopicName, partition)

    def removePidAndTransactions() {
      var numPidsRemoved = 0

      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        ownedPartitions.remove(partition)

        for (transactionalId <- pidMetadataCache.keys) {
          if (partitionFor(transactionalId) == partition) {
            // we do not need to worry about whether the pid has any ongoing transaction or not since
            // the new leader will handle it
            pidMetadataCache.remove(transactionalId)
            numPidsRemoved += 1
          }
        }
      }

      scheduler.schedule(topicPartition.toString, removePidAndTransactions)

      if (numPidsRemoved > 0)
        info(s"Removed $numPidsRemoved cached pid metadata for $topicPartition on follower transition")
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


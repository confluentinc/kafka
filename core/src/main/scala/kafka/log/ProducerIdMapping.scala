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
package kafka.log

import java.io._
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import kafka.common.KafkaException
import kafka.utils.{Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{DuplicateSequenceNumberException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.record.LogEntry
import org.apache.kafka.common.utils.Utils

import scala.collection.{immutable, mutable}

private[log] case class PidEntry(lastSeq: Int,
                    epoch: Short,
                    lastOffset: Long,
                    numRecords: Int,
                    timestamp: Long
                   ) {
  def firstSeq(): Int = {
    lastSeq - numRecords + 1
  }

  def firstOffset(): Long = {
    lastOffset - numRecords + 1
  }
}
private[log] case class PidEntryRange(firstEntry: PidEntry, lastEntry: PidEntry)
private[log] class CorruptSnapshotException(msg: String) extends KafkaException(msg)

object ProducerIdMapping {
  private val DirnamePrefix = "pid-mapping-"
  private val FilenameSuffix = "snapshot"
  private val FilenamePattern = s"^\\d{1,}.$FilenameSuffix".r
  private val PidSnapshotVersion: Short = 1

  private val VersionField = "version"
  private val CrcField = "crc"
  private val PidField = "pid"
  private val SequenceField = "sequence"
  private val EpochField = "epoch"
  private val OffsetField = "offset"
  private val NumRecordsField = "num_records"
  private val TimestampField = "timestamp"
  private val PidEntriesField = "pid_entries"

  private val VersionOffset = 0
  private val CrcOffset = VersionOffset + 2
  private val PidEntriesOffset = CrcOffset + 4

  val PidSnapshotEntrySchema = new Schema(
    new Field(PidField, Type.INT64, "The producer ID"),
    new Field(EpochField, Type.INT16, "Current epoch of the producer"),
    new Field(SequenceField, Type.INT32, "Last written sequence of the producer"),
    new Field(OffsetField, Type.INT64, "Last written offset of the producer"),
    new Field(NumRecordsField, Type.INT32, "The number of records written in the last log entry"),
    new Field(TimestampField, Type.INT64, "Timestamp of the last written entry"));
  val PidSnapshotMapSchema = new Schema(
    new Field(VersionField, Type.INT16, "Version of the snapshot file"),
    new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
    new Field(PidEntriesField, new ArrayOf(PidSnapshotEntrySchema), "The entries in the PID table"))

  def validatePidEntries(pid: Long, existingEntry: PidEntry, incomingEntry: PidEntry): Unit = {
    // Zombie writer
    if (existingEntry.epoch > incomingEntry.epoch)
      throw new ProducerFencedException(s"Invalid epoch (zombie writer): ${incomingEntry.epoch} (request epoch), ${existingEntry.epoch}")
    if (existingEntry.epoch < incomingEntry.epoch) {
      if (incomingEntry.firstSeq != 0)
        throw new OutOfOrderSequenceException(s"Invalid sequence number for new epoch: ${incomingEntry.epoch} (request epoch), ${incomingEntry.firstSeq} (seq. number)")
    } else {
      if (incomingEntry.firstSeq > existingEntry.lastSeq + 1L)
        throw new OutOfOrderSequenceException(s"Invalid sequence number: $pid (id), ${incomingEntry.firstSeq} (seq. number), ${existingEntry.lastSeq} (expected seq. number)")
      else if (incomingEntry.firstSeq <= existingEntry.lastSeq)
        throw new DuplicateSequenceNumberException(s"Duplicate sequence number: $pid (id), ${incomingEntry.firstSeq} (seq. number), ${existingEntry.firstSeq} (expected seq. number)")
    }
  }

  private def loadSnapshot(file: File, pidMap: mutable.Map[Long, PidEntry]) {
    val buffer = Files.readAllBytes(file.toPath)
    val struct = PidSnapshotMapSchema.read(ByteBuffer.wrap(buffer))

    val version = struct.getShort(VersionField)
    if (version != PidSnapshotVersion)
      throw new IllegalArgumentException(s"Unhandled snapshot file version $version")

    val crc = struct.getUnsignedInt(CrcField)
    if (crc != Utils.computeChecksum(buffer, PidEntriesOffset, buffer.length - PidEntriesOffset))
      throw new CorruptSnapshotException("Snapshot file is corrupted (CRC is no longer valid)")

    struct.getArray(PidEntriesField).foreach { pidEntryObj =>
      val pidEntry = pidEntryObj.asInstanceOf[Struct]
      val pid = pidEntry.getLong(PidField)
      val epoch = pidEntry.getShort(EpochField)
      val seq = pidEntry.getInt(SequenceField)
      val offset = pidEntry.getLong(OffsetField)
      val timestamp = pidEntry.getLong(TimestampField)
      val numRecords = pidEntry.getInt(NumRecordsField)
      pidMap.put(pid, PidEntry(seq, epoch, offset, numRecords, timestamp))
    }
  }

  private def writeSnapshot(file: File, entries: mutable.Map[Long, PidEntry]) {
    val struct = new Struct(PidSnapshotMapSchema)
    struct.set(VersionField, PidSnapshotVersion)
    struct.set(CrcField, 0L) // we'll fill this after writing the entries
    val entriesArray = entries.map {
      case (pid, PidEntry(seq, epoch, offset, numRecords, timestamp)) =>
        val pidEntryStruct = struct.instance(PidEntriesField)
        pidEntryStruct.set(PidField, pid)
        pidEntryStruct.set(EpochField, epoch)
        pidEntryStruct.set(SequenceField, seq)
        pidEntryStruct.set(OffsetField, offset)
        pidEntryStruct.set(NumRecordsField, numRecords)
        pidEntryStruct.set(TimestampField, timestamp)
        pidEntryStruct
    }.toArray
    struct.set(PidEntriesField, entriesArray)

    val buffer = ByteBuffer.allocate(struct.sizeOf)
    struct.writeTo(buffer)
    buffer.flip()

    // now fill in the CRC
    val crc = Utils.computeChecksum(buffer, PidEntriesOffset, buffer.limit - PidEntriesOffset)
    Utils.writeUnsignedInt(buffer, CrcOffset, crc)

    val fos = new FileOutputStream(file)
    try {
      fos.write(buffer.array, buffer.arrayOffset, buffer.limit)
    } finally {
      fos.close()
    }
  }

  private def verifyFileName(name: String): Boolean = FilenamePattern.findFirstIn(name).isDefined

  private def offsetFromFile(file: File): Long = {
    s"${file.getName.replace(s".$FilenameSuffix", "")}".toLong
  }

  private def formatDirName(topicPartition: TopicPartition): String = {
    s"$DirnamePrefix-${topicPartition.topic}-${topicPartition.partition}"
  }

  private def formatFileName(lastOffset: Long): String = {
    s"$lastOffset.$FilenameSuffix"
  }

}

/**
 * Maintains a mapping from identifiers to a triple:
 *   <sequence number, epoch, offset>
 *
 * The sequence number is the last number successfully appended to the partition for the given identifier.
 * The epoch is used for fencing against zombie writers. The offset is the one of the last successful message
 * appended to the partition.
 *
 * @param topicPartition
 * @param config
 * @param snapParentDir
 */
@nonthreadsafe
class ProducerIdMapping(val topicPartition: TopicPartition,
                        val config: LogConfig,
                        val snapParentDir: File) extends Logging {
  import ProducerIdMapping._

  val snapDir: File = new File(snapParentDir, formatDirName(topicPartition))
  snapDir.mkdir()

  private val pidMap = mutable.Map[Long, PidEntry]()
  private var lastMapOffset = -1L
  private var lastSnapOffset = -1L

  /**
   * Returns the last offset of this map
   */
  def mapEndOffset = lastMapOffset

  def activePids: immutable.Map[Long, PidEntry] = pidMap.toMap

  /**
   * Load a snapshot of the id mapping or return empty maps
   * in the case the snapshot doesn't exist (first time).
   *
   * @return
   */
  private def loadFromSnapshot(logEndOffset: Long) {
    pidMap.clear()
    lastSnapshotFile(logEndOffset) match {
      case Some(file) =>
        try {
          trace("Loading snapshot from file: %s".format(file.toPath))
          loadSnapshot(file, pidMap)
          lastSnapOffset = offsetFromFile(file)
          lastMapOffset = lastSnapOffset
        } catch {
          case e: CorruptSnapshotException =>
            // TODO: Delete the file and try to use the next snapshot
            throw e
        }

      case None =>
        lastSnapOffset = -1
        lastMapOffset = -1
        snapDir.mkdir()
    }
  }

  def truncateAndReload(logEndOffset: Long) = {
    truncateSnapshotFiles(logEndOffset)
    loadFromSnapshot(logEndOffset)
  }

  /**
   * Update the mapping to the given epoch and sequence number.
   */
  def update(pid: Long, pidEntry: PidEntry): Unit = {
    if (pid != LogEntry.NO_PID) {
      pidMap.put(pid, pidEntry)
      lastMapOffset = pidEntry.lastOffset
    }
  }

  /**
   * Verify the epoch and next expected sequence number. Invoked prior to appending the entries to
   * the log.
   * @param pid
   */
  def checkSeqAndEpoch(pid: Long, incomingEntry: PidEntry) {
    if (pid != LogEntry.NO_PID) {
      pidMap.get(pid) match {
        case None =>
          if (incomingEntry.firstSeq != 0)
            throw new OutOfOrderSequenceException(s"Invalid sequence number: $pid (id), ${incomingEntry.firstSeq} (seq. number)")
        case Some(tuple) =>
          validatePidEntries(pid, tuple, incomingEntry)
     }
    }
  }


  def entryForPid(pid: Long): Option[PidEntry] = {
    pidMap.get(pid)
  }

  /**
    * Serialize and write the bytes to a file. The file name is a concatenation of:
    *   - offset
    *   - a ".snapshot" suffix
    *
    * The parent directory is a concatenation of:
    *   - log.dir
    *   - base name
    *   - topic
    *   - partition id
    *
    *   The offset is supposed to increase over time and
    */
  def maybeTakeSnapshot() {
    // If not a new offset, then it is not worth taking another snapshot
    if (lastMapOffset > lastSnapOffset) {
      val file = new File(snapDir, formatFileName(lastMapOffset))
      writeSnapshot(file, pidMap)

      // Update the last snap offset according to the serialized map
      lastSnapOffset = lastMapOffset

      maybeRemove()
    }
  }

  /**
    * When we remove the head of the log due to retention, we need to
    * clean up the id map. This method takes the new start offset and
    * expires all ids that have a smaller offset.
    *
    * @param startOffset New start offset for the log associated to
    *                    this id map instance
    */
  def cleanFrom(startOffset: Long) {
    pidMap.retain((pid, entry) => entry.lastOffset >= startOffset)
    if (pidMap.isEmpty)
      lastMapOffset = -1L
  }

  def clean(): Unit = {
    pidMap.clear()
  }

  private def maybeRemove() {
    val list = listSnapshotFiles()
    if (list.size > config.maxIdMapSnapshots) {
      // Get file with the smallest offset
      val toDelete = list.minBy(offsetFromFile)
      // Delete the last
      toDelete.delete()
    }
  }

  private def listSnapshotFiles(): List[File] = {
    if (snapDir.exists && snapDir.isDirectory)
      snapDir.listFiles.filter(f => f.isFile && verifyFileName(f.getName)).toList
    else
      List.empty[File]
  }

  /**
   * Returns the last valid snapshot with offset smaller than the
   * base offset provided as a constructor parameter for loading
   *
   * @return
   */
  private def lastSnapshotFile(maxOffset: Long): Option[File] = {
    val files = listSnapshotFiles()
    if (files != null && files.nonEmpty) {
      val targetOffset = files.foldLeft(0L) { (accOffset, file) =>
        val snapshotLastOffset = offsetFromFile(file)
        if ((maxOffset >= snapshotLastOffset) && (snapshotLastOffset > accOffset))
          snapshotLastOffset
        else
          accOffset
      }
      val snap = new File(snapDir, formatFileName(targetOffset))
      if (snap.exists)
        Some(snap)
      else
        None
    } else
      None
  }

  private def truncateSnapshotFiles(maxOffset: Long) {
    listSnapshotFiles().foreach { file =>
      val snapshotLastOffset = offsetFromFile(file)
      if (snapshotLastOffset > maxOffset)
        file.delete()
    }
  }
}

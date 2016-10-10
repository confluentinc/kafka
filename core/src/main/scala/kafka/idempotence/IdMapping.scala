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

package kafka.idempotence


import java.io._
import java.nio.ByteBuffer
import java.nio.file.{Paths, Files}

import kafka.common.TopicAndPartition
import kafka.log.LogConfig
import kafka.utils.Logging
import org.apache.kafka.common.protocol.types._
import org.apache.kafka.common.utils.Crc32

import scala.collection.immutable.HashMap


object IdMapping {
  // Strings used to compose the snapshot file name

  /**
   * Base directory name
   */
  val BaseName = "id-mapping-"

  /**
    * File suffix
    */
  val FilenameSuffix = "snap"

  /**
    * Version of snapshot schema
    */
  val SnapVersion: Byte = 0

  /**
    * Snapshot schema
    */
  val SnapshotSchema = new Schema(new Field("id_map_snap_version", Type.INT8, "The controller id."),
                                  new Field("id_map_crc", Type.INT32, "The controller epoch."),
                                  new Field("id_map", Type.BYTES, "The controller epoch."))
  /**
    * Formats the directory name to store
    * snapshots using the topic partition
    * name and id.
    *
    * @param topicPartition
    * @return
    */
  def formatDirName(topicPartition: TopicAndPartition): String = {
    s"$BaseName-${topicPartition.topic}-${topicPartition.partition}"
  }

  def formatFileName(lastOffset: Long): String = {
    s"$lastOffset.$FilenameSuffix"
  }

  /**
    * Evaluates to a list of snapshot files
    *
    * @return
    */
  def listOfSnapshots(snapDir: File):List[File] = {
    if (snapDir.exists && snapDir.isDirectory) {
      snapDir.listFiles.filter(f => {f.isFile && verifyFileName(f.getName)}).toList
    } else {
      List[File]()
    }
  }

  private val regexp = s"^\\d{1,}.${IdMapping.FilenameSuffix}".r

  /**
    * Verifies the format of a file name.
    *
    * @param name
    * @return
    */
  def verifyFileName(name: String): Boolean = {
    (regexp findFirstIn name) match {
      case Some(s) => true
      case None => false
    }
  }

  /**
    * Returns the last valid snapshot with offset smaller than the
    * base offset provided as a constructor parameter for loading
    *
    * @return
    */
  def lastSnapshot(snapDir: File, maxOffset: Long): Option[File] = {
    val files = snapDir.listFiles()
    if(files != null && files.length > 0) {
      val targetOffset = files.foldLeft[Long](0L)((accOffset, f) => {
        val snapshotLastOffset = extractOffsetFromName(f)
        if ((maxOffset >= snapshotLastOffset) && (snapshotLastOffset > accOffset))
          snapshotLastOffset
        else
          accOffset
      })
      val snap = new File(snapDir, IdMapping.formatFileName(targetOffset))
      if(snap.exists())
        Some(snap)
      else
        None
    } else
      None
  }

  /**
    * Extracts offset from file name
    *
    * @param file
    * @return
    */
  def extractOffsetFromName(file: File): Long = {
    s"${file.getName.replace(s".${IdMapping.FilenameSuffix}", "")}".toLong
  }

  /**
    * Returns the earliest snapshot based on the offset in the file name
    *
    * @param first First file
    * @param second Second file
    * @return
    */
  def findMin(first: File, second: File): File = {
    if(IdMapping.extractOffsetFromName(first) > IdMapping.extractOffsetFromName(second))
      second
    else
      first
  }
}

/**
  * Maintains a mapping from identifiers to a triple:
  *   <sequence number, epoch, offset>
  *
  * The sequence number is the last number successfully
  * appended to the partition for the given identifier.
  * The epoch is used for fencing against zombie writers.
  * The offset is the one of the last successful message
  * appended to the partition.
  *
  * @param topicPartition
  */
class IdMapping (topicPartition: TopicAndPartition,
                 config: LogConfig,
                 snapParentDir: File,
                 logStartOffset: Long = 0,
                 logEndOffset: Long = Long.MaxValue) extends Logging {
  val snapDir: File = new File(snapParentDir, IdMapping.formatDirName(topicPartition))

  private var idMap = loadMap()
  @volatile private var lastMapOffset = -1L
  @volatile private var lastSnapOffset = -1L

  case class IdMapTuple(seqNumber: Long, epoch: Int, offset: Long)

  // Clean the map according to the start offset if needed
  maybeClean(logStartOffset)

  /**
    * Returns the last offset of this map
    */
  def mapEndOffset = lastMapOffset

  /**
    * Load a snapshot of the id mapping or return empty maps
    * in the case the snapshot doesn't exist (first time).
    *
    * @return
    */
  private def loadMap(): HashMap[Long, IdMapTuple] = {
    var idMap = HashMap.empty[Long, IdMapTuple]
    IdMapping.lastSnapshot(snapDir, logEndOffset) match {
      case Some(f) => {
        // TODO: Loading the whole file at once is probably not a good
        // idea. We should consider changing this logic to stream from
        // file instead.
        val buffer = Files.readAllBytes(f.toPath)
        val struct = IdMapping.SnapshotSchema.read(ByteBuffer.wrap(buffer))
        // TODO: Check crc
        val version = struct.get("id_map_snap_version").asInstanceOf[Byte]
        val mapBytes = struct.get("id_map").asInstanceOf[ByteBuffer]
        val crc = struct.get("id_map_crc").asInstanceOf[Int]

        while(mapBytes.remaining > 0) {
          val id = mapBytes.getLong()
          val seq = mapBytes.getLong()
          val epoch = mapBytes.getInt()
          val offset = mapBytes.getLong()
          idMap += (id -> new IdMapTuple(seq, epoch, offset))
        }
        lastSnapOffset = IdMapping.extractOffsetFromName(f)
        lastMapOffset = lastSnapOffset
      }
      case None => {
        snapDir.mkdir()
      }
    }
    idMap
  }

  /**
    * Check that the sequence number and update the id mapping
    *
    * @param id
    * @param seq
    * @param epoch
    * @return
    */
  def checkAndUpdate(id: Long, seq: Long, epoch: Int, lastOffset: Long): Unit = {
    checkAndUpdate(id, seq, seq, epoch, lastOffset)
  }

  /**
    * Check that the sequence numbers and epoch are correct for the
    * given identifier. Unlike the previous method, we assume
    * here a range of consecutive numbers, so we check that the first
    * sequence number is the expected one and update the mapping using
    * the last one.
    *
    * @param id
    * @param firstSeq
    * @param lastSeq
    * @param epoch
    * @return
    */
  def checkAndUpdate(id: Long, firstSeq: Long, lastSeq: Long, epoch: Int, lastOffset: Long): Unit = {
    checkSeqAndEpoch(id, firstSeq, epoch)
    idMap synchronized {
      idMap += (id -> new IdMapTuple(lastSeq, epoch, lastOffset))
      lastMapOffset = lastOffset
    }
  }

  private def checkSeqAndEpoch(id: Long, seq: Long, epoch: Int): Unit = {
    idMap.synchronized {
      idMap get id
    } match {
      case None => {
        if (seq != 0)
          // TODO: InvalidSequenceNumberException
          throw new Exception(s"Invalid sequence number: $id (id), $seq (seq. number)")
      }
      case Some(tuple) => {
        // Zombie writer
        if (tuple.epoch > epoch)
          // TODO: InvalidEpochException
          throw new Exception(s"Invalid epoch (zombie writer): $epoch (request epoch), ${tuple.epoch}")
        if (tuple.epoch < epoch) {
          if (seq != 0)
            // TODO InvalidSequenceNumberException
            throw new Exception(s"Invalid sequence number for new epoch: $epoch (request epoch), $seq (seq. number)")
        } else {
          if (seq != (tuple.seqNumber + 1L)) {
            // TODO: InvalidSequenceNumberException
            throw new Exception(s"Invalid sequence number: $id (id), $seq (seq. number), ${tuple.seqNumber} (expected seq. number)")
          }
        }
      }
    }
  }

  /**
    * Serialize and write the bytes to a file. The file name is a concatenation of:
    *   - offset
    *   - a ".snap" suffix
    *
    * The parent directory is a concatenation of:
    *   - log.dir
    *   - base name
    *   - topic
    *   - partition id
    *
    *   The offset is supposed to increase over time and
    */
  def takeSnapshot(): Unit = {
    var fos: FileOutputStream = null
    var buffer: ByteBuffer = null
    idMap synchronized {
      // If not a new offset, then it is not worth
      // taking another snapshot
      if (lastMapOffset > lastSnapOffset) {
        fos = new FileOutputStream(new File(snapDir, IdMapping.formatFileName(lastMapOffset)))
        buffer = ByteBuffer.allocate(idMap.keySet.size * 28)
        idMap.foreach ( x => {
          buffer.putLong(x._1)
          buffer.putLong(x._2.seqNumber)
          buffer.putInt(x._2.epoch)
          buffer.putLong(x._2.offset)
        })
        buffer.flip()
        // Update the last snap offset according to the serialized map
        lastSnapOffset = lastMapOffset
      }
    }
    // The write to the file doesn't have to be synchronized, only obtaining the
    // bytes from the id map.
    if (fos != null) {
      val struct = new Struct(IdMapping.SnapshotSchema)
      struct.set("id_map_snap_version", IdMapping.SnapVersion)
      struct.set("id_map", buffer)
      val crc = new Crc32()
      crc.update(buffer.array(), 0, buffer.array().length)
      struct.set("id_map_crc", crc.getValue.asInstanceOf[Int])
      val snapBytes = struct.toBytes
      // Write bytes, length, and CRC to snapshot file
      snapBytes.foreach ( b => {
        b.flip()
        fos.write(b.array())
      })

      fos.flush()
      fos.close()

      // Remove old files
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
  def maybeClean(startOffset: Long): Unit = {
    idMap synchronized {
      idMap = idMap.filter { case (id, tuple) =>
        tuple.offset >= startOffset
      }
      if(idMap.isEmpty)
        lastMapOffset = -1L
    }
  }

  private def maybeRemove(): Unit = {
    val list = IdMapping.listOfSnapshots(snapDir)
    if (list.size > config.maxIdMapSnapshots) {
      // Get the smallest offset
      val toDelete = list.reduceLeft(IdMapping.findMin)
      // Delete the last
      toDelete.delete()
    }
  }
}
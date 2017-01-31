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

import kafka.utils.{Json, Logging, ZkUtils}

/**
  * PID Manager is part of the transaction coordinator that provides PIDs in a unique way such that the same PID will not be
  * assigned twice across multiple transaction coordinators.
  *
  * PIDs are managed via ZooKeeper as the coordination mechanism.
  */

object PIDManager {
  val version: Long = 1L
  val PIDBlockSize: Int = 1000
  val PIDBLockPrefix: String = "pid_block_"
}


class PIDManager(val brokerId: Int,
                 val zkUtils: ZkUtils) extends Logging {

  this.logIdent = "[PID Manager " + brokerId + "]: "

  private var blockStartPID: Long = -1L
  private var blockEndPID: Long = -1L
  private var currentPID: Long = -1L

  // grab the first block of PIDs
  this synchronized {
    getNewPIDBlock()
    currentPID = blockStartPID
  }

  private def getNewPIDBlock(): Unit = {
    val pIDBlock: String = zkUtils.createSequentialPersistentPath(
      ZkUtils.IdempotentPIDPath + "/" + PIDManager.PIDBLockPrefix,
      generatePIDBlockJson())

    blockStartPID = pIDBlock.stripPrefix(PIDManager.PIDBLockPrefix).toLong
    blockEndPID = blockStartPID + PIDManager.PIDBlockSize - 1

    debug("Acquired new block from %d to %d".format(blockStartPID, blockEndPID))
  }

  private def generatePIDBlockJson(): String = {
    Json.encode(Map("version" -> PIDManager.version, "broker" -> brokerId))
  }

  def getNewPID(): Long = {
    this synchronized {
      // grab a new block of PIDs if this block has been exhausted
      if (currentPID + 1 > blockEndPID) {
        getNewPIDBlock()
        currentPID = blockStartPID
      } else {
        currentPID += 1
      }

      currentPID
    }
  }

  def shutdown() {
    info("Shutdown complete: last PID assigned %d".format(currentPID))
  }
}

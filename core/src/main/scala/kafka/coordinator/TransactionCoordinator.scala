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

import java.util.concurrent.atomic.AtomicBoolean

import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.{Logging, Pool, ZkUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time

/**
  * Transaction coordinator handles message transactions sent by producers and communicate with brokers
  * to update ongoing transaction's status.
  *
  * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
  * producers. Producers with specific transactional ids are assigned to their corresponding coordinators;
  * Producers with no specific transactional id may talk to a random broker as their coordinators.
  */
object TransactionCoordinator {

  def apply(config: KafkaConfig, zkUtils: ZkUtils, time: Time): TransactionCoordinator = {

    val pIDManager = new PIDManager(config.brokerId, zkUtils)
    val logManager = new TransactionLogManager(config.brokerId, zkUtils)
    new TransactionCoordinator(config.brokerId, pIDManager, logManager)
  }
}

class TransactionCoordinator(val brokerId: Int,
                             val pIDManager: PIDManager,
                             val logManager: TransactionLogManager) extends Logging {

  this.logIdent = "[Transaction Coordinator " + brokerId + "]: "

  type InitPIDCallback = InitPIDResult => Unit

  /* Active flag of the coordinator */
  private val isActive = new AtomicBoolean(false)

  /* TxnID to PID metadata map cache */
  private val pIDMetadataCache = new Pool[String, PIDMetadata]

  def handleInitPID(txnID: String,
                    responseCallback: InitPIDCallback): Unit = {
    if (txnID == null || txnID.isEmpty) {
      // if the txnID is not specified, then always blindly accept the request
      // and return a new PID from the PID manager
      val pID: Long = pIDManager.getNewPID()

      responseCallback(InitPIDResult(pID, -1 /* epoch */, Errors.NONE))
    } else if(!logManager.isCoordinatorFor(txnID)) {
      // check if it is the assigned coordinator for the txnID
      responseCallback(initPIDError(Errors.NOT_COORDINATOR_FOR_GROUP))
    } else {
      // only try to get a new PID and update the cache if the txnID is unknown
      getPIDMetadata(txnID) match {
        case None =>
          val pID: Long = pIDManager.getNewPID()
          val metadata = addPIDMetadata(txnID, new PIDMetadata(pID))

          responseCallback(initPIDMetadata(metadata))

        case Some(metadata) =>
          metadata.epoch = (metadata.epoch + 1).toShort

          responseCallback(initPIDMetadata(metadata))
      }
    }
  }

  def handleTxnImmigration(offsetTopicPartitionId: Int) {
    logManager.addPartitionOwnership(offsetTopicPartitionId)
  }

  def handleTxnEmigration(offsetTopicPartitionId: Int) {
    logManager.removePartitionOwnership(offsetTopicPartitionId)
  }

  /**
    * Startup logic executed at the same time when the server starts up.
    */
  def startup() {
    info("Starting up.")
    isActive.set(true)
    info("Startup complete.")
  }

  /**
    * Shutdown logic executed at the same time when server shuts down.
    * Ordering of actions should be reversed from the startup process.
    */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    pIDManager.shutdown()
    info("Shutdown complete.")
  }

  private def getPIDMetadata(txnID: String): Option[PIDMetadata] = {
    Option(pIDMetadataCache.get(txnID))
  }

  private def addPIDMetadata(txnID: String, pIDMetadata: PIDMetadata): PIDMetadata = {
    val currentMetadata = pIDMetadataCache.putIfNotExists(txnID, pIDMetadata)
    if (currentMetadata != null) {
      currentMetadata
    } else {
      pIDMetadata
    }
  }

  private def initPIDError(error: Errors): InitPIDResult = {
    InitPIDResult(-1L /* PID */, -1 /* epoch */, error)
  }

  private def initPIDMetadata(pIDMetadata: PIDMetadata): InitPIDResult = {
    InitPIDResult(pIDMetadata.PID, pIDMetadata.epoch, Errors.NONE)
  }
}



case class InitPIDResult(pID: Long, epoch: Short, error: Errors)
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

import kafka.server.{KafkaConfig, ReplicaManager}
import kafka.utils.{Logging, Pool, ZkUtils}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.Time

/**
  * Transaction coordinator handles message transactions sent by producers and communicate with brokers
  * to update ongoing transaction's status.
  *
  * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
  * producers. Producers with specific appIDs are assigned to their corresponding coordinators;
  * Producers with no specific appIDs may talk to a random broker as their coordinators.
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

  /* AppID to PID metadata map cache */
  private val pIDMetadataCache = new Pool[String, PIDMetadata]

  def handleInitPID(appID: String,
                    responseCallback: InitPIDCallback): Unit = {
    if (appID == null || appID.isEmpty) {
      // if the appID is not specified, then always blindly accept the request
      // and return a new PID from the PID manager
      val pID: Long = pIDManager.getNewPID()

      responseCallback(InitPIDResult(pID, -1 /* epoch */, Errors.NONE.code()))
    } else if(!logManager.isCoordinatorFor(appID)) {
      // check if it is the assigned coordinator for the appID
      responseCallback(initPIDError(Errors.NOT_COORDINATOR_FOR_GROUP.code()))
    } else {
      // only try to get a new PID and update the cache if the appID is unknown
      getPIDMetadata(appID) match {
        case None =>
          val pID: Long = pIDManager.getNewPID()
          val metadata = addPIDMetadata(appID, new PIDMetadata(pID))

          responseCallback(initPIDMetadata(metadata))

        case Some(metadata) =>
          metadata.epoch += 1

          responseCallback(initPIDMetadata(metadata))
      }
    }
  }

  private def getPIDMetadata(appID: String): Option[PIDMetadata] = {
    Option(pIDMetadataCache.get(appID))
  }

  private def addPIDMetadata(appID: String, pIDMetadata: PIDMetadata): PIDMetadata = {
    val currentMetadata = pIDMetadataCache.putIfNotExists(appID, pIDMetadata)
    if (currentMetadata != null) {
      currentMetadata
    } else {
      pIDMetadata
    }
  }

  private def initPIDError(errorCode: Short): InitPIDResult = {
    InitPIDResult(-1L /* PID */, -1 /* epoch */, errorCode)
  }

  private def initPIDMetadata(pIDMetadata: PIDMetadata): InitPIDResult = {
    InitPIDResult(pIDMetadata.PID, pIDMetadata.epoch, Errors.NONE.code())
  }
}



case class InitPIDResult(pID: Long, epoch: Short, errorCode: Short)
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

import kafka.utils.{Logging, Pool}
import org.apache.kafka.common.protocol.Errors

/**
  * Transaction coordinator handles message transactions sent by producers and communicate with brokers
  * to update ongoing transaction's status.
  *
  * Each Kafka server instantiates a transaction coordinator which is responsible for a set of
  * producers. Producers with specific appIDs are assigned to their corresponding coordinators;
  * Producers with no specific appIDs may talk to a random broker as their coordinators.
  */
class TransactionCoordinator(val brokerId: Int,
                             val pIDManager: PIDManager) extends Logging {

  this.logIdent = "[Transaction Coordinator " + brokerId + "]: "

  type InitPIDCallback = InitPIDResult => Unit

  private val pIDMetadataCache = new Pool[String, PIDMetadata]

  def handleInitPID(appID: String,
                    responseCallback: InitPIDCallback): Unit = {
    if (appID.equals("")) {
      // if the appID is not specified, then always blindly accept the request
      // and return a new PID from the PID manager
      val pID: Long = pIDManager.getNewPID()
      responseCallback(InitPIDResult(pID, -1 /* epoch */, Errors.NONE.code()))
    } else {

    }
  }

  private
}


case class InitPIDResult(pID: Long, epoch: Short, errorCode: Short)
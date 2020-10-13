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
package kafka.server

import java.io.IOException
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledFuture, TimeUnit}

import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.BrokerHeartbeatManagerImpl.SchedulerTaskPollTime
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.BrokerHeartbeatRequestData
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{FeatureCollection, ListenerCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerHeartbeatRequest, BrokerHeartbeatResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.BrokerState

import scala.concurrent.Promise

/**
 * Schedules heartbeats from the broker to the active controller.
 * Explicit broker state transitions are performed by co-ordinating
 * with the controller through the heartbeats.
 *
 */
trait BrokerHeartbeatManager {

  // Initiate broker registration and start the heartbeat scheduler loop
  def start(listeners: ListenerCollection, features: FeatureCollection): Unit

  // Enqueue a heartbeat request to be sent to the active controller
  // Specify the target state for the broker
  // - For the periodic heartbeat, this is always the current state of the controller
  // Note: This does not enforce any specific order of the state transitions. The target
  //       state is sent out as enqueued.
  // Returns a promise that indicates success if completed w/o exception
  def enqueue(state: BrokerState): Promise[Unit]

  // Current broker state
  def brokerState: BrokerState

  // Last successful heartbeat time
  def lastSuccessfulHeartbeatTime: Long

  // Stop the scheduler loop
  def stop(): Unit
}

/**
 * Implements the BrokerHeartbeatManager trait. Uses a concurrent queue to process state changes/notifications.
 * Also responsible for maintaining the broker state based on the response from the controller.
 *
 * @param controllerChannelManager
 *        - Channel to interact with the active controller
 * @param brokerID
 *        - This broker's ID
 * @param rack
 *        - The rack the broker is hosted on
 * @param metadataOffset
 *        - The last committed/processed metadata offset for this broker
 * @param brokerEpoch
 *        - This broker's current epoch
 */
class BrokerHeartbeatManagerImpl(val controllerChannelManager: BrokerToControllerChannelManager,
                                 val scheduler: Scheduler,
                                 val time: Time,
                                 val brokerID: Int,
                                 val rack: String,
                                 val metadataOffset: () => Long,
                                 val brokerEpoch: () => Long) extends BrokerHeartbeatManager with Logging with KafkaMetricsGroup {

  // TODO: Dedicated Heartbeat scheduler thread?
  // private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "broker-heartbeat")

  // Request queue
  private val requestQueue: util.Queue[(BrokerState, Promise[Unit])] = new ConcurrentLinkedQueue[(BrokerState, Promise[Unit])]()

  // Scheduler task
  private var schedulerTask: Option[ScheduledFuture[_]] = None

  // Broker states - Current and Target/Next
  private var currentState: BrokerState = _
  private val pendingStateChange = new AtomicBoolean(false)

  // Metrics - Histogram of broker heartbeat request/response time
  // FIXME: Tags
  private val heartbeatResponseTime = newHistogram(
    name = "BrokerHeartbeatResponseTimeMs",
    biased = true,
    Map("request" -> "BrokerHeartBeat")
  )
  private var _lastSuccessfulHeartbeatTime: Long = 0 // milliseconds

  override def start(listeners: ListenerCollection, features: FeatureCollection): Unit = {
    // TODO: Handle broker registration?
    currentState = BrokerState.NOT_RUNNING
    // TODO: Configurable schedule period
    schedulerTask = Some(scheduler.schedule(
      "send-broker-heartbeat",
      processHeartbeatRequests,
      SchedulerTaskPollTime,
      SchedulerTaskPollTime,
      TimeUnit.MILLISECONDS)
    )
  }

  override def enqueue(state: BrokerState): Promise[Unit] = {
    // TODO: Ignore requests if requested state is the same as the current state?
    val p = Promise[Unit]()
    requestQueue.add((state, p))
    p
  }

  override def stop(): Unit = {
    schedulerTask foreach {
      task => task.cancel(true)
    }
    requestQueue.clear()
  }

  private def processHeartbeatRequests(): Unit = {

    // Ensure that there are no outstanding state change requests
    // TODO: Do we want to allow some sort preemption to prioritize critical state changes?
    if (pendingStateChange.compareAndSet(false, true)) {
      // If any state transition was requested, get target state from the queue
      // Else schedule a regular heartbeat w/ targetState = currentState
      // TODO: Check for errors in the previous periodic heartbeat
      val state = if (requestQueue.isEmpty) (currentState, Promise[Unit]()) else requestQueue.poll
      sendHeartbeat(state)
    }
  }

  private def sendHeartbeat(requestState: (BrokerState, Promise[Unit])): Unit = {
    val sendTime = time.nanoseconds()

    // Construct broker heartbeat request
    def request: BrokerHeartbeatRequestData = {
      new BrokerHeartbeatRequestData()
        .setBrokerEpoch(brokerEpoch())
        .setBrokerId(brokerID)
        .setCurrentMetadataOffset(metadataOffset())
        .setCurrentState(currentState.value())
        .setTargetState(requestState._1.value())
    }

    def responseHandler(response: ClientResponse): Unit = {
      // Check for any transport errors
      if (response.authenticationException() != null) {
        requestState._2.tryFailure(response.authenticationException())
      } else if (response.versionMismatch() != null) {
        requestState._2.tryFailure(response.versionMismatch())
      } else if (response.wasDisconnected()) {
        requestState._2.tryFailure(new IOException("Client was disconnected"))
      } else if (!response.hasResponse) {
        requestState._2.tryFailure(new IOException("No response found"))
      } else {
        // Update metrics
        heartbeatResponseTime.update(time.nanoseconds() - sendTime)

        // Extract API response
        val body = response.responseBody().asInstanceOf[BrokerHeartbeatResponse]
        handleBrokerHeartbeatResponse(body) match {
          case None => requestState._2.trySuccess(())
          case Some(errorMsg) => requestState._2.tryFailure(new KafkaException(errorMsg.toString))
        }
        pendingStateChange.compareAndSet(true, false)
      }
    }

    debug(s"Sending BrokerHeartbeatRequest to controller $request")
    controllerChannelManager.sendRequest(new BrokerHeartbeatRequest.Builder(request), responseHandler)
  }

  private def handleBrokerHeartbeatResponse(response: BrokerHeartbeatResponse): Option[Errors] = {
    if (response.data().errorCode() != 0) {
      // TODO: Maintain last successful heartbeat time and FENCE broker if > registration.lease.timeout.ms
      val errorMsg = Errors.forCode(response.data().errorCode())
      error(s"Broker heartbeat failure: $errorMsg")
      Some(errorMsg)
    } else {
      currentState = BrokerState.fromValue(response.data().nextState())
      _lastSuccessfulHeartbeatTime = time.milliseconds
      None
    }
  }

  override def brokerState: BrokerState = currentState

  // In milliseconds
  override def lastSuccessfulHeartbeatTime: Long = _lastSuccessfulHeartbeatTime
}

object BrokerHeartbeatManagerImpl {
  // Minimum possible time for heartbeats
  val SchedulerTaskPollTime = 50 // ms
}
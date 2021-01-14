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
package kafka.server

import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection
import org.apache.kafka.common.message.{BrokerHeartbeatRequestData, BrokerRegistrationRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.common.utils.EventQueue.DeadlineFunction
import org.apache.kafka.common.utils.{EventQueue, ExponentialBackoff, KafkaEventQueue, LogContext, Time}
import org.apache.kafka.metadata.{BrokerState, VersionRange}

import java.util
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._

class BrokerLifecycleManager(val config: KafkaConfig,
                             val time: Time,
                             val threadNamePrefix: Option[String]) extends Logging {
  val logContext = new LogContext(s"[BrokerLifecycleManager id=${config.brokerId}] ")

  this.logIdent = logContext.logPrefix()

  /**
   * The broker id.
   */
  private val brokerId = config.brokerId

  /**
   * The broker rack, or null if there is no configured rack.
   */
  private val rack = config.rack.orNull

  /**
   * How long to wait for registration to succeed before failing the startup process.
   */
  private val initialTimeoutNs = NANOSECONDS.
    convert(config.initialRegistrationTimeoutMs.longValue(), TimeUnit.MILLISECONDS)

  /**
   * The exponential backoff to use for resending communication.
   */
  private val resendExponentialBackoff =
    new ExponentialBackoff(100, 2, config.brokerSessionTimeoutMs.toLong, 0.1)

  /**
   * The number of tries we've tried to communicate.
   */
  private var failedAttempts = 0L

  /**
   * The broker incarnation ID.  This ID uniquely identifies each time we start the broker
   */
  val incarnationId = Uuid.randomUuid()

  /**
   * A future which is completed just as soon as the broker has caught up with the latest
   * metadata offset for the first time.
   */
  val initialCatchUpFuture = new CompletableFuture[Void]()

  /**
   * A future which is completed when the controller approves our shutdown request
   */
  val shutdownReadinessFuture = new CompletableFuture[Void]()

  /**
   * The broker epoch, or -1 if the broker has not yet registered.
   * This variable can only be written from the event queue thread.
   */
  @volatile private var _brokerEpoch = -1L

  /**
   * The current broker state.
   * This variable can only be written from the event queue thread.
   */
  @volatile private var _state = BrokerState.NOT_RUNNING

  /**
   * A callback function which gives this manager the current highest metadata offset.
   * This function must be thread-safe.
   */
  private var _highestMetadataOffsetProvider: () => Long = null

  /**
   * True only if we are ready to unfence the broker.
   * This variable can only be accessed from the event queue thread.
   */
  private var readyToUnfence = false

  /**
   * True if the broker wants to initiate a controlled shutdown.
   * This variable can only be accessed from the event queue thread.
   */
  private var startShutdown = false

  /**
   * Whether or not we this broker is registered with the controller quorum.
   * This variable can only be accessed from the event queue thread.
   */
  private var registered = false

  /**
   * True if the initial registration succeeded.
   * This variable can only be accessed from the event queue thread.
   */
  private var initialRegistrationSucceeded = false

  /**
   * The cluster ID, or null if this manager has not been started yet.
   * This variable can only be accessed from the event queue thread.
   */
  private var _clusterId: Uuid = null

  /**
   * The listeners which this broker advertises.
   */
  private var _advertisedListeners: ListenerCollection = null

  /**
   * The features supported by this broker.
   */
  private var _supportedFeatures: util.Map[String, VersionRange] = null

  /**
   * The channel manager, or null if this manager has not been started yet.
   * This variable can only be accessed from the event queue thread.
   */
  var _channelManager: BrokerToControllerChannelManager = null

  /**
   * The event queue.
   */
  val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  /**
   * Start the BrokerLifecycleManager.
   *
   * @param highestMetadataOffsetProvider Provides the current highest metadata offset.
   * @param channelManager                The brokerToControllerChannelManager to use.
   * @param clusterId                     The cluster ID.
   */
  def start(highestMetadataOffsetProvider: () => Long,
            channelManager: BrokerToControllerChannelManager,
            clusterId: Uuid,
            advertisedListeners: ListenerCollection,
            supportedFeatures: util.Map[String, VersionRange]): Unit = {
    eventQueue.append(new StartupEvent(highestMetadataOffsetProvider,
      channelManager, clusterId, advertisedListeners, supportedFeatures))
  }

  def setReadyToUnfence(): Unit = {
    eventQueue.append(new SetReadyToUnfenceEvent())
  }

  def brokerEpoch(): Long = _brokerEpoch

  def state(): BrokerState = _state

  def controlledBrokerShutdown(): Unit = {
    _state = BrokerState.PENDING_CONTROLLED_SHUTDOWN
    readyToUnfence = false
    startShutdown = true
    scheduleNextCommunicationImmediately()
  }

  /**
   * Start shutting down the BrokerLifecycleManager, but do not block.
   */
  def beginShutdown(): Unit = {
    // Shutdown event queue here
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  /**
   * Shut down the BrokerLifecycleManager and block until all threads are joined.
   */
  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }

  class SetReadyToUnfenceEvent() extends EventQueue.Event {
    override def run(): Unit = {
      readyToUnfence = true
      scheduleNextCommunicationImmediately()
    }
  }

  class StartupEvent(highestMetadataOffsetProvider: () => Long,
                     channelManager: BrokerToControllerChannelManager,
                     clusterId: Uuid,
                     advertisedListeners: ListenerCollection,
                     supportedFeatures: util.Map[String, VersionRange]) extends EventQueue.Event {
    override def run(): Unit = {
      _highestMetadataOffsetProvider = highestMetadataOffsetProvider
      _channelManager = channelManager
      _channelManager.start()
      _state = BrokerState.STARTING
      _clusterId = clusterId
      _advertisedListeners = advertisedListeners
      _supportedFeatures = supportedFeatures
      eventQueue.scheduleDeferred("initialRegistrationTimeout",
        new DeadlineFunction(time.nanoseconds() + initialTimeoutNs),
        new RegistrationTimeoutEvent())
      sendBrokerRegistration()
      info(s"Incarnation ${incarnationId} of broker ${brokerId} in cluster ${clusterId} " +
        "is now STARTING.")
    }
  }

  private def sendBrokerRegistration(): Unit = {
    val features = new BrokerRegistrationRequestData.FeatureCollection()
    _supportedFeatures.asScala.foreach {
      case (name, range) => features.add(new BrokerRegistrationRequestData.Feature().
        setName(name).
        setMinSupportedVersion(range.min()).
        setMaxSupportedVersion(range.max()))
    }
    val data = new BrokerRegistrationRequestData().
        setBrokerId(brokerId).
        setClusterId(_clusterId).
        setFeatures(features).
        setIncarnationId(incarnationId).
        setListeners(_advertisedListeners).
        setRack(rack)
    _channelManager.sendRequest(new BrokerRegistrationRequest.Builder(data),
      new BrokerRegistrationResponseHandler())
  }

  class BrokerRegistrationResponseHandler extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      if (response.authenticationException() != null) {
        error(s"Unable to register broker ${brokerId} because of an authentication exception.",
          response.authenticationException());
        scheduleNextCommunicationAfterFailure()
      } else if (response.versionMismatch() != null) {
        error(s"Unable to register broker ${brokerId} because of an API version problem.",
          response.versionMismatch());
        scheduleNextCommunicationAfterFailure()
      } else if (response.responseBody() == null) {
        warn(s"Unable to register broker ${brokerId}.")
        scheduleNextCommunicationAfterFailure()
      } else if (!response.responseBody().isInstanceOf[BrokerRegistrationResponse]) {
        error(s"Unable to register broker ${brokerId} because the controller returned an " +
          "invalid response type.")
        scheduleNextCommunicationAfterFailure()
      } else {
        val message = response.responseBody().asInstanceOf[BrokerRegistrationResponse]
        val errorCode = Errors.forCode(message.data().errorCode())
        if (errorCode == Errors.NONE) {
          failedAttempts = 0
          _brokerEpoch = message.data().brokerEpoch()
          registered = true
          initialRegistrationSucceeded = true
          info(s"Successfully registered broker ${brokerId} with broker epoch ${_brokerEpoch}")
          scheduleNextCommunicationImmediately() // Immediately send a heartbeat
        } else {
          info(s"Unable to register broker ${brokerId} because the controller returned " +
            s"error ${errorCode}")
          scheduleNextCommunicationAfterFailure()
        }
      }
    }
  }

  private def sendBrokerHeartbeat(): Unit = {
    val metadataOffset = _highestMetadataOffsetProvider()
    val data = new BrokerHeartbeatRequestData().
      setBrokerEpoch(_brokerEpoch).
      setBrokerId(brokerId).
      setCurrentMetadataOffset(metadataOffset).
      setShouldFence(!readyToUnfence).
      setShouldShutdown(startShutdown)
    _channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data),
      new BrokerHeartbeatResponseHandler())
  }

  class BrokerHeartbeatResponseHandler extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      if (response.authenticationException() != null) {
        error(s"Unable to send broker heartbeat for ${brokerId} because of an " +
          "authentication exception.", response.authenticationException());
        scheduleNextCommunicationAfterFailure()
      } else if (response.versionMismatch() != null) {
        error(s"Unable to send broker heartbeat for ${brokerId} because of an API " +
          "version problem.", response.versionMismatch());
        scheduleNextCommunicationAfterFailure()
      } else if (response.responseBody() == null) {
        warn(s"Unable to send broker heartbeat for ${brokerId}. Retrying.")
        scheduleNextCommunicationAfterFailure()
      } else if (!response.responseBody().isInstanceOf[BrokerHeartbeatResponse]) {
        error(s"Unable to send broker heartbeat for ${brokerId} because the controller " +
          "returned an invalid response type.")
        scheduleNextCommunicationAfterFailure()
      } else {
        val message = response.responseBody().asInstanceOf[BrokerHeartbeatResponse]
        val errorCode = Errors.forCode(message.data().errorCode())
        if (errorCode == Errors.NONE) {
          failedAttempts = 0
          if (_state == BrokerState.STARTING) {
            if (message.data().isCaughtUp()) {
              info(s"Broker ${brokerId} has caught up. Transitioning to RECOVERY state.")
              _state = BrokerState.RECOVERY
              initialCatchUpFuture.complete(null)
            } else {
              info(s"Broker ${brokerId} is still waiting to catch up.")
            }
            scheduleNextCommunicationAfterSuccess()
          } else if (_state == BrokerState.RECOVERY) {
            if (!message.data().isFenced()) {
              info(s"Broker ${brokerId} has been unfenced. Transitioning to RUNNING state.")
              _state = BrokerState.RUNNING
            } else {
              info(s"Broker ${brokerId} is still waiting to be unfenced.")
            }
            scheduleNextCommunicationAfterSuccess()
          } else if (_state == BrokerState.RUNNING) {
            debug(s"Broker ${brokerId} processed heartbeat response from RUNNING state.")
            scheduleNextCommunicationAfterSuccess()
          } else if (_state == BrokerState.PENDING_CONTROLLED_SHUTDOWN) {
            debug(s"Broker ${brokerId} is waiting to shutdown")
            if (message.data.shouldShutdown) {
              info(s"Broker ${brokerId} is ready to shutdown")
              shutdownReadinessFuture.complete(null)
            } else {
              scheduleNextCommunicationAfterSuccess()
            }
          } else if (_state == BrokerState.SHUTTING_DOWN) {
            info(s"Broker ${brokerId} is ignoring the heartbeat response since it is " +
              "SHUTTING_DOWN.")
          } else {
            error(s"Unexpected broker state ${_state}")
            scheduleNextCommunicationAfterSuccess()
          }
        } else {
          warn(s"Broker ${brokerId} sent a heartbeat request but received error ${errorCode}.")
          scheduleNextCommunicationAfterFailure()
        }
      }
    }
  }

  private def scheduleNextCommunicationImmediately(): Unit = scheduleNextCommunication(0)

  private def scheduleNextCommunicationAfterFailure(): Unit = {
    val delayMs = resendExponentialBackoff.backoff(failedAttempts)
    failedAttempts = failedAttempts + 1
    scheduleNextCommunication(NANOSECONDS.convert(delayMs, MILLISECONDS))
  }

  private def scheduleNextCommunicationAfterSuccess(): Unit = {
    scheduleNextCommunication(NANOSECONDS.convert(
      config.brokerHeartbeatIntervalMs.longValue() , MILLISECONDS))
  }

  private def scheduleNextCommunication(intervalNs: Long): Unit = {
    trace(s"Scheduling next communication at ${MILLISECONDS.convert(intervalNs, NANOSECONDS)} " +
      "ms from now.")
    val deadlineNs = time.nanoseconds() + intervalNs
    eventQueue.scheduleDeferred("communication",
      new DeadlineFunction(deadlineNs),
      new CommunicationEvent())
  }

  class RegistrationTimeoutEvent extends EventQueue.Event {
    override def run(): Unit = {
      if (!initialRegistrationSucceeded) {
        error("Shutting down because we were unable to register with the controller quorum.")
        eventQueue.beginShutdown("registrationTimeout", new ShutdownEvent())
      }
    }
  }

  class CommunicationEvent extends EventQueue.Event {
    override def run(): Unit = {
      if (registered) {
        sendBrokerHeartbeat()
      } else {
        sendBrokerRegistration()
      }
    }
  }

  class ShutdownEvent extends EventQueue.Event {
    override def run(): Unit = {
      initialCatchUpFuture.cancel(false)
      _state = BrokerState.SHUTTING_DOWN
      _channelManager.shutdown()
    }
  }
}

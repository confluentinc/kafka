package unit.kafka.server

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import kafka.common.KafkaException
import kafka.server.{BrokerLifecycleManagerImpl, BrokerToControllerChannelManager, Defaults, KafkaConfig}
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.clients.{ClientResponse, RequestCompletionHandler}
import org.apache.kafka.common.message.BrokerHeartbeatResponseData
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{FeatureCollection, ListenerCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerHeartbeatRequest, BrokerHeartbeatResponse}
import org.apache.kafka.metadata.BrokerState
import org.easymock.{EasyMock, IAnswer}
import org.junit.{Before, Test}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Promise
import scala.util.{Failure, Random, Success}

class BrokerLifecycleManagerTest {

  val MaxNetworkDelay = 100 // milliseconds
  val MaxConditionWaitTime = 10_000 // milliseconds

  // We want to ensure we don't simulate any network delay longer than the maximum test wait time
  assert(MaxNetworkDelay + Defaults.RegistrationHeartbeatIntervalMs <= MaxConditionWaitTime)

  val time = new MockTime
  val brokerID = 99
  val rack = "rack-1"
  val random = new Random(System.currentTimeMillis())
  val configProperties: Properties = TestUtils.createBrokerConfig(brokerID, TestUtils.MockZkConnect)
  var config: KafkaConfig = _

  var brokerToControllerChannel: BrokerToControllerChannelManager = _
  @Before
  def setup(): Unit = {
    brokerToControllerChannel = EasyMock.createMock(classOf[BrokerToControllerChannelManager])

    // Seed config w/ defaults for the relevant heartbeat timeouts
    configProperties.setProperty(KafkaConfig.RegistrationHeartbeatIntervalMsProp, Defaults.RegistrationHeartbeatIntervalMs.toString)
    configProperties.setProperty(KafkaConfig.RegistrationLeaseTimeoutMsProp, Defaults.RegistrationLeaseTimeoutMs.toString)

    config = KafkaConfig.fromProps(configProperties)
  }

  private def simulateNetworkDelay(delay: Int = MaxNetworkDelay): Unit = {
    // Sleep for at least config.registrationHeartbeatIntervalMs
    time.sleep(config.registrationHeartbeatIntervalMs + random.nextInt(delay))
  }

  private def waitForPromise(promise: Promise[Unit], maxWaitTime: Int = MaxConditionWaitTime) : Unit = {
    // Wait for promise to be completed -> The "network" thread simulates a network delay
    for (_ <- 0 to maxWaitTime by config.registrationHeartbeatIntervalMs) {
      time.sleep(config.registrationHeartbeatIntervalMs.longValue)
      if (promise.isCompleted) return
    }

    // Fail the promise
    promise.tryFailure(new Error("Promise failed to complete in time"))
  }

  /**
   * Enqueue a single state change request
   *
   */
  @Test
  def testSingleStateChange(): Unit = {
    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    val pendingStateChanges = Set(BrokerState.REGISTERING)
    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          simulateNetworkDelay()
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).once()
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    var stateChangePromise: Promise[Unit] = null
    pendingStateChanges foreach {
      state => stateChangePromise = brokerLifecycleManager.enqueue(state)
    }

    // Wait for promise to be completed -> The "network" thread simulates a network delay
    // Tick time to process state change while we wait
    waitForPromise(stateChangePromise)
    assert(stateChangePromise.future.value.get.isSuccess)
    assert(brokerLifecycleManager.brokerState == BrokerState.REGISTERING)
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Enqueue multiple state change requests
   */
  @Test
  def testMultipleStateChanges(): Unit = {
    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    val pendingStateChanges = Set(BrokerState.REGISTERING, BrokerState.FENCED)
    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          simulateNetworkDelay()
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
      // Due to the way Mock time and EasyMock works, time is advanced serially while simulating network delay
      // This leads to the periodic heartbeats being scheduled in addition to the state change heartbeats being
      // tested here. To account for the excess calls, this test expects the mocked function to be called AT LEAST
      // as many as the number of state changes being tested
    ).times(pendingStateChanges.size, Int.MaxValue)
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )

    // Start
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerLifecycleManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Test periodic heartbeats when no state changes are enqueued
   */
  @Test
  def testPeriodicHeartbeats(): Unit = {
    // Number of periodic heartbeats expected
    val numPeriodicHeartbeatsExpected = 10
    val pendingStateChanges = Set(BrokerState.REGISTERING, BrokerState.FENCED)

    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          simulateNetworkDelay()
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).times(pendingStateChanges.size + numPeriodicHeartbeatsExpected, Int.MaxValue)
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerLifecycleManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }

    for (_ <- 1 to numPeriodicHeartbeatsExpected) {
      time.sleep(config.registrationHeartbeatIntervalMs.longValue())
    }
    // Verify that the current broker state is the last requested target state
    assert(brokerLifecycleManager.brokerState == BrokerState.FENCED)
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Enqueue state changes in parallel
   *
   */
  @Test
  def testParallelStateChangeRequests(): Unit = {
    // Cache state changes
    val pendingStateChanges = Set(
      BrokerState.REGISTERING,
      BrokerState.FENCED,
      BrokerState.RECOVERING_FROM_UNCLEAN_SHUTDOWN,
      BrokerState.FENCED,
      BrokerState.RUNNING,
      BrokerState.SHUTTING_DOWN
    )

    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          simulateNetworkDelay()
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).times(pendingStateChanges.size, Int.MaxValue)
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Schedule enqueues asynchronously
    val executor = Executors.newFixedThreadPool(2)
    pendingStateChanges foreach {
      state => executor.submit(
        (() => {
          brokerLifecycleManager.enqueue(state)
        }): Runnable
      )
    }

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerLifecycleManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }

    // Verify that the state actually changed (from the default NOT_RUNNING)
    assert(brokerLifecycleManager.brokerState != BrokerState.NOT_RUNNING)
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Simulate an error during state change
   *
   */
  @Test
  def testStateChangeError(): Unit = {
    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    val pendingStateChanges = Set(BrokerState.REGISTERING)

    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val response = new BrokerHeartbeatResponseData()
            .setNextState(BrokerState.UNKNOWN.value())
            .setErrorCode(Errors.NOT_CONTROLLER.code())
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          simulateNetworkDelay()
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).atLeastOnce()
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerLifecycleManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isFailure)
        stateChangePromise.future.value.get match {
          case Success(_) => assert(assertion = false, "This promise should not have succeeded. Expected error: " + Errors.NOT_CONTROLLER)
          case Failure(exception) => assert(exception.isInstanceOf[KafkaException])
        }

    }

    assert(brokerLifecycleManager.brokerState != BrokerState.UNKNOWN)
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime == 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Test heartbeat timeouts
   * - Attempt broker registration
   * - Wait for 1 periodic heartbeat to succeed
   * - Submit a state change request and simulate an error response after a
   *   network delay (longer than the heartbeat interval)
   * - Verify current time - lastSuccessfulHeartbeat > heartbeat interval
   * - Wait for a periodic heartbeat to be sent out
   * - Verify current time - lastSuccessfulHeartbeat < heartbeat interval
   *
   */
  @Test
  def testHeartbeatTimeout(): Unit = {
    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    var networkDelay = false
    var failChange = false

    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          if (networkDelay) {
            simulateNetworkDelay(config.registrationHeartbeatIntervalMs)
          }
          if (failChange) {
            response.setErrorCode(Errors.NOT_CONTROLLER.code())
          }
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).atLeastOnce()
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Register broker
    var promise = brokerLifecycleManager.enqueue(BrokerState.REGISTERING)
    time.sleep(config.registrationHeartbeatIntervalMs.longValue())
    assert(promise.future.value.get.isSuccess)

    // Wait for a heartbeat to be scheduled
    time.sleep(config.registrationHeartbeatIntervalMs.longValue())
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Delay and error out state change
    networkDelay = true
    failChange = true
    promise = brokerLifecycleManager.enqueue(BrokerState.RUNNING)
    time.sleep(config.registrationHeartbeatIntervalMs.longValue())
    assert(promise.future.value.get.isFailure)
    assert(
      TimeUnit.NANOSECONDS.toMillis(time.nanoseconds - brokerLifecycleManager.lastSuccessfulHeartbeatTime) > config.registrationHeartbeatIntervalMs
    )

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  /**
   * Test Registration lease timeout
   * - Attempt broker registration, fencing and activation
   * - Wait for 1 periodic heartbeat to succeed
   * - Submit a state change request and simulate an error response after a
   *   network delay (longer than the heartbeat interval)
   * - Verify current time - lastSuccessfulHeartbeat > heartbeat interval
   * - Verify current state is still the last target state
   * - Wait for a periodic heartbeat to be sent out that errors out after a
   *   network delay (longer than the registration lease timeout)
   * - Verify current time - lastSuccessfulHeartbeat > registration lease timeout
   * - Verify current state is FENCED
   *
   */
  @Test
  def testRegistrationLeaseTimeout(): Unit = {
    // Setup mock
    val capturedRequest = EasyMock.newCapture[BrokerHeartbeatRequest.Builder]()
    val capturedResponseHandler = EasyMock.newCapture()
    val pendingStateChanges = Set(
      BrokerState.REGISTERING,
      BrokerState.FENCED,
      BrokerState.RUNNING
    )
    var networkDelay = false
    var failChange = false
    var networkDelayTime = 0

    EasyMock.expect(brokerToControllerChannel.sendRequest(EasyMock.capture(capturedRequest), EasyMock.capture(capturedResponseHandler))).andAnswer(
      new IAnswer[Unit]() {
        override def answer(): Unit = {
          val targetState = BrokerState.fromValue(capturedRequest.getValue.build().data().targetState())
          val response = new BrokerHeartbeatResponseData()
            .setNextState(targetState.value())
            .setErrorCode(0)
          val clientResponse = new ClientResponse(
            null, null, null,
            0, 0, false,
            null, null, new BrokerHeartbeatResponse(response))
          if (networkDelay) {
            simulateNetworkDelay(networkDelayTime)
          }
          if (failChange) {
            response.setErrorCode(Errors.NOT_CONTROLLER.code())
          }
          capturedResponseHandler.getValue.asInstanceOf[RequestCompletionHandler].onComplete(clientResponse)
        }
      }
    ).atLeastOnce()
    EasyMock.replay(brokerToControllerChannel)

    // Init BrokerLifecycleManager
    val brokerLifecycleManager = new BrokerLifecycleManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerLifecycleManager.start(new ListenerCollection(), new FeatureCollection())

    // Step 1 - Attempt broker registration, fencing and activation
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerLifecycleManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }

    // Step 2 - Wait for 1 periodic heartbeat to succeed
    time.sleep(config.registrationHeartbeatIntervalMs.longValue())
    assert(brokerLifecycleManager.lastSuccessfulHeartbeatTime != 0)

    // Step 3 - Submit a state change request and simulate a delayed error response
    networkDelay = true
    failChange = true
    networkDelayTime = config.registrationHeartbeatIntervalMs
    var promise = brokerLifecycleManager.enqueue(BrokerState.SHUTTING_DOWN)
    time.sleep(networkDelayTime)
    assert(promise.future.value.get.isFailure)
    assert(
      TimeUnit.NANOSECONDS.toMillis(time.nanoseconds - brokerLifecycleManager.lastSuccessfulHeartbeatTime) > config.registrationHeartbeatIntervalMs
    )
    assert(
      TimeUnit.NANOSECONDS.toMillis(time.nanoseconds - brokerLifecycleManager.lastSuccessfulHeartbeatTime) < config.registrationLeaseTimeoutMs
    )
    assert(brokerLifecycleManager.brokerState == BrokerState.RUNNING)

    // Step 4 - Wait for a periodic heartbeat to be sent out that errors out after registration lease timeout
    networkDelayTime = config.registrationLeaseTimeoutMs
    promise = brokerLifecycleManager.enqueue(BrokerState.SHUTTING_DOWN)
    time.sleep(networkDelayTime)
    assert(promise.future.value.get.isFailure)
    assert(
      TimeUnit.NANOSECONDS.toMillis(time.nanoseconds - brokerLifecycleManager.lastSuccessfulHeartbeatTime) > config.registrationLeaseTimeoutMs
    )
    assert(brokerLifecycleManager.brokerState == BrokerState.FENCED)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }

  // TODO: BrokerRegistrationTests
}

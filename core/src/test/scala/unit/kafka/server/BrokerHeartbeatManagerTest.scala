package unit.kafka.server

import java.util.Properties
import java.util.concurrent.Executors

import kafka.common.KafkaException
import kafka.server.{BrokerHeartbeatManagerImpl, BrokerToControllerChannelManager, Defaults, KafkaConfig}
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

class BrokerHeartbeatManagerTest {

  val MaxNetworkDelay = 100 // milliseconds
  val MaxConditionWaitTime = 10000 // milliseconds

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

    // Init BrokerHeartbeatManager
    val brokerHeartbeatManager = new BrokerHeartbeatManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerHeartbeatManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    var stateChangePromise: Promise[Unit] = null
    pendingStateChanges foreach {
      state => stateChangePromise = brokerHeartbeatManager.enqueue(state)
    }

    // Wait for promise to be completed -> The "network" thread simulates a network delay
    // Tick time to process state change while we wait
    waitForPromise(stateChangePromise)
    assert(stateChangePromise.future.value.get.isSuccess)
    assert(brokerHeartbeatManager.brokerState == BrokerState.REGISTERING)
    assert(brokerHeartbeatManager.lastSuccessfulHeartbeatTime != 0)

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

    // Init BrokerHeartbeatManager
    val brokerHeartbeatManager = new BrokerHeartbeatManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )

    // Start
    brokerHeartbeatManager.start(new ListenerCollection(), new FeatureCollection())

    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerHeartbeatManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }
    assert(brokerHeartbeatManager.lastSuccessfulHeartbeatTime != 0)

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

    // Init BrokerHeartbeatManager
    val brokerHeartbeatManager = new BrokerHeartbeatManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerHeartbeatManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerHeartbeatManager.enqueue(state)
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
    assert(brokerHeartbeatManager.brokerState == BrokerState.FENCED)
    assert(brokerHeartbeatManager.lastSuccessfulHeartbeatTime != 0)

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

    // Init BrokerHeartbeatManager
    val brokerHeartbeatManager = new BrokerHeartbeatManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerHeartbeatManager.start(new ListenerCollection(), new FeatureCollection())

    // Schedule enqueues asynchronously
    val executor = Executors.newFixedThreadPool(2)
    pendingStateChanges foreach {
      state => executor.submit(
        (() => {
          brokerHeartbeatManager.enqueue(state)
        }): Runnable
      )
    }

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerHeartbeatManager.enqueue(state)
    }

    // Verify
    pendingPromises foreach {
      stateChangePromise =>
        waitForPromise(stateChangePromise)
        assert(stateChangePromise.future.value.get.isSuccess)
    }

    // Verify that the state actually changed (from the default NOT_RUNNING)
    assert(brokerHeartbeatManager.brokerState != BrokerState.NOT_RUNNING)
    assert(brokerHeartbeatManager.lastSuccessfulHeartbeatTime != 0)

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

    // Init BrokerHeartbeatManager
    val brokerHeartbeatManager = new BrokerHeartbeatManagerImpl(
      config, brokerToControllerChannel, time.scheduler,
      time, brokerID, rack, () => 1337, () => 2020
    )
    brokerHeartbeatManager.start(new ListenerCollection(), new FeatureCollection())

    // Start
    val pendingPromises = ListBuffer[Promise[Unit]]()
    pendingStateChanges foreach {
      state => pendingPromises += brokerHeartbeatManager.enqueue(state)
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

    assert(brokerHeartbeatManager.brokerState != BrokerState.UNKNOWN)
    assert(brokerHeartbeatManager.lastSuccessfulHeartbeatTime == 0)

    // Verify that only a single request was processed per scheduler tick
    EasyMock.verify(brokerToControllerChannel)
  }
}

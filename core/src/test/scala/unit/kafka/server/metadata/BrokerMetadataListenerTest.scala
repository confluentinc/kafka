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

package kafka.server.metadata

import com.yammer.metrics.core.{Gauge, Histogram, MetricName}
import kafka.metrics.KafkaYammerMetrics
import kafka.server.{KafkaConfig, MetadataCache}
import kafka.utils.TestUtils
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.metadata.{FenceBrokerRecord, RegisterBrokerRecord}
import org.apache.kafka.common.protocol.{ApiMessage, ApiMessageAndVersion}
import org.apache.kafka.common.utils.{LogContext, MockTime, Utils}
import org.apache.kafka.metalog.{LocalLogManager, MetaLogLeader}
import org.apache.kafka.test.{TestCondition, TestUtils => KTestUtils}
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{After, Test}
import org.mockito.Mockito.{mock, times, verify}
import org.scalatest.Assertions.intercept

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class BrokerMetadataListenerTest {
  val expectedMetricMBeanPrefix = "kafka.server.metadata:type=BrokerMetadataListener"
  val expectedEventQueueTimeMsMetricName = "EventQueueTimeMs"
  val expectedEventQueueSizeMetricName = "EventQueueSize"
  val eventQueueSizeMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueSizeMetricName"
  val eventQueueTimeMsMetricMBeanName = s"$expectedMetricMBeanPrefix,name=$expectedEventQueueTimeMsMetricName"
  val metadataCache = mock(classOf[MetadataCache])

  def eventQueueSizeGauge() = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter {
    case (k, _) => k.getMBeanName == eventQueueSizeMetricMBeanName
  }.values.headOption.getOrElse(fail(s"Unable to find metric $eventQueueSizeMetricMBeanName")).asInstanceOf[Gauge[Int]]
  def queueTimeHistogram() = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter {
    case (k, _) => k.getMBeanName == eventQueueTimeMsMetricMBeanName
  }.values.headOption.getOrElse(fail(s"Unable to find metric $eventQueueTimeMsMetricMBeanName")).asInstanceOf[Histogram]
  def allRegisteredMetricNames: Set[MetricName] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.keySet
      .filter(_.getMBeanName.startsWith(expectedMetricMBeanPrefix))
      .toSet
  }

  val expectedInitialMetadataOffset = -1

  @After
  def clearMetrics(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testEmptyBrokerMetadataProcessors(): Unit = {
    new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List.empty)
  }

  @Test(expected = classOf[IllegalStateException])
  def testNoConfigMetadataProcessors(): Unit = {
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(mock(classOf[BrokerMetadataProcessor])))
    listener.brokerConfigProperties(1)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testMultipleConfigMetadataProcessors(): Unit = {
    trait ConfigRepositoryProcessor extends BrokerMetadataProcessor with ConfigRepository
    new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(),
      List(mock(classOf[ConfigRepositoryProcessor]), mock(classOf[ConfigRepositoryProcessor])))
  }

  @Test
  def testConfigMetadataProcessor(): Unit = {
    trait ConfigRepositoryProcessor extends BrokerMetadataProcessor with ConfigRepository
    val configProcessor = mock(classOf[ConfigRepositoryProcessor])
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(configProcessor))
    val brokerId = 0
    listener.brokerConfigProperties(brokerId)
    val topicName = "foo"
    listener.topicConfigProperties(topicName)
    verify(configProcessor, times(1)).configProperties(new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString))
    verify(configProcessor, times(1)).configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topicName))
  }

  @Test
  def testEventIsProcessedAfterStartup(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(processor))

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new RegisterBrokerRecord()).asJava, 1)
    listener.put(metadataLogEvent)
    listener.drain()
    assertEquals(List(metadataLogEvent), processor.processed.toList)
  }

  @Test
  def testInitialAndSubsequentMetadataOffsets(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val nextMetadataOffset = expectedInitialMetadataOffset + 2
    val msg0 = mock(classOf[ApiMessage])
    val msg1 = mock(classOf[ApiMessage])
    val apiMessages = List(msg0, msg1)
    val event = MetadataLogEvent(apiMessages.asJava, nextMetadataOffset)
    listener.put(event)
    listener.drain()
    assertEquals(List(event), processor.processed.toList)
    assertEquals(nextMetadataOffset, listener.currentMetadataOffset())
  }

  @Test
  def testOutOfBandHeartbeatMessages(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val brokerEpoch = 1
    val msg0 = FenceBrokerEvent(1, true)
    listener.put(msg0)
    listener.drain()
    assertEquals(List(msg0), processor.processed.toList)

    // offset should not be updated
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    processor.processed.clear()
    val msg1 = FenceBrokerEvent(1, false)
    listener.put(msg1)
    listener.drain()
    assertEquals(List(msg1), processor.processed.toList)

    // offset should not be updated
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())
    // but we should now be "caught up"

  }

  @Test
  def testBadMetadataOffset(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(processor))
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new RegisterBrokerRecord()).asJava, -1)
    listener.put(metadataLogEvent)

    intercept[IllegalStateException] {
      listener.drain()
    }

    // offset should be unchanged
    assertEquals(expectedInitialMetadataOffset, listener.currentMetadataOffset())

    // record should not be processed
    assertEquals(List.empty, processor.processed.toList)
  }

  @Test
  def testMetricsCleanedOnClose(): Unit = {
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(),
      List(new MockMetadataProcessor))
    listener.start()
    assertTrue(allRegisteredMetricNames.nonEmpty)

    listener.close()
    assertTrue(allRegisteredMetricNames.isEmpty)
  }

  @Test
  def testOutOfBandEventIsProcessed(): Unit = {
    val processor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime(), List(processor))
    val logEvent1 = MetadataLogEvent(List(mock(classOf[ApiMessage])).asJava, 1)
    val logEvent2 = MetadataLogEvent(List(mock(classOf[ApiMessage])).asJava, 2)
    val fenceEvent = FenceBrokerEvent(1, true)

    // add the out-of-band messages after the batches
    val expected = List(logEvent1, logEvent2, fenceEvent)
    expected.foreach { listener.put(_) }
    listener.drain()

    // make sure events are handled in order
    assertEquals(expected, processor.processed.toList)
  }

  @Test
  def testEventQueueTime(): Unit = {
    val time = new MockTime()
    val brokerMetadataProcessor = new MockMetadataProcessor

    // The metric should not already exist
    assertTrue(KafkaYammerMetrics.defaultRegistry.allMetrics.asScala.filter { case (k, _) =>
      k.getMBeanName == eventQueueTimeMsMetricMBeanName
    }.values.isEmpty)

    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, time, List(brokerMetadataProcessor))
    val apiMessagesEvent = List(mock(classOf[ApiMessage]))
    listener.put(MetadataLogEvent(apiMessagesEvent.asJava, 1))
    listener.drain()

    listener.put(MetadataLogEvent(apiMessagesEvent.asJava, 2))
    time.sleep(500)
    listener.drain()

    val histogram = queueTimeHistogram()
    assertEquals(2, histogram.count)
    assertEquals(0, histogram.min, 0.01)
    assertEquals(500, histogram.max, 0.01)
  }

  @Test
  def testEventQueueHistogramResetAfterTimeout(): Unit = {
    val time = new MockTime()
    val brokerMetadataProcessor = new MockMetadataProcessor

    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, time, List(brokerMetadataProcessor),
      eventQueueTimeoutMs = 50)
    val histogram = queueTimeHistogram()

    val metadataLogEvent = MetadataLogEvent(List[ApiMessage](new RegisterBrokerRecord()).asJava, 1)
    listener.put(metadataLogEvent)
    listener.drain()
    assertEquals(1, histogram.count())

    listener.poll()
    assertEquals(0, histogram.count())
  }

  @Test
  def testLocalLogManagerEventQueue(): Unit = {
    val logdir = KTestUtils.tempDirectory()
    val leaderEpoch = 0
    val apiVersion: Short = 1
    val brokerMetadataProcessor = new MockMetadataProcessor
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), metadataCache, new MockTime, List(brokerMetadataProcessor),
      eventQueueTimeoutMs = 50)
    val shared = new LocalLogManager.SharedLogData()
    val localLogManager = new LocalLogManager(new LogContext("log-manager-broker-test"),
      1, shared, "log-manager")
    localLogManager.initialize()
    localLogManager.register(listener)

    // Invoke dummy APIs
    val apisInvoked = mutable.ListBuffer[ApiMessageAndVersion]()
    apisInvoked += new ApiMessageAndVersion(new RegisterBrokerRecord, apiVersion)
    apisInvoked += new ApiMessageAndVersion(new FenceBrokerRecord, apiVersion)

    // Schedule writes to the metadata log
    var eventsScheduled = 0
    localLogManager.scheduleWrite(leaderEpoch, apisInvoked.asJava)
    eventsScheduled += 1

    // Wait for all events to be queued
    // FIXME: Jenkins flakiness w/ timeouts
    KTestUtils.waitForCondition(new TestCondition {
      override def conditionMet(): Boolean = {
        listener.pendingEvents == eventsScheduled
      }
    }, 10000, "Wait for all events to be queued")

    // Process all events
    listener.drain()

    // Cleanup
    localLogManager.beginShutdown()
    localLogManager.close()
    listener.beginShutdown()
    listener.close()
    Utils.delete(logdir)

    // Verify that the events were processed
    assertEquals(apisInvoked.size, listener.currentMetadataOffset())
  }

  @Test
  def testLocalLogManagerNewLeaderNotification(): Unit = {
    val logdir = KTestUtils.tempDirectory()
    val leaderEpoch = 0
    val brokerMetadataProcessor = new MockMetadataProcessor
    val brokerID = 1;

    // Use a real metadata cache
    val realMetadataCache = new MetadataCache(brokerID)
    val listener = new BrokerMetadataListener(mock(classOf[KafkaConfig]), realMetadataCache, new MockTime, List(brokerMetadataProcessor),
      eventQueueTimeoutMs = 50)

    // Controller ID change notification
    val newControllerID = 999;

    // Let BrokerMetadataListener know of the new controller ID
    listener.handleNewLeader(new MetaLogLeader(newControllerID, leaderEpoch + 1))

    // Cleanup
    listener.beginShutdown()
    listener.close()
    Utils.delete(logdir)

    // Verify that the leadership change was processed
    assertTrue(realMetadataCache.getControllerId.isDefined)
    assertEquals(realMetadataCache.getControllerId.get, newControllerID)
  }

  private class MockMetadataProcessor extends BrokerMetadataProcessor {
    val processed: mutable.Buffer[BrokerMetadataEvent] = mutable.Buffer.empty

    override def process(event: BrokerMetadataEvent): Unit = {
      processed += event
    }
  }
}

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

import kafka.server.ClientQuotaManager.{BaseUserEntity, KafkaQuotaEntity}
import org.apache.kafka.common.Cluster

import java.net.InetAddress
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.server.config.ClientQuotaManagerConfig
import org.apache.kafka.network.Session
import org.apache.kafka.server.quota.{ClientQuotaCallback, ClientQuotaEntity, ClientQuotaType, QuotaType}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.{Collections, Map, HashMap}

class ClientQuotaManagerTest extends BaseClientQuotaManagerTest {
  private val config = new ClientQuotaManagerConfig()

  private def testQuotaParsing(config: ClientQuotaManagerConfig, client1: UserClient, client2: UserClient, randomClient: UserClient, defaultConfigClient: UserClient): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "")

    try {
      // Case 1: Update the quota. Assert that the new quota value is returned
      clientQuotaManager.updateQuota(
        client1.configUser,
        client1.configClientEntity,
        Some(new Quota(2000, true))
      )
      clientQuotaManager.updateQuota(
        client2.configUser,
        client2.configClientEntity,
        Some(new Quota(4000, true))
      )

      assertEquals(Long.MaxValue.toDouble, clientQuotaManager.quota(randomClient.user, randomClient.clientId).bound, 0.0,
        "Default producer quota should be " + Long.MaxValue.toDouble)
      assertEquals(2000, clientQuotaManager.quota(client1.user, client1.clientId).bound, 0.0,
        "Should return the overridden value (2000)")
      assertEquals(4000, clientQuotaManager.quota(client2.user, client2.clientId).bound, 0.0,
        "Should return the overridden value (4000)")

      // p1 should be throttled using the overridden quota
      var throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 2500 * config.numQuotaSamples)
      assertTrue(throttleTimeMs > 0, s"throttleTimeMs should be > 0. was $throttleTimeMs")

      // Case 2: Change quota again. The quota should be updated within KafkaMetrics as well since the sensor was created.
      // p1 should not longer be throttled after the quota change
      clientQuotaManager.updateQuota(
        client1.configUser,
        client1.configClientEntity,
        Some(new Quota(3000, true))
      )
      assertEquals(3000, clientQuotaManager.quota(client1.user, client1.clientId).bound, 0.0, "Should return the newly overridden value (3000)")

      throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 0)
      assertEquals(0, throttleTimeMs, s"throttleTimeMs should be 0. was $throttleTimeMs")

      // Case 3: Change quota back to default. Should be throttled again
      clientQuotaManager.updateQuota(
        client1.configUser,
        client1.configClientEntity,
        Some(new Quota(500, true))
      )
      assertEquals(500, clientQuotaManager.quota(client1.user, client1.clientId).bound, 0.0, "Should return the default value (500)")

      throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 0)
      assertTrue(throttleTimeMs > 0, s"throttleTimeMs should be > 0. was $throttleTimeMs")

      // Case 4: Set high default quota, remove p1 quota. p1 should no longer be throttled
      clientQuotaManager.updateQuota(
        client1.configUser,
        client1.configClientEntity,
        None
      )
      clientQuotaManager.updateQuota(
        defaultConfigClient.configUser,
        defaultConfigClient.configClientEntity,
        Some(new Quota(4000, true))
      )
      assertEquals(4000, clientQuotaManager.quota(client1.user, client1.clientId).bound, 0.0, "Should return the newly overridden value (4000)")

      throttleTimeMs = maybeRecord(clientQuotaManager, client1.user, client1.clientId, 1000 * config.numQuotaSamples)
      assertEquals(0, throttleTimeMs, s"throttleTimeMs should be 0. was $throttleTimeMs")

    } finally {
      clientQuotaManager.shutdown()
    }
  }

  /**
   * Tests parsing for <user> quotas when client-id default quota properties are set.
   */
  @Test
  def testUserQuotaParsingWithDefaultClientIdQuota(): Unit = {
    val client1 = UserClient("User1", "p1", Some(ClientQuotaManager.UserEntity("User1")), None)
    val client2 = UserClient("User2", "p2", Some(ClientQuotaManager.UserEntity("User2")), None)
    val randomClient = UserClient("RandomUser", "random-client-id", None, None)
    val defaultConfigClient = UserClient("", "", Some(ClientQuotaManager.DefaultUserEntity), None)
    testQuotaParsing(config, client1, client2, randomClient, defaultConfigClient)
  }

  private def checkQuota(quotaManager: ClientQuotaManager, user: String, clientId: String, expectedBound: Long, value: Int, expectThrottle: Boolean): Unit = {
    assertEquals(expectedBound.toDouble, quotaManager.quota(user, clientId).bound, 0.0)
    val session = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, user), InetAddress.getLocalHost)
    val expectedMaxValueInQuotaWindow =
      if (expectedBound < Long.MaxValue) config.quotaWindowSizeSeconds * (config.numQuotaSamples - 1) * expectedBound.toDouble
      else Double.MaxValue
    assertEquals(expectedMaxValueInQuotaWindow, quotaManager.getMaxValueInQuotaWindow(session, clientId), 0.01)

    val throttleTimeMs = maybeRecord(quotaManager, user, clientId, value * config.numQuotaSamples)
    if (expectThrottle)
      assertTrue(throttleTimeMs > 0, s"throttleTimeMs should be > 0. was $throttleTimeMs")
    else
      assertEquals(0, throttleTimeMs, s"throttleTimeMs should be 0. was $throttleTimeMs")
  }

  @Test
  def testGetMaxValueInQuotaWindowWithNonDefaultQuotaWindow(): Unit = {
    val numFullQuotaWindows = 3   // 3 seconds window (vs. 10 seconds default)
    val nonDefaultConfig = new ClientQuotaManagerConfig(numFullQuotaWindows + 1)
    val clientQuotaManager = new ClientQuotaManager(nonDefaultConfig, metrics, QuotaType.FETCH, time, "")
    val userSession = new Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "userA"), InetAddress.getLocalHost)

    try {
      // no quota set
      assertEquals(Double.MaxValue, clientQuotaManager.getMaxValueInQuotaWindow(userSession, "client1"), 0.01)

      // Set default <user> quota config
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity), 
        None, 
        Some(new Quota(10, true))
      )
      assertEquals(10 * numFullQuotaWindows, clientQuotaManager.getMaxValueInQuotaWindow(userSession, "client1"), 0.01)
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testSetAndRemoveDefaultUserQuota(): Unit = {
    // quotaTypesEnabled will be QuotaTypes.NoQuotas initially
    val clientQuotaManager = new ClientQuotaManager(new ClientQuotaManagerConfig(),
      metrics, QuotaType.PRODUCE, time, "")

    try {
      // no quota set yet, should not throttle
      checkQuota(clientQuotaManager, "userA", "client1", Long.MaxValue, 1000, expectThrottle = false)

      // Set default <user> quota config
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity),
        None, 
        Some(new Quota(10, true))
      )
      checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, expectThrottle = true)

      // Remove default <user> quota config, back to no quotas
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity),
        None, 
        None
      )
      checkQuota(clientQuotaManager, "userA", "client1", Long.MaxValue, 1000, expectThrottle = false)
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testSetAndRemoveUserQuota(): Unit = {
    // quotaTypesEnabled will be QuotaTypes.NoQuotas initially
    val clientQuotaManager = new ClientQuotaManager(new ClientQuotaManagerConfig(),
      metrics, QuotaType.PRODUCE, time, "")

    try {
      // Set <user> quota config
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")), 
        None, 
        Some(new Quota(10, true))
      )
      checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, expectThrottle = true)

      // Remove <user> quota config, back to no quotas
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")), 
        None, 
        None
      )
      checkQuota(clientQuotaManager, "userA", "client1", Long.MaxValue, 1000, expectThrottle = false)
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testSetAndRemoveUserClientQuota(): Unit = {
    // quotaTypesEnabled will be QuotaTypes.NoQuotas initially
    val clientQuotaManager = new ClientQuotaManager(new ClientQuotaManagerConfig(),
      metrics, QuotaType.PRODUCE, time, "")

    try {
      // Set <user, client-id> quota config
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client1")), 
        Some(new Quota(10, true))
      )
      checkQuota(clientQuotaManager, "userA", "client1", 10, 1000, expectThrottle = true)

      // Remove <user, client-id> quota config, back to no quotas
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client1")), 
        None
      )
      checkQuota(clientQuotaManager, "userA", "client1", Long.MaxValue, 1000, expectThrottle = false)
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testQuotaConfigPrecedence(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(new ClientQuotaManagerConfig(),
      metrics, QuotaType.PRODUCE, time, "")

    try {
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity), 
        None, 
        Some(new Quota(1000, true))
      )
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(2000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity),
        Some(ClientQuotaManager.DefaultClientIdEntity), 
        Some(new Quota(3000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")), 
        None, 
        Some(new Quota(4000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")), 
        Some(ClientQuotaManager.ClientIdEntity("client1")),
        Some(new Quota(5000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userB")), 
        None, 
        Some(new Quota(6000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userB")),
        Some(ClientQuotaManager.ClientIdEntity("client1")),
        Some(new Quota(7000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userB")),
        Some(ClientQuotaManager.DefaultClientIdEntity), 
        Some(new Quota(8000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userC")), 
        None, 
        Some(new Quota(10000, true))
      )
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.ClientIdEntity("client1")),
        Some(new Quota(9000, true))
      )

      checkQuota(clientQuotaManager, "userA", "client1", 5000, 4500, expectThrottle = false) // <user, client> quota takes precedence over <user>
      checkQuota(clientQuotaManager, "userA", "client2", 4000, 4500, expectThrottle = true)  // <user> quota takes precedence over <client> and defaults
      checkQuota(clientQuotaManager, "userA", "client3", 4000, 0, expectThrottle = true)     // <user> quota is shared across clients of user
      checkQuota(clientQuotaManager, "userA", "client1", 5000, 0, expectThrottle = false)    // <user, client> is exclusive use, unaffected by other clients

      checkQuota(clientQuotaManager, "userB", "client1", 7000, 8000, expectThrottle = true)
      checkQuota(clientQuotaManager, "userB", "client2", 8000, 7000, expectThrottle = false) // Default per-client quota for exclusive use of <user, client>
      checkQuota(clientQuotaManager, "userB", "client3", 8000, 7000, expectThrottle = false)

      checkQuota(clientQuotaManager, "userD", "client1", 3000, 3500, expectThrottle = true)  // Default <user, client> quota
      checkQuota(clientQuotaManager, "userD", "client2", 3000, 2500, expectThrottle = false)
      checkQuota(clientQuotaManager, "userE", "client1", 3000, 2500, expectThrottle = false)

      // Remove default <user, client> quota config, revert to <user> default
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity),
        Some(ClientQuotaManager.DefaultClientIdEntity),
        None
      )
      checkQuota(clientQuotaManager, "userD", "client1", 1000, 0, expectThrottle = false)    // Metrics tags changed, restart counter
      checkQuota(clientQuotaManager, "userE", "client4", 1000, 1500, expectThrottle = true)
      checkQuota(clientQuotaManager, "userF", "client4", 1000, 800, expectThrottle = false)  // Default <user> quota shared across clients of user
      checkQuota(clientQuotaManager, "userF", "client5", 1000, 800, expectThrottle = true)

      // Remove default <user> quota config, revert to <client-id> default
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.DefaultUserEntity),
        None, 
        None
      )
      checkQuota(clientQuotaManager, "userF", "client4", 2000, 0, expectThrottle = false)  // Default <client-id> quota shared across client-id of all users
      checkQuota(clientQuotaManager, "userF", "client5", 2000, 0, expectThrottle = false)
      checkQuota(clientQuotaManager, "userF", "client5", 2000, 2500, expectThrottle = true)
      checkQuota(clientQuotaManager, "userG", "client5", 2000, 0, expectThrottle = true)

      // Update quotas
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")), 
        None, 
        Some(new Quota(8000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client1")),
        Some(new Quota(10000, true))
      )
      checkQuota(clientQuotaManager, "userA", "client2", 8000, 0, expectThrottle = false)
      checkQuota(clientQuotaManager, "userA", "client2", 8000, 4500, expectThrottle = true) // Throttled due to sum of new and earlier values
      checkQuota(clientQuotaManager, "userA", "client1", 10000, 0, expectThrottle = false)
      checkQuota(clientQuotaManager, "userA", "client1", 10000, 6000, expectThrottle = true)
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client1")),
        None
      )
      checkQuota(clientQuotaManager, "userA", "client6", 8000, 0, expectThrottle = true)    // Throttled due to shared user quota
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client6")),
        Some(new Quota(11000, true))
      )
      checkQuota(clientQuotaManager, "userA", "client6", 11000, 8500, expectThrottle = false)
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(12000, true))
      )
      clientQuotaManager.updateQuota(
        Some(ClientQuotaManager.UserEntity("userA")),
        Some(ClientQuotaManager.ClientIdEntity("client6")),
        None
      )
      checkQuota(clientQuotaManager, "userA", "client6", 12000, 4000, expectThrottle = true) // Throttled due to sum of new and earlier values

    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testQuotaViolation(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "")
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", "Produce", ""))
    try {
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(500, true))
      )

      // We have 10 seconds windows. Make sure that there is no quota violation
      // if we produce under the quota
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 400))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // Create a spike.
      // 400*10 + 2000 + 300 = 6300/10.5 = 600 bytes per second.
      // (600 - quota)/quota*window-size = (600-500)/500*10.5 seconds = 2100
      // 10.5 seconds because the last window is half complete
      time.sleep(500)
      val throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 2300)

      assertEquals(2100, throttleTime, "Should be throttled")
      throttle(clientQuotaManager, "ANONYMOUS", "unknown", throttleTime, callback)
      assertEquals(1, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientQuotaManager.throttledChannelReaper.doWork()
      assertEquals(0, numCallbacks)
      time.sleep(throttleTime)

      // Callback can only be triggered after the delay time passes
      clientQuotaManager.throttledChannelReaper.doWork()
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      assertEquals(1, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (_ <- 0 until 10) {
        maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 400)
        time.sleep(1000)
      }

      assertEquals(0, maybeRecord(clientQuotaManager, "ANONYMOUS", "unknown", 0),
        "Should be unthrottled since bursty sample has rolled over")
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testExpireThrottleTimeSensor(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "")
    try {
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(500, true))
      )

      maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 100)
      // remove the throttle time sensor
      metrics.removeSensor("ProduceThrottleTime-:client1")
      // should not throw an exception even if the throttle time sensor does not exist.
      val throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 10000)
      assertTrue(throttleTime > 0, "Should be throttled")
      // the sensor should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertNotNull(throttleTimeSensor, "Throttle time sensor should exist")
      assertNotNull(throttleTimeSensor, "Throttle time sensor should exist")
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testExpireQuotaSensors(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "")
    try {
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(500, true))
      )

      maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 100)
      // remove all the sensors
      metrics.removeSensor("ProduceThrottleTime-:client1")
      metrics.removeSensor("Produce-ANONYMOUS:client1")
      // should not throw an exception
      val throttleTime = maybeRecord(clientQuotaManager, "ANONYMOUS", "client1", 10000)
      assertTrue(throttleTime > 0, "Should be throttled")

      // all the sensors should get recreated
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:client1")
      assertNotNull(throttleTimeSensor, "Throttle time sensor should exist")

      val byteRateSensor = metrics.getSensor("Produce-:client1")
      assertNotNull(byteRateSensor, "Byte rate sensor should exist")
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testClientIdNotSanitized(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.PRODUCE, time, "")
    val clientId = "client@#$%"
    try {
      clientQuotaManager.updateQuota(
        None,
        Some(ClientQuotaManager.DefaultClientIdEntity),
        Some(new Quota(500, true))
      )

      maybeRecord(clientQuotaManager, "ANONYMOUS", clientId, 100)

      // The metrics should use the raw client ID, even if the reporters internally sanitize them
      val throttleTimeSensor = metrics.getSensor("ProduceThrottleTime-:" + clientId)
      assertNotNull(throttleTimeSensor, "Throttle time sensor should exist")

      val byteRateSensor = metrics.getSensor("Produce-:"  + clientId)
      assertNotNull(byteRateSensor, "Byte rate sensor should exist")
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testQuotaTypesEnabledUpdatesWithDefaultCallback(): Unit = {
    val clientQuotaManager = new ClientQuotaManager(config, metrics, QuotaType.CONTROLLER_MUTATION, time, "")
    try {
      // Initially, quotaTypesEnabled should be QuotaTypes.NoQuotas and quotasEnabled should be false
      assertEquals(QuotaTypes.NoQuotas, clientQuotaManager.quotaTypesEnabled)
      assertFalse(clientQuotaManager.quotasEnabled)

      // Add a client-id quota, quotaTypesEnabled should be QuotaTypes.ClientIdQuotaEnabled
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client1")), Some(new Quota(5, true)))
      assertEquals(QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Add a user quota, quotaTypesEnabled should be QuotaTypes.UserQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), None, Some(new Quota(5, true)))
      assertEquals(QuotaTypes.UserQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Add a duplicate client-id quota, quotaTypesEnabled should remain unchanged
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client2")), Some(new Quota(5, true)))
      assertEquals(QuotaTypes.UserQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Add duplicate user quota, quotaTypesEnabled should remain unchanged
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userB")), None, Some(new Quota(5, true)))
      assertEquals(QuotaTypes.UserQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Add a user-client-id quota, quotaTypesEnabled should be QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), Some(new Quota(10, true)))
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Add Duplicate user-client-id quota, quotaTypesEnabled should remain unchanged
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), Some(new Quota(12, true)))
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Remove the first user quota, quotaTypesEnabled should remain unchanged
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), None, None)
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled | QuotaTypes.UserQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Remove the second user quota, quotaTypesEnabled should be QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userB")), None, None)
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Remove the first client-id quota, quotaTypesEnabled should remain unchanged
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client1")), None)
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled | QuotaTypes.ClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Remove the second client-id quota, quotaTypesEnabled should be QuotaTypes.UserClientIdQuotaEnabled
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client2")), None)
      assertEquals(QuotaTypes.UserClientIdQuotaEnabled, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled)

      // Remove the first user-client-id quota, quotaTypesEnabled should be noQuotas as both user-client-id quotas has the same user client but different quota
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), None)
      assertEquals(QuotaTypes.NoQuotas, clientQuotaManager.quotaTypesEnabled)
      assertFalse(clientQuotaManager.quotasEnabled)

      // Remove the second user-client-id quota, quotaTypesEnabled should be QuotaTypes.NoQuotas and quotasEnabled should be false
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), None)
      assertEquals(QuotaTypes.NoQuotas, clientQuotaManager.quotaTypesEnabled)
      assertFalse(clientQuotaManager.quotasEnabled)
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  @Test
  def testQuotaTypesEnabledUpdatesWithCustomCallback(): Unit = {
    val customQuotaCallback = new ClientQuotaCallback {
      val quotas = new HashMap[ClientQuotaEntity, Quota]()
      override def configure(configs: Map[String, _]): Unit = {}

      override def quotaMetricTags(quotaType: ClientQuotaType, principal: KafkaPrincipal, clientId: String): Map[String, String] = Collections.emptyMap()

      override def quotaLimit(quotaType: ClientQuotaType, metricTags: Map[String, String]): java.lang.Double = 1
      override def updateClusterMetadata(cluster: Cluster): Boolean = false

      override def updateQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity, newValue: Double): Unit = {
        quotas.put(entity.asInstanceOf[KafkaQuotaEntity], new Quota(newValue.toLong, true))
      }

      override def removeQuota(quotaType: ClientQuotaType, entity: ClientQuotaEntity): Unit = {
        quotas.remove(entity.asInstanceOf[KafkaQuotaEntity])
      }

      override def quotaResetRequired(quotaType: ClientQuotaType): Boolean = false

      override def close(): Unit = {}
    }
    val clientQuotaManager = new ClientQuotaManager(
      config = new ClientQuotaManagerConfig(),
      metrics = metrics,
      quotaType = QuotaType.CONTROLLER_MUTATION,
      time = time,
      threadNamePrefix = "",
      clientQuotaCallbackPlugin = Some(Plugin.wrapInstance(customQuotaCallback, metrics, ""))
    )

    try {
      // Initially, quotaTypesEnabled should be CustomQuotas and quotasEnabled should be true
      assertEquals(QuotaTypes.CustomQuotas, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled, "quotasEnabled should be true with custom callback")

      // Add a client-id quota, quotaTypesEnabled should remain QuotaTypes.CustomQuotas
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client1")), Some(new Quota(12, true)))
      assertEquals(QuotaTypes.CustomQuotas, clientQuotaManager.quotaTypesEnabled)

      // Add a user quota, quotaTypesEnabled quotaTypesEnabled should remain QuotaTypes.CustomQuotas
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), None, Some(new Quota(12, true)))
      assertEquals(QuotaTypes.CustomQuotas, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled, "quotasEnabled should remain true")

      // Add a user-client-id quota,  quotaTypesEnabled should remain QuotaTypes.CustomQuotas
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), Some(new Quota(12, true)))
      assertEquals(QuotaTypes.CustomQuotas, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled, "quotasEnabled should remain true")

      // Remove all quotas, quotaTypesEnabled should be QuotaTypes.CustomQuotas
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), Some(ClientQuotaManager.ClientIdEntity("client1")), None)
      clientQuotaManager.updateQuota(Some(ClientQuotaManager.UserEntity("userA")), None, None)
      clientQuotaManager.updateQuota(None, Some(ClientQuotaManager.ClientIdEntity("client1")), None)
      assertEquals(QuotaTypes.CustomQuotas, clientQuotaManager.quotaTypesEnabled)
      assertTrue(clientQuotaManager.quotasEnabled, "quotasEnabled should remain true")
    } finally {
      clientQuotaManager.shutdown()
    }
  }

  private case class UserClient(
    user: String,
    clientId: String,
    configUser: Option[BaseUserEntity] = None,
    configClientEntity: Option[ClientQuotaManager.ClientIdEntity] = None
  )
}

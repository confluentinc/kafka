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

import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.server.config.ClientQuotaManagerConfig
import org.apache.kafka.server.quota.{ClientQuotaManager, QuotaType}
import org.apache.kafka.server.quota.ClientQuotaEntity
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.Optional

class ClientRequestQuotaManagerTest extends BaseClientQuotaManagerTest {
  private val config = new ClientQuotaManagerConfig()

  @Test
  def testRequestPercentageQuotaViolation(): Unit = {
    val clientRequestQuotaManager = new ClientRequestQuotaManager(config, metrics, time, "", Optional.empty())
    val userEntity: ClientQuotaEntity.ConfigEntity = new ClientQuotaManager.UserEntity("ANONYMOUS")
    val clientEntity: ClientQuotaEntity.ConfigEntity = new ClientQuotaManager.ClientIdEntity("test-client")

    clientRequestQuotaManager.updateQuota(
      Optional.of(userEntity),
      Optional.of(clientEntity),
      Optional.of(Quota.upperBound(1))
    )
    val queueSizeMetric = metrics.metrics().get(metrics.metricName("queue-size", QuotaType.REQUEST.toString, ""))
    def millisToPercent(millis: Double) = millis * 1000 * 1000 * ClientRequestQuotaManager.NANOS_TO_PERCENTAGE_PER_SECOND
    try {
      // We have 10 seconds windows. Make sure that there is no quota violation
      // if we are under the quota
      for (_ <- 0 until 10) {
        assertEquals(0, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(4)))
        time.sleep(1000)
      }
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)

      // Create a spike.
      // quota = 1% (10ms per second)
      // 4*10 + 67.1 = 107.1/10.5 = 10.2ms per second.
      // (10.2 - quota)/quota*window-size = (10.2-10)/10*10.5 seconds = 210ms
      // 10.5 seconds interval because the last window is half complete
      time.sleep(500)
      val throttleTime = maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(67.1))

      assertEquals(210, throttleTime, "Should be throttled")

      throttle(clientRequestQuotaManager, "ANONYMOUS", "test-client", throttleTime, callback)
      assertEquals(1, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      // After a request is delayed, the callback cannot be triggered immediately
      clientRequestQuotaManager.processThrottledChannelReaperDoWork()
      assertEquals(0, numCallbacks)
      time.sleep(throttleTime)

      // Callback can only be triggered after the delay time passes
      clientRequestQuotaManager.processThrottledChannelReaperDoWork()
      assertEquals(0, queueSizeMetric.metricValue.asInstanceOf[Double].toInt)
      assertEquals(1, numCallbacks)

      // Could continue to see delays until the bursty sample disappears
      for (_ <- 0 until 11) {
        maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(4))
        time.sleep(1000)
      }

      assertEquals(0,
        maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0), "Should be unthrottled since bursty sample has rolled over")

      // Create a very large spike which requires > one quota window to bring within quota
      assertEquals(1000, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", millisToPercent(500)))
      for (_ <- 0 until 10) {
        time.sleep(1000)
        assertEquals(1000, maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0))
      }
      time.sleep(1000)
      assertEquals(0,
        maybeRecord(clientRequestQuotaManager, "ANONYMOUS", "test-client", 0), "Should be unthrottled since bursty sample has rolled over")

    } finally {
      clientRequestQuotaManager.shutdown()
    }
  }
}


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.kafka.clients.admin.MockAdminClient
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.api.Assertions._

@Timeout(120000)
class ClusterToolTest {

  @Test
  def testPrintClusterId(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      clusterId("QtNwvtfVQ3GEFpzOmDEE-w").
      build()
    val stream = new ByteArrayOutputStream()
    ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient)
    assertEquals(
      s"""Cluster ID: QtNwvtfVQ3GEFpzOmDEE-w
""", stream.toString())
  }

  @Test
  def testClusterTooOldToHaveId(): Unit = {
    val adminClient = new MockAdminClient.Builder().
      clusterId(null).
      build()
    val stream = new ByteArrayOutputStream()
    ClusterTool.clusterIdCommand(new PrintStream(stream), adminClient)
    assertEquals(
      s"""No cluster ID found. The Kafka version is probably too old.
""", stream.toString())
  }

  @Test
  def testDecommissionBroker(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(3).
      kip500Mode(true).
      build()
    val stream = new ByteArrayOutputStream()
    ClusterTool.decommissionCommand(new PrintStream(stream), adminClient, 0)
    assertEquals(
      s"""Broker 0 is no longer registered. Note that if the broker is still running, or is restarted, it will re-register.
""", stream.toString())
  }

  @Test
  def testLegacyModeClusterCannotDecommissionBroker(): Unit = {
    val adminClient = new MockAdminClient.Builder().numBrokers(3).
      kip500Mode(false).
      build()
    val stream = new ByteArrayOutputStream()
    ClusterTool.decommissionCommand(new PrintStream(stream), adminClient, 0)
    assertEquals(
      s"""The target cluster does not support broker decommissioning.
""", stream.toString())
  }
}
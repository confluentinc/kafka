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

import kafka.server.metadata.{MetadataBroker, MetadataImageBuilder}
import org.apache.kafka.common.Node
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.metalog.{MetaLogLeader, MetaLogManager}
import org.junit.Assert.assertEquals
import org.junit.rules.Timeout
import org.junit.{Rule, Test}
import org.mockito.Mockito
import org.slf4j.LoggerFactory

class BrokerToControllerChannelManagerTest {
  private val log = LoggerFactory.getLogger(classOf[BrokerToControllerChannelManagerTest])

  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Test
  def testMetadataCacheControllerNodeProvider(): Unit = {
    val cache = new MetadataCache(0)
    val provider = new MetadataCacheControllerNodeProvider(cache, new ListenerName("INTERNAL"))
    assertEquals(None, provider.controllerNode())

    val builder = MetadataImageBuilder(cache.brokerId, log, cache.currentImage())
    builder.setControllerId(Some(2))
    cache.setImage(builder.build())
    assertEquals(None, provider.controllerNode())

    val builder2 = MetadataImageBuilder(cache.brokerId, log, cache.currentImage())
    builder2.brokersBuilder().add(new MetadataBroker(0, null,
      Map("INTERNAL" -> new Node(0, "localhost", 100)), false))
    builder2.brokersBuilder().add(new MetadataBroker(1, null,
      Map("INTERNAL" -> new Node(1, "localhost", 101)), false))
    builder2.brokersBuilder().add(new MetadataBroker(2, null,
      Map("INTERNAL" -> new Node(2, "localhost", 102)), false))
    cache.setImage(builder2.build())

    assertEquals(Some(new Node(2, "localhost", 102)), provider.controllerNode())
  }

  @Test
  def testRaftControllerNodeProvider(): Unit = {
    val mockMetaLogManager = Mockito.mock(classOf[MetaLogManager])
    Mockito.when(mockMetaLogManager.leader()).thenReturn(new MetaLogLeader(2, 123L))
    val nodes = Seq(new Node(0, "localhost", 100),
      new Node(1, "localhost", 101),
      new Node(2, "localhost", 102))
    val provider = new RaftControllerNodeProvider(mockMetaLogManager, nodes)
    assertEquals(Some(new Node(2, "localhost", 102)), provider.controllerNode())
  }
}

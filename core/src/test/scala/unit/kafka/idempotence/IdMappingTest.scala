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

package unit.kafka.idempotence

import java.io.File
import java.util.Properties

import kafka.common.TopicAndPartition
import kafka.idempotence.IdMapping
import kafka.log.LogConfig
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class IdMappingTest extends JUnitSuite {
  var idMappingDir: File = null
  var config: LogConfig = null
  var idMapping: IdMapping = null

  @Before
  def setUp(): Unit = {
    // Create configuration including number of snapshots to hold
    val props = new Properties()
    props.setProperty(LogConfig.MaxIdMapSnapshotsProp, "1")
    config = LogConfig(props)

    // Create temporary directory
    idMappingDir = TestUtils.tempDir()

    // Instantiate IdMapping
    idMapping = new IdMapping(new TopicAndPartition("test", 0), config, idMappingDir)
  }

  @After
  def tearDown(): Unit = {
    idMappingDir.listFiles().foreach(f => f.delete())
    idMappingDir.deleteOnExit()
  }

  @Test
  def testBasicIdMapping(): Unit = {
    // First entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Second entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 1L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Incorrect sequence number for id 0
    try {
      idMapping.checkAndUpdate(0L, 1L, 0, 0L)
      fail("Should have thrown exception due to incorrect sequence number")
    } catch {
      case e: Throwable => {
        // Expected exception
      }
    }

    // Change epoch
    try {
      idMapping.checkAndUpdate(0L, 0L, 1, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Incorrect epoch
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
      fail("Should have thrown exception due to incorrect epoch")
    } catch {
      case e: Throwable => {
        // Expected exception
      }
    }
  }

  @Test
  def testTakeSnapshot(): Unit = {
    // First entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Second entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 1L, 0, 1L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Take snapshot
    idMapping.takeSnapshot()

    // Check that file exists and it is not empty
    assertTrue("Directory doesn't contain a single file as expected", idMappingDir.list().length == 1)
    assertTrue("Snapshot file is empty", idMappingDir.list().head.length > 0)
  }

  @Test
  def testRecoverFromSnapshot(): Unit = {
    // First entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Second entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 1L, 0, 1L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Take snapshot
    idMapping.takeSnapshot()

    val nextIdMapping = new IdMapping(new TopicAndPartition("test", 0), config, idMappingDir)

    // Third entry for id 0 added after recovery
    try {
      nextIdMapping.checkAndUpdate(0L, 2L, 0, 2L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }
  }

  @Test
  def testRemoveOldSnapshot(): Unit = {
    // First entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Second entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 1L, 0, 1L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Take snapshot
    try {
      idMapping.takeSnapshot()
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Third entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 2L, 0, 2L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Take snapshot
    try {
      idMapping.takeSnapshot()
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    assertTrue(s"number of snapshot files is incorrect: ${idMappingDir.listFiles().length}",
               idMappingDir.listFiles().length == 1)
  }

  @Test
  def testInvalidOffsetForSnapshot(): Unit = {
    // First entry for id 0 added
    try {
      idMapping.checkAndUpdate(0L, 0L, 0, 0L)
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    // Take snapshot
    try {
      idMapping.takeSnapshot()
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }
    // Take a second snapshot
    try {
      idMapping.takeSnapshot()
      fail("Exception expected")
    } catch {
      case e: Throwable => {
        // expected
      }
    }
  }

  @Test
  def testStartOffset(): Unit = {
    try {
      idMapping.checkAndUpdate(1L, 0L, 0, 0L)
      idMapping.checkAndUpdate(0L, 0L, 0, 1L)
      idMapping.checkAndUpdate(0L, 1L, 0, 2L)
      idMapping.checkAndUpdate(0L, 2L, 0, 3L)
      idMapping.takeSnapshot()
    } catch {
      case e: Throwable => {
        fail(e)
      }
    }

    try {
      val nextIdMapping = new IdMapping(new TopicAndPartition("test", 0), config, idMappingDir, logStartOffset = 1L)
      nextIdMapping.checkAndUpdate(1L, 1L, 0, 4L)
      fail(s"Should have expired id = 1")
    } catch {
      case e: Throwable => {
        // expected
      }
    }
  }
}

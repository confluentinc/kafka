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

package kafka.coordinator

import kafka.utils.ZkUtils

import org.apache.zookeeper.data.ACL
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Test}
import org.junit.Assert._

class PIDManagerTest {

  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])

  @After
  def tearDown(): Unit = {
    EasyMock.reset(zkUtils)
  }

  @Test
  def testGetPID() {
    var block: Long = -1L
    val capturedArgument: Capture[String] = EasyMock.newCapture()
    EasyMock.expect(zkUtils.createSequentialPersistentPath(EasyMock.capture(capturedArgument),
      EasyMock.anyString(),
      EasyMock.anyObject().asInstanceOf[java.util.List[ACL]]))
      .andAnswer(new IAnswer[String] {
        override def answer(): String = {
          block += 1
          capturedArgument + block.toString
        }
      })
      .anyTimes()
    EasyMock.replay(zkUtils)

    val manager1: PIDManager = new PIDManager(0, zkUtils)
    val manager2: PIDManager = new PIDManager(1, zkUtils)

    var pid1: Long = manager1.getNewPID()
    var pid2: Long = manager2.getNewPID()

    assertEquals(0, pid1)
    assertEquals(PIDManager.PIDBlockSize, pid2)

    for (i <- 1 until PIDManager.PIDBlockSize) {
      assertEquals(pid1 + i, manager1.getNewPID())
    }

    for (i <- 1 until PIDManager.PIDBlockSize) {
      assertEquals(pid2 + i, manager2.getNewPID())
    }

    assertEquals(pid2 + PIDManager.PIDBlockSize, manager1.getNewPID())
    assertEquals(pid2 + PIDManager.PIDBlockSize * 2, manager2.getNewPID())
  }

  @Test(expected = classOf[java.lang.NumberFormatException])
  def testExceedPIDLimit() {
    val capturedArgument: Capture[String] = EasyMock.newCapture()
    EasyMock.expect(zkUtils.createSequentialPersistentPath(EasyMock.capture(capturedArgument),
      EasyMock.anyString(),
      EasyMock.anyObject().asInstanceOf[java.util.List[ACL]]))
      .andAnswer(new IAnswer[String] {
        override def answer(): String = {
          capturedArgument + "92233720368547758071"
        }
      })
      .anyTimes()
    EasyMock.replay(zkUtils)

    val manager: PIDManager = new PIDManager(0, zkUtils)
  }
}


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

import kafka.common.Topic
import kafka.utils.ZkUtils
import org.apache.kafka.common.protocol.Errors
import org.apache.zookeeper.data.ACL
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.{After, Before, Test}
import org.junit.Assert._

class TransactionCoordinatorTest {

  var block: Int = -1
  val capturedArgument: Capture[String] = EasyMock.newCapture()
  val zkUtils: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
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
  EasyMock.expect(zkUtils.getTopicPartitionCount(Topic.TransactionLogTopicName))
    .andReturn(Some(2))
    .once()
  EasyMock.replay(zkUtils)

  val pIDManager: PidManager = new PidManager(0, zkUtils)
  val logManager: TransactionLogManager = new TransactionLogManager(0, zkUtils)
  val coordinator: TransactionCoordinator = new TransactionCoordinator(0, pIDManager, logManager)

  var result: InitPIDResult = null

  @Before
  def setUp(): Unit = {
    coordinator.startup()
    // only give one of the two partitions of the transaction topic
    coordinator.handleTxnImmigration(1)
  }

  @After
  def tearDown(): Unit = {
    EasyMock.reset(zkUtils)
    coordinator.shutdown()
  }

  @Test
  def testHandleInitPID() = {
    coordinator.handleInitPID("", initPIDMockCallback)
    assertEquals(InitPIDResult(0L, 0, Errors.NONE), result)

    coordinator.handleInitPID("", initPIDMockCallback)
    assertEquals(InitPIDResult(1L, 0, Errors.NONE), result)

    coordinator.handleInitPID("a", initPIDMockCallback)
    assertEquals(InitPIDResult(2L, 0, Errors.NONE), result)

    coordinator.handleInitPID("a", initPIDMockCallback)
    assertEquals(InitPIDResult(2L, 1, Errors.NONE), result)

    coordinator.handleInitPID("c", initPIDMockCallback)
    assertEquals(InitPIDResult(3L, 0, Errors.NONE), result)

    coordinator.handleInitPID("b", initPIDMockCallback)
    assertEquals(InitPIDResult(-1L, -1, Errors.NOT_COORDINATOR_FOR_GROUP), result)
  }

  def initPIDMockCallback(ret: InitPIDResult): Unit = {
    result = ret
  }
}

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
package kafka.coordinator.transaction

import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.utils.MockTime
import org.easymock.{Capture, EasyMock, IAnswer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

class TransactionCoordinatorTest {

  val time = new MockTime()

  var nextPid: Long = 0L
  val pidManager: PidManager = EasyMock.createNiceMock(classOf[PidManager])

  EasyMock.expect(pidManager.getNewPid())
    .andAnswer(new IAnswer[Long] {
      override def answer(): Long = {
        nextPid += 1
        nextPid - 1
      }
    })
    .anyTimes()

  val transactionManager: TransactionManager = EasyMock.createNiceMock(classOf[TransactionManager])

  EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("a")))
    .andReturn(true)
    .anyTimes()
  EasyMock.expect(transactionManager.isCoordinatorFor(EasyMock.eq("b")))
    .andReturn(false)
    .anyTimes()

  val capturedAPid: Capture[PidMetadata] = EasyMock.newCapture()
  EasyMock.expect(transactionManager.addPid(EasyMock.eq("a"), EasyMock.capture(capturedAPid)))
    .andAnswer(new IAnswer[PidMetadata] {
      override def answer(): PidMetadata = {
        capturedAPid.getValue
      }
    })
    .once()
  EasyMock.expect(transactionManager.getPid(EasyMock.eq("a")))
    .andAnswer(new IAnswer[Option[PidMetadata]] {
      override def answer(): Option[PidMetadata] = {
        if (capturedAPid.hasCaptured) {
          Some(capturedAPid.getValue)
        } else {
          None
        }
      }
    })
    .anyTimes()

  val coordinator: TransactionCoordinator = new TransactionCoordinator(0, pidManager, transactionManager)

  var result: InitPidResult = _

  @Before
  def setUp(): Unit = {
    EasyMock.replay(pidManager, transactionManager)

    coordinator.startup()
    // only give one of the two partitions of the transaction topic
    coordinator.handleTxnImmigration(1)
  }

  @After
  def tearDown(): Unit = {
    EasyMock.reset(pidManager, transactionManager)
    coordinator.shutdown()
  }

  @Test
  def testHandleInitPID() = {
    val transactionTimeoutMs = 1000

    coordinator.handleInitPid("", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(0L, 0, Errors.NONE), result)

    coordinator.handleInitPid("", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(1L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(2L, 0, Errors.NONE), result)

    coordinator.handleInitPid("a", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(2L, 1, Errors.NONE), result)

    coordinator.handleInitPid("b", transactionTimeoutMs, initPIDMockCallback)
    assertEquals(InitPidResult(-1L, -1, Errors.NOT_COORDINATOR), result)
  }

  def initPIDMockCallback(ret: InitPidResult): Unit = {
    result = ret
  }
}

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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.coordinator.transaction.{TransactionState, TxnTransitMetadata}
import org.apache.kafka.server.common.TransactionVersion
import org.apache.kafka.server.common.TransactionVersion.{TV_0, TV_2}
import org.apache.kafka.server.util.MockTime
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Optional

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class TransactionMetadataTest {

  val time = new MockTime()
  val producerId = 23423L
  val transactionalId = "txnlId"

  @Test
  def testInitializeEpoch(): Unit = {
    val producerEpoch = RecordBatch.NO_PRODUCER_EPOCH

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, None)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testNormalEpochBump(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, None)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testBumpEpochNotAllowedIfEpochsExhausted(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    assertThrows(classOf[IllegalStateException], () => txnMetadata.prepareIncrementProducerEpoch(30000,
      None, time.milliseconds()))
  }

  @Test
  def testTransitFromEmptyToPrepareAbortInV2(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = -1,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_2)

    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, noPartitionAdded = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp)
  }

  @Test
  def testTransitFromCompleteAbortToPrepareAbortInV2(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.COMPLETE_ABORT,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds() - 1,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_2)

    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, noPartitionAdded = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp)
  }

  @Test
  def testTransitFromCompleteCommitToPrepareAbortInV2(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.COMPLETE_COMMIT,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds() - 1,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_2)

    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() + 1, noPartitionAdded = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() + 1, txnMetadata.txnStartTimestamp)
  }

  @Test
  def testTolerateUpdateTimeShiftDuringEpochBump(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    // let new time be smaller
    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Option(producerEpoch),
      Some(time.milliseconds() - 1))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(-1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateUpdateTimeResetDuringProducerIdRotation(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareProducerIdRotation(producerId + 1, 30000, time.milliseconds() - 1, recordLastEpoch = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId + 1, txnMetadata.producerId)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(-1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringAddPartitions(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds(),
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    // let new time be smaller; when transiting from TransactionState.EMPTY the start time would be updated to the update-time
    var transitMetadata = txnMetadata.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic1", 0)), time.milliseconds() - 1, TV_0)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(Set[TopicPartition](new TopicPartition("topic1", 0)), txnMetadata.topicPartitions)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)

    // add another partition, check that in TransactionState.ONGOING state the start timestamp would not change to update time
    transitMetadata = txnMetadata.prepareAddPartitions(Set[TopicPartition](new TopicPartition("topic2", 0)), time.milliseconds() - 2, TV_0)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(Set[TopicPartition](new TopicPartition("topic1", 0), new TopicPartition("topic2", 0)), txnMetadata.topicPartitions)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 2, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringPrepareCommit(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, noPartitionAdded = false)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(TransactionState.PREPARE_COMMIT, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testTolerateTimeShiftDuringPrepareAbort(): Unit = {
    val producerEpoch: Short = 1
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, noPartitionAdded = false)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(TransactionState.PREPARE_ABORT, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def testTolerateTimeShiftDuringCompleteCommit(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val producerEpoch: Short = 1
    val lastProducerEpoch: Short = 0
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = TransactionState.PREPARE_COMMIT,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = clientTransactionVersion
    )

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)

    assertEquals(TransactionState.COMPLETE_COMMIT, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def testTolerateTimeShiftDuringCompleteAbort(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    val producerEpoch: Short = 1
    val lastProducerEpoch: Short = 0
    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = TransactionState.PREPARE_ABORT,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = 1L,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = clientTransactionVersion
    )

    // let new time be smaller
    val transitMetadata = txnMetadata.prepareComplete(time.milliseconds() - 1)
    txnMetadata.completeTransitionTo(transitMetadata)

    assertEquals(TransactionState.COMPLETE_ABORT, txnMetadata.state)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(1L, txnMetadata.txnStartTimestamp)
    assertEquals(time.milliseconds() - 1, txnMetadata.txnLastUpdateTimestamp)
  }

  @Test
  def testFenceProducerAfterEpochsExhausted(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    val fencingTransitMetadata = txnMetadata.prepareFenceProducerEpoch()
    assertEquals(Short.MaxValue, fencingTransitMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, fencingTransitMetadata.lastProducerEpoch)
    assertEquals(Some(TransactionState.PREPARE_EPOCH_FENCE), txnMetadata.pendingState)

    // We should reset the pending state to make way for the abort transition.
    txnMetadata.pendingState = None

    val transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, TV_0, RecordBatch.NO_PRODUCER_ID, time.milliseconds(), noPartitionAdded = false)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, transitMetadata.producerId)
  }

  @Test
  def testInvalidTransitionFromCompleteCommitToFence(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.COMPLETE_COMMIT,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    assertThrows(classOf[IllegalStateException], () => txnMetadata.prepareFenceProducerEpoch())
  }

  @Test
  def testInvalidTransitionFromCompleteAbortToFence(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.COMPLETE_ABORT,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    assertThrows(classOf[IllegalStateException], () => txnMetadata.prepareFenceProducerEpoch())
  }

  @Test
  def testFenceProducerNotAllowedIfItWouldOverflow(): Unit = {
    val producerEpoch = Short.MaxValue

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)
    assertTrue(txnMetadata.isProducerEpochExhausted)
    assertThrows(classOf[IllegalStateException], () => txnMetadata.prepareFenceProducerEpoch())
  }

  @Test
  def testRotateProducerId(): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val newProducerId = 9893L
    val transitMetadata = txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), recordLastEpoch = true)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(newProducerId, txnMetadata.producerId)
    assertEquals(producerId, txnMetadata.prevProducerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testEpochBumpOnEndTxn(): Unit = {
    time.sleep(100)
    val producerEpoch = 10.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds(),
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_2)

    var transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_2, RecordBatch.NO_PRODUCER_ID, time.milliseconds() - 1, noPartitionAdded = false)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertEquals(TV_2, txnMetadata.clientTransactionVersion)

    transitMetadata = txnMetadata.prepareComplete(time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals((producerEpoch + 1).toShort, txnMetadata.producerEpoch)
    assertEquals(TV_2, txnMetadata.clientTransactionVersion)
  }

  @Test
  def testEpochBumpOnEndTxnOverflow(): Unit = {
    time.sleep(100)
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.ONGOING,
      topicPartitions = mutable.Set.empty,
      txnStartTimestamp = time.milliseconds(),
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_2)
    assertTrue(txnMetadata.isProducerEpochExhausted)

    val newProducerId = 9893L
    var transitMetadata = txnMetadata.prepareAbortOrCommit(TransactionState.PREPARE_COMMIT, TV_2, newProducerId, time.milliseconds() - 1, noPartitionAdded = false)
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(Short.MaxValue, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(TV_2, txnMetadata.clientTransactionVersion)

    transitMetadata = txnMetadata.prepareComplete(time.milliseconds())
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(newProducerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
    assertEquals(TV_2, txnMetadata.clientTransactionVersion)
  }

  @Test
  def testRotateProducerIdInOngoingState(): Unit = {
    assertThrows(classOf[IllegalStateException], () => testRotateProducerIdInOngoingState(TransactionState.ONGOING, TV_0))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def testRotateProducerIdInPrepareAbortState(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    assertThrows(classOf[IllegalStateException], () => testRotateProducerIdInOngoingState(TransactionState.PREPARE_ABORT, clientTransactionVersion))
  }

  @ParameterizedTest
  @ValueSource(shorts = Array(0, 2))
  def testRotateProducerIdInPrepareCommitState(transactionVersion: Short): Unit = {
    val clientTransactionVersion = TransactionVersion.fromFeatureLevel(transactionVersion)
    assertThrows(classOf[IllegalStateException], () => testRotateProducerIdInOngoingState(TransactionState.PREPARE_COMMIT, clientTransactionVersion))
  }

  @Test
  def testAttemptedEpochBumpWithNewlyCreatedMetadata(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(producerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(0, txnMetadata.producerEpoch)
    assertEquals(RecordBatch.NO_PRODUCER_EPOCH, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testEpochBumpWithCurrentEpochProvided(): Unit = {
    val producerEpoch = 735.toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(producerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch + 1, txnMetadata.producerEpoch)
    assertEquals(producerEpoch, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testAttemptedEpochBumpWithLastEpoch(): Unit = {
    val producerEpoch = 735.toShort
    val lastProducerEpoch = (producerEpoch - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = RecordBatch.NO_PRODUCER_ID,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val transitMetadata = prepareSuccessfulIncrementProducerEpoch(txnMetadata, Some(lastProducerEpoch))
    txnMetadata.completeTransitionTo(transitMetadata)
    assertEquals(producerId, txnMetadata.producerId)
    assertEquals(producerEpoch, txnMetadata.producerEpoch)
    assertEquals(lastProducerEpoch, txnMetadata.lastProducerEpoch)
  }

  @Test
  def testAttemptedEpochBumpWithFencedEpoch(): Unit = {
    val producerEpoch = 735.toShort
    val lastProducerEpoch = (producerEpoch - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = producerId,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = lastProducerEpoch,
      txnTimeoutMs = 30000,
      state = TransactionState.EMPTY,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = TV_0)

    val result = txnMetadata.prepareIncrementProducerEpoch(30000, Some((lastProducerEpoch - 1).toShort),
      time.milliseconds())
    assertEquals(Left(Errors.PRODUCER_FENCED), result)
  }

  @Test
  def testTransactionStateIdAndNameMapping(): Unit = {
    for (state <- TransactionState.ALL_STATES.asScala) {
      assertEquals(state, TransactionState.fromId(state.id))
      assertEquals(Optional.of(state), TransactionState.fromName(state.stateName))

      if (state != TransactionState.DEAD) {
        val clientTransactionState = org.apache.kafka.clients.admin.TransactionState.parse(state.stateName)
        assertEquals(state.stateName, clientTransactionState.toString)
        assertNotEquals(org.apache.kafka.clients.admin.TransactionState.UNKNOWN, clientTransactionState)
      }
    }
  }

  @Test
  def testAllTransactionStatesAreMapped(): Unit = {
    val unmatchedStates = mutable.Set(
      TransactionState.EMPTY,
      TransactionState.ONGOING,
      TransactionState.PREPARE_COMMIT,
      TransactionState.PREPARE_ABORT,
      TransactionState.COMPLETE_COMMIT,
      TransactionState.COMPLETE_ABORT,
      TransactionState.PREPARE_EPOCH_FENCE,
      TransactionState.DEAD
    )

    // The exhaustive match is intentional here to ensure that we are
    // forced to update the test case if a new state is added.
    TransactionState.ALL_STATES.asScala.foreach {
      case TransactionState.EMPTY => assertTrue(unmatchedStates.remove(TransactionState.EMPTY))
      case TransactionState.ONGOING => assertTrue(unmatchedStates.remove(TransactionState.ONGOING))
      case TransactionState.PREPARE_COMMIT => assertTrue(unmatchedStates.remove(TransactionState.PREPARE_COMMIT))
      case TransactionState.PREPARE_ABORT => assertTrue(unmatchedStates.remove(TransactionState.PREPARE_ABORT))
      case TransactionState.COMPLETE_COMMIT => assertTrue(unmatchedStates.remove(TransactionState.COMPLETE_COMMIT))
      case TransactionState.COMPLETE_ABORT => assertTrue(unmatchedStates.remove(TransactionState.COMPLETE_ABORT))
      case TransactionState.PREPARE_EPOCH_FENCE => assertTrue(unmatchedStates.remove(TransactionState.PREPARE_EPOCH_FENCE))
      case TransactionState.DEAD => assertTrue(unmatchedStates.remove(TransactionState.DEAD))
    }

    assertEquals(Set.empty, unmatchedStates)
  }

  private def testRotateProducerIdInOngoingState(state: TransactionState, clientTransactionVersion: TransactionVersion): Unit = {
    val producerEpoch = (Short.MaxValue - 1).toShort

    val txnMetadata = new TransactionMetadata(
      transactionalId = transactionalId,
      producerId = producerId,
      prevProducerId = producerId,
      nextProducerId = RecordBatch.NO_PRODUCER_ID,
      producerEpoch = producerEpoch,
      lastProducerEpoch = RecordBatch.NO_PRODUCER_EPOCH,
      txnTimeoutMs = 30000,
      state = state,
      topicPartitions = mutable.Set.empty,
      txnLastUpdateTimestamp = time.milliseconds(),
      clientTransactionVersion = clientTransactionVersion)
    val newProducerId = 9893L
    txnMetadata.prepareProducerIdRotation(newProducerId, 30000, time.milliseconds(), recordLastEpoch = false)
  }

  private def prepareSuccessfulIncrementProducerEpoch(txnMetadata: TransactionMetadata,
                                                      expectedProducerEpoch: Option[Short],
                                                      now: Option[Long] = None): TxnTransitMetadata = {
    val result = txnMetadata.prepareIncrementProducerEpoch(30000, expectedProducerEpoch,
      now.getOrElse(time.milliseconds()))
    result.getOrElse(throw new AssertionError(s"prepareIncrementProducerEpoch failed with $result"))
  }

}

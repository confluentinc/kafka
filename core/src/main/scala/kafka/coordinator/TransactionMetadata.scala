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

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.TopicPartition

private[coordinator] sealed trait TransactionState { def state: Byte }

/**
  * Transaction has started and ongoing
  *
  * transition: received EndTxnRequest with commit => PrepareCommit
  *             received EndTxnRequest with abort => PrepareCommit
  *             received AddPartitionsToTxnRequest => Ongoing
  *             received AddOffsetsToTxnRequest => Ongoing
  */
private[coordinator] case object Ongoing extends TransactionState { val state: Byte = 1 }

/**
  * Group is preparing to commit
  *
  * transition: received acks from all partitions => CompleteCommit
  */
private[coordinator] case object PrepareCommit extends TransactionState { val state: Byte = 2}

/**
  * Group is preparing to abort
  *
  * transition: received acks from all partitions => CompleteAbort
  */
private[coordinator] case object PrepareAbort extends TransactionState { val state: Byte = 3 }

/**
  * Group has completed commit
  *
  * Will soon be removed from the ongoing transaction cache
  */
private[coordinator] case object CompleteCommit extends TransactionState { val state: Byte = 4 }

/**
  * Group has completed commit
  *
  * Will soon be removed from the ongoing transaction cache
  */
private[coordinator] case object CompleteAbort extends TransactionState { val state: Byte = 5 }

@nonthreadsafe
private[coordinator] class TransactionMetadata(val pidMetadata: PidMetadata,
                                               val topicPartitions: List[TopicPartition]) {

  /* current state of the transaction */
  var state: TransactionState = Ongoing

  override def equals(that: Any): Boolean = that match {
    case other: TransactionMetadata => pidMetadata.equals(other) && topicPartitions.equals(other.topicPartitions)
    case _ => false
  }

}

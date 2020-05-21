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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.Records;

import java.util.concurrent.CompletableFuture;

/**
 * Bridging interface between {@link KafkaRaftClient} and {@link ReplicatedStateMachine}.
 */
public interface RecordAppender {

    /**
     * Append records to the log. The client must be in the leader state to
     * accept an append: it is up to the {@link ReplicatedStateMachine} implementation
     * to ensure a proper usage of append.
     *
     * This method must be thread-safe.
     *
     * @param records The records to append to the log
     * @return A future containing the base offset and epoch of the appended records (if successful)
     *
     * The following exceptions could be cause of the exception thrown through the future:
     *
     * @throws NotLeaderForPartitionException If the client is not the current leader
     * @throws NotEnoughReplicasException If the high watermark is undefined
     * @throws KafkaStorageException If the record queue is full
     * @throws TimeoutException If the request times out
     * @throws InvalidRequestException If the appending records don't get accepted by the state machine
     */
    CompletableFuture<OffsetAndEpoch> append(Records records);
}

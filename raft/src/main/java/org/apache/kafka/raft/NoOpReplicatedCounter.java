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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;

import java.util.concurrent.CompletableFuture;

/**
 * A replicated counter who accepts whatever records produced,
 * without data validation on the sequence order.
 */
public class NoOpReplicatedCounter extends ReplicatedCounter {

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final TopicPartition partition;

    public NoOpReplicatedCounter(int nodeId,
                                 TopicPartition partition,
                                 LogContext logContext,
                                 boolean verbose) {
        super(nodeId, logContext, verbose);
        this.partition = partition;
    }

    @Override
    public synchronized boolean accept(Records records) {
        return true;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            if (super.verbose) {
                for (Record record : batch) {
                    System.out.println(stringDeserializer.deserialize(partition.topic(),
                        Utils.toArray(record.value())));
                }
            }
            super.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    public synchronized CompletableFuture<Integer> appendRecords(Records records) {
        if (appender == null) {
            throw new IllegalStateException("The record appender is not initialized");
        }

        return appender.append(records).thenApply(offsetAndEpoch -> records.sizeInBytes());
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;


/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    public static final Long INVALID_PID = -1L;

    String appId;
    private long pid;
    private short epoch;
    Map<TopicPartition, Integer> sequenceNumbers;

    public TransactionState(String appId) {
        this.appId = appId;
        pid = INVALID_PID;
        epoch = 0;
        sequenceNumbers = new HashMap<>();
    }

    public TransactionState() {
        this(null);
    }

    public boolean pidIsSet() {
        return pid() != INVALID_PID;
    }

    public Long pid() {
        return pid;
    }

    public short epoch() {
        return epoch;
    }

    public void setPid(long pid) {
        if (this.pid == INVALID_PID) {
            this.pid = pid;
        }
    }

    public void setEpoch(short epoch) {
        if (this.epoch == 0) {
            this.epoch = epoch;
        }
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public Integer sequenceNumber(TopicPartition topicPartition) {
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        return sequenceNumbers.get(topicPartition);
    }


    public void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        int currentSequenceNumber = sequenceNumbers.get(topicPartition);
        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }

}

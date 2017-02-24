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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.requests.InitPidResponse.INVALID_PID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    private final String transactionalId;
    private volatile long pid;
    private short epoch;
    private boolean isStateValid;
    private final boolean idempotenceEnabled;
    private final Map<TopicPartition, Integer> sequenceNumbers;


    public TransactionState(String transactionalId, boolean idempotenceEnabled) {
        if (transactionalId != null && 0 < transactionalId.length() && !idempotenceEnabled) {
            throw new ConfigException("Need to set " + ProducerConfig.IDEMPOTENCE_ENABLED_CONFIG
                    + " to true in order to use transactions.");
        }
        this.transactionalId = transactionalId;
        pid = INVALID_PID;
        epoch = 0;
        sequenceNumbers = new HashMap<>();
        this.isStateValid = true;
        this.idempotenceEnabled = idempotenceEnabled;
    }

    public TransactionState() {
        this(false);
    }

    public TransactionState(boolean idempotenceEnabled) {
        this(null, idempotenceEnabled);
    }

    public boolean hasPid() {
        return pid != INVALID_PID;
    }

    public Long pid() {
        return pid;
    }

    public short epoch() {
        return epoch;
    }

    public boolean isIdempotenceEnabled() {
        return idempotenceEnabled;
    }

    public void setPid(long pid) {
        if (this.pid == INVALID_PID) {
            this.pid = pid;
        } else {
            throw new IllegalStateException("Cannot set multiple producer ids for a single producer.");
        }
    }

    public void setEpoch(short epoch) {
        if (this.epoch == 0) {
            this.epoch = epoch;
        }
    }

    public void setStateInvalid() {
        this.isStateValid = false;
    }

    public boolean isStateValid() {
        return isStateValid;
    }

    public String transactionalId() {
        return transactionalId;
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        if (!idempotenceEnabled) {
            throw new IllegalStateException("Attempting to access sequence numbers when idempotence is disabled");
        }
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        return sequenceNumbers.get(topicPartition);
    }


    public synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        if (!idempotenceEnabled) {
            throw new IllegalStateException("Attempt to modify sequence numbers when idempotence is disabled");
        }
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        int currentSequenceNumber = sequenceNumbers.get(topicPartition);
        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }

}

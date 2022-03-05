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
package org.apache.kafka.clients.telemetry;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the producer-level metrics.
 *
 * @see ProducerTopicMetricRecorder for details on recording topic-level metrics.
 */

public class DefaultProducerMetricRecorder extends AbstractClientMetricRecorder implements ProducerMetricRecorder {

    private static final String GROUP_NAME = "producer-telemetry";

    private final MetricName recordBytes;

    private final MetricName recordCount;

    private final MetricName queueMaxBytes;

    private final MetricName queueBytes;

    private final MetricName queueMaxMessages;

    private final MetricName queueMessages;

    public DefaultProducerMetricRecorder(Metrics metrics) {
        super(metrics);

        this.recordBytes = createMetricName(RECORD_BYTES_NAME, GROUP_NAME, RECORD_BYTES_DESCRIPTION);
        this.recordCount = createMetricName(RECORD_COUNT_NAME, GROUP_NAME, RECORD_COUNT_DESCRIPTION);
        this.queueMaxBytes = createMetricName(QUEUE_MAX_BYTES_NAME, GROUP_NAME, QUEUE_MAX_BYTES_DESCRIPTION);
        this.queueBytes = createMetricName(QUEUE_BYTES_NAME, GROUP_NAME, QUEUE_BYTES_DESCRIPTION);
        this.queueMaxMessages = createMetricName(QUEUE_MAX_MESSAGES_NAME, GROUP_NAME, QUEUE_MAX_MESSAGES_DESCRIPTION);
        this.queueMessages = createMetricName(QUEUE_MESSAGES_NAME, GROUP_NAME, QUEUE_MESSAGES_DESCRIPTION);
    }

    @Override
    public void recordRecordBytes(int amount) {
        gaugeSensor(recordBytes).record(amount);
    }

    @Override
    public void recordRecordCount(int amount) {
        gaugeSensor(recordCount).record(amount);
    }

    @Override
    public void recordQueueMaxBytes(int amount) {
        gaugeSensor(queueMaxBytes).record(amount);
    }

    @Override
    public void recordQueueBytes(int amount) {
        gaugeSensor(queueBytes).record(amount);
    }

    @Override
    public void recordQueueMaxMessages(int amount) {
        gaugeSensor(queueMaxMessages).record(amount);
    }

    @Override
    public void recordQueueMessages(int amount) {
        gaugeSensor(queueMessages).record(amount);
    }

}

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

import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.convertToReason;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.formatAcks;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the topic-level metrics for the
 * producer.
 *
 * @see ProducerMetricRecorder for details on the producer-level sensors.
 */

public class DefaultProducerTopicMetricRecorder extends AbstractClientMetricRecorder implements ProducerTopicMetricRecorder {

    private static final String GROUP_NAME = "producer-topic-telemetry";

    private final MetricNameTemplate queueBytes;

    private final MetricNameTemplate queueCount;

    private final MetricNameTemplate latency;

    private final MetricNameTemplate queueLatency;

    private final MetricNameTemplate recordRetries;

    private final MetricNameTemplate recordFailures;

    private final MetricNameTemplate recordSuccess;

    public DefaultProducerTopicMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> topicPartitionAcksTags = appendTags(tags, TOPIC_LABEL, PARTITION_LABEL, ACKS_LABEL);
        Set<String> topicPartitionAcksReasonTags = appendTags(topicPartitionAcksTags, REASON_LABEL);

        this.queueBytes = createMetricNameTemplate(QUEUE_BYTES_NAME, GROUP_NAME, QUEUE_BYTES_DESCRIPTION, topicPartitionAcksTags);
        this.queueCount = createMetricNameTemplate(QUEUE_COUNT_NAME, GROUP_NAME, QUEUE_COUNT_DESCRIPTION, topicPartitionAcksTags);
        this.latency = createMetricNameTemplate(LATENCY_NAME, GROUP_NAME, LATENCY_DESCRIPTION, topicPartitionAcksTags);
        this.queueLatency = createMetricNameTemplate(QUEUE_LATENCY_NAME, GROUP_NAME, QUEUE_LATENCY_DESCRIPTION, topicPartitionAcksTags);
        this.recordRetries = createMetricNameTemplate(RECORD_RETRIES_NAME, GROUP_NAME, RECORD_RETRIES_DESCRIPTION, topicPartitionAcksTags);
        this.recordFailures = createMetricNameTemplate(RECORD_FAILURES_NAME, GROUP_NAME, RECORD_FAILURES_DESCRIPTION, topicPartitionAcksReasonTags);
        this.recordSuccess = createMetricNameTemplate(RECORD_SUCCESS_NAME, GROUP_NAME, RECORD_SUCCESS_DESCRIPTION, topicPartitionAcksTags);
    }

    @Override
    public void queueBytes(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        gaugeSensor(queueBytes, metricsTags).record(amount);
    }

    @Override
    public void queueCount(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        gaugeSensor(queueCount, metricsTags).record(amount);
    }

    @Override
    public void latency(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        histogramSensor(latency, metricsTags).record(amount);
    }

    @Override
    public void queueLatency(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        histogramSensor(queueLatency, metricsTags).record(amount);
    }

    @Override
    public void recordRetries(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        sumSensor(recordRetries, metricsTags).record(amount);
    }

    @Override
    public void recordFailures(TopicPartition topicPartition, short acks, Throwable error, int amount) {
        String reason = convertToReason(error);
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        metricsTags.put(REASON_LABEL, reason);
        sumSensor(recordFailures, metricsTags).record(amount);
    }

    @Override
    public void recordSuccess(TopicPartition topicPartition, short acks, int amount) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        sumSensor(recordSuccess, metricsTags).record(amount);
    }

    private Map<String, String> getMetricsTags(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(ACKS_LABEL, formatAcks(acks));
        metricsTags.put(PARTITION_LABEL, String.valueOf(topicPartition.partition()));
        metricsTags.put(TOPIC_LABEL, topicPartition.topic());
        return metricsTags;
    }
}

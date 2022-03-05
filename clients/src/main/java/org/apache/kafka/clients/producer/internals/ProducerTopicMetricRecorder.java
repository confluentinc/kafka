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
package org.apache.kafka.clients.producer.internals;

import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.formatAcks;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.telemetry.AbstractClientMetricRecorder;
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

public class ProducerTopicMetricRecorder extends AbstractClientMetricRecorder {

    private static final String GROUP_NAME = "producer-topic-telemetry";

    private static final String ACKS_LABEL = "acks";

    private static final String PARTITION_LABEL = "partition";

    private static final String REASON_LABEL = "reason";

    private static final String TOPIC_LABEL = "topic";

    private final MetricNameTemplate queueBytes;

    private final MetricNameTemplate queueCount;

    private final MetricNameTemplate latency;

    private final MetricNameTemplate queueLatency;

    private final MetricNameTemplate recordRetries;

    private final MetricNameTemplate recordFailures;

    private final MetricNameTemplate recordSuccess;

    public ProducerTopicMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> topicPartitionAcksTags = appendTags(tags, "topic", "partition", "acks");
        Set<String> topicPartitionAcksReasonTags = appendTags(topicPartitionAcksTags, "reason");

        this.queueBytes = createTemplate("queue.bytes",
            "Number of bytes queued on partition queue.",
            topicPartitionAcksTags);
        this.queueCount = createTemplate("queue.count",
            "Number of records queued on partition queue.",
            topicPartitionAcksTags);
        this.latency = createTemplate("latency",
            "Total produce record latency, from application calling send()/produce() to ack received from broker.",
            topicPartitionAcksTags);
        this.queueLatency = createTemplate("queue.latency",
            "Time between send()/produce() and record being sent to broker.",
            topicPartitionAcksTags);
        this.recordRetries = createTemplate("record.retries",
            "Number of ProduceRequest retries.",
            topicPartitionAcksTags);
        this.recordFailures = createTemplate("record.failures",
            "Number of records that permanently failed delivery. Reason is a short string representation of the reason, which is typically the name of a Kafka protocol error code, e.g., “RequestTimedOut”.",
            topicPartitionAcksReasonTags);
        this.recordSuccess = createTemplate("record.success",
            "Number of records that have been successfully produced.",
            topicPartitionAcksTags);
    }

    public Sensor queueBytes(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return gaugeSensor(queueBytes, metricsTags);
    }

    public Sensor queueCount(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return gaugeSensor(queueCount, metricsTags);
    }

    public Sensor latency(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return histogramSensor(latency, metricsTags);
    }

    public Sensor queueLatency(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return histogramSensor(queueLatency, metricsTags);
    }

    public Sensor recordRetries(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return sumSensor(recordRetries, metricsTags);
    }

    public Sensor recordFailures(TopicPartition topicPartition, short acks, String reason) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        metricsTags.put(REASON_LABEL, reason);
        return sumSensor(recordFailures, metricsTags);
    }

    public Sensor recordSuccess(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = getMetricsTags(topicPartition, acks);
        return sumSensor(recordSuccess, metricsTags);
    }

    private Map<String, String> getMetricsTags(TopicPartition topicPartition, short acks) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(ACKS_LABEL, formatAcks(acks));
        metricsTags.put(PARTITION_LABEL, String.valueOf(topicPartition.partition()));
        metricsTags.put(TOPIC_LABEL, topicPartition.topic());
        return metricsTags;
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.producer.partition.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}

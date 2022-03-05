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
package org.apache.kafka.clients.consumer.internals;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.telemetry.AbstractClientMetricRecorder;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

public class ConsumerMetricRecorder extends AbstractClientMetricRecorder {

    private static final String GROUP_NAME = "consumer-telemetry";

    private static final String ERROR_LABEL = "error";

    private final MetricName pollInterval;

    private final MetricName pollLast;

    private final MetricName pollLatency;

    private final MetricName commitCount;

    private final MetricName groupAssignmentStrategy;

    private final MetricName groupAssignmentPartitionCount;

    private final MetricName assignmentPartitionCount;

    private final MetricName groupRebalanceCount;

    private final MetricNameTemplate groupErrorCount;

    private final MetricName recordQueueCount;

    private final MetricName recordQueueBytes;

    private final MetricName recordApplicationCount;

    private final MetricName recordApplicationBytes;

    private final MetricName fetchLatency;

    private final MetricName fetchCount;

    private final MetricName fetchFailures;

    public ConsumerMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> errorTags = appendTags(tags, "error");

        this.pollInterval = createMetricName("poll.interval",
            "The interval at which the application calls poll(), in seconds.");
        this.pollLast = createMetricName("poll.last",
            "The number of seconds since the last poll() invocation.");
        this.pollLatency = createMetricName("poll.latency",
            "The time it takes poll() to return a new message to the application.");
        this.commitCount = createMetricName("commit.count",
            "Number of commit requests sent.");
        this.groupAssignmentStrategy = createMetricName("group.assignment.strategy",
            "Current group assignment strategy in use.");
        this.groupAssignmentPartitionCount = createMetricName("group.assignment.partition.count",
            "Number of currently assigned partitions to this consumer by the group leader.");
        this.assignmentPartitionCount = createMetricName("assignment.partition.count",
            "Number of currently assigned partitions to this consumer, either through the group protocol or through assign().");
        this.groupRebalanceCount = createMetricName("group.error.count",
            "Number of group rebalances.");
        this.groupErrorCount = createTemplate("group.error.count",
            "Consumer group error counts. The error label depicts the actual error, e.g., \"MaxPollExceeded\", \"HeartbeatTimeout\", etc.",
            errorTags);
        this.recordQueueCount = createMetricName("record.queue.count",
            "Number of records in consumer pre-fetch queue.");
        this.recordQueueBytes = createMetricName("record.queue.bytes",
            "Amount of record memory in consumer pre-fetch queue. This may also include per-record overhead.");
        this.recordApplicationCount = createMetricName("record.application.count",
            "Number of records consumed by application.");
        this.recordApplicationBytes = createMetricName("record.application.bytes",
            "Memory of records consumed by application.");
        this.fetchLatency = createMetricName("fetch.latency",
            "FetchRequest latency.");
        this.fetchCount = createMetricName("fetch.count",
            "Total number of FetchRequests sent.");
        this.fetchFailures = createMetricName("fetch.failures",
            "Total number of FetchRequest failures.");
    }

    public Sensor pollInterval() {
        return histogramSensor(pollInterval);
    }

    public Sensor pollLast() {
        return gaugeSensor(pollLast);
    }

    public Sensor pollLatency() {
        return histogramSensor(pollLatency);
    }

    public Sensor commitCount() {
        return sumSensor(commitCount);
    }

    public Sensor groupAssignmentStrategy() {
        return stringSensor(groupAssignmentStrategy);
    }

    public Sensor groupAssignmentPartitionCount() {
        return gaugeSensor(groupAssignmentPartitionCount);
    }

    public Sensor assignmentPartitionCount() {
        return gaugeSensor(assignmentPartitionCount);
    }

    public Sensor groupRebalanceCount() {
        return sumSensor(groupRebalanceCount);
    }

    public Sensor groupErrorCount(String error) {
        Map<String, String> metricsTags = Collections.singletonMap(ERROR_LABEL, error);
        return sumSensor(groupErrorCount, metricsTags);
    }

    public Sensor recordQueueCount() {
        return gaugeSensor(recordQueueCount);
    }

    public Sensor recordQueueBytes() {
        return gaugeSensor(recordQueueBytes);
    }

    public Sensor recordApplicationCount() {
        return sumSensor(recordApplicationCount);
    }

    public Sensor recordApplicationBytes() {
        return sumSensor(recordApplicationBytes);
    }

    public Sensor fetchLatency() {
        return histogramSensor(fetchLatency);
    }

    public Sensor fetchCount() {
        return sumSensor(fetchCount);
    }

    public Sensor fetchFailures() {
        return sumSensor(fetchFailures);
    }

    private MetricName createMetricName(String unqualifiedName, String description) {
        return metrics.metricInstance(createTemplate(unqualifiedName, description, tags));
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.consumer.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}

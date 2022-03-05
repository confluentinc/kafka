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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;

public class DefaultConsumerMetricRecorder extends AbstractClientMetricRecorder implements ConsumerMetricRecorder {

    private static final String GROUP_NAME = "consumer-telemetry";

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

    public DefaultConsumerMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> errorTags = appendTags(tags, ERROR_LABEL);

        this.pollInterval = createMetricName(POLL_INTERVAL_NAME, GROUP_NAME, POLL_INTERVAL_DESCRIPTION);
        this.pollLast = createMetricName(POLL_LAST_NAME, GROUP_NAME, POLL_LAST_DESCRIPTION);
        this.pollLatency = createMetricName(POLL_LATENCY_NAME, GROUP_NAME, POLL_LATENCY_DESCRIPTION);
        this.commitCount = createMetricName(COMMIT_COUNT_NAME, GROUP_NAME, COMMIT_COUNT_DESCRIPTION);
        this.groupAssignmentStrategy = createMetricName(GROUP_ASSIGNMENT_STRATEGY_NAME, GROUP_NAME, GROUP_ASSIGNMENT_STRATEGY_DESCRIPTION);
        this.groupAssignmentPartitionCount = createMetricName(GROUP_ASSIGNMENT_PARTITION_COUNT_NAME, GROUP_NAME, GROUP_ASSIGNMENT_PARTITION_COUNT_DESCRIPTION);
        this.assignmentPartitionCount = createMetricName(ASSIGNMENT_PARTITION_COUNT_NAME, GROUP_NAME, ASSIGNMENT_PARTITION_COUNT_DESCRIPTION);
        this.groupRebalanceCount = createMetricName(GROUP_REBALANCE_COUNT_NAME, GROUP_NAME, GROUP_REBALANCE_COUNT_DESCRIPTION);
        this.groupErrorCount = createMetricNameTemplate(GROUP_ERROR_COUNT_NAME, GROUP_NAME, GROUP_ERROR_COUNT_DESCRIPTION, errorTags);
        this.recordQueueCount = createMetricName(RECORD_QUEUE_COUNT_NAME, GROUP_NAME, RECORD_QUEUE_COUNT_DESCRIPTION);
        this.recordQueueBytes = createMetricName(RECORD_QUEUE_BYTES_NAME, GROUP_NAME, RECORD_QUEUE_BYTES_DESCRIPTION);
        this.recordApplicationCount = createMetricName(RECORD_APPLICATION_COUNT_NAME, GROUP_NAME, RECORD_APPLICATION_COUNT_DESCRIPTION);
        this.recordApplicationBytes = createMetricName(RECORD_APPLICATION_BYTES_NAME, GROUP_NAME, RECORD_APPLICATION_BYTES_DESCRIPTION);
        this.fetchLatency = createMetricName(FETCH_LATENCY_NAME, GROUP_NAME, FETCH_LATENCY_DESCRIPTION);
        this.fetchCount = createMetricName(FETCH_COUNT_NAME, GROUP_NAME, FETCH_COUNT_DESCRIPTION);
        this.fetchFailures = createMetricName(FETCH_FAILURES_NAME, GROUP_NAME, FETCH_FAILURES_DESCRIPTION);
    }

    @Override
    public void recordPollInterval(int amount) {
        histogramSensor(pollInterval).record(amount);
    }

    @Override
    public void recordPollLast(int amount) {
        gaugeSensor(pollLast).record(amount);
    }

    @Override
    public void recordPollLatency(int amount) {
        histogramSensor(pollLatency).record(amount);
    }

    @Override
    public void recordCommitCount(int amount) {
        sumSensor(commitCount).record(amount);
    }

    @Override
    public void recordGroupAssignmentStrategy(int amount) {
        stringSensor(groupAssignmentStrategy).record(amount);
    }

    @Override
    public void recordGroupAssignmentPartitionCount(int amount) {
        gaugeSensor(groupAssignmentPartitionCount).record(amount);
    }

    @Override
    public void recordAssignmentPartitionCount(int amount) {
        gaugeSensor(assignmentPartitionCount).record(amount);
    }

    @Override
    public void recordGroupRebalanceCount(int amount) {
        sumSensor(groupRebalanceCount).record(amount);
    }

    @Override
    public void recordGroupErrorCount(String error, int amount) {
        Map<String, String> metricsTags = Collections.singletonMap(ERROR_LABEL, error);
        sumSensor(groupErrorCount, metricsTags).record(amount);
    }

    @Override
    public void recordRecordQueueCount(int amount) {
        gaugeSensor(recordQueueCount).record(amount);
    }

    @Override
    public void recordRecordQueueBytes(int amount) {
        gaugeSensor(recordQueueBytes).record(amount);
    }

    @Override
    public void recordRecordApplicationCount(int amount) {
        sumSensor(recordApplicationCount).record(amount);
    }

    @Override
    public void recordRecordApplicationBytes(int amount) {
        sumSensor(recordApplicationBytes).record(amount);
    }

    @Override
    public void recordFetchLatency(int amount) {
        histogramSensor(fetchLatency).record(amount);
    }

    @Override
    public void recordFetchCount(int amount) {
        sumSensor(fetchCount).record(amount);
    }

    @Override
    public void recordFetchFailures(int amount) {
        sumSensor(fetchFailures).record(amount);
    }
}

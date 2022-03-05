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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A recorder that exposes {@link Sensor}s used to record the client instance-level metrics.
 */

public class DefaultClientInstanceMetricRecorder extends AbstractClientMetricRecorder implements ClientInstanceMetricRecorder {

    private static final String GROUP_NAME = "client-telemetry";

    private final MetricNameTemplate connectionCreations;

    private final MetricName connectionCount;

    private final MetricNameTemplate connectionErrors;

    private final MetricNameTemplate requestRtt;

    private final MetricNameTemplate requestQueueLatency;

    private final MetricNameTemplate requestQueueCount;

    private final MetricNameTemplate requestSuccess;

    private final MetricNameTemplate requestErrors;

    private final MetricName ioWaitTime;

    public DefaultClientInstanceMetricRecorder(Metrics metrics) {
        super(metrics);

        Set<String> brokerIdTags = appendTags(tags, BROKER_ID_LABEL);
        Set<String> reasonTags = appendTags(tags, REASON_LABEL);
        Set<String> brokerIdRequestTypeTags = appendTags(brokerIdTags, REQUEST_TYPE_LABEL);
        Set<String> brokerIdRequestTypeReasonTags = appendTags(brokerIdRequestTypeTags, REASON_LABEL);

        this.connectionCreations = createTemplate("connection.creations",
            "Total number of broker connections made.",
            brokerIdTags);
        this.connectionCount = createMetricName("connection.count",
            "Current number of broker connections.");
        this.connectionErrors = createTemplate("connection.errors",
            "Total number of broker connection failures.",
            reasonTags);
        this.requestRtt = createTemplate("request.rtt",
            "Average request latency / round-trip-time to broker and back.",
            brokerIdRequestTypeTags);
        this.requestQueueLatency = createTemplate("request.queue.latency",
            "Average request queue latency waiting for request to be sent to broker.",
            brokerIdTags);
        this.requestQueueCount = createTemplate("request.queue.count",
            "Number of requests in queue waiting to be sent to broker.",
            brokerIdTags);
        this.requestSuccess = createTemplate("request.success",
            "Number of successful requests to broker, that is where a response is received without no request-level error (but there may be per-sub-resource errors, e.g., errors for certain partitions within an OffsetCommitResponse).",
            brokerIdRequestTypeTags);
        this.requestErrors = createTemplate("request.errors",
            "Number of failed requests.",
            brokerIdRequestTypeReasonTags);
        this.ioWaitTime = createMetricName("io.wait.time",
            "Amount of time waiting for socket I/O writability (POLLOUT). A high number indicates socket send buffer congestion.");
    }

    @Override
    public void recordConnectionCreations(String brokerId, int increment) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        sumSensor(connectionCreations, metricsTags).record(increment);
    }

    @Override
    public void recordConnectionCount(int increment) {
        gaugeSensor(connectionCount).record(increment);
    }

    @Override
    public void recordConnectionErrors(ConnectionErrorReason reason, int increment) {
        Map<String, String> metricsTags = Collections.singletonMap(REASON_LABEL, reason.toString());
        sumSensor(connectionErrors, metricsTags).record(increment);
    }

    @Override
    public void recordRequestRtt(String brokerId, String requestType, int increment) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        histogramSensor(requestRtt, metricsTags).record(increment);
    }

    @Override
    public void recordRequestQueueLatency(String brokerId, int increment) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        histogramSensor(requestQueueLatency, metricsTags).record(increment);
    }

    @Override
    public void recordRequestQueueCount(String brokerId, int increment) {
        Map<String, String> metricsTags = Collections.singletonMap(BROKER_ID_LABEL, brokerId);
        gaugeSensor(requestQueueCount, metricsTags).record(increment);
    }

    @Override
    public void recordRequestSuccess(String brokerId, String requestType, int increment) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        sumSensor(requestSuccess, metricsTags).record(increment);
    }

    @Override
    public void recordRequestErrors(String brokerId, String requestType, RequestErrorReason reason, int increment) {
        Map<String, String> metricsTags = new HashMap<>();
        metricsTags.put(BROKER_ID_LABEL, brokerId);
        metricsTags.put(REQUEST_TYPE_LABEL, requestType);
        metricsTags.put(REASON_LABEL, reason.toString());
        sumSensor(requestErrors, metricsTags).record(increment);
    }

    @Override
    public void recordIoWaitTime(int increment) {
        histogramSensor(ioWaitTime).record(increment);
    }

    private MetricName createMetricName(String unqualifiedName, String description) {
        return metrics.metricInstance(createTemplate(unqualifiedName, description, tags));
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}

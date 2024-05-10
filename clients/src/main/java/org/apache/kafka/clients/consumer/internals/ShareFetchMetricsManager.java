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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.WindowedCount;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.SHARE_CONSUMER_METRIC_GROUP_PREFIX;

public class ShareFetchMetricsManager {
    private final FetchMetricsRegistry metricsRegistry;
    private final Sensor throttleTime;
    private final Sensor bytesFetched;
    private final Sensor recordsFetched;
    private final Sensor fetchLatency;
    private final Sensor sentAcknowledgements;
    private final Sensor failedAcknowledgements;
    public final MetricName acknowledgementSendRate;
    public final MetricName acknowledgementSendTotal;
    public final MetricName acknowledgementErrorRate;
    public final MetricName acknowledgementErrorTotal;
    public final MetricName recordsFetchedRate;
    public final MetricName recordsFetchedTotal;
    public final MetricName recordsPerRequestAvg;
    public final MetricName recordsPerRequestMax;

    public ShareFetchMetricsManager(Metrics metrics, FetchMetricsRegistry fetchMetricsRegistry) {
        this.metricsRegistry = fetchMetricsRegistry;

        String grpName = SHARE_CONSUMER_METRIC_GROUP_PREFIX + "-fetch-manager-metrics";

        acknowledgementSendRate = metrics.metricName("acknowledgements-send-rate", grpName,
                "The average number of record acknowledgements sent per second.");
        acknowledgementSendTotal = metrics.metricName("acknowledgements-send-total", grpName,
                "The total number of record acknowledgements sent.");
        acknowledgementErrorRate = metrics.metricName("acknowledgements-error-rate", grpName,
                "The average number of record acknowledgements that resulted in errors per second.");
        acknowledgementErrorTotal = metrics.metricName("acknowledgements-error-total", grpName,
                "The total number of record acknowledgements that resulted in errors.");
        recordsFetchedRate = metrics.metricName("records-fetched-rate", grpName,
                "The average number of records fetched per second.");
        recordsFetchedTotal = metrics.metricName("records-fetched-total", grpName,
                "The total number of records fetched.");
        recordsPerRequestAvg = metrics.metricName("records-per-request-avg", grpName,
                "The average number of records in each request.");
        recordsPerRequestMax = metrics.metricName("records-per-request-max", grpName,
                "The maximum number of records in a request.");


        this.throttleTime = new SensorBuilder(metrics, "fetch-throttle-time")
                .withAvg(metricsRegistry.fetchThrottleTimeAvg)
                .withMax(metricsRegistry.fetchThrottleTimeMax)
                .build();
        this.bytesFetched = new SensorBuilder(metrics, "bytes-fetched")
                .withAvg(metricsRegistry.fetchSizeAvg)
                .withMax(metricsRegistry.fetchSizeMax)
                .withMeter(metricsRegistry.bytesConsumedRate, metricsRegistry.bytesConsumedTotal)
                .build();
        this.fetchLatency = new SensorBuilder(metrics, "fetch-latency")
                .withAvg(metricsRegistry.fetchLatencyAvg)
                .withMax(metricsRegistry.fetchLatencyMax)
                .withMeter(new WindowedCount(), metricsRegistry.fetchRequestRate, metricsRegistry.fetchRequestTotal)
                .build();
        this.recordsFetched = metrics.sensor("records-fetched");
        recordsFetched.add(recordsPerRequestAvg, new Avg());
        recordsFetched.add(recordsPerRequestMax, new Max());
        recordsFetched.add(new Meter(recordsFetchedRate, recordsFetchedTotal));

        this.sentAcknowledgements = metrics.sensor("sent-acknowledgements");
        sentAcknowledgements.add(new Meter(acknowledgementSendRate, acknowledgementSendTotal));

        this.failedAcknowledgements = metrics.sensor("failed-acknowledgements");
        failedAcknowledgements.add(new Meter(acknowledgementErrorRate, acknowledgementErrorTotal));
    }

    public Sensor throttleTimeSensor() {
        return throttleTime;
    }

    void recordLatency(long requestLatencyMs) {
        fetchLatency.record(requestLatencyMs);
    }

    void recordBytesFetched(int bytes) {
        bytesFetched.record(bytes);
    }

    void recordRecordsFetched(int records) {
        recordsFetched.record(records);
    }

    void recordAcknowledgementSent(int acknowledgements) {
        sentAcknowledgements.record(acknowledgements);
    }

    void recordFailedAcknowledgements(int acknowledgements) {
        failedAcknowledgements.record(acknowledgements);
    }
}

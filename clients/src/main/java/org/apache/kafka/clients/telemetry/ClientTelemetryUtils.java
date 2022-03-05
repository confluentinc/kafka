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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.telemetry.ClientInstanceMetricRecorder.ConnectionErrorReason;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Histogram;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryUtils {

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryUtils.class);

    public static final int DEFAULT_PUSH_INTERVAL_MS = 5 * 60 * 1000;

    public static long timeToNextUpdate(TelemetryState s, TelemetrySubscription subscription, Time time) {
        long t = 0;

        if (s == TelemetryState.subscription_needed) {
            // TODO: TELEMETRY_TODO: verify
            t = 0;

            // TODO: TELEMETRY_TODO: implement case where previous subscription had no metrics and
            //                       we need to wait pushIntervalMs() before requesting it again.
        } else  if (s == TelemetryState.terminated) {
            // TODO: TELEMETRY_TODO: verify and add a good error message
            throw new IllegalTelemetryStateException();
        } else {
            long milliseconds = time.milliseconds();

            if (subscription != null) {
                long fetchMs = subscription.fetchMs();
                long pushIntervalMs = subscription.pushIntervalMs();
                t = fetchMs + pushIntervalMs - milliseconds;

                if (t < 0)
                    t = 0;
            }
        }

        log.debug("For state {}, returning {} for time to next update", s, t);
        return t;
    }

    public static Set<MetricName> validateMetricNames(List<String> requestedMetrics) {
        Set<MetricName> set;

        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            // no metrics
            set = Collections.emptySet();
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            // TODO: TELEMETRY_TODO: determine the set of all metrics
            log.trace("Telemetry subscription has specified a single empty metric name; using all metrics");
            set = new HashSet<>();
        } else {
            // TODO: TELEMETRY_TODO: prefix string match...
            log.trace("Telemetry subscription has specified to include only metrics that are prefixed with the following strings: {}", requestedMetrics);
            set = new HashSet<>();
        }

        return set;
    }

    public static List<CompressionType> validateAcceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
        List<CompressionType> list = null;

        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            list = new ArrayList<>();

            for (Byte b : acceptedCompressionTypes) {
                int compressionId = b.intValue();

                try {
                    CompressionType compressionType = CompressionType.forId(compressionId);
                    list.add(compressionType);
                } catch (IllegalArgumentException e) {
                    log.warn("Accepted compression type with ID {} provided by broker is not a known compression type; ignoring", compressionId);
                }
            }
        }

        // If the set of accepted compression types provided by the server was empty or had
        // nothing valid in it, let's just return a non-null list, and we'll just end up using
        // no compression.
        if (list == null || list.isEmpty())
            list = Collections.emptyList();

        return list;
    }

    public static Uuid validateClientInstanceId(Uuid clientInstanceId) {
        if (clientInstanceId == null)
            throw new IllegalArgumentException("clientInstanceId must be non-null");

        return clientInstanceId.equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : clientInstanceId;
    }

    public static int validatePushIntervalMs(int pushIntervalMs) {
        if (pushIntervalMs < 0) {
            log.warn("Telemetry subscription push interval value from broker was invalid ({}), substituting a value of {}", pushIntervalMs, DEFAULT_PUSH_INTERVAL_MS);
            return DEFAULT_PUSH_INTERVAL_MS;
        }

        log.debug("Telemetry subscription push interval value from broker was {}", pushIntervalMs);
        return pushIntervalMs;
    }

    public static CompressionType preferredCompressionType(List<CompressionType> acceptedCompressionTypes) {
        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            // Broker is providing the compression types in order of preference. Grab the
            // first one.
            return acceptedCompressionTypes.get(0);
        } else {
            return CompressionType.NONE;
        }
    }

    public static MetricType metricType(KafkaMetric kafkaMetric) {
        Measurable measurable = kafkaMetric.measurable();

        if (measurable instanceof Gauge) {
            return MetricType.gauge;
        } else if (measurable instanceof Histogram) {
            return MetricType.histogram;
        } else if (measurable instanceof CumulativeSum) {
            return MetricType.sum;
        } else {
            throw new InvalidMetricTypeException("Could not determine metric type from measurable type " + measurable + " of metric " + kafkaMetric);
        }
    }

    public static List<TelemetryMetric> currentTelemetryMetrics(Collection<KafkaMetric> metrics,
        DeltaValueStore deltaValueStore,
        boolean deltaTemporality) {
        return metrics.stream().map(kafkaMetric -> {
            String name = kafkaMetric.metricName().name();
            Object metricValue = kafkaMetric.metricValue();

            // TODO: TELEMETRY_TODO: not sure if the metric value is always stored as a double,
            //                       but empirically it seems to be. Not sure if there is a better
            //                       way to handle this.
            double doubleValue = Double.parseDouble(metricValue.toString());
            long value = Double.valueOf(doubleValue).longValue();
            Measurable measurable = kafkaMetric.measurable();

            if (measurable instanceof CumulativeSum && deltaTemporality) {
                Long previousValue = deltaValueStore.getAndSet(kafkaMetric.metricName(), value);
                value = previousValue != null ? value - previousValue : value;
            }

            MetricType metricType = metricType(kafkaMetric);
            String description = kafkaMetric.metricName().description();

            return new TelemetryMetric(name, metricType, value, description);
        }).collect(Collectors.toList());
    }

    public static ByteBuffer serialize(Collection<TelemetryMetric> telemetryMetrics,
        CompressionType compressionType,
        TelemetrySerializer telemetrySerializer) {

        try {
            try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(1024)) {
                try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                    telemetrySerializer.serialize(telemetryMetrics, out);
                }

                return (ByteBuffer) compressedOut.buffer().flip();
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static ClientTelemetry maybeCreate(AbstractConfig config, Time time, String clientId) {
        if (config == null)
            throw new IllegalArgumentException("config for ClientTelemetry cannot be null");

        boolean enableMetricsPush = config.getBoolean(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG);
        return maybeCreate(enableMetricsPush, time, clientId);
    }

    public static ClientTelemetry maybeCreate(boolean enableMetricsPush, Time time, String clientId) {
        if (enableMetricsPush)
            return new DefaultClientTelemetry(time, clientId);
        else
            return new NoopClientTelemetry();
    }

    public static String convertToReason(Throwable error) {
        // TODO: TELEMETRY_TODO: properly convert the error to a "reason"
        return String.valueOf(error);
    }

    public static ConnectionErrorReason convertToConnectionErrorReason(Errors errors) {
        // TODO: TELEMETRY_TODO: there's no way this mapping is correct...
        switch (errors) {
            case NETWORK_EXCEPTION:
                return ConnectionErrorReason.disconnect;

            case CLUSTER_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTHORIZATION_FAILED:
            case DELEGATION_TOKEN_AUTH_DISABLED:
            case GROUP_AUTHORIZATION_FAILED:
            case SASL_AUTHENTICATION_FAILED:
            case TOPIC_AUTHORIZATION_FAILED:
            case TRANSACTIONAL_ID_AUTHORIZATION_FAILED:
                return ConnectionErrorReason.auth;

            case REQUEST_TIMED_OUT:
                return ConnectionErrorReason.timeout;

            default:
                // TODO: TELEMETRY_TODO: I think we might need an "unknown" case in the KIP.
                //       change this VVVVVVVVVVVVVVV
                return ConnectionErrorReason.timeout;
        }
    }

    public static int calculateQueueBytes(ApiVersions apiVersions,
        long timestamp,
        byte[] key,
        byte[] value,
        Header[] headers) {
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int offsetDelta = -1;
        byte magic = apiVersions.maxUsableProduceMagic();

        if (magic > RecordBatch.MAGIC_VALUE_V1) {
            return DefaultRecord.sizeInBytes(offsetDelta,
                timestamp,
                key != null ? key.length : 0,
                value != null ? value.length : 0,
                headers);
        } else {
            return LegacyRecord.recordSize(magic,
                key != null ? key.length : 0,
                value != null ? value.length : 0);
        }
    }

    public static void incrementQueueBytesTelemetry(ClientTelemetry clientTelemetry,
        ApiVersions apiVersions,
        short acks,
        TopicPartition tp,
        long timestamp,
        byte[] key,
        byte[] value,
        Header[] headers) {
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int size = calculateQueueBytes(apiVersions, timestamp, key, value, headers);

        ProducerMetricRecorder producerMetricRecorder = clientTelemetry.producerMetricRecorder();
        ProducerTopicMetricRecorder producerTopicMetricRecorder = clientTelemetry.producerTopicMetricRecorder();

        producerMetricRecorder.recordQueueBytes(size);
        producerMetricRecorder.recordQueueMessages(1);

        producerTopicMetricRecorder.queueBytes(tp, acks, size);
        producerTopicMetricRecorder.queueCount(tp, acks, 1);
    }

    public static void decrementQueueBytesTelemetry(ClientTelemetry clientTelemetry,
        short acks,
        TopicPartition tp,
        int size) {
        // TODO: TELEMETRY_TODO: we need an accurate record count passed in. I don't yet know
        //       how to get it from the RecordBatch or MemoryRecord or ???
        // TODO: TELEMETRY_TODO: need to know the proper place to call this
        // TODO: TELEMETRY_TODO: need to know the proper means/place to determine the size
        int recordCount = 0;

        ProducerMetricRecorder producerMetricRecorder = clientTelemetry.producerMetricRecorder();
        ProducerTopicMetricRecorder producerTopicMetricRecorder = clientTelemetry.producerTopicMetricRecorder();

        producerMetricRecorder.recordQueueBytes(-size);
        producerMetricRecorder.recordQueueMessages(-recordCount);
        producerTopicMetricRecorder.queueBytes(tp, acks, -size);
        producerTopicMetricRecorder.queueCount(tp, acks, -recordCount);
    }

    public static String formatAcks(short acks) {
        // TODO: TELEMETRY_TODO: this mapping needs to be verified
        switch (acks) {
            case 0:
                return "none";

            case 1:
                return "leader";

            default:
                return "all";
        }
    }

}

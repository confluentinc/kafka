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

import java.util.Set;
import org.apache.kafka.clients.telemetry.AbstractClientMetricRecorder;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the producer-level metrics.
 *
 * @see ProducerTopicMetricRecorder for details on the topic-level sensors.
 */

public class ProducerMetricRecorder extends AbstractClientMetricRecorder {

    private static final String GROUP_NAME = "producer-telemetry";

    private final MetricName recordBytes;

    private final MetricName recordCount;

    private final MetricName queueMaxBytes;

    private final MetricName queueBytes;

    private final MetricName queueMaxMessages;

    private final MetricName queueMessages;

    public ProducerMetricRecorder(Metrics metrics) {
        super(metrics);

        this.recordBytes = createMetricName("record.bytes",
            "Total number of record memory currently in use by producer. This includes the record fields (key, value, etc) as well as any implementation specific overhead (objects, etc).");
        this.recordCount = createMetricName("record.count",
            "Total number of records currently handled by producer.");
        this.queueMaxBytes = createMetricName("queue.max.bytes",
            "Total amount of queue/buffer memory allowed on the producer queue(s).");
        this.queueBytes = createMetricName("queue.bytes",
            "Current amount of memory used in producer queues.");
        this.queueMaxMessages = createMetricName("queue.max.messages",
            "Maximum amount of messages allowed on the producer queue(s).");
        this.queueMessages = createMetricName("queue.messages",
            "Current number of messages on the producer queue(s).");
    }

    public Sensor recordBytes() {
        return gaugeSensor(recordBytes);
    }

    public Sensor recordCount() {
        return gaugeSensor(recordCount);
    }

    public Sensor queueMaxBytes() {
        return gaugeSensor(queueMaxBytes);
    }

    public Sensor queueBytes() {
        return gaugeSensor(queueBytes);
    }

    public Sensor queueMaxMessages() {
        return gaugeSensor(queueMaxMessages);
    }

    public Sensor queueMessages() {
        return gaugeSensor(queueMessages);
    }

    private MetricName createMetricName(String unqualifiedName, String description) {
        return metrics.metricInstance(createTemplate(unqualifiedName, description, tags));
    }

    private MetricNameTemplate createTemplate(String unqualifiedName, String description, Set<String> tags) {
        String qualifiedName = String.format("org.apache.kafka.client.producer.%s", unqualifiedName);
        return createTemplate(qualifiedName, GROUP_NAME, description, tags);
    }

}

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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the topic-level metrics for the
 * producer.
 *
 * @see ProducerMetricRecorder for details on the producer-level sensors.
 */

public interface ProducerTopicMetricRecorder extends ClientMetricRecorder {

    String ACKS_LABEL = "acks";

    String PARTITION_LABEL = "partition";

    String REASON_LABEL = "reason";

    String TOPIC_LABEL = "topic";

    String PREFIX = ProducerMetricRecorder.PREFIX + "partition.";

    String QUEUE_BYTES_NAME = PREFIX + "queue.bytes";

    String QUEUE_BYTES_DESCRIPTION = "Number of bytes queued on partition queue.";

    String QUEUE_COUNT_NAME = PREFIX + "queue.count";

    String QUEUE_COUNT_DESCRIPTION = "Number of records queued on partition queue.";

    String LATENCY_NAME = PREFIX + "latency";

    String LATENCY_DESCRIPTION = "Total produce record latency, from application calling send()/produce() to ack received from broker.";

    String QUEUE_LATENCY_NAME = PREFIX + "queue.latency";

    String QUEUE_LATENCY_DESCRIPTION = "Time between send()/produce() and record being sent to broker.";

    String RECORD_RETRIES_NAME = PREFIX + "record.retries";

    String RECORD_RETRIES_DESCRIPTION = "Number of ProduceRequest retries.";

    String RECORD_FAILURES_NAME = PREFIX + "record.failures";

    String RECORD_FAILURES_DESCRIPTION = "Number of records that permanently failed delivery. Reason is a short string representation of the reason, which is typically the name of a Kafka protocol error code, e.g., “RequestTimedOut”.";

    String RECORD_SUCCESS_NAME = PREFIX + "record.success";

    String RECORD_SUCCESS_DESCRIPTION = "Number of records that have been successfully produced.";

    void queueBytes(TopicPartition topicPartition, short acks, int amount);

    void queueCount(TopicPartition topicPartition, short acks, int amount);

    void latency(TopicPartition topicPartition, short acks, int amount);

    void queueLatency(TopicPartition topicPartition, short acks, int amount);

    void recordRetries(TopicPartition topicPartition, short acks, int amount);

    void recordFailures(TopicPartition topicPartition, short acks, Throwable error, int amount);

    void recordSuccess(TopicPartition topicPartition, short acks, int amount);
}

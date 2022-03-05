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

import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the producer-level metrics.
 *
 * @see ProducerTopicMetricRecorder for details on the topic-level sensors.
 */
public interface ProducerMetricRecorder extends ClientMetricRecorder {

    String PREFIX = ClientMetricRecorder.PREFIX + "producer.";

    String RECORD_BYTES_NAME = PREFIX + "record.bytes";

    String RECORD_BYTES_DESCRIPTION = "Total number of record memory currently in use by producer. This includes the record fields (key, value, etc) as well as any implementation specific overhead (objects, etc).";

    String RECORD_COUNT_NAME = PREFIX + "record.count";

    String RECORD_COUNT_DESCRIPTION = "Total number of records currently handled by producer.";

    String QUEUE_MAX_BYTES_NAME = PREFIX + "queue.max.bytes";

    String QUEUE_MAX_BYTES_DESCRIPTION = "Total amount of queue/buffer memory allowed on the producer queue(s).";

    String QUEUE_BYTES_NAME = PREFIX + "queue.bytes";

    String QUEUE_BYTES_DESCRIPTION = "Current amount of memory used in producer queues.";

    String QUEUE_MAX_MESSAGES_NAME = PREFIX + "queue.max.messages";

    String QUEUE_MAX_MESSAGES_DESCRIPTION = "Maximum amount of messages allowed on the producer queue(s).";

    String QUEUE_MESSAGES_NAME = PREFIX + "queue.messages";

    String QUEUE_MESSAGES_DESCRIPTION = "Current number of messages on the producer queue(s).";

    void recordRecordBytes(int amount);

    void recordRecordCount(int amount);

    void recordQueueMaxBytes(int amount);

    void recordQueueBytes(int amount);

    void recordQueueMaxMessages(int amount);

    void recordQueueMessages(int amount);

}

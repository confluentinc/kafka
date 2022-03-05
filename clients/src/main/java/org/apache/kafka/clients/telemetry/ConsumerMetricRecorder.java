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

public interface ConsumerMetricRecorder extends ClientMetricRecorder {

    String PREFIX = ClientMetricRecorder.PREFIX + "consumer.";

    String POLL_INTERVAL_NAME = PREFIX + "poll.interval";

    String POLL_INTERVAL_DESCRIPTION =  "The interval at which the application calls poll(), in seconds.";

    String POLL_LAST_NAME = PREFIX + "poll.last";

    String POLL_LAST_DESCRIPTION = "The number of seconds since the last poll() invocation.";

    String POLL_LATENCY_NAME = PREFIX + "poll.latency";

    String POLL_LATENCY_DESCRIPTION = "The time it takes poll() to return a new message to the application.";

    String COMMIT_COUNT_NAME = PREFIX + "commit.count";

    String COMMIT_COUNT_DESCRIPTION = "Number of commit requests sent.";

    String GROUP_ASSIGNMENT_STRATEGY_NAME = PREFIX + "group.assignment.strategy";

    String GROUP_ASSIGNMENT_STRATEGY_DESCRIPTION = "Current group assignment strategy in use.";

    String GROUP_ASSIGNMENT_PARTITION_COUNT_NAME = PREFIX + "group.assignment.partition.count";

    String GROUP_ASSIGNMENT_PARTITION_COUNT_DESCRIPTION = "Number of currently assigned partitions to this consumer by the group leader.";

    String ASSIGNMENT_PARTITION_COUNT_NAME = PREFIX + "assignment.partition.count";

    String ASSIGNMENT_PARTITION_COUNT_DESCRIPTION = "Number of currently assigned partitions to this consumer, either through the group protocol or through assign().";

    String GROUP_REBALANCE_COUNT_NAME = PREFIX + "group.error.count";

    String GROUP_REBALANCE_COUNT_DESCRIPTION = "Number of group rebalances.";

    String GROUP_ERROR_COUNT_NAME = PREFIX + "group.error.count";

    String GROUP_ERROR_COUNT_DESCRIPTION = "Consumer group error counts. The error label depicts the actual error, e.g., \"MaxPollExceeded\", \"HeartbeatTimeout\", etc.";

    String RECORD_QUEUE_COUNT_NAME = PREFIX + "record.queue.count";

    String RECORD_QUEUE_COUNT_DESCRIPTION = "Number of records in consumer pre-fetch queue.";

    String RECORD_QUEUE_BYTES_NAME = PREFIX + "record.queue.bytes";

    String RECORD_QUEUE_BYTES_DESCRIPTION = "Amount of record memory in consumer pre-fetch queue. This may also include per-record overhead.";

    String RECORD_APPLICATION_COUNT_NAME = PREFIX + "record.application.count";

    String RECORD_APPLICATION_COUNT_DESCRIPTION = "Number of records consumed by application.";

    String RECORD_APPLICATION_BYTES_NAME = PREFIX + "record.application.bytes";

    String RECORD_APPLICATION_BYTES_DESCRIPTION = "Memory of records consumed by application.";

    String FETCH_LATENCY_NAME = PREFIX + "fetch.latency";

    String FETCH_LATENCY_DESCRIPTION = "FetchRequest latency.";

    String FETCH_COUNT_NAME = PREFIX + "fetch.count";

    String FETCH_COUNT_DESCRIPTION = "Total number of FetchRequests sent.";

    String FETCH_FAILURES_NAME = PREFIX + "fetch.failures";

    String FETCH_FAILURES_DESCRIPTION = "Total number of FetchRequest failures.";

    String ERROR_LABEL = "error";

    void recordPollInterval(int amount);

    void recordPollLast(int amount);

    void recordPollLatency(int amount);

    void recordCommitCount(int amount);

    void recordGroupAssignmentStrategy(int amount);

    void recordGroupAssignmentPartitionCount(int amount);

    void recordAssignmentPartitionCount(int amount);

    void recordGroupRebalanceCount(int amount);

    void recordGroupErrorCount(String error, int amount);

    void recordRecordQueueCount(int amount);

    void recordRecordQueueBytes(int amount);

    void recordRecordApplicationCount(int amount);

    void recordRecordApplicationBytes(int amount);

    void recordFetchLatency(int amount);

    void recordFetchCount(int amount);

    void recordFetchFailures(int amount);

}

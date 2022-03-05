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

import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.requests.AbstractRequest;

public class NoopClientTelemetry implements ClientTelemetry {

    @Override
    public void close() {
    }

    @Override
    public Optional<String> clientInstanceId(Duration timeout) {
        return Optional.empty();
    }

    @Override
    public void setState(TelemetryState state) {
    }

    @Override
    public TelemetryState state() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TelemetrySubscription subscription() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void telemetrySubscriptionFailed(Throwable error) {
    }

    @Override
    public void pushTelemetryFailed(Throwable error) {
    }

    @Override
    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
    }

    @Override
    public void pushTelemetrySucceeded() {
    }

    @Override
    public long timeToNextUpdate() {
        return Long.MAX_VALUE;
    }

    @Override
    public AbstractRequest.Builder<?> createRequest() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConsumerMetricRecorder consumerMetricRecorder() {
        return new ConsumerMetricRecorder() {
            @Override
            public void recordPollInterval(int amount) {
                
            }

            @Override
            public void recordPollLast(int amount) {

            }

            @Override
            public void recordPollLatency(int amount) {

            }

            @Override
            public void recordCommitCount(int amount) {

            }

            @Override
            public void recordGroupAssignmentStrategy(int amount) {

            }

            @Override
            public void recordGroupAssignmentPartitionCount(int amount) {

            }

            @Override
            public void recordAssignmentPartitionCount(int amount) {

            }

            @Override
            public void recordGroupRebalanceCount(int amount) {

            }

            @Override
            public void recordGroupErrorCount(String error, int amount) {

            }

            @Override
            public void recordRecordQueueCount(int amount) {

            }

            @Override
            public void recordRecordQueueBytes(int amount) {

            }

            @Override
            public void recordRecordApplicationCount(int amount) {

            }

            @Override
            public void recordRecordApplicationBytes(int amount) {

            }

            @Override
            public void recordFetchLatency(int amount) {

            }

            @Override
            public void recordFetchCount(int amount) {

            }

            @Override
            public void recordFetchFailures(int amount) {

            }
        };
    }

    @Override
    public ClientInstanceMetricRecorder clientInstanceMetricRecorder() {
        return new ClientInstanceMetricRecorder() {
            @Override
            public void recordConnectionCreations(String brokerId, int amount) {

            }

            @Override
            public void recordConnectionCount(int amount) {

            }

            @Override
            public void recordConnectionErrors(ConnectionErrorReason reason, int amount) {

            }

            @Override
            public void recordRequestRtt(String brokerId, String requestType, int amount) {

            }

            @Override
            public void recordRequestQueueLatency(String brokerId, int amount) {

            }

            @Override
            public void recordRequestQueueCount(String brokerId, int amount) {

            }

            @Override
            public void recordRequestSuccess(String brokerId, String requestType, int amount) {

            }

            @Override
            public void recordRequestErrors(String brokerId, String requestType, RequestErrorReason reason, int amount) {

            }

            @Override
            public void recordIoWaitTime(int amount) {

            }
        };
    }

    @Override
    public HostProcessMetricRecorder hostProcessMetricRecorder() {
        return new HostProcessMetricRecorder() {
            @Override
            public void recordMemoryBytes(long amount) {

            }

            @Override
            public void recordCpuUserTime(int amount) {

            }

            @Override
            public void recordCpuSystemTime(int amount) {

            }

            @Override
            public void recordPid(short amount) {

            }
        };
    }

    @Override
    public ProducerMetricRecorder producerMetricRecorder() {
        return new ProducerMetricRecorder() {
            @Override
            public void recordRecordBytes(int amount) {
                
            }

            @Override
            public void recordRecordCount(int amount) {

            }

            @Override
            public void recordQueueMaxBytes(int amount) {

            }

            @Override
            public void recordQueueBytes(int amount) {

            }

            @Override
            public void recordQueueMaxMessages(int amount) {

            }

            @Override
            public void recordQueueMessages(int amount) {

            }
        };
    }

    @Override
    public ProducerTopicMetricRecorder producerTopicMetricRecorder() {
        return new ProducerTopicMetricRecorder() {
            @Override
            public void queueBytes(TopicPartition topicPartition, short acks, int amount) {

            }

            @Override
            public void queueCount(TopicPartition topicPartition, short acks, int amount) {

            }

            @Override
            public void latency(TopicPartition topicPartition, short acks, int amount) {

            }

            @Override
            public void queueLatency(TopicPartition topicPartition, short acks, int amount) {

            }

            @Override
            public void recordRetries(TopicPartition topicPartition, short acks, int amount) {

            }

            @Override
            public void recordFailures(TopicPartition topicPartition, short acks, Throwable error, int amount) {

            }

            @Override
            public void recordSuccess(TopicPartition topicPartition, short acks, int amount) {

            }
        };
    }
}

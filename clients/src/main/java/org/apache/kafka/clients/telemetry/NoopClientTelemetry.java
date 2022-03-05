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
import org.apache.kafka.clients.consumer.internals.ConsumerMetricRecorder;
import org.apache.kafka.clients.producer.internals.ProducerMetricRecorder;
import org.apache.kafka.clients.producer.internals.ProducerTopicMetricRecorder;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public ClientInstanceMetricRecorder clientInstanceMetricRecorder() {
        return new ClientInstanceMetricRecorder() {
            @Override
            public void recordConnectionCreations(final String brokerId, final int amount) {

            }

            @Override
            public void recordConnectionCount(final int amount) {

            }

            @Override
            public void recordConnectionErrors(final ConnectionErrorReason reason,
                final int amount) {

            }

            @Override
            public void recordRequestRtt(final String brokerId, final String requestType,
                final int amount) {

            }

            @Override
            public void recordRequestQueueLatency(final String brokerId, final int amount) {

            }

            @Override
            public void recordRequestQueueCount(final String brokerId, final int amount) {

            }

            @Override
            public void recordRequestSuccess(final String brokerId, final String requestType,
                final int amount) {

            }

            @Override
            public void recordRequestErrors(final String brokerId, final String requestType,
                final RequestErrorReason reason, final int amount) {

            }

            @Override
            public void recordIoWaitTime(final int amount) {

            }
        };
    }

    @Override
    public HostProcessMetricRecorder hostProcessMetricRecorder() {
        return new HostProcessMetricRecorder() {
            @Override
            public void recordMemoryBytes(final long amount) {

            }

            @Override
            public void recordCpuUserTime(final long amount) {

            }

            @Override
            public void recordCpuSystemTime(final long amount) {

            }

            @Override
            public void recordPid(final long amount) {

            }
        };
    }

    @Override
    public ProducerMetricRecorder producerMetricRecorder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProducerTopicMetricRecorder producerTopicMetricRecorder() {
        throw new UnsupportedOperationException();
    }
}

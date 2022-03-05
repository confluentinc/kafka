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

/**
 * A {@link ClientMetricRecorder} that exposes methods to record the client instance-level metrics.
 */

public interface ClientInstanceMetricRecorder extends ClientMetricRecorder {

    enum ConnectionErrorReason {
        auth, close, disconnect, timeout, TLS
    }

    enum RequestErrorReason {
        disconnect, error, timeout
    }

    String BROKER_ID_LABEL = "broker_id";

    String REASON_LABEL = "reason";

    String REQUEST_TYPE_LABEL = "request_type";

    void recordConnectionCreations(String brokerId, int amount);

    void recordConnectionCount(int amount);

    void recordConnectionErrors(ConnectionErrorReason reason, int amount);

    void recordRequestRtt(String brokerId, String requestType, int amount);

    void recordRequestQueueLatency(String brokerId, int amount);

    void recordRequestQueueCount(String brokerId, int amount);

    void recordRequestSuccess(String brokerId, String requestType, int amount);

    void recordRequestErrors(String brokerId, String requestType, RequestErrorReason reason, int amount);

    void recordIoWaitTime(int amount);

}

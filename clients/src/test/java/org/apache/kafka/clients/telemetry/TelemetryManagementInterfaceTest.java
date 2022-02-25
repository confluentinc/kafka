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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TelemetryManagementInterfaceTest {

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testSerialize(CompressionType compressionType) throws IOException {
        TelemetryMetric telemetryMetric1 = newTelemetryMetric("test-1", 42);
        TelemetryMetric telemetryMetric2 = newTelemetryMetric("test-2", 123);
        StringTelemetrySerializer telemetrySerializer = new StringTelemetrySerializer();
        List<TelemetryMetric> telemetryMetrics = Arrays.asList(telemetryMetric1, telemetryMetric2);
        ByteBuffer compressed = TelemetryManagementInterface.serialize(telemetryMetrics,
            compressionType,
            telemetrySerializer);

        ByteBuffer decompressed = ByteBuffer.allocate(10000);
        try (InputStream in = compressionType.wrapForInput(compressed, RecordBatch.CURRENT_MAGIC_VALUE, BufferSupplier.create())) {
            Utils.readFully(in, decompressed);
        }

        String s = new String(Utils.readBytes((ByteBuffer) decompressed.flip()));
        String expected = String.format("%s: %s\n%s: %s\n", telemetryMetric1.name(), telemetryMetric1.value(), telemetryMetric2.name(), telemetryMetric2.value());
        assertEquals(expected, s);
    }

    @Test
    public void testMaybeCreateFailsIfClientIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> TelemetryManagementInterface.maybeCreate(true, new MockTime(), null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaybeCreateFailsIfClientIdIsNull(boolean enableMetricsPush) {
        Time time = new MockTime();
        testMaybeCreateFailsIfParametersAreNull(enableMetricsPush, time, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMaybeCreateFailsIfParametersAreNull(boolean enableMetricsPush) {
        String clientId = "test-client";
        testMaybeCreateFailsIfParametersAreNull(enableMetricsPush, null, clientId);
    }

    //
//    @ParameterizedTest
//    @ValueSource(booleans = {true, false}, strings = {NULL_CLIENT_ID, "test-client"})
//    public void testMaybeCreateFailsIfParametersAreNull(boolean enableMetricsPush, String clientId) {
//        Class<NullPointerException> e = NullPointerException.class;
//
//        for (ProducerConfig config : Arrays.asList(null, newConfig(enableMetricsPush))) {
//            for (Time time : Arrays.asList(null, new MockTime())) {
//                for (String clientId : Arrays.asList(null, "test-client")) {
//                    // It ain't gonna throw an exception if they're all valid.
//                    if (config != null && time != null && clientId != null)
//                        continue;
//
//                    assertThrows(e,
//                        () -> TelemetryManagementInterface.maybeCreate(config, time, clientId),
//                        String.format("maybeCreate should have thrown a %s for enableMetricsPush: %s, config: %s, time: %s, clientId: %s",  e.getName(), enableMetricsPush, config, time, clientId));
//                }
//            }
//        }
//
//        for (Time time : Arrays.asList(null, new MockTime())) {
//            for (String clientId : Arrays.asList(null, "test-client")) {
//                assertThrows(e,
//                    () -> TelemetryManagementInterface.maybeCreate(enableMetricsPush, time, clientId),
//                    String.format("maybeCreate should have thrown a %s for enableMetricsPush: %s, time: %s, clientId: %s",  e.getName(), enableMetricsPush, time, clientId));
//            }
//        }
//    }

    @Test
    public void testValidateTransitionForInitialized() {
        TelemetryState currState = TelemetryState.initialized;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown w/o having done anything' case
        validStates.add(TelemetryState.terminating);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForSubscriptionNeeded() {
        TelemetryState currState = TelemetryState.subscription_needed;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.subscription_in_progress);

        // 'Start shutdown w/o having done anything' case
        validStates.add(TelemetryState.terminating);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForSubscriptionInProgress() {
        TelemetryState currState = TelemetryState.subscription_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.push_needed);

        // 'Subscription had errors or requested/matches no metrics' case
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while waiting for the subscription' case
        validStates.add(TelemetryState.terminating);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushNeeded() {
        TelemetryState currState = TelemetryState.push_needed;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.push_in_progress);

        // 'Attempt to send push request failed (maybe a network issue?), so loop back to getting
        // the subscription' case
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while waiting for a telemetry push' case
        validStates.add(TelemetryState.terminating);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForPushInProgress() {
        TelemetryState currState = TelemetryState.push_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.subscription_needed);

        // 'Start shutdown while we happen to be pushing telemetry' case
        validStates.add(TelemetryState.terminating);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminating() {
        TelemetryState currState = TelemetryState.terminating;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.terminating_push_in_progress);

        // 'Forced shutdown w/o terminal push' case
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminatingPushInProgress() {
        TelemetryState currState = TelemetryState.terminating_push_in_progress;

        List<TelemetryState> validStates = new ArrayList<>();
        // Happy path...
        validStates.add(TelemetryState.terminated);

        testValidateTransition(currState, validStates);
    }

    @Test
    public void testValidateTransitionForTerminated() {
        TelemetryState currState = TelemetryState.terminated;

        // There's no transitioning out of the terminated state
        testValidateTransition(currState, Collections.emptyList());
    }

    private void testValidateTransition(TelemetryState oldState, List<TelemetryState> validStates) {
        for (TelemetryState newState : validStates)
            TelemetryManagementInterface.validateTransition(oldState, newState);

        // Have to copy to a new list because asList returns an unmodifiable list
        List<TelemetryState> invalidStates = new ArrayList<>(Arrays.asList(TelemetryState.values()));

        // Remove the valid states from the list of all states, leaving only the invalid
        invalidStates.removeAll(validStates);

        for (TelemetryState newState : invalidStates) {
            Executable e = () -> TelemetryManagementInterface.validateTransition(oldState, newState);
            String unexpectedSuccessMessage = "Should have thrown an IllegalTelemetryStateException for transitioning from " + oldState + " to " + newState;
            assertThrows(IllegalTelemetryStateException.class, e, unexpectedSuccessMessage);
        }
    }

    private void testMaybeCreateFailsIfParametersAreNull(boolean enableMetricsPush, Time time, String clientId) {
        // maybeCreate won't (or at least it shouldn't) fail if these are both non-null
        if (time != null && clientId != null)
            return;

        // maybeCreate won't fail if we don't attempt to construct metrics in the first place
        if (!enableMetricsPush)
            return;

        Class<IllegalArgumentException> e = IllegalArgumentException.class;

        assertThrows(e,
            () -> TelemetryManagementInterface.maybeCreate(enableMetricsPush, time, clientId),
            String.format("maybeCreate should have thrown a %s for enableMetricsPush: %s, time: %s, clientId: %s", e.getName(), enableMetricsPush, time, clientId));

        ProducerConfig config = newConfig(enableMetricsPush);
        assertThrows(e,
            () -> TelemetryManagementInterface.maybeCreate(config, time, clientId),
            String.format("maybeCreate should have thrown a %s for config: %s, time: %s, clientId: %s",  e.getName(), config.getBoolean(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG), time, clientId));
    }

    private TelemetryMetric newTelemetryMetric(String name, long value) {
        return new TelemetryMetric(name,
            MetricType.sum,
            value,
            "Description for " + name);
    }

    private ProducerConfig newConfig(Boolean enableMetricsPush) {
        Map<String, Object> map = new HashMap<>();
        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        if (enableMetricsPush != null)
            map.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, enableMetricsPush);

        return new ProducerConfig(map);
    }

}

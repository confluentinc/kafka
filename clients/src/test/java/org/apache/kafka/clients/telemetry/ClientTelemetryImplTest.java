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
import java.util.List;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTelemetryImplTest {

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    public void testSerialize(CompressionType compressionType) throws IOException {
        TelemetryMetric telemetryMetric1 = newTelemetryMetric("test-1", 42);
        TelemetryMetric telemetryMetric2 = newTelemetryMetric("test-2", 123);
        StringTelemetrySerializer telemetrySerializer = new StringTelemetrySerializer();
        List<TelemetryMetric> telemetryMetrics = Arrays.asList(telemetryMetric1, telemetryMetric2);
        ByteBuffer compressed = ClientTelemetryUtils.serialize(telemetryMetrics,
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
    public void testValidateMetricNames() {
        // empty metric names
        assertTrue(ClientTelemetryUtils.validateMetricNames(Collections.emptyList()).isEmpty());
        assertTrue(ClientTelemetryUtils.validateMetricNames(null).isEmpty());
    }

    @Test
    public void testValidateAcceptedCompressionTypes() {
        // invalid compression types
        assertEquals(0, ClientTelemetryUtils.validateAcceptedCompressionTypes(null).size());
        assertEquals(0, ClientTelemetryUtils.validateAcceptedCompressionTypes(Collections.emptyList()).size());

        List<Byte> compressionTypes = new ArrayList<>();
        compressionTypes.add((byte) CompressionType.GZIP.id);
        compressionTypes.add((byte) CompressionType.LZ4.id);
        compressionTypes.add((byte) CompressionType.SNAPPY.id);
        compressionTypes.add((byte) CompressionType.ZSTD.id);
        compressionTypes.add((byte) CompressionType.NONE.id);
        compressionTypes.add((byte) -1);

        // should take the first compression type
        assertEquals(5, ClientTelemetryUtils.validateAcceptedCompressionTypes(compressionTypes).size());
    }

    @Test
    public void testValidateClientInstanceId() {
        assertThrows(IllegalArgumentException.class, () -> ClientTelemetryUtils.validateClientInstanceId(null));
        Uuid uuid = Uuid.randomUuid();
        assertEquals(uuid, ClientTelemetryUtils.validateClientInstanceId(uuid));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1})
    public void testValidatePushIntervalInvalid(int pushIntervalMs) {
        assertEquals(DefaultClientTelemetry.DEFAULT_PUSH_INTERVAL_MS, ClientTelemetryUtils.validatePushIntervalMs(pushIntervalMs));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, DefaultClientTelemetry.DEFAULT_PUSH_INTERVAL_MS, Integer.MAX_VALUE - 1, Integer.MAX_VALUE})
    public void testValidatePushIntervalValid(int pushIntervalMs) {
        assertEquals(pushIntervalMs, ClientTelemetryUtils.validatePushIntervalMs(pushIntervalMs));
    }

    @Test
    public void testPreferredCompressionType() {
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(Collections.emptyList()));
        assertEquals(CompressionType.NONE, ClientTelemetryUtils.preferredCompressionType(null));
        assertEquals(CompressionType.GZIP, ClientTelemetryUtils.preferredCompressionType(Arrays.asList(CompressionType.GZIP, CompressionType.LZ4, CompressionType.ZSTD)));
        assertEquals(CompressionType.LZ4, ClientTelemetryUtils.preferredCompressionType(Collections.singletonList(CompressionType.LZ4)));
    }

    @Test
    public void testMaybeCreateFailsIfClientIdIsNull() {
        assertThrows(IllegalArgumentException.class, () -> ClientTelemetryUtils.maybeCreate(true, new MockTime(), null));
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
            oldState.validateTransition(newState);

        // Have to copy to a new list because asList returns an unmodifiable list
        List<TelemetryState> invalidStates = new ArrayList<>(Arrays.asList(TelemetryState.values()));

        // Remove the valid states from the list of all states, leaving only the invalid
        invalidStates.removeAll(validStates);

        for (TelemetryState newState : invalidStates) {
            Executable e = () -> oldState.validateTransition(newState);
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
            () -> ClientTelemetryUtils.maybeCreate(true, time, clientId),
            String.format("maybeCreate should have thrown a %s for time: %s and clientId: %s", e.getName(), time, clientId));
    }

    private TelemetryMetric newTelemetryMetric(String name, long value) {
        return new TelemetryMetric(name,
            MetricType.sum,
            value,
            "Description for " + name);
    }

}

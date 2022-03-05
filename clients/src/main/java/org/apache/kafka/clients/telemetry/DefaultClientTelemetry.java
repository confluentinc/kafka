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

import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.currentTelemetryMetrics;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.preferredCompressionType;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.serialize;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateAcceptedCompressionTypes;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateClientInstanceId;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validateMetricNames;
import static org.apache.kafka.clients.telemetry.ClientTelemetryUtils.validatePushIntervalMs;
import static org.apache.kafka.common.Uuid.ZERO_UUID;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultClientTelemetry implements ClientTelemetry {

    private static final Logger log = LoggerFactory.getLogger(DefaultClientTelemetry.class);

    private static final String CONTEXT = "kafka.telemetry";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    private final Time time;

    private final Metrics metrics;

    private final DeltaValueStore deltaValueStore;

    private final TelemetryMetricsReporter telemetryMetricsReporter;

    private final TelemetrySerializer telemetrySerializer;

    private final Object subscriptionLock = new Object();

    private TelemetrySubscription subscription;

    private final Object stateLock = new Object();

    private TelemetryState state = TelemetryState.subscription_needed;

    private final ClientInstanceMetricRecorder clientInstanceMetricRecorder;

    private final ConsumerMetricRecorder consumerMetricRecorder;

    private final HostProcessMetricRecorder hostProcessMetricRecorder;

    private final ProducerMetricRecorder producerMetricRecorder;

    private final ProducerTopicMetricRecorder producerTopicMetricRecorder;

    public DefaultClientTelemetry(Time time, String clientId) {
        if (time == null)
            throw new IllegalArgumentException("time for ClientTelemetryImpl cannot be null");

        if (clientId == null)
            throw new IllegalArgumentException("clientId for ClientTelemetryImpl cannot be null");

        this.time = Objects.requireNonNull(time, "time must be non-null");
        this.telemetrySerializer = new OtlpTelemetrySerializer();
        this.deltaValueStore = new DeltaValueStore();
        this.telemetryMetricsReporter = new TelemetryMetricsReporter(deltaValueStore);

        Map<String, String> metricsTags = Collections.singletonMap(CLIENT_ID_METRIC_TAG, clientId);
        MetricConfig metricConfig = new MetricConfig()
            .tags(metricsTags);
        MetricsContext metricsContext = new KafkaMetricsContext(CONTEXT);

        this.metrics = new Metrics(metricConfig,
            Collections.singletonList(telemetryMetricsReporter),
            time,
            metricsContext);

        this.clientInstanceMetricRecorder = new DefaultClientInstanceMetricRecorder(this.metrics);
        this.consumerMetricRecorder = new DefaultConsumerMetricRecorder(this.metrics);
        this.hostProcessMetricRecorder = new DefaultHostProcessMetricRecorder(this.metrics);
        this.producerMetricRecorder = new DefaultProducerMetricRecorder(this.metrics);
        this.producerTopicMetricRecorder = new DefaultProducerTopicMetricRecorder(this.metrics);
    }

    @Override
    public void close() {
        log.trace("close");
        boolean shouldClose = false;

        synchronized (stateLock) {
            TelemetryState currState = state();

            // TODO: TELEMETRY_TODO: support ability to close multiple times.
            if (currState != TelemetryState.terminating) {
                shouldClose = true;
                setState(TelemetryState.terminating);
            }
        }

        if (shouldClose) {
            telemetryMetricsReporter.close();
            metrics.close();
        }
    }

    private void setSubscription(TelemetrySubscription subscription) {
        synchronized (subscriptionLock) {
            this.subscription = subscription;
            subscriptionLock.notifyAll();
        }
    }

    public TelemetrySubscription subscription() {
        synchronized (subscriptionLock) {
            return subscription;
        }
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * the specific enclosing client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destination(s).
     *
     * This method waits up to <code>timeout</code> for the subscription to become available in
     * order to complete the request.
     *
     * @param timeout The maximum time to wait for enclosing client instance to determine its
     *                client instance ID. The value should be non-negative. Specifying a timeout
     *                of zero means do not wait for the initial request to complete if it hasn't
     *                already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        enclosing client instance is otherwise unusable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     * @return Human-readable string representation of the client instance ID
     */
    @Override
    public Optional<String> clientInstanceId(Duration timeout) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        synchronized (subscriptionLock) {
            if (subscription == null) {
                // If we have a non-negative timeout and no-subscription, let's wait for one to
                // be retrieved.
                log.debug("Waiting for telemetry subscription containing the client instance ID with timeoutMillis = {} ms.", timeoutMs);

                try {
                    subscriptionLock.wait(timeoutMs);
                } catch (InterruptedException e) {
                    throw new InterruptException(e);
                }
            }

            if (subscription == null)
                throw new IllegalTelemetryStateException("Client instance ID could not be retrieved with timeout: " + timeout);

            Uuid clientInstanceId = subscription.clientInstanceId();

            if (clientInstanceId == null) {
                log.debug("Client instance ID was null in telemetry subscription while in state {}", state());
                return Optional.empty();
            }

            return Optional.of(clientInstanceId.toString());
        }
    }

    @Override
    public void setState(TelemetryState state) {
        synchronized (stateLock) {
            log.trace("Setting state from {} to {}", this.state, state);
            this.state = this.state.validateTransition(state);
        }
    }

    @Override
    public TelemetryState state() {
        synchronized (stateLock) {
            return state;
        }
    }

    @Override
    public void telemetrySubscriptionFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to retrieve telemetry subscription; using existing subscription", error);
        else
            log.warn("Failed to retrieve telemetry subscription; using existing subscription");

        setState(TelemetryState.subscription_needed);
    }

    @Override
    public void pushTelemetryFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to push telemetry", error);
        else
            log.warn("Failed to push telemetry", new Exception());

        TelemetryState state = state();

        if (state == TelemetryState.push_in_progress)
            setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after failed push telemetry from state %s", state));
    }

    @Override
    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
        Set<MetricName> metricNames = validateMetricNames(data.requestedMetrics());
        List<CompressionType> acceptedCompressionTypes = validateAcceptedCompressionTypes(data.acceptedCompressionTypes());
        Uuid clientInstanceId = validateClientInstanceId(data.clientInstanceId());
        int pushIntervalMs = validatePushIntervalMs(data.pushIntervalMs());

        TelemetrySubscription telemetrySubscription = new TelemetrySubscription(time.milliseconds(),
            data.throttleTimeMs(),
            clientInstanceId,
            data.subscriptionId(),
            acceptedCompressionTypes,
            pushIntervalMs,
            data.deltaTemporality(),
            metricNames);

        log.debug("Successfully retrieved telemetry subscription: {}", telemetrySubscription);
        setSubscription(telemetrySubscription);

        if (metricNames.isEmpty()) {
            // TODO: TELEMETRY_TODO: this is the case where no metrics are requested and/or match
            //                       the filters. We need to wait pushIntervalMs then retry.
            setState(TelemetryState.subscription_needed);
        } else {
            setState(TelemetryState.push_needed);
        }
    }

    @Override
    public void pushTelemetrySucceeded() {
        log.trace("Successfully pushed telemetry");

        TelemetryState state = state();

        if (state == TelemetryState.push_in_progress)
            setState(TelemetryState.subscription_needed);
        else if (state == TelemetryState.terminating_push_in_progress)
            setState(TelemetryState.terminated);
        else
            throw new IllegalTelemetryStateException(String.format("Could not transition state after successful push telemetry from state %s", state));
    }

    @Override
    public long timeToNextUpdate() {
        TelemetryState s = state();
        TelemetrySubscription subscription = subscription();
        long t = ClientTelemetryUtils.timeToNextUpdate(s, subscription, time);
        log.debug("For state {}, returning {} for time to next update", s, t);
        return t;
    }

    @Override
    public AbstractRequest.Builder<?> createRequest() {
        TelemetrySubscription subscription = subscription();

        if (state() == TelemetryState.subscription_needed) {
            setState(TelemetryState.subscription_in_progress);
            Uuid clientInstanceId = subscription != null ? subscription.clientInstanceId() : ZERO_UUID;
            return new GetTelemetrySubscriptionRequest.Builder(clientInstanceId);
        } else if (state() == TelemetryState.push_needed) {
            if (subscription == null)
                throw new IllegalStateException(String.format("Telemetry state is %s but subscription is null", state()));

            boolean terminating = state() == TelemetryState.terminating;
            CompressionType compressionType = preferredCompressionType(subscription.acceptedCompressionTypes());
            Collection<TelemetryMetric> telemetryMetrics = currentTelemetryMetrics(telemetryMetricsReporter.current(),
                deltaValueStore,
                subscription.deltaTemporality());
            ByteBuffer buf = serialize(telemetryMetrics, compressionType, telemetrySerializer);
            Bytes metricsData =  Bytes.wrap(Utils.readBytes(buf));

            if (terminating)
                setState(TelemetryState.terminating_push_in_progress);
            else
                setState(TelemetryState.push_in_progress);

            return new PushTelemetryRequest.Builder(subscription.clientInstanceId(),
                subscription.subscriptionId(),
                terminating,
                compressionType,
                metricsData);
        } else {
            throw new IllegalTelemetryStateException(String.format("Cannot make telemetry request as telemetry is in state: %s", state()));
        }
    }

    @Override
    public ClientInstanceMetricRecorder clientInstanceMetricRecorder() {
        return clientInstanceMetricRecorder;
    }

    @Override
    public ConsumerMetricRecorder consumerMetricRecorder() {
        return consumerMetricRecorder;
    }

    @Override
    public HostProcessMetricRecorder hostProcessMetricRecorder() {
        return hostProcessMetricRecorder;
    }

    @Override
    public ProducerMetricRecorder producerMetricRecorder() {
        return producerMetricRecorder;
    }

    @Override
    public ProducerTopicMetricRecorder producerTopicMetricRecorder() {
        return producerTopicMetricRecorder;
    }
}

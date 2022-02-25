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

import static org.apache.kafka.common.Uuid.ZERO_UUID;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Histogram;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryManagementInterface implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TelemetryManagementInterface.class);

    private static final String CONTEXT = "kafka.telemetry";

    private static final String CLIENT_ID_METRIC_TAG = "client-id";

    public static final int DEFAULT_PUSH_INTERVAL_MS = 5 * 60 * 1000;

    private final Time time;

    private final Metrics metrics;

    private final DeltaValueStore deltaValueStore;

    private final TelemetryMetricsReporter telemetryMetricsReporter;

    private final TelemetrySerializer telemetrySerializer;

    private final Object subscriptionLock = new Object();

    private TelemetrySubscription subscription;

    private final Object stateLock = new Object();

    private TelemetryState state = TelemetryState.initialized;

    public TelemetryManagementInterface(Time time, String clientId) {
        if (time == null)
            throw new IllegalArgumentException("time for TelemetryManagementInterface cannot be null");

        if (clientId == null)
            throw new IllegalArgumentException("clientId for TelemetryManagementInterface cannot be null");

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
    }

    public Time time() {
        return time;
    }

    public Metrics metrics() {
        return metrics;
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

    public void setSubscription(TelemetrySubscription subscription) {
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
    public String clientInstanceId(Duration timeout) {
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

            if (clientInstanceId == null)
                throw new IllegalTelemetryStateException("Client instance ID was null in telemetry subscription while in state " + state());

            return clientInstanceId.toString();
        }
    }

    void setState(TelemetryState state) {
        synchronized (stateLock) {
            log.trace("Setting state from {} to {}", this.state, state);
            this.state = validateTransition(this.state, state);
        }
    }

    TelemetryState state() {
        synchronized (stateLock) {
            return state;
        }
    }

    public void telemetrySubscriptionFailed(Throwable error) {
        if (error != null)
            log.warn("Failed to retrieve telemetry subscription; using existing subscription", error);
        else
            log.warn("Failed to retrieve telemetry subscription; using existing subscription");

        setState(TelemetryState.subscription_needed);
    }

    public void pushTelemetryFailed(Exception error) {
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

    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
        Time time = time();
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

    public boolean isNetworkState() {
        TelemetryState s = state();
        return s == TelemetryState.subscription_in_progress || s == TelemetryState.push_in_progress || s == TelemetryState.terminating_push_in_progress;
    }

    public boolean isTerminatedState() {
        TelemetryState s = state();
        return s == TelemetryState.terminated;
    }

    public long timeToNextUpdate() {
        long t = 0;
        TelemetryState s = state();

        if (s == TelemetryState.initialized || s == TelemetryState.subscription_needed) {
            // TODO: TELEMETRY_TODO: verify
            t = 0;

            // TODO: TELEMETRY_TODO: implement case where previous subscription had no metrics and
            //                       we need to wait pushIntervalMs() before requesting it again.
        } else  if (s == TelemetryState.terminated) {
            // TODO: TELEMETRY_TODO: verify and add a good error message
            throw new IllegalTelemetryStateException();
        } else {
            long milliseconds = time().milliseconds();
            TelemetrySubscription subscription = subscription();

            if (subscription != null) {
                long fetchMs = subscription.fetchMs();
                long pushIntervalMs = subscription.pushIntervalMs();
                t = fetchMs + pushIntervalMs - milliseconds;

                if (t < 0)
                    t = 0;
            }
        }

        log.debug("For state {}, returning {} for time to next update", s, t);
        return t;
    }

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
            Collection<TelemetryMetric> telemetryMetrics = currentTelemetryMetrics(subscription.deltaTemporality());
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

    public static Set<MetricName> validateMetricNames(List<String> requestedMetrics) {
        Set<MetricName> set;

        if (requestedMetrics == null || requestedMetrics.isEmpty()) {
            // no metrics
            set = Collections.emptySet();
        } else if (requestedMetrics.size() == 1 && requestedMetrics.get(0).isEmpty()) {
            // TODO: TELEMETRY_TODO: determine the set of all metrics
            set = new HashSet<>();
        } else {
            // TODO: TELEMETRY_TODO: prefix string match...
            set = new HashSet<>();
        }

        return set;
    }

    public static List<CompressionType> validateAcceptedCompressionTypes(List<Byte> acceptedCompressionTypes) {
        List<CompressionType> list = null;

        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            list = new ArrayList<>();

            for (Byte b : acceptedCompressionTypes) {
                int compressionId = b.intValue();

                try {
                    CompressionType compressionType = CompressionType.forId(compressionId);
                    list.add(compressionType);
                } catch (IllegalArgumentException e) {
                    log.warn("Accepted compression type with ID {} provided by broker is not a known compression type; ignoring", compressionId);
                }
            }
        }

        // If the set of accepted compression types provided by the server was empty or had
        // nothing valid in it, let's just return a non-null list, and we'll just end up using
        // no compression.
        if (list == null || list.isEmpty())
            list = Collections.emptyList();

        return list;
    }

    public static Uuid validateClientInstanceId(Uuid clientInstanceId) {
        if(clientInstanceId == null) {
            throw new IllegalArgumentException("invalid client instance id");
        }
        return clientInstanceId.equals(Uuid.ZERO_UUID) ? Uuid.randomUuid() : clientInstanceId;
    }

    public static int validatePushIntervalMs(int pushIntervalMs) {
        if (pushIntervalMs < 0) {
            log.warn("Telemetry subscription push interval value from broker was invalid ({}), substituting a value of {}", pushIntervalMs, DEFAULT_PUSH_INTERVAL_MS);
            return DEFAULT_PUSH_INTERVAL_MS;
        }

        log.debug("Telemetry subscription push interval value from broker was {}", pushIntervalMs);
        return pushIntervalMs;
    }

    public static CompressionType preferredCompressionType(List<CompressionType> acceptedCompressionTypes) {
        if (acceptedCompressionTypes != null && !acceptedCompressionTypes.isEmpty()) {
            // Broker is providing the compression types in order of preference. Grab the
            // first one.
            return acceptedCompressionTypes.get(0);
        } else {
            return CompressionType.NONE;
        }
    }

    public static MetricType metricType(KafkaMetric kafkaMetric) {
        Measurable measurable = kafkaMetric.measurable();

        if (measurable instanceof Gauge) {
            return MetricType.gauge;
        } else if (measurable instanceof Histogram) {
            return MetricType.histogram;
        } else if (measurable instanceof CumulativeSum) {
            return MetricType.sum;
        } else {
            throw new InvalidMetricTypeException("Could not determine metric type from measurable type " + measurable + " of metric " + kafkaMetric);
        }
    }

    public List<TelemetryMetric> currentTelemetryMetrics(boolean deltaTemporality) {
        return telemetryMetricsReporter.current().stream().map(kafkaMetric -> {
            String name = kafkaMetric.metricName().name();
            Object metricValue = kafkaMetric.metricValue();

            // TODO: TELEMETRY_TODO: not sure if the metric value is always stored as a double,
            //                       but empirically it seems to be. Not sure if there is a better
            //                       way to handle this.
            double doubleValue = Double.parseDouble(metricValue.toString());
            long value = Double.valueOf(doubleValue).longValue();
            Measurable measurable = kafkaMetric.measurable();

            if (measurable instanceof CumulativeSum && deltaTemporality) {
                Long previousValue = deltaValueStore.getAndSet(kafkaMetric.metricName(), value);
                value = previousValue != null ? value - previousValue : value;
            }

            MetricType metricType = metricType(kafkaMetric);
            String description = kafkaMetric.metricName().description();

            return new TelemetryMetric(name, metricType, value, description);
        }).collect(Collectors.toList());
    }

    public static ByteBuffer serialize(Collection<TelemetryMetric> telemetryMetrics,
        CompressionType compressionType,
        TelemetrySerializer telemetrySerializer) {

        try {
            try (ByteBufferOutputStream compressedOut = new ByteBufferOutputStream(1024)) {
                try (OutputStream out = compressionType.wrapForOutput(compressedOut, RecordBatch.CURRENT_MAGIC_VALUE)) {
                    telemetrySerializer.serialize(telemetryMetrics, out);
                }

                return (ByteBuffer) compressedOut.buffer().flip();
            }
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public static TelemetryManagementInterface maybeCreate(AbstractConfig config, Time time, String clientId) {
        if (config == null)
            throw new IllegalArgumentException("config for TelemetryManagementInterface cannot be null");

        boolean enableMetricsPush = config.getBoolean(CommonClientConfigs.ENABLE_METRICS_PUSH_CONFIG);
        return maybeCreate(enableMetricsPush, time, clientId);
    }

    public static TelemetryManagementInterface maybeCreate(boolean enableMetricsPush, Time time, String clientId) {
        if (enableMetricsPush)
            return new TelemetryManagementInterface(time, clientId);
        else
            return null;
    }

    public static String convertErrorToReason(Throwable error) {
        // TODO: TELEMETRY_TODO: properly convert the error to a "reason"
        return String.valueOf(error);
    }

    /**
     * Internal helper method that validates that the state transition from <code>currState</code>
     * to <code>newState</code> is valid.
     *
     * @param currState State that the telemetry is already in; must be non-<code>null</code>
     * @param newState State into which the telemetry is trying to transition; must be
     *                 non-<code>null</code>
     * @return {@link TelemetryState} that is <code>newState</code>; this is done for assignment
     * chaining
     * @throws IllegalTelemetryStateException if the state transition isn't valid
     */

    static TelemetryState validateTransition(TelemetryState currState, TelemetryState newState) {
        if (currState == null)
            throw new IllegalTelemetryStateException("currState cannot be null");

        if (newState == null)
            throw new IllegalTelemetryStateException("newState cannot be null");

        switch (currState) {
            case initialized:
                // If we're just starting up, the happy path is to next request a subscription.
                //
                // Don't forget, there are probably error cases for which we might transition
                // straight to terminating without having done any "real" work.
                return validateTransitionAllowed(currState, newState, TelemetryState.subscription_needed, TelemetryState.terminating);

            case subscription_needed:
                // If we need a subscription, the main thing we can do is request one.
                //
                // However, it's still possible that we don't get very far before terminating.
                return validateTransitionAllowed(currState, newState, TelemetryState.subscription_in_progress, TelemetryState.terminating);

            case subscription_in_progress:
                // If we are finished awaiting our subscription, the most likely step is to next
                // push the telemetry. But, it's possible for there to be no telemetry requested,
                // at which point we would go back to waiting a bit before requesting the next
                // subscription.
                //
                // As before, it's possible that we don't get our response before we have to
                // terminate.
                return validateTransitionAllowed(currState, newState, TelemetryState.push_needed, TelemetryState.subscription_needed, TelemetryState.terminating);

            case push_needed:
                // If we are transitioning out of this state, chances are that we are doing so
                // because we want to push the telemetry. Alternatively, it's possible for the
                // push to fail (network issues, the subscription might have changed, etc.),
                // at which point we would again go back to waiting and requesting the next
                // subscription.
                //
                // But guess what? Yep - it's possible that we don't get to push before we have
                // to terminate.
                return validateTransitionAllowed(currState, newState, TelemetryState.push_in_progress, TelemetryState.subscription_needed, TelemetryState.terminating);

            case push_in_progress:
                // If we are transitioning out of this state, I'm guessing it's because we
                // did a successful push. We're going to want to sit tight before requesting
                // our subscription.
                //
                // But it's also possible that the push failed (again: network issues, the
                // subscription might have changed, etc.). We're not going to attempt to
                // re-push, but rather, take a breather and wait to request the
                // next subscription.
                //
                // So in either case, noting that we're now waiting for a subscription is OK.
                //
                // Again, it's possible that we don't get our response before we have to terminate.
                return validateTransitionAllowed(currState, newState, TelemetryState.subscription_needed, TelemetryState.terminating);

            case terminating:
                // If we are moving out of this state, we are hopefully doing so because we're
                // going to try to send our last push. Either that or we want to be fully
                // terminated.
                return validateTransitionAllowed(currState, newState, TelemetryState.terminated, TelemetryState.terminating_push_in_progress);

            case terminating_push_in_progress:
                // If we are done in this state, we should only be transitioning to fully
                // terminated.
                return validateTransitionAllowed(currState, newState, TelemetryState.terminated);

            case terminated:
                // We should never be able to transition out of this state...
                return validateTransitionAllowed(currState, newState);

            default:
                // We shouldn't get to here unless we somehow accidentally try to transition out
                // of the terminated state.
                return validateTransitionAllowed(currState, newState);
        }
    }

    /**
     * Internal helper method that validates that the <code>newState</code> parameter value is
     * one of the options in <code>allowableStates</code>.
     *
     * @param currState State that the telemetry is already in; only used for error messages but
     *                 most be non-<code>null</code>
     * @param newState State into which the telemetry is trying to transition; must be
     *                 non-<code>null</code>
     * @param allowableStates Array of {@link TelemetryState}s to which it is valid for
     *                    <code>currState</code> to transition
     * @return {@link TelemetryState} that is <code>newState</code>; this is done for assignment
     * chaining
     * @throws IllegalTelemetryStateException if the state transition isn't valid
     */

    private static TelemetryState validateTransitionAllowed(TelemetryState currState, TelemetryState newState, TelemetryState... allowableStates) {
        if (allowableStates != null) {
            for (TelemetryState allowableState : allowableStates) {
                if (newState == allowableState)
                    return newState;
            }
        }

        // We didn't find a match above, so now we're just formatting a nice error message...
        String validStatesClause;

        if (allowableStates != null && allowableStates.length > 0) {
            validStatesClause = String.format("the valid telemetry state transitions from %s are: %s",
                currState,
                Utils.join(allowableStates, ", "));
        } else {
            validStatesClause = String.format("there are no valid telemetry state transitions from %s", currState);
        }

        String message = String.format("Invalid telemetry state transition from %s to %s; %s",
            currState,
            newState,
            validStatesClause);

        throw new IllegalTelemetryStateException(message);
    }

}

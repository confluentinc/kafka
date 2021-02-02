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

package org.apache.kafka.controller;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.errors.DuplicateBrokerRegistrationException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FeatureManager;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class ClusterControlManager {
    class ReadyBrokersFuture {
        private final CompletableFuture<Void> future;
        private final int minBrokers;

        ReadyBrokersFuture(CompletableFuture<Void> future, int minBrokers) {
            this.future = future;
            this.minBrokers = minBrokers;
        }

        boolean check() {
            int numUnfenced = 0;
            for (BrokerRegistration registration : brokerRegistrations.values()) {
                if (!registration.fenced()) {
                    numUnfenced++;
                }
                if (numUnfenced >= minBrokers) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * The SLF4J log context.
     */
    private final LogContext logContext;

    /**
     * The SLF4J log object.
     */
    private final Logger log;

    /**
     * The Kafka clock object to use.
     */
    private final Time time;

    /**
     * How long sessions should last, in nanoseconds.
     */
    private final long sessionTimeoutNs;

    /**
     * The replica placement policy to use.
     */
    private final ReplicaPlacementPolicy placementPolicy;

    /**
     * Maps broker IDs to broker registrations.
     */
    private final TimelineHashMap<Integer, BrokerRegistration> brokerRegistrations;

    /**
     * The broker heartbeat manager, or null if this controller is on standby.
     */
    private BrokerHeartbeatManager heartbeatManager;

    /**
     * A future which is completed as soon as we have the given number of brokers
     * ready.
     */
    private Optional<ReadyBrokersFuture> readyBrokersFuture;

    ClusterControlManager(LogContext logContext,
                          Time time,
                          SnapshotRegistry snapshotRegistry,
                          long sessionTimeoutNs,
                          ReplicaPlacementPolicy placementPolicy) {
        this.logContext = logContext;
        this.log = logContext.logger(ClusterControlManager.class);
        this.time = time;
        this.sessionTimeoutNs = sessionTimeoutNs;
        this.placementPolicy = placementPolicy;
        this.brokerRegistrations = new TimelineHashMap<>(snapshotRegistry, 0);
        this.heartbeatManager = null;
        this.readyBrokersFuture = Optional.empty();
    }

    /**
     * Transition this ClusterControlManager to active.
     */
    public void activate() {
        heartbeatManager = new BrokerHeartbeatManager(logContext, time, sessionTimeoutNs);
        for (BrokerRegistration registration : brokerRegistrations.values()) {
            heartbeatManager.touch(registration.id(), registration.fenced(), -1);
        }
    }

    /**
     * Transition this ClusterControlManager to standby.
     */
    public void deactivate() {
        heartbeatManager = null;
    }

    // VisibleForTesting
    Map<Integer, BrokerRegistration> brokerRegistrations() {
        return brokerRegistrations;
    }

    /**
     * Process an incoming broker registration request.
     */
    public ControllerResult<BrokerRegistrationReply> registerBroker(
            BrokerRegistrationRequestData request, long brokerEpoch,
            FeatureManager.FinalizedFeaturesAndEpoch finalizedFeaturesAndEpoch) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        int brokerId = request.brokerId();
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        if (existing != null) {
            if (heartbeatManager.hasValidSession(brokerId)) {
                if (!existing.incarnationId().equals(request.incarnationId())) {
                    throw new DuplicateBrokerRegistrationException("Another broker is " +
                        "registered with that broker id.");
                }
            } else {
                if (!existing.incarnationId().equals(request.incarnationId())) {
                    // Remove any existing session for the old broker incarnation.
                    heartbeatManager.remove(brokerId);
                    existing = null;
                }
            }
        }

        RegisterBrokerRecord record = new RegisterBrokerRecord().setBrokerId(brokerId).
            setIncarnationId(request.incarnationId()).
            setBrokerEpoch(brokerEpoch).
            setRack(request.rack());
        for (BrokerRegistrationRequestData.Listener listener : request.listeners()) {
            record.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                setHost(listener.host()).
                setName(listener.name()).
                setPort(listener.port()).
                setSecurityProtocol(listener.securityProtocol()));
        }
        for (BrokerRegistrationRequestData.Feature feature : request.features()) {
            VersionRange supported = finalizedFeaturesAndEpoch.finalizedFeatures().
                getOrDefault(feature.name(), VersionRange.ALL);
            if (!supported.contains(new VersionRange(feature.minSupportedVersion(),
                    feature.maxSupportedVersion()))) {
                throw new UnsupportedVersionException("Unable to register because " +
                    "the broker has an unsupported version of " + feature.name());
            }
            record.features().add(new RegisterBrokerRecord.BrokerFeature().
                setName(feature.name()).
                setMinSupportedVersion(feature.minSupportedVersion()).
                setMaxSupportedVersion(feature.maxSupportedVersion()));
        }

        if (existing == null) {
            heartbeatManager.touch(brokerId, true, -1);
        } else {
            heartbeatManager.touch(brokerId, existing.fenced(), -1);
        }

        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(record, (short) 0));
        return new ControllerResult<>(records, new BrokerRegistrationReply(brokerEpoch));
    }

    public ControllerResult<Void> decommissionBroker(int brokerId) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        BrokerRegistration existing = brokerRegistrations.get(brokerId);
        if (existing == null) {
            return new ControllerResult<>(new ArrayList<>(), null);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(
            new UnregisterBrokerRecord().
                setBrokerId(brokerId).
                setBrokerEpoch(existing.epoch()),
            (short) 0));
        heartbeatManager.remove(brokerId);
        return new ControllerResult<>(records, null);
    }

    public ControllerResult<BrokerHeartbeatReply> processBrokerHeartbeat(
            BrokerHeartbeatRequestData request,
            long lastCommittedOffset,
            boolean movingLeadersForShutDown) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        int brokerId = request.brokerId();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new StaleBrokerEpochException("No registration found for broker " +
                request.brokerId());
        }
        if (request.brokerEpoch() != registration.epoch()) {
            throw new StaleBrokerEpochException("Expected broker epoch " +
                request.brokerEpoch() + "; got epoch " + registration.epoch());
        }
        boolean isCaughtUp = request.currentMetadataOffset() >= lastCommittedOffset;
        List<ApiMessageAndVersion> records = new ArrayList<>();
        boolean isFenced = registration.fenced();
        boolean shouldShutdown = false;
        if (isFenced) {
            if (request.wantShutDown()) {
                // If the broker is fenced, and requests a shutdown, do it immediately.
                isFenced = true;
                shouldShutdown = true;
            } else if (isCaughtUp && !request.wantFence()) {
                records.add(new ApiMessageAndVersion(new UnfenceBrokerRecord().
                    setId(brokerId).setEpoch(request.brokerEpoch()), (short) 0));
                isFenced = false;
                shouldShutdown = false;
            }
        } else {
            if (request.wantFence()) {
                records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
                    setId(brokerId).setEpoch(request.brokerEpoch()), (short) 0));
                isFenced = true;
                shouldShutdown = request.wantShutDown();
            } else {
                if (request.wantShutDown()) {
                    heartbeatManager.beginBrokerShutDown(request.brokerId(), movingLeadersForShutDown);
                }
                if (heartbeatManager.shouldShutDown(request.brokerId())) {
                    isFenced = true;
                    shouldShutdown = true;
                    records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
                        setId(brokerId).setEpoch(request.brokerEpoch()), (short) 0));
                } else {
                    isFenced = false;
                    shouldShutdown = false;
                }
            }
        }
        heartbeatManager.touch(brokerId, isFenced, request.currentMetadataOffset());
        ControllerResult<BrokerHeartbeatReply> result = new ControllerResult<>(records,
            new BrokerHeartbeatReply(isCaughtUp, isFenced, shouldShutdown));
        if (log.isTraceEnabled()) {
            log.trace("processed broker heartbeat {}. lastCommittedOffset = {}, " +
                    "movingLeadersForShutDown = {}, result = {}", request,
                lastCommittedOffset, movingLeadersForShutDown, result);
        }
        return result;
    }

    public void replay(RegisterBrokerRecord record) {
        int brokerId = record.brokerId();
        List<Endpoint> listeners = new ArrayList<>();
        for (RegisterBrokerRecord.BrokerEndpoint endpoint : record.endPoints()) {
            listeners.add(new Endpoint(endpoint.name(),
                SecurityProtocol.forId(endpoint.securityProtocol()),
                endpoint.host(), endpoint.port()));
        }
        Map<String, VersionRange> features = new HashMap<>();
        for (RegisterBrokerRecord.BrokerFeature feature : record.features()) {
            features.put(feature.name(), new VersionRange(
                feature.minSupportedVersion(), feature.maxSupportedVersion()));
        }
        // Normally, all newly registered brokers start off in the fenced state.
        // If this registration record is for a broker incarnation that was already
        // registered, though, we preserve the existing fencing state.
        boolean fenced = true;
        BrokerRegistration prevRegistration = brokerRegistrations.get(brokerId);
        if (prevRegistration != null &&
                prevRegistration.incarnationId().equals(record.incarnationId())) {
            fenced = prevRegistration.fenced();
        }
        // Update broker registrations.
        brokerRegistrations.put(brokerId, new BrokerRegistration(brokerId,
            record.brokerEpoch(), record.incarnationId(), listeners, features,
            Optional.ofNullable(record.rack()), fenced));

        if (prevRegistration == null) {
            log.info("Registered new broker: {}", record);
        } else if (prevRegistration.incarnationId().equals(record.incarnationId())) {
            log.info("Re-registered broker incarnation: {}", record);
        } else {
            log.info("Re-registered broker id {}: {}", brokerId, record);
        }
    }

    public void replay(UnregisterBrokerRecord record) {
        int brokerId = record.brokerId();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.brokerEpoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.remove(brokerId);
            log.info("Unregistered broker: {}", record);
        }
    }

    public void replay(FenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.epoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.put(brokerId, registration.cloneWithFencing(true));
            log.info("Fenced broker: {}", record);
        }
    }

    public void replay(UnfenceBrokerRecord record) {
        int brokerId = record.id();
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration found for that id", record.toString()));
        } else if (registration.epoch() !=  record.epoch()) {
            throw new RuntimeException(String.format("Unable to replay %s: no broker " +
                "registration with that epoch found", record.toString()));
        } else {
            brokerRegistrations.put(brokerId, registration.cloneWithFencing(false));
            log.info("Unfenced broker: {}", record);
        }
        if (readyBrokersFuture.isPresent()) {
            if (readyBrokersFuture.get().check()) {
                readyBrokersFuture.get().future.complete(null);
                readyBrokersFuture = Optional.empty();
            }
        }
    }

    public List<List<Integer>> placeReplicas(int numPartitions, short numReplicas) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        return heartbeatManager.placeReplicas(numPartitions, numReplicas,
            id -> brokerRegistrations.get(id).rack(), placementPolicy);
    }

    public boolean unfenced(int brokerId) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) return false;
        return !registration.fenced();
    }

    public ControllerResult<Set<Integer>> maybeFenceLeastRecentlyContacted() {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        Set<Integer> newlyFenced = new HashSet<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        while (true) {
            int brokerId = heartbeatManager.maybeFenceLeastRecentlyContacted();
            if (brokerId == -1) {
                return new ControllerResult<>(records, newlyFenced);
            }
            newlyFenced.add(brokerId);
            BrokerRegistration registration = brokerRegistrations.get(brokerId);
            if (registration == null) {
                throw new RuntimeException("Failed to find registration for " +
                    "broker " + brokerId);
            }
            records.add(new ApiMessageAndVersion(new FenceBrokerRecord().
                setId(brokerId).setEpoch(registration.epoch()), (short) 0));
            log.info("Fencing broker {} because its session has timed out.", brokerId);
        }
    }

    public long nextCheckTimeNs() {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        return heartbeatManager.nextCheckTimeNs();
    }

    public void checkBrokerEpoch(int brokerId, long brokerEpoch) {
        BrokerRegistration registration = brokerRegistrations.get(brokerId);
        if (registration == null) {
            throw new StaleBrokerEpochException("No broker registration found for " +
                "broker id " + brokerId);
        }
        if (registration.epoch() != brokerEpoch) {
            throw new StaleBrokerEpochException("Expected broker epoch " +
                registration.epoch() + ", but got broker epoch " + brokerEpoch);
        }
    }

    public void updateShutdownOffset(int brokerId, long offset) {
        if (heartbeatManager == null) {
            throw new RuntimeException("ClusterControlManager is not active.");
        }
        heartbeatManager.updateShutdownOffset(brokerId, offset);
    }

    public void addReadyBrokersFuture(CompletableFuture<Void> future, int minBrokers) {
        readyBrokersFuture = Optional.of(new ReadyBrokersFuture(future, minBrokers));
        if (readyBrokersFuture.get().check()) {
            readyBrokersFuture.get().future.complete(null);
            readyBrokersFuture = Optional.empty();
        }
    }
}

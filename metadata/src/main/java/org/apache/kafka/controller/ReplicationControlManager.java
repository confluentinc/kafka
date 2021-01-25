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

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.AlterIsrRequestData;
import org.apache.kafka.common.message.AlterIsrResponseData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.IsrChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;


public class ReplicationControlManager {
    private final static int[] EMPTY = new int[0];

    private final static int[] NO_LEADER = new int[] {-1};

    static class TopicControlInfo {
        private final Uuid id;
        private final TimelineHashMap<Integer, PartitionControlInfo> parts;

        TopicControlInfo(SnapshotRegistry snapshotRegistry, Uuid id) {
            this.id = id;
            this.parts = new TimelineHashMap<>(snapshotRegistry, 0);
        }
    }

    static class PartitionControlInfo {
        private final int[] replicas;
        private final int[] isr;
        private final int[] removingReplicas;
        private final int[] addingReplicas;
        private final int leader;
        private final int leaderEpoch;
        private final int partitionEpoch;

        PartitionControlInfo(PartitionRecord record) {
            this(Replicas.toArray(record.replicas()),
                Replicas.toArray(record.isr()),
                Replicas.toArray(record.removingReplicas()),
                Replicas.toArray(record.addingReplicas()),
                record.leader(),
                record.leaderEpoch(),
                record.partitionEpoch());
        }

        PartitionControlInfo(int[] replicas, int[] isr, int[] removingReplicas,
                             int[] addingReplicas, int leader, int leaderEpoch,
                             int partitionEpoch) {
            this.replicas = replicas;
            this.isr = isr;
            this.removingReplicas = removingReplicas;
            this.addingReplicas = addingReplicas;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
            this.partitionEpoch = partitionEpoch;
        }

        PartitionControlInfo merge(IsrChangeRecord record) {
            return new PartitionControlInfo(replicas,
                Replicas.toArray(record.isr()),
                removingReplicas,
                addingReplicas,
                record.leader(),
                record.leaderEpoch(),
                record.partitionEpoch());
        }

        String diff(PartitionControlInfo prev) {
            StringBuilder builder = new StringBuilder();
            String prefix = "";
            if (!Arrays.equals(replicas, prev.replicas)) {
                builder.append("replicas=").append(Arrays.toString(replicas));
                prefix = ", ";
            }
            if (!Arrays.equals(isr, prev.isr)) {
                builder.append(prefix).append("isr=").append(Arrays.toString(isr));
                prefix = ", ";
            }
            if (!Arrays.equals(removingReplicas, prev.removingReplicas)) {
                builder.append(prefix).append("removingReplicas=").
                    append(Arrays.toString(removingReplicas));
                prefix = ", ";
            }
            if (!Arrays.equals(addingReplicas, prev.addingReplicas)) {
                builder.append(prefix).append("addingReplicas=").
                    append(Arrays.toString(addingReplicas));
                prefix = ", ";
            }
            if (leader != prev.leader) {
                builder.append(prefix).append("leader=").append(leader);
                prefix = ", ";
            }
            if (leaderEpoch != prev.leaderEpoch) {
                builder.append(prefix).append("leaderEpoch=").append(leaderEpoch);
            }
            if (partitionEpoch != prev.partitionEpoch) {
                builder.append(prefix).append("partitionEpoch=").append(partitionEpoch);
            }
            return builder.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isr, removingReplicas, addingReplicas, leader,
                leaderEpoch, partitionEpoch);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PartitionControlInfo)) return false;
            PartitionControlInfo other = (PartitionControlInfo) o;
            return diff(other).isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder("PartitionControlInfo(");
            builder.append("replicas=").append(Arrays.toString(replicas));
            builder.append(", isr=").append(Arrays.toString(isr));
            builder.append(", removingReplicas=").append(Arrays.toString(removingReplicas));
            builder.append(", addingReplicas=").append(Arrays.toString(addingReplicas));
            builder.append(", leader=").append(leader);
            builder.append(", leaderEpoch=").append(leaderEpoch);
            builder.append(", partitionEpoch=").append(partitionEpoch);
            builder.append(")");
            return builder.toString();
        }

        public int chooseNewLeader(int[] newIsr) {
            for (int i = 0; i < replicas.length; i++) {
                int replica = replicas[i];
                if (Replicas.contains(newIsr, replica)) {
                    return replica;
                }
            }
            return -1;
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;

    /**
     * The random number generator used by this object.
     */
    private final Random random;

    /**
     * The KIP-464 default replication factor that is used if a CreateTopics request does
     * not specify one.
     */
    private final short defaultReplicationFactor;

    /**
     * The KIP-464 default number of partitions that is used if a CreateTopics request does
     * not specify a number of partitions.
     */
    private final int defaultNumPartitions;

    /**
     * A reference to the controller's configuration control manager.
     */
    private final ConfigurationControlManager configurationControl;

    /**
     * A reference to the controller's cluster control manager.
     */
    final ClusterControlManager clusterControl;

    /**
     * Maps topic names to topic UUIDs.
     */
    private final TimelineHashMap<String, Uuid> topicsByName;

    /**
     * Maps topic UUIDs to structures containing topic information, including partitions.
     */
    private final TimelineHashMap<Uuid, TopicControlInfo> topics;

    /**
     * A map of broker IDs to the partitions that the broker is in the ISR for.
     * Partitions with no isr members appear in this map under id -1.
     */
    private final TimelineHashMap<Integer, TimelineHashMap<Uuid, int[]>> isrMembers;

    ReplicationControlManager(SnapshotRegistry snapshotRegistry,
                              LogContext logContext,
                              Random random,
                              short defaultReplicationFactor,
                              int defaultNumPartitions,
                              ConfigurationControlManager configurationControl,
                              ClusterControlManager clusterControl) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(ReplicationControlManager.class);
        this.random = random;
        this.defaultReplicationFactor = defaultReplicationFactor;
        this.defaultNumPartitions = defaultNumPartitions;
        this.configurationControl = configurationControl;
        this.clusterControl = clusterControl;
        this.topicsByName = new TimelineHashMap<>(snapshotRegistry, 0);
        this.topics = new TimelineHashMap<>(snapshotRegistry, 0);
        this.isrMembers = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    public void replay(TopicRecord record) {
        topicsByName.put(record.name(), record.topicId());
        topics.put(record.topicId(), new TopicControlInfo(snapshotRegistry, record.topicId()));
        log.info("Created topic {} with ID {}.", record.name(), record.topicId());
    }

    public void replay(PartitionRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo newPartInfo = new PartitionControlInfo(record);
        PartitionControlInfo prevPartInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartInfo == null) {
            log.info("Created partition {}:{} with {}.", record.topicId(),
                record.partitionId(), newPartInfo.toString());
            topicInfo.parts.put(record.partitionId(), newPartInfo);
            updateIsrMembers(record.topicId(), record.partitionId(), null, newPartInfo.isr);
        } else {
            String diff = newPartInfo.diff(prevPartInfo);
            if (!diff.isEmpty()) {
                log.info("Modified partition {}:{}: {}.", record.topicId(),
                    record.partitionId(), diff);
                topicInfo.parts.put(record.partitionId(), newPartInfo);
                updateIsrMembers(record.topicId(), record.partitionId(),
                    prevPartInfo.isr, newPartInfo.isr);
            }
        }
    }

    public void replay(IsrChangeRecord record) {
        TopicControlInfo topicInfo = topics.get(record.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo prevPartitionInfo = topicInfo.parts.get(record.partitionId());
        if (prevPartitionInfo == null) {
            throw new RuntimeException("Tried to create partition " + record.topicId() +
                ":" + record.partitionId() + ", but no partition with that id was found.");
        }
        PartitionControlInfo newPartitionInfo = prevPartitionInfo.merge(record);
        topicInfo.parts.put(record.partitionId(), newPartitionInfo);
        updateIsrMembers(record.topicId(), record.partitionId(),
            prevPartitionInfo.isr, newPartitionInfo.isr);
        log.debug("Applied ISR change record: {}", record.toString());
    }

    /**
     * Update our records of a partition's ISR.
     *
     * @param topicId       The topic ID of the partition.
     * @param partitionId   The partition ID of the partition.
     * @param prevIsr       The previous ISR, or null if the partition is new.
     * @param nextIsr       The new ISR, or null if the partition is being removed.
     */
    private void updateIsrMembers(Uuid topicId, int partitionId, int[] prevIsr, int[] nextIsr) {
        if (prevIsr == null) {
            prevIsr = EMPTY;
        } else if (prevIsr.length == 0) {
            prevIsr = NO_LEADER;
        }
        if (nextIsr == null) {
            nextIsr = EMPTY;
        } else if (nextIsr.length == 0) {
            nextIsr = NO_LEADER;
        }
        List<Integer> added = new ArrayList<>();
        List<Integer> removed = new ArrayList<>();
        int i = 0, j = 0;
        while (true) {
            if (i == prevIsr.length) {
                if (j == nextIsr.length) {
                    break;
                }
                added.add(nextIsr[j]);
                j++;
            } else if (j == nextIsr.length) {
                removed.add(prevIsr[i]);
                i++;
            } else {
                int prevPartition = prevIsr[i];
                int newPartition = nextIsr[j];
                if (prevPartition < newPartition) {
                    removed.add(prevPartition);
                    i++;
                } else if (prevPartition > newPartition) {
                    added.add(newPartition);
                    j++;
                } else {
                    i++;
                    j++;
                }
            }
        }
        for (Integer addedBroker : added) {
            TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(addedBroker);
            if (topicMap == null) {
                topicMap = new TimelineHashMap<>(snapshotRegistry, 0);
                isrMembers.put(addedBroker, topicMap);
            }
            int[] partitions = topicMap.get(topicId);
            int[] newPartitions;
            if (partitions == null) {
                newPartitions = new int[1];
                newPartitions[0] = partitionId;
            } else {
                newPartitions = new int[partitions.length + 1];
                System.arraycopy(partitions, 0, newPartitions, 0, partitions.length);
                newPartitions[newPartitions.length - 1] = partitionId;
                Arrays.sort(newPartitions);
            }
            topicMap.put(topicId, newPartitions);
        }
        for (Integer removedBroker : removed) {
            TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(removedBroker);
            if (topicMap == null) {
                throw new RuntimeException("Broker " + removedBroker + " is not in " +
                    "isrMembers for " + topicId + ":" + partitionId);
            }
            int[] partitions = topicMap.get(topicId);
            if (partitions == null) {
                throw new RuntimeException("Broker " + removedBroker + " has no " +
                    "partition entry in isrMembers for " + topicId + ":" + partitionId);
            }
            if (partitions.length == 1) {
                topicMap.remove(topicId);
                if (topicMap.isEmpty()) {
                    isrMembers.remove(removedBroker);
                }
            } else {
                int[] newPartitions = new int[partitions.length - 1];
                int a = 0;
                for (int b = 0; b < partitions.length; b++) {
                    int prev = partitions[b];
                    if (prev != partitionId) {
                        newPartitions[a++] = prev;
                    }
                }
                topicMap.put(topicId, newPartitions);
            }
        }
    }

    public ControllerResult<CreateTopicsResponseData>
            createTopics(CreateTopicsRequestData request) {
        Map<String, ApiError> topicErrors = new HashMap<>();
        List<ApiMessageAndVersion> records = new ArrayList<>();

        // Check the topic names.
        validateNewTopicNames(topicErrors, request.topics());

        // Identify topics that already exist and mark them with the appropriate error
        request.topics().stream().filter(creatableTopic -> topicsByName.containsKey(creatableTopic.name()))
                .forEach(t -> topicErrors.put(t.name(), new ApiError(Errors.TOPIC_ALREADY_EXISTS)));

        // Verify that the configurations for the new topics are OK, and figure out what
        // ConfigRecords should be created.
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges =
            computeConfigChanges(topicErrors, request.topics());
        ControllerResult<Map<ConfigResource, ApiError>> configResult =
            configurationControl.incrementalAlterConfigs(configChanges);
        for (Entry<ConfigResource, ApiError> entry : configResult.response().entrySet()) {
            if (entry.getValue().isFailure()) {
                topicErrors.put(entry.getKey().name(), entry.getValue());
            }
        }
        records.addAll(configResult.records());

        // Try to create whatever topics are needed.
        Map<String, CreatableTopicResult> successes = new HashMap<>();
        for (CreatableTopic topic : request.topics()) {
            if (topicErrors.containsKey(topic.name())) continue;
            ApiError error = createTopic(topic, records, successes);
            if (error.isFailure()) {
                topicErrors.put(topic.name(), error);
            }
        }

        // Create responses for all topics.
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        StringBuilder resultsBuilder = new StringBuilder();
        String resultsPrefix = "";
        for (CreatableTopic topic : request.topics()) {
            ApiError error = topicErrors.get(topic.name());
            if (error != null) {
                data.topics().add(new CreatableTopicResult().
                    setName(topic.name()).
                    setErrorCode(error.error().code()).
                    setErrorMessage(error.message()));
                resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                    append(error.error()).append(" (").append(error.message()).append(")");
                resultsPrefix = ", ";
                continue;
            }
            CreatableTopicResult result = successes.get(topic.name());
            data.topics().add(result);
            resultsBuilder.append(resultsPrefix).append(topic).append(": ").
                append("SUCCESS");
            resultsPrefix = ", ";
        }
        log.info("createTopics result(s): {}", resultsBuilder.toString());
        return new ControllerResult<>(records, data);
    }

    private ApiError createTopic(CreatableTopic topic,
                                 List<ApiMessageAndVersion> records,
                                 Map<String, CreatableTopicResult> successes) {
        Map<Integer, PartitionControlInfo> newParts = new HashMap<>();
        if (!topic.assignments().isEmpty()) {
            if (topic.replicationFactor() != -1) {
                return new ApiError(Errors.INVALID_REQUEST,
                    "A manual partition assignment was specified, but replication " +
                    "factor was not set to -1.");
            } else if (topic.assignments().size() != topic.numPartitions()) {
                return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT, "" + topic.numPartitions() +
                    " partitions were specified, but only " + topic.assignments().size() +
                    " manual partition assignments were given.");
            }
            for (CreatableReplicaAssignment assignment : topic.assignments()) {
                if (newParts.containsKey(assignment.partitionIndex())) {
                    return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                        "Found multiple manual partition assignments for partition " +
                            assignment.partitionIndex());
                }
                HashSet<Integer> brokerIds = new HashSet<>();
                for (int brokerId : assignment.brokerIds()) {
                    if (!brokerIds.add(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment specifies the same node " +
                                "id more than once.");
                    } else if (!clusterControl.unfenced(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment contains node " + brokerId +
                                ", but that node is not usable.");
                    }
                }
                int[] replicas = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    replicas[i] = assignment.brokerIds().get(i);
                }
                int[] isr = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    isr[i] = assignment.brokerIds().get(i);
                }
                newParts.put(assignment.partitionIndex(),
                    new PartitionControlInfo(replicas, isr, null, null, isr[0], 0, 0));
            }
        } else if (topic.replicationFactor() < -1 || topic.replicationFactor() == 0) {
            return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                "Replication factor was set to an invalid non-positive value.");
        } else if (!topic.assignments().isEmpty()) {
            return new ApiError(Errors.INVALID_REQUEST,
                "Replication factor was not set to -1 but a manual partition " +
                    "assignment was specified.");
        } else if (topic.numPartitions() < -1 || topic.numPartitions() == 0) {
            return new ApiError(Errors.INVALID_PARTITIONS,
                "Number of partitions was set to an invalid non-positive value.");
        } else {
            int numPartitions = topic.numPartitions() == -1 ?
                defaultNumPartitions : topic.numPartitions();
            short replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            try {
                List<List<Integer>> replicas = clusterControl.
                    placeReplicas(numPartitions, replicationFactor);
                for (int partitionId = 0; partitionId < replicas.size(); partitionId++) {
                    int[] r = Replicas.toArray(replicas.get(partitionId));
                    newParts.put(partitionId,
                        new PartitionControlInfo(r, r, null, null, r[0], 0, 0));
                }
            } catch (InvalidReplicationFactorException e) {
                return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                    "Unable to replicate the partition " + replicationFactor +
                        " times: " + e.getMessage());
            }
        }
        Uuid topicId = new Uuid(random.nextLong(), random.nextLong());
        successes.put(topic.name(), new CreatableTopicResult().
            setName(topic.name()).
            setErrorCode((short) 0).
            setErrorMessage(null).
            setNumPartitions(newParts.size()).
            setReplicationFactor((short) newParts.get(0).replicas.length));
        records.add(new ApiMessageAndVersion(new TopicRecord().
            setName(topic.name()).
            setTopicId(topicId), (short) 0));
        for (Entry<Integer, PartitionControlInfo> partEntry : newParts.entrySet()) {
            int partitionIndex = partEntry.getKey();
            PartitionControlInfo info = partEntry.getValue();
            records.add(new ApiMessageAndVersion(new PartitionRecord().
                setPartitionId(partitionIndex).
                setTopicId(topicId).
                setReplicas(Replicas.toList(info.replicas)).
                setIsr(Replicas.toList(info.isr)).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(info.leader).
                setLeaderEpoch(info.leaderEpoch).
                setPartitionEpoch(0), (short) 0));
        }
        return ApiError.NONE;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (InvalidTopicException e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_TOPIC_EXCEPTION, e.getMessage()));
            }
        }
    }

    static Map<ConfigResource, Map<String, Entry<OpType, String>>>
            computeConfigChanges(Map<String, ApiError> topicErrors,
                                 CreatableTopicCollection topics) {
        Map<ConfigResource, Map<String, Entry<OpType, String>>> configChanges = new HashMap<>();
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            Map<String, Entry<OpType, String>> topicConfigs = new HashMap<>();
            for (CreateTopicsRequestData.CreateableTopicConfig config : topic.configs()) {
                topicConfigs.put(config.name(), new SimpleImmutableEntry<>(SET, config.value()));
            }
            if (!topicConfigs.isEmpty()) {
                configChanges.put(new ConfigResource(TOPIC, topic.name()), topicConfigs);
            }
        }
        return configChanges;
    }

    // VisibleForTesting
    PartitionControlInfo getPartition(Uuid topicId, int partitionId) {
        TopicControlInfo topic = topics.get(topicId);
        if (topic == null) {
            return null;
        }
        return topic.parts.get(partitionId);
    }

    public ControllerResult<AlterIsrResponseData> alterIsr(AlterIsrRequestData request) {
        clusterControl.checkBrokerEpoch(request.brokerId(), request.brokerEpoch());
        AlterIsrResponseData response = new AlterIsrResponseData();
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (AlterIsrRequestData.TopicData topicData : request.topics()) {
            AlterIsrResponseData.TopicData responseTopicData =
                new AlterIsrResponseData.TopicData().setName(topicData.name());
            response.topics().add(responseTopicData);
            Uuid topicId = topicsByName.get(topicData.name());
            if (topicId == null || !topics.containsKey(topicId)) {
                for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                }
                continue;
            }
            TopicControlInfo topic = topics.get(topicId);
            for (AlterIsrRequestData.PartitionData partitionData : topicData.partitions()) {
                PartitionControlInfo partition = topic.parts.get(partitionData.partitionIndex());
                if (partition == null) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
                    continue;
                }
                if (partitionData.leaderEpoch() != partition.leaderEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.FENCED_LEADER_EPOCH.code()));
                    continue;
                }
                if (partitionData.currentIsrVersion() != partition.partitionEpoch) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_UPDATE_VERSION.code()));
                    continue;
                }
                int[] newIsr = Replicas.toArray(partitionData.newIsr());
                if (!Replicas.validateIsr(partition.replicas, newIsr)) {
                    responseTopicData.partitions().add(new AlterIsrResponseData.PartitionData().
                        setPartitionIndex(partitionData.partitionIndex()).
                        setErrorCode(Errors.INVALID_REQUEST.code()));
                }
                int newLeader, newLeaderEpoch;
                if (Replicas.contains(newIsr, partition.leader)) {
                    newLeader = partition.leader;
                    newLeaderEpoch = partition.leaderEpoch;
                } else {
                    newLeader = partition.chooseNewLeader(newIsr);
                    newLeaderEpoch = partition.leaderEpoch + 1;
                }
                records.add(new ApiMessageAndVersion(new IsrChangeRecord().
                    setPartitionId(partitionData.partitionIndex()).
                    setTopicId(topic.id).
                    setIsr(partitionData.newIsr()).
                    setLeader(newLeader).
                    setLeaderEpoch(newLeaderEpoch).
                    setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
            }
        }
        return new ControllerResult<>(records, response);
    }

    public List<ApiMessageAndVersion> handleNewlyFenced(Set<Integer> newlyFenced) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        for (Integer brokerId : newlyFenced) {
            TimelineHashMap<Uuid, int[]> topicMap = isrMembers.get(brokerId);
            for (Entry<Uuid, int[]> entry : topicMap.entrySet()) {
               Uuid topicId = entry.getKey();
               int[] partitionIds = entry.getValue();
               TopicControlInfo topic = topics.get(topicId);
               if (topic == null) {
                   throw new RuntimeException("Topic ID " + topicId + " existed in " +
                       "isrMembers, but not in the topics map.");
               }
               for (int partitionId : partitionIds) {
                   PartitionControlInfo partition = topic.parts.get(partitionId);
                   if (partition == null) {
                       throw new RuntimeException("Partition " +
                           topicId + ":" + partitionId + " existed in " +
                           "isrMembers, but not in the partitions map.");
                   }
                   int[] newIsr = Replicas.copyWithout(partition.isr, brokerId);
                   int newLeader, newLeaderEpoch;
                   if (partition.leader == brokerId) {
                       newLeader = partition.chooseNewLeader(newIsr);
                       newLeaderEpoch = partition.leaderEpoch + 1;
                   } else {
                       newLeader = partition.leader;
                       newLeaderEpoch = partition.leaderEpoch;
                   }
                   records.add(new ApiMessageAndVersion(new IsrChangeRecord().
                       setPartitionId(partitionId).
                       setTopicId(topic.id).
                       setIsr(Replicas.toList(newIsr)).
                       setLeader(newLeader).
                       setLeaderEpoch(newLeaderEpoch).
                       setPartitionEpoch(partition.partitionEpoch + 1), (short) 0));
               }
            }
        }
        return records;
    }
}

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
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
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

import static org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;


public class ReplicationControlManager {
    private final static int[] EMPTY = new int[0];

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

        PartitionControlInfo(PartitionRecord record) {
            this(toArray(record.replicas()),
                toArray(record.isr()),
                toArray(record.removingReplicas()),
                toArray(record.addingReplicas()),
                record.leader(),
                record.leaderEpoch());
        }

        PartitionControlInfo(int[] replicas, int[] isr, int[] removingReplicas,
                             int[] addingReplicas, int leader, int leaderEpoch) {
            this.replicas = replicas;
            this.isr = isr;
            this.removingReplicas = removingReplicas;
            this.addingReplicas = addingReplicas;
            this.leader = leader;
            this.leaderEpoch = leaderEpoch;
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
            return builder.toString();
        }

        @Override
        public int hashCode() {
            return Objects.hash(replicas, isr, removingReplicas, addingReplicas, leader,
                leaderEpoch);
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
            builder.append(")");
            return builder.toString();
        }
    }

    private final SnapshotRegistry snapshotRegistry;
    private final Logger log;

    /**
     * The random number generator used by this object.
     */
    private final RandomSource random;

    /**
     * The KIP-464 default replication factor that is used if a CreateTopics request does
     * not specify one.
     */
    private final int defaultReplicationFactor;

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
                              RandomSource random,
                              int defaultReplicationFactor,
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

    public void replay(PartitionRecord message) {
        TopicControlInfo topicInfo = topics.get(message.topicId());
        if (topicInfo == null) {
            throw new RuntimeException("Tried to create partition " + message.topicId() +
                "-" + message.partitionId() + ", but no topic with that ID was found.");
        }
        PartitionControlInfo newPartInfo = new PartitionControlInfo(message);
        PartitionControlInfo prevPartInfo = topicInfo.parts.get(message.partitionId());
        if (prevPartInfo == null) {
            log.info("Created partition {}-{} with {}.", message.topicId(),
                message.partitionId(), newPartInfo.toString());
            topicInfo.parts.put(message.partitionId(), newPartInfo);
            updateIsrMembers(message.topicId(), message.partitionId(), EMPTY, newPartInfo.isr);
        } else {
            String diff = newPartInfo.diff(prevPartInfo);
            if (!diff.isEmpty()) {
                log.info("Modified partition {}-{}: {}.", message.topicId(),
                    message.partitionId(), diff);
                topicInfo.parts.put(message.partitionId(), newPartInfo);
                updateIsrMembers(message.topicId(), message.partitionId(),
                    prevPartInfo.isr, newPartInfo.isr);
            }
        }
    }

    private void updateIsrMembers(Uuid topicId, int partitionId, int[] prevIsr, int[] nextIsr) {
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
                    } else if (!clusterControl.isUsable(brokerId)) {
                        return new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
                            "The manual partition assignment contains node " + brokerId +
                                ", but that node is not usable.");
                    }
                }
                int[] replicas = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    replicas[i] = assignment.brokerIds().get(0);
                }
                int[] isr = new int[assignment.brokerIds().size()];
                for (int i = 0; i < replicas.length; i++) {
                    isr[i] = assignment.brokerIds().get(0);
                }
                newParts.put(assignment.partitionIndex(),
                    new PartitionControlInfo(replicas, isr, null, null, isr[0], 0));
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
            int replicationFactor = topic.replicationFactor() == -1 ?
                defaultReplicationFactor : topic.replicationFactor();
            for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
                List<Integer> replicas;
                try {
                    replicas = clusterControl.chooseRandomUsable(random, replicationFactor);
                } catch (Exception e) {
                    return new ApiError(Errors.INVALID_REPLICATION_FACTOR,
                        "Unable to replicate the partition " + replicationFactor +
                            " times: " + e.getMessage());
                }
                newParts.put(partitionId, new PartitionControlInfo(toArray(replicas),
                    toArray(replicas), null, null, replicas.get(0), 0));
            }
        }
        Uuid topicId = Uuid.randomUuid();
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
                setReplicas(toList(info.replicas)).
                setIsr(toList(info.isr)).
                setRemovingReplicas(null).
                setAddingReplicas(null).
                setLeader(info.leader).
                setLeaderEpoch(info.leaderEpoch), (short) 0));
        }
        return ApiError.NONE;
    }

    private static List<Integer> toList(int[] array) {
        if (array == null) return null;
        ArrayList<Integer> list = new ArrayList<>(array.length);
        for (int i = 0; i < array.length; i++) {
            list.add(array[i]);
        }
        return list;
    }

    private static int[] toArray(List<Integer> list) {
        if (list == null) return null;
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }

    static void validateNewTopicNames(Map<String, ApiError> topicErrors,
                                      CreatableTopicCollection topics) {
        for (CreatableTopic topic : topics) {
            if (topicErrors.containsKey(topic.name())) continue;
            try {
                Topic.validate(topic.name());
            } catch (Exception e) {
                topicErrors.put(topic.name(),
                    new ApiError(Errors.INVALID_REQUEST, "Illegal topic name."));
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
}

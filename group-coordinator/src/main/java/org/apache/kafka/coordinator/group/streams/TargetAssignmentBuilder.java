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
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.GroupSpecImpl;
import org.apache.kafka.coordinator.group.streams.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignor;
import org.apache.kafka.coordinator.group.streams.assignor.TaskAssignorException;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredTopology;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Build the new target member assignments based on the provided parameters by calling the task assignor.
 * As a result,
 * it yields the records that must be persisted to the log and the new member assignments as a map from member ID to tasks tuple.
 * <p>
 * Records are only created for members which have a new target assignment. If their assignment did not change, no new record is needed.
 * <p>
 * When a member is deleted, it is assumed that its target assignment record is deleted as part of the member deletion process. In other
 * words, this class does not yield a tombstone for removed members.
 */
public class TargetAssignmentBuilder {

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The time.
     */
    private Time time;

    /**
     * The group ID.
     */
    private final String groupId;
    /**
     * The group epoch.
     */
    private final int groupEpoch;

    /**
     * The partition assignor used to compute the assignment.
     */
    private final TaskAssignor assignor;

    /**
     * The assignment configs.
     */
    private final Map<String, String> assignmentConfigs;

    /**
     * The members which have been updated or deleted. A null value signals deleted members.
     */
    private final Map<String, StreamsGroupMember> updatedMembers = new HashMap<>();

    /**
     * The members in the group.
     */
    private Map<String, StreamsGroupMember> members = Map.of();

    /**
     * The metadata image.
     */
    private CoordinatorMetadataImage metadataImage = CoordinatorMetadataImage.EMPTY;

    /**
     * The existing target assignment.
     */
    private Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment = Map.of();

    /**
     * The topology.
     */
    private ConfiguredTopology topology;

    /**
     * The static members in the group.
     */
    private Map<String, String> staticMembers = Map.of();

    /**
     * Constructs the object.
     *
     * @param groupId    The group ID.
     * @param groupEpoch The group epoch to compute a target assignment for.
     * @param assignor   The assignor to use to compute the target assignment.
     */
    public TargetAssignmentBuilder(
        LogContext logContext,
        String groupId,
        int groupEpoch,
        TaskAssignor assignor,
        Map<String, String> assignmentConfigs
    ) {
        this.log = logContext.logger(TargetAssignmentBuilder.class);
        this.groupId = Objects.requireNonNull(groupId);
        this.groupEpoch = groupEpoch;
        this.assignor = Objects.requireNonNull(assignor);
        this.assignmentConfigs = Objects.requireNonNull(assignmentConfigs);
    }

    static AssignmentMemberSpec createAssignmentMemberSpec(
        StreamsGroupMember member,
        TasksTuple targetAssignment
    ) {
        return new AssignmentMemberSpec(
            member.instanceId(),
            member.rackId(),
            targetAssignment.activeTasks(),
            targetAssignment.standbyTasks(),
            targetAssignment.warmupTasks(),
            member.processId(),
            member.clientTags(),
            Map.of(),
            Map.of()
        );
    }

    /**
     * Sets the time.
     *
     * @param time The time.
     * @return This object.
     */
    public TargetAssignmentBuilder withTime(Time time) {
        this.time = time;
        return this;
    }

    /**
     * Adds all the existing members.
     *
     * @param members The existing members in the streams group.
     * @return This object.
     */
    public TargetAssignmentBuilder withMembers(
        Map<String, StreamsGroupMember> members
    ) {
        this.members = members;
        return this;
    }

    /**
     * Adds all the existing static members.
     *
     * @param staticMembers The existing static members in the streams group.
     * @return This object.
     */
    public TargetAssignmentBuilder withStaticMembers(
        Map<String, String> staticMembers
    ) {
        this.staticMembers = staticMembers;
        return this;
    }

    /**
     * Adds the metadata image to use.
     *
     * @param metadataImage The metadata image.
     * @return This object.
     */
    public TargetAssignmentBuilder withMetadataImage(
        CoordinatorMetadataImage metadataImage
    ) {
        this.metadataImage = metadataImage;
        return this;
    }

    /**
     * Adds the existing target assignment.
     *
     * @param targetAssignment The existing target assignment.
     * @return This object.
     */
    public TargetAssignmentBuilder withTargetAssignment(
        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> targetAssignment
    ) {
        this.targetAssignment = targetAssignment;
        return this;
    }

    /**
     * Adds the topology image.
     *
     * @param topology The topology.
     * @return This object.
     */
    public TargetAssignmentBuilder withTopology(
        ConfiguredTopology topology
    ) {
        this.topology = topology;
        return this;
    }

    /**
     * Adds or updates a member. This is useful when the updated member is not yet materialized in memory.
     *
     * @param memberId The member ID.
     * @param member   The member to add or update.
     * @return This object.
     */
    public TargetAssignmentBuilder addOrUpdateMember(
        String memberId,
        StreamsGroupMember member
    ) {
        this.updatedMembers.put(memberId, member);
        return this;
    }

    /**
     * Removes a member. This is useful when the removed member is not yet materialized in memory.
     *
     * @param memberId The member ID.
     * @return This object.
     */
    public TargetAssignmentBuilder removeMember(
        String memberId
    ) {
        return addOrUpdateMember(memberId, null);
    }

    /**
     * Builds the new target assignment.
     *
     * @return The new target assignment.
     * @throws TaskAssignorException if the target assignment cannot be computed.
     */
    public GroupAssignment buildTargetAssignment() throws TaskAssignorException {
        Map<String, AssignmentMemberSpec> memberSpecs = new HashMap<>();

        // Prepare the member spec for all members.
        members.forEach((memberId, member) -> memberSpecs.put(memberId, createAssignmentMemberSpec(
            member,
            targetAssignment.getOrDefault(memberId, org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY)
        )));

        // Update the member spec if updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberSpecs.remove(memberId);
            } else {
                org.apache.kafka.coordinator.group.streams.TasksTuple assignment = targetAssignment.getOrDefault(memberId,
                    org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY);

                // A new static member joins and needs to replace an existing departed one.
                if (updatedMemberOrNull.instanceId().isPresent()) {
                    String previousMemberId = staticMembers.get(updatedMemberOrNull.instanceId().get());
                    if (previousMemberId != null && !previousMemberId.equals(memberId)) {
                        assignment = targetAssignment.getOrDefault(previousMemberId,
                            org.apache.kafka.coordinator.group.streams.TasksTuple.EMPTY);
                    }
                }

                memberSpecs.put(memberId, createAssignmentMemberSpec(
                    updatedMemberOrNull,
                    assignment
                ));
            }
        });

        // Compute the assignment.
        GroupAssignment newGroupAssignment;
        if (topology.isReady()) {
            if (topology.subtopologies().isEmpty()) {
                throw new IllegalStateException("Subtopologies must be present if topology is ready.");
            }
            newGroupAssignment = assignor.assign(
                new GroupSpecImpl(
                    Collections.unmodifiableMap(memberSpecs),
                    assignmentConfigs
                ),
                new TopologyMetadata(metadataImage, topology.subtopologies().get())
            );
        } else {
            newGroupAssignment = new GroupAssignment(
                memberSpecs.keySet().stream().collect(Collectors.toMap(x -> x, x -> MemberAssignment.empty())));
        }

        return newGroupAssignment;
    }

    /**
     * Builds the records for the new target assignment, when the set of members and static members
     * have not changed since the assignment was built.
     *
     * @param newGroupAssignment    The new target assignment.
     * @return A TargetAssignmentResult which contains the records to update
     *         the existing target assignment.
     */
    public TargetAssignmentResult buildRecords(GroupAssignment newGroupAssignment) {
        return buildRecords(newGroupAssignment, Optional.empty(), Optional.empty());
    }

    /**
     * Builds the records for the new target assignment, when the set of members and static members
     * may have changed since the assignment was built.
     *
     * @param newGroupAssignment    The new target assignment.
     * @param currentMemberIds      The current set of member ids.
     * @param currentStaticMembers  The current static members.
     * @return A TargetAssignmentResult which contains the records to update
     *         the existing target assignment.
     */
    public TargetAssignmentResult buildRecords(
        GroupAssignment newGroupAssignment,
        Set<String> currentMemberIds,
        Map<String, String> currentStaticMembers
    ) {
        return buildRecords(newGroupAssignment, Optional.of(currentMemberIds), Optional.of(currentStaticMembers));
    }

    /**
     * Builds the records for the new target assignment.
     *
     * @param newGroupAssignment    The new target assignment.
     * @param currentMemberIds      The current set of member ids, if they may have changed since the assignment was built.
     * @param currentStaticMembers  The current static members, if they may have changed since the assignment was built.
     * @return A TargetAssignmentResult which contains the records to update
     *         the existing target assignment.
     */
    public TargetAssignmentResult buildRecords(
        GroupAssignment newGroupAssignment,
        Optional<Set<String>> currentMemberIds,
        Optional<Map<String, String>> currentStaticMembers
    ) {
        Set<String> memberIds = new HashSet<>(members.keySet());

        // Update the member ids for updated or deleted members.
        updatedMembers.forEach((memberId, updatedMemberOrNull) -> {
            if (updatedMemberOrNull == null) {
                memberIds.remove(memberId);
            } else {
                memberIds.add(memberId);
            }
        });

        // Build map of replacement member ids for static members that have churned.
        Map<String, String> staticMemberIdRemapping = new HashMap<>();
        if (currentStaticMembers.isPresent()) {
            for (Map.Entry<String, String> entry : staticMembers.entrySet()) {
                String instanceId = entry.getKey();
                String oldMemberId = entry.getValue();
                String newMemberId = currentStaticMembers.get().get(instanceId);
                staticMemberIdRemapping.put(oldMemberId, newMemberId);

                if (log.isDebugEnabled()) {
                    if (newMemberId == null) {
                        log.debug("[GroupId {}] Skipping target assignment record for removed static member {} with instance id {}.",
                            groupId, oldMemberId, instanceId);
                    } else if (!oldMemberId.equals(newMemberId)) {
                        log.debug("[GroupId {}] Replacing static member id {} with {} for instance id {} in target assignment.",
                            groupId, oldMemberId, newMemberId, instanceId);
                    }
                }
            }
        }

        // Compute delta from previous to new target assignment and create the
        // relevant records.
        List<CoordinatorRecord> records = new ArrayList<>();
        Map<String, org.apache.kafka.coordinator.group.streams.TasksTuple> newTargetAssignment = new HashMap<>();

        memberIds.forEach(memberId -> {
            String newMemberId = memberId;

            // Run the member id through the static member remapping.
            if (staticMemberIdRemapping.containsKey(memberId)) {
                newMemberId = staticMemberIdRemapping.get(memberId);
                if (newMemberId == null) {
                    // The static member has been removed.
                    return;
                }
            }

            // Don't emit records for members that have been removed.
            if (currentMemberIds.isPresent() && !currentMemberIds.get().contains(newMemberId)) {
                log.debug("[GroupId {}] Skipping target assignment record for removed member {}.", groupId, newMemberId);
                return;
            }

            org.apache.kafka.coordinator.group.streams.TasksTuple oldMemberAssignment = targetAssignment.get(memberId);
            org.apache.kafka.coordinator.group.streams.TasksTuple newMemberAssignment = newMemberAssignment(newGroupAssignment, memberId);

            newTargetAssignment.put(newMemberId, newMemberAssignment);

            if (oldMemberAssignment == null) {
                // If the member had no assignment, we always create a record for it.
                records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                    groupId,
                    newMemberId,
                    newMemberAssignment
                ));
            } else {
                // If the member had an assignment, we only create a record if the
                // new assignment is different.
                if (!newMemberAssignment.equals(oldMemberAssignment)) {
                    records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(
                        groupId,
                        newMemberId,
                        newMemberAssignment
                    ));
                }
            }
        });

        // Bump the target assignment epoch.
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(
            groupId,
            groupEpoch,
            time.milliseconds()
        ));

        return new TargetAssignmentResult(records, newTargetAssignment);
    }

    private TasksTuple newMemberAssignment(
        GroupAssignment newGroupAssignment,
        String memberId
    ) {
        MemberAssignment newMemberAssignment = newGroupAssignment.members().get(memberId);
        if (newMemberAssignment != null) {
            return new TasksTuple(
                newMemberAssignment.activeTasks(),
                newMemberAssignment.standbyTasks(),
                newMemberAssignment.warmupTasks()
            );
        } else {
            return TasksTuple.EMPTY;
        }
    }

    /**
     * The assignment result returned by {{@link TargetAssignmentBuilder#build()}}.
     *
     * @param records          The records that must be applied to the __consumer_offsets topics to persist the new target assignment.
     * @param targetAssignment The new target assignment for the group.
     */
    public record TargetAssignmentResult(
        List<CoordinatorRecord> records,
        Map<String, TasksTuple> targetAssignment
    ) {
        public TargetAssignmentResult {
            Objects.requireNonNull(records);
            Objects.requireNonNull(targetAssignment);
        }
    }
}

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
package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.OffsetsUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

public class ConsumerGroupCommand {

    static final String MISSING_COLUMN_VALUE = "-";

    public static void main(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        try {
            List<OptionSpec<?>> actions = List.of(
                opts.listOpt,
                opts.describeOpt,
                opts.deleteOpt,
                opts.resetOffsetsOpt,
                opts.deleteOffsetsOpt,
                opts.validateRegexOpt
            );

            // Should have exactly one action.
            if (actions.stream().filter(opts.options::has).count() != 1) {
                CommandLineUtils.printUsageAndExit(
                    opts.parser,
                    String.format(
                        "Command must include exactly one action: %s",
                        actions.stream().map(opt ->
                            "--" + opt.options().get(0)
                        ).collect(Collectors.joining(", "))
                    )
                );
            }

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    static void run(ConsumerGroupCommandOptions opts) {
        if (opts.options.has(opts.validateRegexOpt)) {
            validateRegex(opts.options.valueOf(opts.validateRegexOpt));
            return;
        }

        try (ConsumerGroupService consumerGroupService = new ConsumerGroupService(opts, Map.of())) {
            if (opts.options.has(opts.listOpt))
                consumerGroupService.listGroups();
            else if (opts.options.has(opts.describeOpt))
                consumerGroupService.describeGroups();
            else if (opts.options.has(opts.deleteOpt))
                consumerGroupService.deleteGroups();
            else if (opts.options.has(opts.resetOffsetsOpt)) {
                Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsToReset = consumerGroupService.resetOffsets();
                if (opts.options.has(opts.exportOpt)) {
                    String exported = consumerGroupService.exportOffsetsToCsv(offsetsToReset);
                    System.out.println(exported);
                } else
                    OffsetsUtils.printOffsetsToReset(offsetsToReset);
            } else if (opts.options.has(opts.deleteOffsetsOpt)) {
                consumerGroupService.deleteOffsets();
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing consumer group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static void validateRegex(String regex) {
        try {
            Pattern.compile(regex);
            System.out.printf("The regular expression `%s` is valid.%n", regex);
        } catch (PatternSyntaxException ex) {
            System.out.printf("The regular expression `%s` is invalid: %s.%n", regex, ex.getDescription());
        }
    }

    static Set<GroupState> groupStatesFromString(String input) {
        Set<GroupState> parsedStates = Arrays.stream(input.split(",")).map(s -> GroupState.parse(s.trim())).collect(Collectors.toSet());
        Set<GroupState> validStates = GroupState.groupStatesForType(GroupType.CONSUMER);
        if (!validStates.containsAll(parsedStates)) {
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " +
                    validStates.stream().map(GroupState::toString).collect(Collectors.joining(", ")));
        }
        return parsedStates;
    }

    @SuppressWarnings("Regexp")
    static Set<GroupType> consumerGroupTypesFromString(String input) {
        Set<GroupType> validTypes = Set.of(GroupType.CLASSIC, GroupType.CONSUMER);
        Set<GroupType> parsedTypes = Stream.of(input.toLowerCase().split(",")).map(s -> GroupType.parse(s.trim())).collect(Collectors.toSet());
        if (!validTypes.containsAll(parsedTypes)) {
            throw new IllegalArgumentException("Invalid types list '" + input + "'. Valid types are: " +
                String.join(", ", validTypes.stream().map(GroupType::toString).collect(Collectors.toSet())));
        }
        return parsedTypes;
    }

    static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    @SuppressWarnings("ClassFanOutComplexity")
    static class ConsumerGroupService implements AutoCloseable {
        final ConsumerGroupCommandOptions opts;
        final Map<String, String> configOverrides;
        private final Admin adminClient;
        private final OffsetsUtils offsetsUtils;

        ConsumerGroupService(ConsumerGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            this.configOverrides = configOverrides;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.offsetsUtils = new OffsetsUtils(adminClient, opts.parser, getOffsetsUtilsOptions(opts));
        }

        private OffsetsUtils.OffsetsUtilsOptions getOffsetsUtilsOptions(ConsumerGroupCommandOptions opts) {
            return
                new OffsetsUtils.OffsetsUtilsOptions(opts.options.valuesOf(opts.groupOpt),
                    opts.options.valuesOf(opts.resetToOffsetOpt),
                    opts.options.valuesOf(opts.resetFromFileOpt),
                    opts.options.valuesOf(opts.resetToDatetimeOpt),
                    opts.options.valueOf(opts.resetByDurationOpt),
                    opts.options.valueOf(opts.resetShiftByOpt),
                    opts.options.valueOf(opts.timeoutMsOpt));
        }

        void listGroups() throws ExecutionException, InterruptedException {
            boolean includeType = opts.options.has(opts.typeOpt);
            boolean includeState = opts.options.has(opts.stateOpt);

            if (includeType || includeState) {
                Set<GroupType> types = typeValues();
                Set<GroupState> states = stateValues();
                List<GroupListing> listings = listConsumerGroupsWithFilters(types, states);

                printGroupInfo(listings, includeType, includeState);
            } else {
                listConsumerGroups().forEach(System.out::println);
            }
        }

        private Set<GroupState> stateValues() {
            String stateValue = opts.options.valueOf(opts.stateOpt);
            return (stateValue == null || stateValue.isEmpty())
                ? Set.of()
                : groupStatesFromString(stateValue);
        }

        private Set<GroupType> typeValues() {
            String typeValue = opts.options.valueOf(opts.typeOpt);
            return (typeValue == null || typeValue.isEmpty())
                ? Set.of()
                : consumerGroupTypesFromString(typeValue);
        }

        private void printGroupInfo(List<GroupListing> groups, boolean includeType, boolean includeState) {
            Function<GroupListing, String> groupId = GroupListing::groupId;
            Function<GroupListing, String> groupType = groupListing -> groupListing.type().orElse(GroupType.UNKNOWN).toString();
            Function<GroupListing, String> groupState = groupListing -> groupListing.groupState().orElse(GroupState.UNKNOWN).toString();

            OptionalInt maybeMax = groups.stream().mapToInt(groupListing -> Math.max(15, groupId.apply(groupListing).length())).max();
            int maxGroupLen = maybeMax.orElse(15) + 10;
            String format = "%-" + maxGroupLen + "s";
            List<String> header = new ArrayList<>();
            header.add("GROUP");
            List<Function<GroupListing, String>> extractors = new ArrayList<>();
            extractors.add(groupId);

            if (includeType) {
                header.add("TYPE");
                extractors.add(groupType);
                format += " %-20s";
            }

            if (includeState) {
                header.add("STATE");
                extractors.add(groupState);
                format += " %-20s";
            }

            System.out.printf(format + "%n", header.toArray(new Object[0]));

            for (GroupListing groupListing : groups) {
                Object[] info = extractors.stream().map(extractor -> extractor.apply(groupListing)).toArray(Object[]::new);
                System.out.printf(format + "%n", info);
            }
        }

        List<String> listConsumerGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(withTimeoutMs(ListGroupsOptions.forConsumerGroups()));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().map(GroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listConsumerGroupsWithFilters(Set<GroupType> types, Set<GroupState> states) throws ExecutionException, InterruptedException {
            ListGroupsOptions listGroupsOptions = withTimeoutMs(ListGroupsOptions.forConsumerGroups());
            listGroupsOptions
                .inGroupStates(states)
                .withTypes(types);
            ListGroupsResult result = adminClient.listGroups(listGroupsOptions);
            return new ArrayList<>(result.all().get());
        }

        private boolean shouldPrintMemberState(String group, Optional<GroupState> state, Optional<Integer> numRows) {
            // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
            // if it's undefined or 0, there is no relevant group information to display.
            if (numRows.isEmpty()) {
                printError("The consumer group '" + group + "' does not exist.", Optional.empty());
                return false;
            }

            int num = numRows.get();

            GroupState state0 = state.orElse(GroupState.UNKNOWN);
            switch (state0) {
                case DEAD:
                    printError("Consumer group '" + group + "' does not exist.", Optional.empty());
                    break;
                case EMPTY:
                    System.err.println("\nConsumer group '" + group + "' has no active members.");
                    break;
                case PREPARING_REBALANCE:
                case COMPLETING_REBALANCE:
                case ASSIGNING:
                case RECONCILING:
                    System.err.println("\nWarning: Consumer group '" + group + "' is rebalancing.");
                    break;
                case STABLE:
                    break;
                default:
                    // the control should never reach here
                    throw new KafkaException("Expected a valid consumer group state, but found '" + state0 + "'.");
            }

            return !state0.equals(GroupState.DEAD) && num > 0;
        }

        private Optional<Integer> size(Optional<? extends Collection<?>> colOpt) {
            return colOpt.map(Collection::size);
        }

        private void printOffsets(
            Map<String, Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>>> offsets,
            boolean verbose
        ) {
            offsets.forEach((groupId, tuple) -> {
                Optional<GroupState> state = tuple.getKey();
                Optional<Collection<PartitionAssignmentState>> assignments = tuple.getValue();

                if (shouldPrintMemberState(groupId, state, size(assignments))) {
                    String format = printOffsetFormat(assignments, verbose);

                    if (verbose) {
                        System.out.printf(format, "GROUP", "TOPIC", "PARTITION", "LEADER-EPOCH", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");
                    } else {
                        System.out.printf(format, "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");
                    }

                    if (assignments.isPresent()) {
                        Collection<PartitionAssignmentState> consumerAssignments = assignments.get();
                        for (PartitionAssignmentState consumerAssignment : consumerAssignments) {
                            if (verbose) {
                                System.out.printf(format,
                                    consumerAssignment.group,
                                    consumerAssignment.topic.orElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.leaderEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.offset.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.lag.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.host.orElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.orElse(MISSING_COLUMN_VALUE)
                                );
                            } else {
                                System.out.printf(format,
                                    consumerAssignment.group,
                                    consumerAssignment.topic.orElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.offset.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.lag.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.orElse(MISSING_COLUMN_VALUE),
                                    consumerAssignment.host.orElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.orElse(MISSING_COLUMN_VALUE)
                                );
                            }
                        }
                        System.out.println();
                    }
                }
            });
        }

        private static String printOffsetFormat(
            Optional<Collection<PartitionAssignmentState>> assignments,
            boolean verbose
        ) {
            // find proper columns width
            int maxGroupLen = 15, maxTopicLen = 15, maxConsumerIdLen = 15, maxHostLen = 15;
            if (assignments.isPresent()) {
                Collection<PartitionAssignmentState> consumerAssignments = assignments.get();
                for (PartitionAssignmentState consumerAssignment : consumerAssignments) {
                    maxGroupLen = Math.max(maxGroupLen, consumerAssignment.group.length());
                    maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.orElse(MISSING_COLUMN_VALUE).length());
                    maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.orElse(MISSING_COLUMN_VALUE).length());
                    maxHostLen = Math.max(maxHostLen, consumerAssignment.host.orElse(MISSING_COLUMN_VALUE).length());

                }
            }

            if (verbose) {
                return "\n%" + (-maxGroupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %-15s %-15s %-15s %" + (-maxConsumerIdLen) + "s %" + (-maxHostLen) + "s %s";
            } else {
                return "\n%" + (-maxGroupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %-15s %-15s %" + (-maxConsumerIdLen) + "s %" + (-maxHostLen) + "s %s";
            }
        }

        private void printMembers(Map<String, Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>>> members, boolean verbose) {
            members.forEach((groupId, tuple) -> {
                Optional<GroupState> groupState = tuple.getKey();
                Optional<Collection<MemberAssignmentState>> assignments = tuple.getValue();
                int maxGroupLen = 15, maxConsumerIdLen = 15, maxGroupInstanceIdLen = 17, maxHostLen = 15, maxClientIdLen = 15,
                    maxCurrentAssignment = 20, maxTargetAssignment = 20;
                boolean includeGroupInstanceId = false;
                boolean hasClassicMember = false;
                boolean hasConsumerMember = false;

                if (shouldPrintMemberState(groupId, groupState, size(assignments))) {
                    // find proper columns width
                    if (assignments.isPresent()) {
                        for (MemberAssignmentState memberAssignment : assignments.get()) {
                            maxGroupLen = Math.max(maxGroupLen, memberAssignment.group.length());
                            maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length());
                            maxGroupInstanceIdLen =  Math.max(maxGroupInstanceIdLen, memberAssignment.groupInstanceId.length());
                            maxHostLen = Math.max(maxHostLen, memberAssignment.host.length());
                            maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length());
                            includeGroupInstanceId = includeGroupInstanceId || !memberAssignment.groupInstanceId.isEmpty();
                            String currentAssignment = memberAssignment.assignment.isEmpty() ?
                                MISSING_COLUMN_VALUE : getAssignmentString(memberAssignment.assignment);
                            String targetAssignment = memberAssignment.targetAssignment.isEmpty() ?
                                MISSING_COLUMN_VALUE : getAssignmentString(memberAssignment.targetAssignment);
                            maxCurrentAssignment = Math.max(maxCurrentAssignment, currentAssignment.length());
                            maxTargetAssignment = Math.max(maxTargetAssignment, targetAssignment.length());
                            hasClassicMember = hasClassicMember || (memberAssignment.upgraded.isPresent() && !memberAssignment.upgraded.get());
                            hasConsumerMember = hasConsumerMember || (memberAssignment.upgraded.isPresent() && memberAssignment.upgraded.get());
                        }
                    }
                }

                String formatWithGroupInstanceId = "%" + -maxGroupLen + "s %" + -maxConsumerIdLen + "s %" + -maxGroupInstanceIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-15s ";
                String formatWithoutGroupInstanceId = "%" + -maxGroupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-15s ";
                if (includeGroupInstanceId) {
                    System.out.printf("\n" + formatWithGroupInstanceId, "GROUP", "CONSUMER-ID", "GROUP-INSTANCE-ID", "HOST", "CLIENT-ID", "#PARTITIONS");
                } else {
                    System.out.printf("\n" + formatWithoutGroupInstanceId, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS");
                }

                String formatWithUpgrade = "%-15s %" + -maxCurrentAssignment + "s %-15s %" + -maxTargetAssignment + "s %s";
                String formatWithoutUpgrade = "%-15s %" + -maxCurrentAssignment + "s %-15s %" + -maxTargetAssignment + "s";
                boolean hasMigrationMember = hasClassicMember && hasConsumerMember;
                if (verbose) {
                    if (hasMigrationMember) {
                        System.out.printf(formatWithUpgrade, "CURRENT-EPOCH", "CURRENT-ASSIGNMENT", "TARGET-EPOCH", "TARGET-ASSIGNMENT", "UPGRADED");
                    } else {
                        System.out.printf(formatWithoutUpgrade, "CURRENT-EPOCH", "CURRENT-ASSIGNMENT", "TARGET-EPOCH", "TARGET-ASSIGNMENT");
                    }
                }
                System.out.println();

                if (assignments.isPresent()) {
                    printMembersHelper(assignments.get(), verbose, includeGroupInstanceId, hasMigrationMember,
                        formatWithGroupInstanceId, formatWithoutGroupInstanceId, formatWithUpgrade, formatWithoutUpgrade);
                }
            });
        }

        private void printMembersHelper(
            Collection<MemberAssignmentState> memberAssignments,
            boolean verbose,
            boolean includeGroupInstanceId,
            boolean hasMigrationMember,
            String formatWithGroupInstanceId,
            String formatWithoutGroupInstanceId,
            String formatWithUpgrade,
            String formatWithoutUpgrade
        ) {
            for (MemberAssignmentState memberAssignment : memberAssignments) {
                if (includeGroupInstanceId) {
                    System.out.printf(formatWithGroupInstanceId, memberAssignment.group, memberAssignment.consumerId,
                        memberAssignment.groupInstanceId, memberAssignment.host, memberAssignment.clientId,
                        memberAssignment.numPartitions);
                } else {
                    System.out.printf(formatWithoutGroupInstanceId, memberAssignment.group, memberAssignment.consumerId,
                        memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions);
                }
                if (verbose) {
                    String currentEpoch = memberAssignment.currentEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE);
                    String currentAssignment = memberAssignment.assignment.isEmpty() ?
                        MISSING_COLUMN_VALUE : getAssignmentString(memberAssignment.assignment);
                    String targetEpoch = memberAssignment.targetEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE);
                    String targetAssignment = memberAssignment.targetAssignment.isEmpty() ?
                        MISSING_COLUMN_VALUE : getAssignmentString(memberAssignment.targetAssignment);
                    if (hasMigrationMember) {
                        System.out.printf(formatWithUpgrade, currentEpoch, currentAssignment, targetEpoch, targetAssignment,
                            memberAssignment.upgraded.map(Object::toString).orElse(MISSING_COLUMN_VALUE));
                    } else {
                        System.out.printf(formatWithoutUpgrade, currentEpoch, currentAssignment, targetEpoch, targetAssignment);
                    }
                }
                System.out.println();
            }
        }

        private String getAssignmentString(List<TopicPartition> assignment) {
            Map<String, List<TopicPartition>> grouped = new HashMap<>();
            assignment.forEach(tp ->
                grouped
                    .computeIfAbsent(tp.topic(), key -> new ArrayList<>())
                    .add(tp)
            );
            return grouped.entrySet().stream().map(entry -> {
                String topicName = entry.getKey();
                List<TopicPartition> topicPartitions = entry.getValue();
                return topicPartitions
                    .stream()
                    .map(TopicPartition::partition)
                    .sorted()
                    .map(Object::toString)
                    .collect(Collectors.joining(",", topicName + ":", ""));
            }).sorted().collect(Collectors.joining(";"));
        }

        private void printStates(Map<String, GroupInformation> states, boolean verbose) {
            states.forEach((groupId, state) -> {
                if (shouldPrintMemberState(groupId, Optional.of(state.groupState), Optional.of(1))) {
                    String coordinator = state.coordinator.host() + ":" + state.coordinator.port() + "  (" + state.coordinator.idString() + ")";
                    int coordinatorColLen = Math.max(25, coordinator.length());
                    int groupColLen = Math.max(15, state.group.length());

                    String assignmentStrategy = state.assignmentStrategy.isEmpty() ? MISSING_COLUMN_VALUE : state.assignmentStrategy;

                    if (verbose) {
                        String format = "\n%" + -groupColLen + "s %" + -coordinatorColLen + "s %-20s %-20s %-15s %-25s %s";
                        System.out.printf(format, "GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE",
                            "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
                        System.out.printf(format, state.group, coordinator, assignmentStrategy, state.groupState,
                            state.groupEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE), state.targetAssignmentEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE), state.numMembers);
                    } else {
                        String format = "\n%" + -groupColLen + "s %" + -coordinatorColLen + "s %-20s %-20s %s";
                        System.out.printf(format, "GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS");
                        System.out.printf(format, state.group, coordinator, assignmentStrategy, state.groupState, state.numMembers);
                    }
                    System.out.println();
                }
            });
        }

        void describeGroups() throws Exception {
            Collection<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);
            boolean membersOptPresent = opts.options.has(opts.membersOpt);
            boolean stateOptPresent = opts.options.has(opts.stateOpt);
            boolean offsetsOptPresent = opts.options.has(opts.offsetsOpt);
            long subActions = Stream.of(membersOptPresent, offsetsOptPresent, stateOptPresent).filter(x -> x).count();

            if (subActions == 0 || offsetsOptPresent) {
                TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>>> offsets
                    = collectGroupsOffsets(groupIds);
                printOffsets(offsets, opts.options.has(opts.verboseOpt));
            } else if (membersOptPresent) {
                TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>>> members
                    = collectGroupsMembers(groupIds);
                printMembers(members, opts.options.has(opts.verboseOpt));
            } else {
                TreeMap<String, GroupInformation> states = collectGroupsState(groupIds);
                printStates(states, opts.options.has(opts.verboseOpt));
            }
        }

        private Collection<PartitionAssignmentState> collectConsumerAssignment(
            String group,
            Optional<Node> coordinator,
            Collection<TopicPartition> topicPartitions,
            Map<TopicPartition, OffsetAndMetadata> committedOffsets,
            Optional<String> consumerIdOpt,
            Optional<String> hostOpt,
            Optional<String> clientIdOpt
        ) {
            if (topicPartitions.isEmpty()) {
                return Set.of(
                    new PartitionAssignmentState(group, coordinator, Optional.empty(), Optional.empty(), Optional.empty(),
                        getLag(Optional.empty(), Optional.empty()), consumerIdOpt, hostOpt, clientIdOpt, Optional.empty(), Optional.empty())
                );
            } else {
                return describePartitions(group, coordinator, topicPartitions, committedOffsets, consumerIdOpt, hostOpt, clientIdOpt);
            }
        }

        private Optional<Long> getLag(Optional<Long> offset, Optional<Long> logEndOffset) {
            return offset.filter(o -> o != -1).flatMap(offset0 -> logEndOffset.map(end -> end - offset0));
        }

        private Collection<PartitionAssignmentState> describePartitions(
            String group,
            Optional<Node> coordinator,
            Collection<TopicPartition> topicPartitions,
            Map<TopicPartition, OffsetAndMetadata> committedOffsets,
            Optional<String> consumerIdOpt,
            Optional<String> hostOpt,
            Optional<String> clientIdOpt
        ) {
            BiFunction<TopicPartition, Optional<Long>, PartitionAssignmentState> getDescribePartitionResult = (topicPartition, logEndOffsetOpt) -> {
                // The admin client returns `null` as a value to indicate that there is not committed offset for a partition.
                Optional<Long> offset = Optional.ofNullable(committedOffsets.get(topicPartition)).map(OffsetAndMetadata::offset);
                Optional<Integer> leaderEpoch = Optional.ofNullable(committedOffsets.get(topicPartition)).flatMap(OffsetAndMetadata::leaderEpoch);
                return new PartitionAssignmentState(group, coordinator, Optional.of(topicPartition.topic()),
                    Optional.of(topicPartition.partition()), offset, getLag(offset, logEndOffsetOpt),
                    consumerIdOpt, hostOpt, clientIdOpt, logEndOffsetOpt, leaderEpoch);
            };

            List<TopicPartition> topicPartitionsWithoutLeader = offsetsUtils.filterNoneLeaderPartitions(topicPartitions);
            List<TopicPartition> topicPartitionsWithLeader = topicPartitions.stream().filter(tp -> !topicPartitionsWithoutLeader.contains(tp)).toList();

            // prepare data for partitions with leaders
            List<PartitionAssignmentState> existLeaderAssignments = offsetsUtils.getLogEndOffsets(topicPartitionsWithLeader).entrySet().stream().map(logEndOffsetResult -> {
                if (logEndOffsetResult.getValue() instanceof OffsetsUtils.LogOffset)
                    return getDescribePartitionResult.apply(
                        logEndOffsetResult.getKey(),
                        Optional.of(((OffsetsUtils.LogOffset) logEndOffsetResult.getValue()).value())
                    );
                else if (logEndOffsetResult.getValue() instanceof OffsetsUtils.Unknown)
                    return getDescribePartitionResult.apply(logEndOffsetResult.getKey(), Optional.empty());
                else if (logEndOffsetResult.getValue() instanceof OffsetsUtils.Ignore)
                    return null;

                throw new IllegalStateException("Unknown LogOffset subclass: " + logEndOffsetResult.getValue());
            }).toList();

            // prepare data for partitions without leaders
            List<PartitionAssignmentState> noneLeaderAssignments = topicPartitionsWithoutLeader.stream()
                    .map(tp -> getDescribePartitionResult.apply(tp, Optional.empty())).toList();

            // concat the data and then sort them
            return Stream.concat(existLeaderAssignments.stream(), noneLeaderAssignments.stream())
                    .sorted(Comparator.<PartitionAssignmentState, String>comparing(
                            state -> state.topic.orElse(""), String::compareTo)
                            .thenComparingInt(state -> state.partition.orElse(-1)))
                    .collect(Collectors.toList());
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets() {
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);

            Map<String, KafkaFuture<ConsumerGroupDescription>> consumerGroups = adminClient.describeConsumerGroups(
                groupIds,
                withTimeoutMs(new DescribeConsumerGroupsOptions())
            ).describedGroups();

            Map<String, Map<TopicPartition, OffsetAndMetadata>> result = new HashMap<>();

            consumerGroups.forEach((groupId, groupDescription) -> {
                try {
                    String state = groupDescription.get().groupState().toString();
                    switch (state) {
                        case "Empty":
                        case "Dead":
                            result.put(groupId, resetOffsetsForInactiveGroup(groupId));
                            break;
                        default:
                            printError("Assignments can only be reset if the group '" + groupId + "' is inactive, but the current state is " + state + ".", Optional.empty());
                            result.put(groupId, Map.of());
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof GroupIdNotFoundException) {
                        result.put(groupId, resetOffsetsForInactiveGroup(groupId));
                    } else {
                        throw new RuntimeException(ee);
                    }
                }
            });

            return result;
        }

        private Map<TopicPartition, OffsetAndMetadata> resetOffsetsForInactiveGroup(String groupId) {
            try {
                Collection<TopicPartition> partitionsToReset = getPartitionsToReset(groupId);
                Map<TopicPartition, OffsetAndMetadata> preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset);

                // Dry-run is the default behavior if --execute is not specified
                boolean dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt);
                if (!dryRun) {
                    adminClient.alterConsumerGroupOffsets(
                        groupId,
                        preparedOffsets,
                        withTimeoutMs(new AlterConsumerGroupOffsetsOptions())
                    ).all().get();
                }

                return preparedOffsets;
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof KafkaException) {
                    throw (KafkaException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }

        Entry<Errors, Map<TopicPartition, Throwable>> deleteOffsets(String groupId, List<String> topics) {
            Map<TopicPartition, Throwable> partitionLevelResult = new HashMap<>();
            Set<String> topicWithPartitions = new HashSet<>();
            Set<String> topicWithoutPartitions = new HashSet<>();

            for (String topic : topics) {
                if (topic.contains(":"))
                    topicWithPartitions.add(topic);
                else
                    topicWithoutPartitions.add(topic);
            }

            List<TopicPartition> knownPartitions = topicWithPartitions.stream().flatMap(offsetsUtils::parseTopicsWithPartitions).toList();

            // Get the partitions of topics that the user did not explicitly specify the partitions
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(
                topicWithoutPartitions,
                withTimeoutMs(new DescribeTopicsOptions()));

            Iterator<TopicPartition> unknownPartitions = describeTopicsResult.topicNameValues().entrySet().stream().flatMap(e -> {
                String topic = e.getKey();
                try {
                    return e.getValue().get().partitions().stream().map(partition ->
                        new TopicPartition(topic, partition.partition()));
                } catch (ExecutionException | InterruptedException err) {
                    partitionLevelResult.put(new TopicPartition(topic, -1), err);
                    return Stream.empty();
                }
            }).iterator();

            Set<TopicPartition> partitions = new HashSet<>(knownPartitions);

            unknownPartitions.forEachRemaining(partitions::add);

            DeleteConsumerGroupOffsetsResult deleteResult = adminClient.deleteConsumerGroupOffsets(
                groupId,
                partitions,
                withTimeoutMs(new DeleteConsumerGroupOffsetsOptions())
            );

            Errors topLevelException = Errors.NONE;

            try {
                deleteResult.all().get();
            } catch (ExecutionException | InterruptedException e) {
                topLevelException = Errors.forException(e.getCause());
            }

            partitions.forEach(partition -> {
                try {
                    deleteResult.partitionResult(partition).get();
                    partitionLevelResult.put(partition, null);
                } catch (ExecutionException | InterruptedException e) {
                    partitionLevelResult.put(partition, e);
                }
            });

            return new SimpleImmutableEntry<>(topLevelException, partitionLevelResult);
        }

        void deleteOffsets() {
            String groupId = opts.options.valueOf(opts.groupOpt);
            List<String> topics = opts.options.valuesOf(opts.topicOpt);

            Entry<Errors, Map<TopicPartition, Throwable>> res = deleteOffsets(groupId, topics);

            Errors topLevelResult = res.getKey();
            Map<TopicPartition, Throwable> partitionLevelResult = res.getValue();

            switch (topLevelResult) {
                case NONE:
                    System.out.println("Request succeeded for deleting offsets from group " + groupId + ".");
                    break;
                case INVALID_GROUP_ID:
                case GROUP_ID_NOT_FOUND:
                case GROUP_AUTHORIZATION_FAILED:
                case NON_EMPTY_GROUP:
                    printError(topLevelResult.message(), Optional.empty());
                    break;
                case GROUP_SUBSCRIBED_TO_TOPIC:
                case TOPIC_AUTHORIZATION_FAILED:
                case UNKNOWN_TOPIC_OR_PARTITION:
                    printError("Encountered some partition-level error, see the follow-up details.", Optional.empty());
                    break;
                default:
                    printError("Encountered some unknown error: " + topLevelResult, Optional.empty());
            }

            int maxTopicLen = 15;
            for (TopicPartition tp : partitionLevelResult.keySet()) {
                maxTopicLen = Math.max(maxTopicLen, tp.topic().length());
            }

            String format = "%n%" + (-maxTopicLen) + "s %-10s %-15s";

            System.out.printf(format, "TOPIC", "PARTITION", "STATUS");
            partitionLevelResult.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().topic() + e.getKey().partition()))
                .forEach(e -> {
                    TopicPartition tp = e.getKey();
                    Throwable error = e.getValue();
                    System.out.printf(format,
                        tp.topic(),
                        tp.partition() >= 0 ? tp.partition() : MISSING_COLUMN_VALUE,
                        error != null ? "Error: " + error.getMessage() : "Successful"
                    );
                });
            System.out.println();
        }

        Map<String, ConsumerGroupDescription> describeConsumerGroups(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> res = new HashMap<>();
            Map<String, KafkaFuture<ConsumerGroupDescription>> stringKafkaFutureMap = adminClient.describeConsumerGroups(
                groupIds,
                withTimeoutMs(new DescribeConsumerGroupsOptions())
            ).describedGroups();

            for (Entry<String, KafkaFuture<ConsumerGroupDescription>> e : stringKafkaFutureMap.entrySet()) {
                res.put(e.getKey(), e.getValue().get());
            }
            return res;
        }

        /**
         * Returns the state of the specified consumer group and partition assignment states
         */
        Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>> collectGroupOffsets(String groupId) throws Exception {
            return collectGroupsOffsets(List.of(groupId)).getOrDefault(groupId, new SimpleImmutableEntry<>(Optional.empty(), Optional.empty()));
        }

        /**
         * Returns states of the specified consumer groups and partition assignment states
         */
        TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>>> collectGroupsOffsets(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<PartitionAssignmentState>>>> groupOffsets = new TreeMap<>();

            consumerGroups.forEach((groupId, consumerGroup) -> {
                GroupState state = consumerGroup.groupState();
                Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffsets(groupId);
                List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
                Comparator<MemberDescription> comparator =
                    Comparator.<MemberDescription>comparingInt(m -> m.assignment().topicPartitions().size()).reversed();
                List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
                consumerGroup.members().stream().filter(m -> !m.assignment().topicPartitions().isEmpty())
                    .sorted(comparator)
                    .forEach(consumerSummary -> {
                        Set<TopicPartition> topicPartitions = consumerSummary.assignment().topicPartitions();
                        assignedTopicPartitions.addAll(topicPartitions);
                        rowsWithConsumer.addAll(collectConsumerAssignment(
                            groupId,
                            Optional.of(consumerGroup.coordinator()),
                            topicPartitions,
                            committedOffsets,
                            Optional.of(consumerSummary.consumerId()),
                            Optional.of(consumerSummary.host()),
                            Optional.of(consumerSummary.clientId()))
                        );
                    });
                Map<TopicPartition, OffsetAndMetadata> unassignedPartitions = new HashMap<>();
                committedOffsets.entrySet().stream().filter(e -> !assignedTopicPartitions.contains(e.getKey()))
                    .forEach(e -> unassignedPartitions.put(e.getKey(), e.getValue()));
                Collection<PartitionAssignmentState> rowsWithoutConsumer = !unassignedPartitions.isEmpty()
                    ? collectConsumerAssignment(
                        groupId,
                        Optional.of(consumerGroup.coordinator()),
                        unassignedPartitions.keySet(),
                        committedOffsets,
                        Optional.of(MISSING_COLUMN_VALUE),
                        Optional.of(MISSING_COLUMN_VALUE),
                        Optional.of(MISSING_COLUMN_VALUE))
                    : List.of();

                rowsWithConsumer.addAll(rowsWithoutConsumer);

                groupOffsets.put(groupId, new SimpleImmutableEntry<>(Optional.of(state), Optional.of(rowsWithConsumer)));
            });

            return groupOffsets;
        }

        Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>> collectGroupMembers(String groupId) throws Exception {
            return collectGroupsMembers(Set.of(groupId)).get(groupId);
        }

        TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>>> collectGroupsMembers(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, Entry<Optional<GroupState>, Optional<Collection<MemberAssignmentState>>>> res = new TreeMap<>();

            consumerGroups.forEach((groupId, consumerGroup) -> {
                GroupState state = consumerGroup.groupState();
                List<MemberAssignmentState> memberAssignmentStates = consumerGroup.members().stream().map(consumer ->
                    new MemberAssignmentState(
                        groupId,
                        consumer.consumerId(),
                        consumer.host(),
                        consumer.clientId(),
                        consumer.groupInstanceId().orElse(""),
                        consumer.assignment().topicPartitions().size(),
                        consumer.assignment().topicPartitions().stream().toList(),
                        consumer.targetAssignment().map(a -> a.topicPartitions().stream().toList()).orElse(List.of()),
                        consumer.memberEpoch(),
                        consumerGroup.targetAssignmentEpoch(),
                        consumer.upgraded()
                )).collect(Collectors.toList());
                res.put(groupId, new SimpleImmutableEntry<>(Optional.of(state), Optional.of(memberAssignmentStates)));
            });
            return res;
        }

        GroupInformation collectGroupState(String groupId) throws Exception {
            return collectGroupsState(Set.of(groupId)).get(groupId);
        }

        TreeMap<String, GroupInformation> collectGroupsState(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, GroupInformation> res = new TreeMap<>();
            consumerGroups.forEach((groupId, groupDescription) ->
                res.put(groupId, new GroupInformation(
                    groupId,
                    groupDescription.coordinator(),
                    groupDescription.partitionAssignor(),
                    groupDescription.groupState(),
                    groupDescription.members().size(),
                    groupDescription.groupEpoch(),
                    groupDescription.targetAssignmentEpoch()
                )));
            return res;
        }

        @Override
        public void close() {
            adminClient.close();
        }

        // Visibility for testing
        protected Admin createAdminClient(Map<String, String> configOverrides) throws IOException {
            Properties props = opts.options.has(opts.commandConfigOpt) ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) : new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            props.putAll(configOverrides);
            return Admin.create(props);
        }

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }

        private Collection<TopicPartition> getPartitionsToReset(String groupId) throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.allTopicsOpt)) {
                return getCommittedOffsets(groupId).keySet();
            } else if (opts.options.has(opts.topicOpt)) {
                List<String> topics = opts.options.valuesOf(opts.topicOpt);
                return offsetsUtils.parseTopicPartitionsToReset(topics);
            } else {
                if (!opts.options.has(opts.resetFromFileOpt))
                    CommandLineUtils.printUsageAndExit(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.");

                return List.of();
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(String groupId) {
            try {
                return adminClient.listConsumerGroupOffsets(
                    Map.of(groupId, new ListConsumerGroupOffsetsSpec()),
                    withTimeoutMs(new ListConsumerGroupOffsetsOptions())
                ).partitionsToOffsetAndMetadata(groupId).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToReset(String groupId, Collection<TopicPartition> partitionsToReset) {
            // ensure all partitions are valid, otherwise throw a runtime exception
            offsetsUtils.checkAllTopicPartitionsValid(partitionsToReset);

            if (opts.options.has(opts.resetToOffsetOpt)) {
                return offsetsUtils.resetToOffset(partitionsToReset);
            } else if (opts.options.has(opts.resetToEarliestOpt)) {
                return offsetsUtils.resetToEarliest(partitionsToReset);
            } else if (opts.options.has(opts.resetToLatestOpt)) {
                return offsetsUtils.resetToLatest(partitionsToReset);
            } else if (opts.options.has(opts.resetShiftByOpt)) {
                Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets = getCommittedOffsets(groupId);
                return offsetsUtils.resetByShiftBy(partitionsToReset, currentCommittedOffsets);
            } else if (opts.options.has(opts.resetToDatetimeOpt)) {
                return offsetsUtils.resetToDateTime(partitionsToReset);
            } else if (opts.options.has(opts.resetByDurationOpt)) {
                return offsetsUtils.resetByDuration(partitionsToReset);
            } else if (offsetsUtils.resetPlanFromFile().isPresent()) {
                return offsetsUtils.resetFromFile(groupId);
            } else if (opts.options.has(opts.resetToCurrentOpt)) {
                Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets = getCommittedOffsets(groupId);
                return offsetsUtils.resetToCurrent(partitionsToReset, currentCommittedOffsets);
            }

            CommandLineUtils.printUsageAndExit(opts.parser, String.format("Option '%s' requires one of the following scenarios: %s", opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts));
            return null;
        }

        String exportOffsetsToCsv(Map<String, Map<TopicPartition, OffsetAndMetadata>> assignments) {
            boolean isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1;
            ObjectWriter csvWriter = isSingleGroupQuery
                ? CsvUtils.writerFor(CsvUtils.CsvRecordNoGroup.class)
                : CsvUtils.writerFor(CsvUtils.CsvRecordWithGroup.class);

            return assignments.entrySet().stream().flatMap(e -> {
                String groupId = e.getKey();
                Map<TopicPartition, OffsetAndMetadata> partitionInfo = e.getValue();

                return partitionInfo.entrySet().stream().map(e1 -> {
                    TopicPartition k = e1.getKey();
                    OffsetAndMetadata v = e1.getValue();
                    Object csvRecord = isSingleGroupQuery
                        ? new CsvUtils.CsvRecordNoGroup(k.topic(), k.partition(), v.offset())
                        : new CsvUtils.CsvRecordWithGroup(groupId, k.topic(), k.partition(), v.offset());

                    try {
                        return csvWriter.writeValueAsString(csvRecord);
                    } catch (JsonProcessingException err) {
                        throw new RuntimeException(err);
                    }
                });
            }).collect(Collectors.joining());
        }

        Map<String, Throwable> deleteGroups() {
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);

            Map<String, KafkaFuture<Void>> groupsToDelete = adminClient.deleteConsumerGroups(
                groupIds,
                withTimeoutMs(new DeleteConsumerGroupsOptions())
            ).deletedGroups();

            Map<String, Throwable> success = new HashMap<>();
            Map<String, Throwable> failed = new HashMap<>();

            groupsToDelete.forEach((g, f) -> {
                try {
                    f.get();
                    success.put(g, null);
                } catch (InterruptedException ie) {
                    failed.put(g, ie);
                } catch (ExecutionException e) {
                    failed.put(g, e.getCause());
                }
            });

            if (failed.isEmpty())
                System.out.println("Deletion of requested consumer groups (" + success.keySet().stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")) + ") was successful.");
            else {
                printError("Deletion of some consumer groups failed:", Optional.empty());
                failed.forEach((group, error) -> System.out.println("* Group '" + group + "' could not be deleted due to: " + error));

                if (!success.isEmpty())
                    System.out.println("\nThese consumer groups were deleted successfully: " + success.keySet().stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")));
            }

            failed.putAll(success);

            return failed;
        }
    }
}

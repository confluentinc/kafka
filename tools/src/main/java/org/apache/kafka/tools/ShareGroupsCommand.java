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
package org.apache.kafka.tools;

import joptsimple.OptionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.ListShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareGroupListing;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShareGroupsCommand {

    public static void main(String[] args) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        try {
            opts.checkArgs();
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all share groups, describe a share group, delete share group info, or reset share group offsets.");

            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(ShareGroupCommandOptions opts) {
        try {
            Admin adminClient = createAdminClient(Collections.emptyMap(), opts);
            ShareGroupService shareGroupService = new ShareGroupService(opts, Collections.emptyMap(), adminClient);
            // Currently the tool only supports listing of share groups
            if (opts.options.has(opts.listOpt))
                shareGroupService.listGroups();
            else if (opts.options.has(opts.describeOpt)) {
                shareGroupService.describeGroups();
            }
            shareGroupService.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing share group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<ShareGroupState> shareGroupStatesFromString(String input) {
        Set<ShareGroupState> parsedStates = Arrays.stream(input.split(",")).map(s -> ShareGroupState.parse(s.trim())).collect(Collectors.toSet());
        if (parsedStates.contains(ShareGroupState.UNKNOWN)) {
            Collection<ShareGroupState> validStates = Arrays.stream(ShareGroupState.values()).filter(s -> s != ShareGroupState.UNKNOWN).collect(Collectors.toList());
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " + Utils.join(validStates, ", "));
        }
        return parsedStates;
    }

    public static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    // Visibility for testing
    public static Admin createAdminClient(Map<String, String> configOverrides, ShareGroupCommandOptions opts) throws IOException {
        Properties props = opts.options.has(opts.commandConfigOpt) ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) : new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        props.putAll(configOverrides);
        return Admin.create(props);
    }

    static class ShareGroupService {
        final ShareGroupCommandOptions opts;
        final Map<String, String> configOverrides;
        private final Admin adminClient;

        public ShareGroupService(ShareGroupCommandOptions opts, Map<String, String> configOverrides, Admin adminClient) {
            this.opts = opts;
            this.configOverrides = configOverrides;
            this.adminClient = adminClient;
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<ShareGroupState> states = (stateValue == null || stateValue.isEmpty())
                        ? Collections.emptySet()
                        : shareGroupStatesFromString(stateValue);
                List<ShareGroupListing> listings = listShareGroupsWithState(states);
                printGroupStates(listings.stream().map(e -> new Tuple2<>(e.groupId(), e.state().toString())).collect(Collectors.toList()));
            } else
                listShareGroups().forEach(System.out::println);
        }

        List<String> listShareGroups() {
            try {
                ListShareGroupsResult result = adminClient.listShareGroups(withTimeoutMs(new ListShareGroupsOptions()));
                Collection<ShareGroupListing> listings = result.all().get();
                return listings.stream().map(ShareGroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<ShareGroupListing> listShareGroupsWithState(Set<ShareGroupState> states) throws ExecutionException, InterruptedException {
            ListShareGroupsOptions listShareGroupsOptions = withTimeoutMs(new ListShareGroupsOptions());
            listShareGroupsOptions.inStates(states);
            ListShareGroupsResult result = adminClient.listShareGroups(listShareGroupsOptions);
            return new ArrayList<>(result.all().get());
        }

        private void printGroupStates(List<Tuple2<String, String>> groupsAndStates) {
            // find proper columns width
            int maxGroupLen = 15;
            for (Tuple2<String, String> tuple : groupsAndStates) {
                String groupId = tuple.v1;
                maxGroupLen = Math.max(maxGroupLen, groupId.length());
            }
            System.out.printf("%" + (-maxGroupLen) + "s %s", "GROUP", "STATE");
            for (Tuple2<String, String> tuple : groupsAndStates) {
                String groupId = tuple.v1;
                String state = tuple.v2;
                System.out.printf("%" + (-maxGroupLen) + "s %s", groupId, state);
            }
        }

        void describeGroups() throws ExecutionException, InterruptedException {
            ShareGroupDescription description = getDescribeGroups();
            printGroupDescriptionTable(description);
        }
        ShareGroupDescription getDescribeGroups() throws ExecutionException, InterruptedException {
            String group = opts.options.valueOf(opts.groupOpt);
            DescribeShareGroupsResult result = adminClient.describeShareGroups(Collections.singletonList(group));
            Map<String, ShareGroupDescription> descriptionMap = result.all().get();
            if (descriptionMap.containsKey(group)) {
                return descriptionMap.get(group);
            }
            return null;
        }

        private void printGroupDescriptionTable(ShareGroupDescription description) {
            boolean shouldPrintState = opts.options.has(opts.stateOpt);
            boolean shouldPrintMemDetails = opts.options.has(opts.membersOpt);
            if (description == null) {
                return;
            }
            if (shouldPrintMemDetails) {
                printMemberDetails(description.members());
                return;
            }
            List<String> lineItem = new ArrayList<>();
            lineItem.add(description.groupId());
            System.out.println(description.members());
            lineItem.add(description.members().stream().map(MemberDescription::consumerId).collect(Collectors.joining(",")));
            lineItem.add(description.coordinator().idString());
            if (shouldPrintState) {
                lineItem.add(description.state().toString());
            }

            int maxItemLength = 20;
            for (String item : lineItem) {
                if (item != null) {
                    maxItemLength = Math.max(maxItemLength, item.length());
                }
            }

            // header
            String formatAtom = "%" + (-maxItemLength) + "s";
            String formatHeader = "";
            if (shouldPrintState) {
                formatHeader = String.format(formatAtom + " " + formatAtom + " " + formatAtom + " " + formatAtom, "GROUP_ID", "MEMBERS", "COORDINATOR_NODE", "STATE");
            } else {
                formatHeader = String.format(formatAtom + " " + formatAtom + " " + formatAtom, "GROUP_ID", "MEMBERS", "COORDINATOR_NODE");
            }
            System.out.println(formatHeader);
            for (String item : lineItem) {
                System.out.printf(formatAtom + " ", item);
            }
            System.out.println();
        }

        private void printMemberDetails(Collection<MemberDescription> members) {
            List<List<String>> lineItems = new ArrayList<>();
            int maxLen = 20;
            for (MemberDescription member : members) {
                List<String> lineItem = new ArrayList<>();
                lineItem.add(member.consumerId());
                lineItem.add(member.groupInstanceId().isPresent() ? member.groupInstanceId().get() : "");
                lineItem.add(member.clientId());
                lineItem.add(member.host());
                lineItem.add(member.assignment().topicPartitions().stream().map(part -> part.topic() + ":" + part.partition()).collect(Collectors.joining(",")));
                lineItem.add(member.targetAssignment().isPresent()
                    ? member.targetAssignment().get().topicPartitions().stream().map(part -> part.topic() + ":" + part.partition()).collect(Collectors.joining(","))
                    : "");
                for (String item : lineItem) {
                    if (item != null) {
                        maxLen = Math.max(maxLen, item.length());
                    }
                }
                lineItems.add(lineItem);
            }

            String fmt = "%" + (-maxLen) + "s";
            String header = fmt + " " + fmt + " " + fmt + " " + fmt + " " + fmt + " " + fmt;
            System.out.printf(header, "MEMBER_ID", "GRP_INSTANCE_ID", "CLIENT_ID", "HOST", "ASSIGNMENT", "TARGET_ASSIGNMENT");
            System.out.println();
            for (List<String> item : lineItems) {
                for (String atom : item) {
                    System.out.printf(fmt + " ", atom);
                }
                System.out.println();
            }
        }

        public void close() {
            adminClient.close();
        }

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }
    }
}
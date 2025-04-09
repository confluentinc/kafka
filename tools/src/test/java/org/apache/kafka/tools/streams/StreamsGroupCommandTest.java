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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamsGroupCommandTest {

    @Test
    public void testListStreamsGroups() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult result = mock(ListGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(result);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs, adminClient);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));

        final Set[] foundGroups = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listStreamsGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
        service.close();
    }

    @Test
    public void testListWithUnrecognizedOption() {
        String bootstrapServer = "localhost:9092";
        String[] cgcArgs = new String[]{"--frivolous-nonsense", "--bootstrap-server", bootstrapServer, "--list"};
        final Exception exception = assertThrows(OptionException.class, () -> {
            getStreamsGroupService(cgcArgs, new MockAdminClient());
        });
        assertEquals("frivolous-nonsense is not a recognized option", exception.getMessage());
    }

    @Test
    public void testListStreamsGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        String bootstrapServer = "localhost:9092";

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServer, "--list", "--state"};
        Admin adminClient = mock(KafkaAdminClient.class);
        ListGroupsResult resultWithAllStates = mock(ListGroupsResult.class);
        when(resultWithAllStates.all()).thenReturn(KafkaFuture.completedFuture(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithAllStates);
        StreamsGroupCommand.StreamsGroupService service = getStreamsGroupService(cgcArgs, adminClient);
        Set<GroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)),
                new GroupListing(secondGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.EMPTY))));

        final Set[] foundListing = new Set[]{Set.of()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(new HashSet<>(Arrays.asList(GroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        ListGroupsResult resultWithStableState = mock(ListGroupsResult.class);
        when(resultWithStableState.all()).thenReturn(KafkaFuture.completedFuture(List.of(
                new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE))
        )));
        when(adminClient.listGroups(any(ListGroupsOptions.class))).thenReturn(resultWithStableState);
        Set<GroupListing> expectedListingStable = Set.of(
                new GroupListing(firstGroup, Optional.of(GroupType.STREAMS), "streams", Optional.of(GroupState.STABLE)));

        foundListing[0] = Set.of();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listStreamsGroupsInStates(Set.of(GroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
        service.close();
    }

    @Test
    public void testGroupStatesFromString() {
        Set<GroupState> result = StreamsGroupCommand.groupStatesFromString("empty");
        assertEquals(new HashSet<>(List.of(GroupState.EMPTY)), result);
        result = StreamsGroupCommand.groupStatesFromString("EMPTY");
        assertEquals(new HashSet<>(List.of(GroupState.EMPTY)), result);

        result = StreamsGroupCommand.groupStatesFromString("notready");
        assertEquals(new HashSet<>(List.of(GroupState.NOT_READY)), result);
        result = StreamsGroupCommand.groupStatesFromString("notReady");
        assertEquals(new HashSet<>(List.of(GroupState.NOT_READY)), result);

        result = StreamsGroupCommand.groupStatesFromString("assigning");
        assertEquals(new HashSet<>(List.of(GroupState.ASSIGNING)), result);
        result = StreamsGroupCommand.groupStatesFromString("ASSIGNING");
        assertEquals(new HashSet<>(List.of(GroupState.ASSIGNING)), result);

        result = StreamsGroupCommand.groupStatesFromString("RECONCILING");
        assertEquals(new HashSet<>(List.of(GroupState.RECONCILING)), result);
        result = StreamsGroupCommand.groupStatesFromString("reconCILING");
        assertEquals(new HashSet<>(List.of(GroupState.RECONCILING)), result);

        result = StreamsGroupCommand.groupStatesFromString("STABLE");
        assertEquals(new HashSet<>(List.of(GroupState.STABLE)), result);
        result = StreamsGroupCommand.groupStatesFromString("stable");
        assertEquals(new HashSet<>(List.of(GroupState.STABLE)), result);

        result = StreamsGroupCommand.groupStatesFromString("DEAD");
        assertEquals(new HashSet<>(List.of(GroupState.DEAD)), result);
        result = StreamsGroupCommand.groupStatesFromString("dead");
        assertEquals(new HashSet<>(List.of(GroupState.DEAD)), result);

        assertThrow("preparingRebalance");
        assertThrow("completingRebalance");
        assertThrow("bad, wrong");
        assertThrow("  bad, Stable");
        assertThrow("   ,   ,");
    }

    StreamsGroupCommand.StreamsGroupService getStreamsGroupService(String[] args, Admin adminClient) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        return new StreamsGroupCommand.StreamsGroupService(opts, adminClient);
    }

    private static void assertThrow(final String wrongState) {
        final Set<String> validStates = new HashSet<>(Arrays.asList("Assigning", "Dead", "Empty", "Reconciling", "Stable", "NotReady"));

        final Exception exception = assertThrows(IllegalArgumentException.class, () -> StreamsGroupCommand.groupStatesFromString(wrongState));

        assertTrue(exception.getMessage().contains(" Valid states are: "));

        final String[] exceptionMessage = exception.getMessage().split(" Valid states are: ");
        assertEquals("Invalid state list '" + wrongState + "'.", exceptionMessage[0]);
        assertEquals(Arrays.stream(exceptionMessage[1].split(","))
            .map(String::trim)
            .collect(Collectors.toSet()), validStates);
    }
}

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
import org.apache.kafka.clients.admin.ShareGroupListing;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.tools.ShareGroupsCommand.ShareGroupService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ListShareGroupTest extends ShareGroupsCommandTest {
    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testListShareGroups(String quorum) throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        addShareGroupExecutor(1, firstGroup);
        addShareGroupExecutor(1, secondGroup);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        ShareGroupService service = getShareGroupService(cgcArgs);
        Set<String> expectedGroups = new HashSet<>(Arrays.asList(firstGroup, secondGroup));
        final Set[] foundGroups = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundGroups[0] = new HashSet<>(service.listShareGroups());
            return Objects.equals(expectedGroups, foundGroups[0]);
        }, "Expected --list to show groups " + expectedGroups + ", but found " + foundGroups[0] + ".");
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testListWithUnrecognizedNewConsumerOption() {
        String[] cgcArgs = new String[]{"--new-consumer", "--bootstrap-server", bootstrapServers(listenerName()), "--list"};
        assertThrows(OptionException.class, () -> getShareGroupService(cgcArgs));
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testListShareGroupsWithStates() throws Exception {
        String firstGroup = "first-group";
        String secondGroup = "second-group";
        addShareGroupExecutor(1, firstGroup);
        addShareGroupExecutor(1, secondGroup);

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--list", "--state"};
        ShareGroupService service = getShareGroupService(cgcArgs);

        Set<ShareGroupListing> expectedListing = new HashSet<>(Arrays.asList(
                new ShareGroupListing(firstGroup),
                new ShareGroupListing(secondGroup)));

        final Set[] foundListing = new Set[]{Collections.emptySet()};
        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(new HashSet<>(Arrays.asList(ShareGroupState.values()))));
            return Objects.equals(expectedListing, foundListing[0]);
        }, "Expected to show groups " + expectedListing + ", but found " + foundListing[0]);

        Set<ShareGroupListing> expectedListingStable = new HashSet<>(Arrays.asList(
                new ShareGroupListing(firstGroup),
                new ShareGroupListing(secondGroup)));

        foundListing[0] = Collections.emptySet();

        TestUtils.waitForCondition(() -> {
            foundListing[0] = new HashSet<>(service.listShareGroupsWithState(Collections.singleton(ShareGroupState.STABLE)));
            return Objects.equals(expectedListingStable, foundListing[0]);
        }, "Expected to show groups " + expectedListingStable + ", but found " + foundListing[0]);
    }

    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testShareGroupStatesFromString(String quorum) {
        Set<ShareGroupState> result = ShareGroupsCommand.shareGroupStatesFromString("Stable");
        assertEquals(Collections.singleton(ShareGroupState.STABLE), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("stable");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.STABLE)), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("dead");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.DEAD)), result);

        result = ShareGroupsCommand.shareGroupStatesFromString("empty");
        assertEquals(new HashSet<>(Arrays.asList(ShareGroupState.EMPTY)), result);

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("bad, wrong"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("  bad, Stable"));

        assertThrows(IllegalArgumentException.class, () -> ShareGroupsCommand.shareGroupStatesFromString("   ,   ,"));
    }
}

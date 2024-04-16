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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.LogContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.requests.ShareFetchMetadata.INITIAL_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for ShareSessionHandler.
 */
@Timeout(120)
public class ShareSessionHandlerTest {
    private static final LogContext LOG_CONTEXT = new LogContext("[ShareSessionHandler]=");

    private static LinkedHashMap<TopicPartition, TopicIdPartition> reqMap(TopicIdPartition... entries) {
        LinkedHashMap<TopicPartition, TopicIdPartition> map = new LinkedHashMap<>();
        for (TopicIdPartition entry : entries) {
            map.put(entry.topicPartition(), entry);
        }
        return map;
    }

    private static void assertMapEquals(Map<TopicPartition, TopicIdPartition> expected,
                                        Map<TopicPartition, TopicIdPartition> actual) {
        Iterator<Map.Entry<TopicPartition, TopicIdPartition>> expectedIter =
                expected.entrySet().iterator();
        Iterator<Map.Entry<TopicPartition, TopicIdPartition>> actualIter =
                actual.entrySet().iterator();
        int i = 1;
        while (expectedIter.hasNext()) {
            Map.Entry<TopicPartition, TopicIdPartition> expectedEntry = expectedIter.next();
            if (!actualIter.hasNext()) {
                fail("Element " + i + " not found.");
            }
            Map.Entry<TopicPartition, TopicIdPartition> actualEntry = actualIter.next();
            assertEquals(expectedEntry.getKey(), actualEntry.getKey(), "Element " + i +
                    " had a different TopicPartition than expected.");
            assertEquals(expectedEntry.getValue(), actualEntry.getValue(), "Element " + i +
                    " had different PartitionData than expected.");
            i++;
        }
        if (actualIter.hasNext()) {
            fail("Unexpected element " + i + " found.");
        }
    }

    @SafeVarargs
    private static void assertMapsEqual(Map<TopicPartition, TopicIdPartition> expected,
                                        Map<TopicPartition, TopicIdPartition>... actuals) {
        for (Map<TopicPartition, TopicIdPartition> actual : actuals) {
            assertMapEquals(expected, actual);
        }
    }

    private static void assertListEquals(List<TopicIdPartition> expected, List<TopicIdPartition> actual) {
        for (TopicIdPartition expectedPart : expected) {
            if (!actual.contains(expectedPart)) {
                fail("Failed to find expected partition " + expectedPart);
            }
        }
        for (TopicIdPartition actualPart : actual) {
            if (!expected.contains(actualPart)) {
                fail("Found unexpected partition " + actualPart);
            }
        }
    }

    private static final class RespEntry {
        final TopicIdPartition part;
        final ShareFetchResponseData.PartitionData data;

        RespEntry(String topic, int partition, Uuid topicId) {
            this.part = new TopicIdPartition(topicId, partition, topic);
            this.data = new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(partition);
        }
    }

    private static List<ShareFetchResponseData.ShareFetchableTopicResponse> respList(RespEntry... entries) {
        HashMap<TopicIdPartition, ShareFetchResponseData.ShareFetchableTopicResponse> map = new HashMap<>();
        for (RespEntry entry : entries) {
            ShareFetchResponseData.ShareFetchableTopicResponse response = map.computeIfAbsent(entry.part, topicIdPartition ->
                    new ShareFetchResponseData.ShareFetchableTopicResponse().setTopicId(topicIdPartition.topicId()));
            response.partitions().add(new ShareFetchResponseData.PartitionData()
                    .setPartitionIndex(entry.part.partition()));
        }
        return new ArrayList<>(map.values());
    }

    @Test
    public void testShareSession() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        ShareSessionHandler.Builder builder = handler.newBuilder();
        Uuid fooId = addTopicId(topicIds, topicNames, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        builder.add(foo0, null);
        builder.add(foo1, null);
        ShareSessionHandler.ShareFetchRequestData data = builder.build();
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        assertListEquals(expectedToSend1, data.toSend());
        assertEquals(memberId, data.metadata().memberId());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, fooId),
                                new RespEntry("foo", 1, fooId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Test a fetch request which adds one partition
        ShareSessionHandler.Builder builder2 = handler.newBuilder();
        Uuid barId = addTopicId(topicIds, topicNames, "bar");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        builder2.add(foo0, null);
        builder2.add(foo1, null);
        builder2.add(bar0, null);
        ShareSessionHandler.ShareFetchRequestData data2 = builder2.build();
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                data2.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend2 = new ArrayList<>();
        expectedToSend2.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend2, data2.toSend());

        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 1, fooId))));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        // A top-level error code will reset the session epoch
        ShareFetchResponse resp3 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.INVALID_SHARE_SESSION_EPOCH.code()));
        handler.handleResponse(resp3, ApiKeys.SHARE_FETCH.latestVersion(true));

        ShareSessionHandler.Builder builder4 = handler.newBuilder();
        builder4.add(foo0, null);
        builder4.add(foo1, null);
        builder4.add(bar0, null);
        ShareSessionHandler.ShareFetchRequestData data4 = builder4.build();
        assertEquals(data2.metadata().memberId(), data4.metadata().memberId());
        assertEquals(INITIAL_EPOCH, data4.metadata().epoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                data4.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend4 = new ArrayList<>();
        expectedToSend4.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend4.add(new TopicIdPartition(fooId, 1, "foo"));
        expectedToSend4.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend4, data4.toSend());

    }

    @Test
    public void testDoubleBuildFails() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        ShareSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicIdPartition(Uuid.randomUuid(), 0, "foo"), null);
        builder.build();
        boolean expectedExceptionThrown = false;
        try {
            builder.build();
        } catch (Throwable t) {
            // expected
            expectedExceptionThrown = true;
        }
        if (!expectedExceptionThrown) {
            fail("Expected calling build twice to fail.");
        }
    }

    @Test
    public void testPartitionRemoval() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Map<String, Uuid> topicIds = new HashMap<>();
        Map<Uuid, String> topicNames = new HashMap<>();
        ShareSessionHandler.Builder builder = handler.newBuilder();
        Uuid fooId = addTopicId(topicIds, topicNames, "foo");
        Uuid barId = addTopicId(topicIds, topicNames, "bar");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, 0, "foo");
        TopicIdPartition foo1 = new TopicIdPartition(fooId, 1, "foo");
        TopicIdPartition bar0 = new TopicIdPartition(barId, 0, "bar");
        builder.add(foo0, null);
        builder.add(foo1, null);
        builder.add(bar0, null);
        ShareSessionHandler.ShareFetchRequestData data = builder.build();
        assertMapsEqual(reqMap(
                        new TopicIdPartition(fooId, 0, "foo"),
                        new TopicIdPartition(fooId, 1, "foo"),
                        new TopicIdPartition(barId, 0, "bar")),
                data.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToSend1.add(new TopicIdPartition(fooId, 1, "foo"));
        expectedToSend1.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToSend1, data.toSend());
        assertEquals(memberId, data.metadata().memberId());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, fooId),
                                new RespEntry("foo", 1, fooId),
                                new RespEntry("bar", 0, barId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Test a fetch request which removes two partitions
        ShareSessionHandler.Builder builder2 = handler.newBuilder();
        builder2.add(foo1, null);
        ShareSessionHandler.ShareFetchRequestData data2 = builder2.build();
        assertEquals(memberId, data2.metadata().memberId());
        assertEquals(1, data2.metadata().epoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 1, "foo")),
                data2.sessionPartitions());
        assertTrue(data2.toSend().isEmpty());
        ArrayList<TopicIdPartition> expectedToForget2 = new ArrayList<>();
        expectedToForget2.add(new TopicIdPartition(fooId, 0, "foo"));
        expectedToForget2.add(new TopicIdPartition(barId, 0, "bar"));
        assertListEquals(expectedToForget2, data2.toForget());

        // A top-level error code will reset the session epoch
        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.INVALID_SHARE_SESSION_EPOCH.code()));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        ShareSessionHandler.Builder builder3 = handler.newBuilder();
        builder3.add(foo0, null);
        ShareSessionHandler.ShareFetchRequestData data3 = builder3.build();
        assertEquals(memberId, data3.metadata().memberId());
        assertEquals(INITIAL_EPOCH, data3.metadata().epoch());
        assertMapsEqual(reqMap(new TopicIdPartition(fooId, 0, "foo")),
                data3.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend3 = new ArrayList<>();
        expectedToSend3.add(new TopicIdPartition(fooId, 0, "foo"));
        assertListEquals(expectedToSend3, data3.toSend());
    }

    @Test
    public void testTopicIdReplaced() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Uuid topicId1 = Uuid.randomUuid();
        TopicIdPartition tp = new TopicIdPartition(topicId1, 0, "foo");
        ShareSessionHandler.Builder builder = handler.newBuilder();
        builder.add(tp, null);
        ShareSessionHandler.ShareFetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId1, 0, "foo")),
                data.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId1, 0, "foo"));
        assertListEquals(expectedToSend1, data.toSend());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId1))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Try to add a new topic ID
        ShareSessionHandler.Builder builder2 = handler.newBuilder();
        Uuid topicId2 = Uuid.randomUuid();
        TopicIdPartition tp2 = new TopicIdPartition(topicId2, 0, "foo");
        // Use the same data besides the topic ID
        builder2.add(tp2, null);
        ShareSessionHandler.ShareFetchRequestData data2 = builder2.build();

        // If we started with an ID, only a new ID will count towards replaced.
        // The old topic ID partition should be in toReplace, and the new one should be in toSend.
        assertEquals(Collections.singletonList(tp), data2.toReplace());
        assertMapsEqual(reqMap(new TopicIdPartition(topicId2, 0, "foo")),
                data2.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend2 = new ArrayList<>();
        expectedToSend2.add(new TopicIdPartition(topicId2, 0, "foo"));
        assertListEquals(expectedToSend2, data2.toSend());

        // Should have the same session ID, and next epoch and can use topic IDs if it ended with topic IDs.
        assertEquals(memberId, data2.metadata().memberId(), "Did not use same session");
        assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch");
    }

    @Test
    public void testForgottenPartitions() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        // We want to test when all topics are removed from the session
        ShareSessionHandler.Builder builder = handler.newBuilder();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition foo0 = new TopicIdPartition(topicId, 0, "foo");
        builder.add(foo0, null);
        ShareSessionHandler.ShareFetchRequestData data = builder.build();
        assertMapsEqual(reqMap(foo0), data.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, data.toSend());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Remove the topic from the session
        ShareSessionHandler.Builder builder2 = handler.newBuilder();
        ShareSessionHandler.ShareFetchRequestData data2 = builder2.build();
        assertEquals(Collections.singletonList(foo0), data2.toForget());

        // Should have the same session ID, next epoch, and same ID usage
        assertEquals(memberId, data2.metadata().memberId(), "Did not use same session");
        assertEquals(1, data2.metadata().epoch(), "Did not have correct epoch");
    }

    @Test
    public void testAddNewIdAfterTopicRemovedFromSession() {
        Uuid memberId = Uuid.randomUuid();
        ShareSessionHandler handler = new ShareSessionHandler(LOG_CONTEXT, 1, memberId);

        Uuid topicId = Uuid.randomUuid();
        ShareSessionHandler.Builder builder = handler.newBuilder();
        builder.add(new TopicIdPartition(topicId, 0, "foo"), null);
        ShareSessionHandler.ShareFetchRequestData data = builder.build();
        assertMapsEqual(reqMap(new TopicIdPartition(topicId, 0, "foo")),
                data.sessionPartitions());
        ArrayList<TopicIdPartition> expectedToSend1 = new ArrayList<>();
        expectedToSend1.add(new TopicIdPartition(topicId, 0, "foo"));
        assertListEquals(expectedToSend1, data.toSend());

        ShareFetchResponse resp = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList(
                                new RespEntry("foo", 0, topicId))));
        handler.handleResponse(resp, ApiKeys.SHARE_FETCH.latestVersion(true));

        // Remove the partition from the session
        ShareSessionHandler.Builder builder2 = handler.newBuilder();
        ShareSessionHandler.ShareFetchRequestData data2 = builder2.build();
        assertTrue(data2.sessionPartitions().isEmpty());
        ShareFetchResponse resp2 = new ShareFetchResponse(
                new ShareFetchResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setThrottleTimeMs(0)
                        .setResponses(respList()));
        handler.handleResponse(resp2, ApiKeys.SHARE_FETCH.latestVersion(true));

        // After the topic is removed, add a recreated topic with a new ID
        ShareSessionHandler.Builder builder3 = handler.newBuilder();
        builder3.add(new TopicIdPartition(Uuid.randomUuid(), 0, "foo"), null);
        ShareSessionHandler.ShareFetchRequestData data3 = builder3.build();

        // Should have the same session ID and epoch 2.
        assertEquals(memberId, data3.metadata().memberId(), "Did not use same session");
        assertEquals(2, data3.metadata().epoch(), "Did not have the correct session epoch");
    }


    private Uuid addTopicId(Map<String, Uuid> topicIds, Map<Uuid, String> topicNames, String name) {
        Uuid id = Uuid.randomUuid();
        topicIds.put(name, id);
        topicNames.put(id, name);
        return id;
    }
}

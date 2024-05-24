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
package kafka.server;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidShareSessionEpochException;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.group.share.NoOpShareStatePersister;
import org.apache.kafka.server.group.share.PartitionFactory;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.group.share.PersisterStateBatch;
import org.apache.kafka.server.group.share.ReadShareGroupStateResult;
import org.apache.kafka.server.group.share.TopicData;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.internal.verification.VerificationModeFactory.atLeast;
import static org.mockito.internal.verification.VerificationModeFactory.atMost;

@Timeout(120)
public class SharePartitionManagerTest {

    private static final int PARTITION_MAX_BYTES = 40000;
    static final int RECORD_LOCK_DURATION_MS = 30000;
    private static final short MAX_IN_FLIGHT_MESSAGES = 200;
    static final int MAX_DELIVERY_COUNT = 5;
    private static Timer mockTimer;
    private static MockedStatic<Utils> mockUtils;
    private static Persister persister = NoOpShareStatePersister.getInstance();

    @BeforeEach
    public void setUp() {
        mockTimer = new SystemTimerReaper("sharePartitionTestReaper",
                new SystemTimer("sharePartitionTestTimer"));
        // For mocking Utils.newInstance() function.
        mockUtils = Mockito.mockStatic(Utils.class);
    }

    @AfterEach
    public void tearDown() throws Exception {
        mockTimer.close();
        mockUtils.close();
    }

    @Test
    public void testSharePartitionKey() {
        SharePartitionManager.SharePartitionKey sharePartitionKey1 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey2 = new SharePartitionManager.SharePartitionKey("mock-group-2",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey3 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(1L, 1L), new TopicPartition("test-1", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey4 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 1)));
        SharePartitionManager.SharePartitionKey sharePartitionKey5 = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 0L), new TopicPartition("test-2", 0)));
        SharePartitionManager.SharePartitionKey sharePartitionKey1Copy = new SharePartitionManager.SharePartitionKey("mock-group-1",
                new TopicIdPartition(new Uuid(0L, 1L), new TopicPartition("test", 0)));

        assertEquals(sharePartitionKey1, sharePartitionKey1Copy);
        assertNotEquals(sharePartitionKey1, sharePartitionKey2);
        assertNotEquals(sharePartitionKey1, sharePartitionKey3);
        assertNotEquals(sharePartitionKey1, sharePartitionKey4);
        assertNotEquals(sharePartitionKey1, sharePartitionKey5);
        assertNotEquals(sharePartitionKey1, null);
    }

    @Test
    public void testNewContextReturnsFinalContext() {
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();

        ShareFetchMetadata newReqMetadata = new ShareFetchMetadata(Uuid.ZERO_UUID, -1);
        ShareFetchContext shareFetchContext = sharePartitionManager.newContext("grp", new HashMap<>(), new ArrayList<>(), newReqMetadata);
        assertEquals(shareFetchContext.getClass(), SharePartitionManager.FinalContext.class);

        // If the final fetch request has topics to add, it should fail as an invalid request
        Uuid topicId = Uuid.randomUuid();
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData = new HashMap<>();
        shareFetchData.put(new TopicIdPartition(topicId, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(topicId, 4000));
        assertThrows(InvalidRequestException.class,
                () -> sharePartitionManager.newContext("grp", shareFetchData, new ArrayList<>(), new ShareFetchMetadata(Uuid.ZERO_UUID, -1)));

        // shareFetchData is not empty, but the maxBytes of topic partition is 0, which means this is added only for acknowledgements.
        // New context should be created successfully
        shareFetchData.clear();
        shareFetchData.put(new TopicIdPartition(topicId, new TopicPartition("foo", 0)),
                new ShareFetchRequest.SharePartitionData(topicId, 0));
        shareFetchContext = sharePartitionManager.newContext("grp", shareFetchData, new ArrayList<>(), newReqMetadata);
        assertEquals(shareFetchContext.getClass(), SharePartitionManager.FinalContext.class);
    }

    private ImplicitLinkedHashCollection<SharePartitionManager.CachedSharePartition> dummyCreate(int size) {
        ImplicitLinkedHashCollection<SharePartitionManager.CachedSharePartition> cacheMap = new
                ImplicitLinkedHashCollection<>(size);
        for (int i = 0; i < size; i++)
            cacheMap.add(new SharePartitionManager.CachedSharePartition("test", Uuid.randomUuid(), i, false));
        return cacheMap;
    }

    public void assertShareCacheContains(SharePartitionManager.ShareSessionCache cache,
                                         ArrayList<SharePartitionManager.ShareSessionKey> sessionKeys) {
        int i = 0;
        for (SharePartitionManager.ShareSessionKey sessionKey : sessionKeys) {
            i++;
            assertFalse(cache.get(sessionKey).isEmpty(),
                    "Missing session " + i + " out of " + sessionKeys.size() + " ( " + sessionKey + " )");
        }
        assertEquals(sessionKeys.size(), cache.size());
    }

    @Test
    public void testShareSessionCache() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(3, 100);
        assertEquals(0, cache.size());
        SharePartitionManager.ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                10, dummyCreate(10));
        SharePartitionManager.ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 10,
                20, dummyCreate(20));
        SharePartitionManager.ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 20,
                30, dummyCreate(30));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 30,
                40, dummyCreate(40)));
        assertNull(cache.maybeCreateSession("grp", Uuid.randomUuid(), 40,
                5, dummyCreate(5)));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2, key3)));
        cache.touch(cache.get(key1), 200);
        SharePartitionManager.ShareSessionKey key4 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 210,
                11, dummyCreate(11));
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key3, key4)));
        cache.touch(cache.get(key1), 400);
        cache.touch(cache.get(key3), 390);
        cache.touch(cache.get(key4), 400);
        SharePartitionManager.ShareSessionKey key5 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 410,
                50, dummyCreate(50));
        assertNull(key5);
    }

    @Test
    public void testResizeCachedSessions() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(2, 100);
        assertEquals(0, cache.size());
        assertEquals(0, cache.totalPartitions());
        SharePartitionManager.ShareSessionKey key1 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                2, dummyCreate(2));
        assertNotNull(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key1)));
        SharePartitionManager.ShareSession session1 = cache.get(key1);
        assertEquals(2, session1.size());
        assertEquals(2, cache.totalPartitions());
        assertEquals(1, cache.size());

        SharePartitionManager.ShareSessionKey key2 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 0,
                4, dummyCreate(4));
        assertNotNull(key2);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        SharePartitionManager.ShareSession session2 = cache.get(key2);
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.touch(session1, 200);
        cache.touch(session2, 200);

        SharePartitionManager.ShareSessionKey key3 = cache.maybeCreateSession("grp", Uuid.randomUuid(), 200,
                5, dummyCreate(5));
        assertNull(key3);
        assertShareCacheContains(cache, new ArrayList<>(Arrays.asList(key1, key2)));
        assertEquals(6, cache.totalPartitions());
        assertEquals(2, cache.size());
        cache.remove(key1);
        assertShareCacheContains(cache, new ArrayList<>(Collections.singletonList(key2)));
        assertEquals(1, cache.size());
        assertEquals(4, cache.totalPartitions());

        Iterator<SharePartitionManager.CachedSharePartition> iterator = session2.partitionMap().iterator();
        iterator.next();
        iterator.remove();
        assertEquals(3, session2.size());
        assertEquals(4, session2.cachedSize());
        cache.touch(session2, session2.lastUsedMs());
        assertEquals(3, cache.totalPartitions());
    }

    private final List<TopicIdPartition> emptyPartList = Collections.unmodifiableList(new ArrayList<>());

    @Test
    public void testShareFetchRequests() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        Map<String, Uuid> topicIds = topicNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        String groupId = "grp";

        // Verify that final epoch requests get a FinalContext
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, new HashMap<>(), emptyPartList,
                new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context1.getClass(), SharePartitionManager.FinalContext.class);

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));


        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList, reqMetadata2);
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());

        ((SharePartitionManager.ShareSessionContext) context2).shareFetchData().forEach((topicIdPartition, sharePartitionData) -> {
            assertTrue(reqData2.containsKey(topicIdPartition));
            assertEquals(reqData2.get(topicIdPartition), sharePartitionData);
        });

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));

        SharePartitionManager.ShareSessionKey shareSessionKey2 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(memberId4, 1)));

        // Continue the first share session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = new LinkedHashMap<>();
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(context5.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context5).isSubsequent());

        SharePartitionManager.ShareSessionContext shareSessionContext5 = (SharePartitionManager.ShareSessionContext) context5;
        synchronized (shareSessionContext5.session()) {
            shareSessionContext5.session().partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                ShareFetchRequest.SharePartitionData data = cachedSharePartition.reqData();
                assertTrue(reqData2.containsKey(topicIdPartition));
                assertEquals(reqData2.get(topicIdPartition), data);
            });
        }
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(0, resp5.responseData(topicNames).size());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = new LinkedHashMap<>();
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        // Close the subsequent share session.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData8 = new LinkedHashMap<>();
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, reqData8, emptyPartList,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context8.getClass(), SharePartitionManager.FinalContext.class);
        assertEquals(0, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
    }

    @Test
    public void testCachedSharePartitionEqualsAndHashCode() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "topic";
        int partition = 0;

        SharePartitionManager.CachedSharePartition cachedSharePartitionWithIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, topicId, partition, false);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithIdAndNoName = new
                SharePartitionManager.CachedSharePartition(null, topicId, partition, false);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithDifferentIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, Uuid.randomUuid(), partition, false);
        SharePartitionManager.CachedSharePartition cachedSharePartitionWithZeroIdAndName = new
                SharePartitionManager.CachedSharePartition(topicName, Uuid.ZERO_UUID, partition, false);

        // CachedSharePartitions with valid topic IDs will compare topic ID and partition but not topic name.
        assertEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithIdAndNoName);
        assertEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithIdAndNoName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        // CachedSharePartitions will null name and valid IDs will act just like ones with valid names

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithDifferentIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithDifferentIdAndName.hashCode());

        assertNotEquals(cachedSharePartitionWithIdAndNoName, cachedSharePartitionWithZeroIdAndName);
        assertNotEquals(cachedSharePartitionWithIdAndNoName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());

        assertEquals(cachedSharePartitionWithZeroIdAndName.hashCode(), cachedSharePartitionWithZeroIdAndName.hashCode());
    }

    @Test
    public void testShareSessionExpiration() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(2, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new share session, session 1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session1req = new LinkedHashMap<>();
        session1req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session1req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        String groupId = "grp";
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session1context = sharePartitionManager.newContext(groupId, session1req, emptyPartList, reqMetadata1);
        assertEquals(session1context.getClass(), SharePartitionManager.ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData1.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session1resp = session1context.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, session1resp.error());
        assertEquals(2, session1resp.responseData(topicNames).size());

        SharePartitionManager.ShareSessionKey session1Key = new SharePartitionManager.ShareSessionKey(groupId, reqMetadata1.memberId());
        // check share session entered into cache
        assertNotNull(cache.get(session1Key));

        time.sleep(500);

        // Create a second new share session
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session2req = new LinkedHashMap<>();
        session2req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session2req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session2context = sharePartitionManager.newContext(groupId, session2req, emptyPartList, reqMetadata2);
        assertEquals(session2context.getClass(), SharePartitionManager.ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData2.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session2resp = session2context.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, session2resp.error());
        assertEquals(2, session2resp.responseData(topicNames).size());

        SharePartitionManager.ShareSessionKey session2Key = new SharePartitionManager.ShareSessionKey(groupId, reqMetadata2.memberId());

        // both newly created entries are present in cache
        assertNotNull(cache.get(session1Key));
        assertNotNull(cache.get(session2Key));

        time.sleep(500);

        // Create a subsequent share fetch context for session 1
        ShareFetchContext session1context2 = sharePartitionManager.newContext(groupId, new LinkedHashMap<>(), emptyPartList,
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
        assertEquals(session1context2.getClass(), SharePartitionManager.ShareSessionContext.class);

        // total sleep time will now be large enough that share session 1 will be evicted if not correctly touched
        time.sleep(501);

        // create one final share session to test that the least recently used entry is evicted
        // the second share session should be evicted because the first share session was incrementally fetched
        // more recently than the second session was created
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> session3req = new LinkedHashMap<>();
        session3req.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        session3req.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        ShareFetchMetadata reqMetadata3 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext session3context = sharePartitionManager.newContext(groupId, session3req, emptyPartList, reqMetadata3);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData3.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse session3resp = session3context.updateAndGenerateResponseData(groupId, reqMetadata3.memberId(), respData3);
        assertEquals(Errors.NONE, session3resp.error());
        assertEquals(2, session3resp.responseData(topicNames).size());

        SharePartitionManager.ShareSessionKey session3Key = new SharePartitionManager.ShareSessionKey(groupId, reqMetadata3.memberId());

        assertNotNull(cache.get(session1Key));
        assertNull(cache.get(session2Key), "share session 2 should have been evicted by latest share session, " +
                "as share session 1 was used more recently");
        assertNotNull(cache.get(session3Key));
    }

    @Test
    public void testSubsequentShareSession() {
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        // Create a new share session with foo-0 and foo-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        String groupId = "grp";
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, emptyPartList, reqMetadata1);
        assertEquals(context1.getClass(), SharePartitionManager.ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp0.partition()));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent fetch request that removes foo-0 and adds bar-0
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(tp0);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, removed2,
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);

        Set<TopicIdPartition> expectedTopicIdPartitions2 = new HashSet<>();
        expectedTopicIdPartitions2.add(tp1);
        expectedTopicIdPartitions2.add(tp2);
        Set<TopicIdPartition> actualTopicIdPartitions2 = new HashSet<>();
        SharePartitionManager.ShareSessionContext shareSessionContext = (SharePartitionManager.ShareSessionContext) context2;
        shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
            TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                    TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
            actualTopicIdPartitions2.add(topicIdPartition);
        });

        assertEquals(expectedTopicIdPartitions2, actualTopicIdPartitions2);
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp1.partition()));
        respData2.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(tp2.partition()));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(1, resp2.data().responses().size());
        assertEquals(barId, resp2.data().responses().get(0).topicId());
        assertEquals(1, resp2.data().responses().get(0).partitions().size());
        assertEquals(0, resp2.data().responses().get(0).partitions().get(0).partitionIndex());
        assertEquals(1, resp2.responseData(topicNames).size());
    }

    @Test
    public void testZeroSizeShareSession() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid fooId = Uuid.randomUuid();
        topicNames.put(fooId, "foo");
        TopicIdPartition foo0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition foo1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        // Create a new share session with foo-0 and foo-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo0, new ShareFetchRequest.SharePartitionData(foo0.topicId(), 100));
        reqData1.put(foo1, new ShareFetchRequest.SharePartitionData(foo1.topicId(), 100));

        String groupId = "grp";
        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);

        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, emptyPartList, reqMetadata1);
        assertEquals(context1.getClass(), SharePartitionManager.ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(foo0, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo0.partition()));
        respData1.put(foo1, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo1.partition()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share request that removes foo-0 and foo-1
        // Verify that the previous share session was closed.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        List<TopicIdPartition> removed2 = new ArrayList<>();
        removed2.add(foo0);
        removed2.add(foo1);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, removed2,
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertTrue(resp2.responseData(topicNames).isEmpty());
        assertEquals(1, cache.size());
    }

    private ShareFetchResponseData.PartitionData noErrorShareFetchResponse() {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0);
    }

    private ShareFetchResponseData.PartitionData errorShareFetchResponse(Short errorCode) {
        return new ShareFetchResponseData.PartitionData().setPartitionIndex(0).setErrorCode(errorCode);
    }

    private void mockUpdateAndGenerateResponseData(ShareFetchContext context, String groupId, Uuid memberId) {
        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> data = new LinkedHashMap<>();
        if (context.getClass() == SharePartitionManager.ShareSessionContext.class) {
            SharePartitionManager.ShareSessionContext shareSessionContext = (SharePartitionManager.ShareSessionContext) context;
            if (!shareSessionContext.isSubsequent()) {
                shareSessionContext.shareFetchData().forEach((topicIdPartition, sharePartitionData) -> data.put(topicIdPartition,
                        topicIdPartition.topic() == null ? errorShareFetchResponse(Errors.UNKNOWN_TOPIC_ID.code()) :
                                noErrorShareFetchResponse()));
            } else {
                synchronized (shareSessionContext.session()) {
                    shareSessionContext.session().partitionMap().forEach(cachedSharePartition -> {
                        TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                                TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                        data.put(topicIdPartition, topicIdPartition.topic() == null ? errorShareFetchResponse(Errors.UNKNOWN_TOPIC_ID.code()) :
                                        noErrorShareFetchResponse());
                    });
                }
            }
        }
        context.updateAndGenerateResponseData(groupId, memberId, data);
    }

    private void assertPartitionsPresent(SharePartitionManager.ShareSessionContext context, List<TopicIdPartition> partitions) {
        Set<TopicIdPartition> partitionsInContext = new HashSet<>();
        if (!context.isSubsequent()) {
            context.shareFetchData().forEach((topicIdPartition, sharePartitionData) ->
                    partitionsInContext.add(topicIdPartition));
        } else {
            context.session().partitionMap().forEach(cachedSharePartition -> {
                TopicIdPartition topicIdPartition = new TopicIdPartition(cachedSharePartition.topicId(), new
                        TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()));
                partitionsInContext.add(topicIdPartition);
            });
        }
        Set<TopicIdPartition> partitionsSet = new HashSet<>(partitions);
        assertTrue(partitionsSet.equals(partitionsInContext));
    }

    @Test
    public void testToForgetPartitions() {
        String groupId = "grp";
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 0));

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");

        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo, new ShareFetchRequest.SharePartitionData(foo.topicId(), 100));
        reqData1.put(bar, new ShareFetchRequest.SharePartitionData(bar.topicId(), 100));


        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, emptyPartList, reqMetadata1);
        assertEquals(SharePartitionManager.ShareSessionContext.class, context1.getClass());
        assertPartitionsPresent((SharePartitionManager.ShareSessionContext) context1, Arrays.asList(foo, bar));

        mockUpdateAndGenerateResponseData(context1, groupId, reqMetadata1.memberId());

        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, new LinkedHashMap<>(), Collections.singletonList(foo),
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));

        // So foo is removed but not the others.
        assertPartitionsPresent((SharePartitionManager.ShareSessionContext) context2, Collections.singletonList(bar));

        mockUpdateAndGenerateResponseData(context2, groupId, reqMetadata1.memberId());

        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, new LinkedHashMap<>(), Collections.singletonList(bar),
                new ShareFetchMetadata(reqMetadata1.memberId(), 2));
        assertPartitionsPresent((SharePartitionManager.ShareSessionContext) context3, Collections.emptyList());
    }

    // This test simulates a share session where the topic ID changes broker side (the one handling the request) in both the metadata cache and the log
    // -- as though the topic is deleted and recreated.
    @Test
    public void testShareSessionUpdateTopicIdsBrokerSide() {
        String groupId = "grp";
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition foo = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition bar = new TopicIdPartition(barId, new TopicPartition("bar", 1));

        Map<Uuid, String> topicNames = new HashMap<>();
        topicNames.put(fooId, "foo");
        topicNames.put(barId, "bar");

        // Create a new share session with foo-0 and bar-1
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(foo, new ShareFetchRequest.SharePartitionData(foo.topicId(), 100));
        reqData1.put(bar, new ShareFetchRequest.SharePartitionData(bar.topicId(), 100));

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, emptyPartList, reqMetadata1);

        assertEquals(SharePartitionManager.ShareSessionContext.class, context1.getClass());
        assertFalse(((SharePartitionManager.ShareSessionContext) context1).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(bar, new ShareFetchResponseData.PartitionData().setPartitionIndex(bar.partition()));
        respData1.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());
        assertEquals(2, resp1.responseData(topicNames).size());

        // Create a subsequent share fetch request as though no topics changed.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        Map<Uuid, String> topicNamesFooChanged = new HashMap<>();
        topicNamesFooChanged.put(Uuid.randomUuid(), "foo");
        topicNamesFooChanged.put(barId, "bar");
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(reqMetadata1.memberId(), 1));

        assertEquals(SharePartitionManager.ShareSessionContext.class, context2.getClass());
        assertTrue(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        // Likely if the topic ID is different in the broker, it will be different in the log. Simulate the log check finding an inconsistent ID.
        respData2.put(foo, new ShareFetchResponseData.PartitionData().setPartitionIndex(foo.partition()).setErrorCode(
                Errors.INCONSISTENT_TOPIC_ID.code()));
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        // We should have the inconsistent topic ID error on the partition
        assertEquals(Errors.INCONSISTENT_TOPIC_ID.code(), resp2.responseData(topicNames).get(foo).errorCode());
    }

    @Test
    public void testAcknowledgeShareSessionCacheUpdate() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        Time time = new MockTime();
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        // Manually create a share session in cache
        long now1 = time.milliseconds();
        cache.maybeCreateSession(groupId, memberId, now1, 0, new ImplicitLinkedHashCollection<>());
        SharePartitionManager.ShareSessionKey shareSessionKey = new SharePartitionManager.ShareSessionKey(groupId, memberId);
        assertEquals(1, cache.size());
        assertEquals(now1, cache.get(shareSessionKey).creationMs());
        assertEquals(now1, cache.get(shareSessionKey).lastUsedMs());
        assertEquals(1, cache.lastUsed().size());
        assertTrue(cache.lastUsed().containsKey(cache.get(shareSessionKey).lastUsedKey()));

        // Since request epoch is -1 (FINAL_EPOCH), nothing should be updated anywhere
        sharePartitionManager.acknowledgeShareSessionCacheUpdate(groupId, memberId, -1);
        assertEquals(1, cache.size());
        assertEquals(now1, cache.get(shareSessionKey).creationMs());
        assertEquals(now1, cache.get(shareSessionKey).lastUsedMs());
        assertEquals(1, cache.lastUsed().size());
        assertTrue(cache.lastUsed().containsKey(cache.get(shareSessionKey).lastUsedKey()));

        // Testing if lastUsed time was successfully updated in acknowledgeShareSessionCacheUpdate
        time.sleep(100);
        sharePartitionManager.acknowledgeShareSessionCacheUpdate(groupId, memberId, 1);
        assertEquals(1, cache.size());
        assertEquals(now1, cache.get(shareSessionKey).creationMs());
        assertNotEquals(now1, cache.get(shareSessionKey).lastUsedMs());
        assertEquals(1, cache.lastUsed().size());
        assertTrue(cache.lastUsed().containsKey(cache.get(shareSessionKey).lastUsedKey()));
//        assertEquals(Errors.NONE, sharePartitionManager.acknowledgeShareSessionCacheUpdate(groupId, memberId, -1, isAcknowledgementPiggybackedOnFetch));
        cache.remove(shareSessionKey);
        assertEquals(0, cache.size());
    }

    @Test
    public void testAcknowledgeShareSessionCacheUpdateForPiggybackedAcknowledgement() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        Time time = new MockTime();
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        boolean isAcknowledgementPiggybackedOnFetch = true;
        long now1 = time.milliseconds();
        cache.maybeCreateSession(groupId, memberId, now1, 0, new ImplicitLinkedHashCollection<>());
        assertEquals(1, cache.size());
        assertEquals(1, cache.size());

        assertEquals(1, cache.size());
        SharePartitionManager.ShareSession shareSession = cache.get(new SharePartitionManager.ShareSessionKey(groupId, memberId));
        // manually increment the session epoch by 1 because this is taken care of in the newContext() function
        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
        assertEquals(2, shareSession.epoch());

        assertEquals(1, cache.size());
        shareSession = cache.get(new SharePartitionManager.ShareSessionKey(groupId, memberId));
        // manually increment the session epoch by 1 because this is taken care of in the newContext() function
        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
        assertEquals(3, shareSession.epoch());

        // manually remove the share session from the cache because this is taken care of in the newContext() function
        cache.remove(new SharePartitionManager.ShareSessionKey(groupId, memberId));
        assertEquals(0, cache.size());
    }

    private void assertErroneousAndValidTopicIdPartitions(SharePartitionManager.ErroneousAndValidPartitionData erroneousAndValidPartitionData,
                                                    List<TopicIdPartition> expectedErroneous, List<TopicIdPartition> expectedValid) {
        Set<TopicIdPartition> expectedErroneousSet = new HashSet<>(expectedErroneous);
        Set<TopicIdPartition> expectedValidSet = new HashSet<>(expectedValid);
        Set<TopicIdPartition> actualErroneousPartitions = new HashSet<>();
        Set<TopicIdPartition> actualValidPartitions = new HashSet<>();
        erroneousAndValidPartitionData.erroneous().forEach(topicIdPartitionPartitionDataTuple2 ->
            actualErroneousPartitions.add(topicIdPartitionPartitionDataTuple2._1));
        erroneousAndValidPartitionData.validTopicIdPartitions().forEach(topicIdPartitionPartitionDataTuple2 ->
                actualValidPartitions.add(topicIdPartitionPartitionDataTuple2._1));
        assertTrue(expectedErroneousSet.equals(actualErroneousPartitions));
        assertTrue(expectedValidSet.equals(actualValidPartitions));
    }

    @Test
    public void testGetErroneousAndValidTopicIdPartitions() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        Uuid tpId2 = Uuid.randomUuid();
        Uuid tpId3 = Uuid.randomUuid();
        Uuid tpId4 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        topicNames.put(tpId2, null);
        topicNames.put(tpId3, null);
        topicNames.put(tpId4, null);
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));
        TopicIdPartition tpNull1 = new TopicIdPartition(tpId2, new TopicPartition(null, 0));
        TopicIdPartition tpNull2 = new TopicIdPartition(tpId3, new TopicPartition(null, 1));
        TopicIdPartition tpNull3 = new TopicIdPartition(tpId4, new TopicPartition(null, 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));
        reqData2.put(tpNull1, new ShareFetchRequest.SharePartitionData(tpNull1.topicId(), 100));


        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList, reqMetadata2);
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());
        assertErroneousAndValidTopicIdPartitions(context2.getErroneousAndValidTopicIdPartitions(), Collections.singletonList(tpNull1), Arrays.asList(tp0, tp1));

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        respData2.put(tpNull1, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());

        SharePartitionManager.ShareSessionKey shareSessionKey2 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Check for throttled response
        ShareFetchResponse resp2Throttle = context2.throttleResponse(100);
        assertEquals(Errors.NONE, resp2Throttle.error());
        assertEquals(100, resp2Throttle.throttleTimeMs());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();

        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(memberId4, 1)));

        // Continue the first share session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = new LinkedHashMap<>();
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(context5.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context5).isSubsequent());

        assertErroneousAndValidTopicIdPartitions(context5.getErroneousAndValidTopicIdPartitions(), Collections.singletonList(tpNull1), Arrays.asList(tp0, tp1));

        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp5.error());

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = new LinkedHashMap<>();
        reqData7.put(tpNull2, new ShareFetchRequest.SharePartitionData(tpNull2.topicId(), 100));
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        // Check for throttled response
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());

        assertErroneousAndValidTopicIdPartitions(context7.getErroneousAndValidTopicIdPartitions(), Arrays.asList(tpNull1, tpNull2), Arrays.asList(tp0, tp1));

        // Close the subsequent share session.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData8 = new LinkedHashMap<>();
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, reqData8, emptyPartList,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context8.getClass(), SharePartitionManager.FinalContext.class);
        assertEquals(0, cache.size());


        assertErroneousAndValidTopicIdPartitions(context8.getErroneousAndValidTopicIdPartitions(), Collections.emptyList(), Collections.emptyList());
        // Check for throttled response
        ShareFetchResponse resp8 = context8.throttleResponse(100);
        assertEquals(Errors.NONE, resp8.error());
        assertEquals(100, resp8.throttleTimeMs());
    }

    @Test
    public void testShareFetchContextResponseSize() {
        Time time = new MockTime();
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).withTime(time).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));

        String groupId = "grp";

        // Create a new share session with an initial share fetch request
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData2.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        // For response size expected value calculation
        ObjectSerializationCache objectSerializationCache = new ObjectSerializationCache();
        short version = ApiKeys.SHARE_FETCH.latestVersion();

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(Uuid.randomUuid(), ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList, reqMetadata2);
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData2.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize2 = context2.responseSize(respData2, version);
        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());
        assertEquals(respData2, resp2.responseData(topicNames));
        assertEquals(4 + resp2.data().size(objectSerializationCache, version), respSize2);

        SharePartitionManager.ShareSessionKey shareSessionKey2 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata2.memberId());

        // Test trying to create a new session with an invalid epoch
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test trying to create a new session with a non-existent session key
        Uuid memberId4 = Uuid.randomUuid();
        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(memberId4, 1)));

        // Continue the first share session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = new LinkedHashMap<>();
        reqData5.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(context5.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context5).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        respData5.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        int respSize5 = context5.responseSize(respData5, version);
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData5);
        assertEquals(Errors.NONE, resp5.error());
        assertEquals(4 + resp5.data().size(objectSerializationCache, version), respSize5);

        // Test setting an invalid share session epoch.
        assertThrows(InvalidShareSessionEpochException.class, () -> sharePartitionManager.newContext(groupId, reqData2, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 5)));

        // Test generating a throttled response for a subsequent share session
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData7 = new LinkedHashMap<>();
        ShareFetchContext context7 = sharePartitionManager.newContext(groupId, reqData7, emptyPartList,
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));

        int respSize7 = context7.responseSize(respData2, version);
        ShareFetchResponse resp7 = context7.throttleResponse(100);
        assertEquals(Errors.NONE, resp7.error());
        assertEquals(100, resp7.throttleTimeMs());
        assertEquals(4 + new ShareFetchResponseData().size(objectSerializationCache, version), respSize7);

        // Close the subsequent share session.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData8 = new LinkedHashMap<>();
        ShareFetchContext context8 = sharePartitionManager.newContext(groupId, reqData8, emptyPartList,
                new ShareFetchMetadata(reqMetadata2.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context8.getClass(), SharePartitionManager.FinalContext.class);
        assertEquals(0, cache.size());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData8 = new LinkedHashMap<>();
        respData8.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        int respSize8 = context8.responseSize(respData8, version);
        ShareFetchResponse resp8 = context8.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData8);
        assertEquals(Errors.NONE, resp8.error());
        assertEquals(4 + resp8.data().size(objectSerializationCache, version), respSize8);
    }

    @Test
    public void testAcknowledgeSuccess() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 4));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRequestException("Batch record not found. The base offset is not found in the cache."))
        ));
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);
        Map<TopicIdPartition, List<SharePartition.AcknowledgementBatch>> acknowledgeTopics = new HashMap<>();

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();
        acknowledgeTopics.put(tp1, Arrays.asList(
                new SharePartition.AcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new SharePartition.AcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp2, Arrays.asList(
                new SharePartition.AcknowledgementBatch(5, 17, Collections.singletonList((byte) 3)),
                new SharePartition.AcknowledgementBatch(19, 26, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp3, Arrays.asList(
                new SharePartition.AcknowledgementBatch(45, 60, Collections.singletonList((byte) 2)),
                new SharePartition.AcknowledgementBatch(67, 82, Collections.singletonList((byte) 2))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.INVALID_REQUEST.code(), result.get(tp2).errorCode());
        assertEquals(4, result.get(tp3).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp3).errorCode());
    }

    @Test
    public void testAcknowledgeIncorrectGroupId() {
        String groupId = "grp";
        String groupId2 = "grp2";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo3", 4));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRequestException("Batch record not found. The base offset is not found in the cache."))
        ));
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);
        Map<TopicIdPartition, List<SharePartition.AcknowledgementBatch>> acknowledgeTopics = new HashMap<>();

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        acknowledgeTopics.put(tp1, Arrays.asList(
                new SharePartition.AcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new SharePartition.AcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp2, Arrays.asList(
                new SharePartition.AcknowledgementBatch(5, 17, Collections.singletonList((byte) 3)),
                new SharePartition.AcknowledgementBatch(19, 26, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp3, Arrays.asList(
                new SharePartition.AcknowledgementBatch(45, 60, Collections.singletonList((byte) 2)),
                new SharePartition.AcknowledgementBatch(67, 82, Collections.singletonList((byte) 2))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId2, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp2).errorCode());
        assertEquals(4, result.get(tp3).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp3).errorCode());
    }

    @Test
    public void testAcknowledgeIncorrectMemberId() {
        String groupId = "grp";
        String memberId2 = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo1", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo2", 2));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.acknowledge(ArgumentMatchers.eq(memberId2), any())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRequestException("Member is not the owner of batch record"))
        ));
        when(sp2.acknowledge(ArgumentMatchers.eq(memberId2), any())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRequestException("Member is not the owner of batch record"))
        ));
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);
        Map<TopicIdPartition, List<SharePartition.AcknowledgementBatch>> acknowledgeTopics = new HashMap<>();

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        acknowledgeTopics.put(tp1, Arrays.asList(
                new SharePartition.AcknowledgementBatch(12, 20, Collections.singletonList((byte) 1)),
                new SharePartition.AcknowledgementBatch(24, 56, Collections.singletonList((byte) 1))
        ));
        acknowledgeTopics.put(tp2, Arrays.asList(
                new SharePartition.AcknowledgementBatch(5, 17, Collections.singletonList((byte) 3)),
                new SharePartition.AcknowledgementBatch(19, 26, Collections.singletonList((byte) 1))
        ));

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId2, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(2, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.INVALID_REQUEST.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.INVALID_REQUEST.code(), result.get(tp2).errorCode());
    }

    @Test
    public void testAcknowledgeEmptyPartitionCacheMap() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo4", 3));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.acknowledge(ArgumentMatchers.eq(memberId), any())).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRequestException("Batch record not found. The base offset is not found in the cache."))
        ));
        Map<TopicIdPartition, List<SharePartition.AcknowledgementBatch>> acknowledgeTopics = new HashMap<>();

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();
        acknowledgeTopics.put(tp, Arrays.asList(
                new SharePartition.AcknowledgementBatch(78, 90, Collections.singletonList((byte) 2)),
                new SharePartition.AcknowledgementBatch(94, 99, Collections.singletonList((byte) 2))
        ));
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.acknowledge(memberId, groupId, acknowledgeTopics);
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp));
        assertEquals(3, result.get(tp).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp).errorCode());
    }

    @Test
    public void testMultipleSequentialShareFetches() {
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(barId, new TopicPartition("bar", 1));
        TopicIdPartition tp4 = new TopicIdPartition(fooId, new TopicPartition("foo", 2));
        TopicIdPartition tp5 = new TopicIdPartition(barId, new TopicPartition("bar", 2));
        TopicIdPartition tp6 = new TopicIdPartition(fooId, new TopicPartition("foo", 3));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp3, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp4, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp5, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp6, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withReplicaManager(replicaManager).build();

        doAnswer(invocation -> {
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).when(replicaManager).fetchMessages(any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, Arrays.asList(tp0, tp1, tp2, tp3), partitionMaxBytes);
        Mockito.verify(replicaManager, times(1)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, Collections.singletonList(tp4), partitionMaxBytes);
        Mockito.verify(replicaManager, times(2)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());

        sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, Arrays.asList(tp5, tp6), partitionMaxBytes);
        Mockito.verify(replicaManager, times(3)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
    }

    @Test
    public void testProcessFetchResponse() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
            k -> new SharePartition(groupId, tp0, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                    RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), persister, replicaManager));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
            k -> new SharePartition(groupId, tp1, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                    RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), persister, replicaManager));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
            Arrays.asList(tp0, tp1), future, partitionMaxBytes);

        MemoryRecords records = MemoryRecords.withRecords(CompressionType.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        MemoryRecords records1 = MemoryRecords.withRecords(100L, CompressionType.NONE,
            new SimpleRecord("0".getBytes(), "v".getBytes()),
            new SimpleRecord("1".getBytes(), "v".getBytes()),
            new SimpleRecord("2".getBytes(), "v".getBytes()),
            new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            records, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 100L,
            records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1)),
            resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1)),
            resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testProcessFetchResponseWithEmptyRecords() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("foo", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
            k -> new SharePartition(groupId, tp0, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                    RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), persister, replicaManager));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
            k -> new SharePartition(groupId, tp1, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                    RECORD_LOCK_DURATION_MS, mockTimer, new MockTime(), persister, replicaManager));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
            new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
            1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()), groupId, memberId,
            Arrays.asList(tp0, tp1), future, partitionMaxBytes);

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = new ArrayList<>();
        responseData.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        responseData.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
            MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
            OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result =
            sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData);

        assertTrue(result.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData = result.join();
        assertEquals(2, resultData.size());
        assertTrue(resultData.containsKey(tp0));
        assertTrue(resultData.containsKey(tp1));
        assertEquals(0, resultData.get(tp0).partitionIndex());
        assertEquals(1, resultData.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData.get(tp1).errorCode());
        assertEquals(Collections.emptyList(), resultData.get(tp0).acquiredRecords());
        assertEquals(Collections.emptyList(), resultData.get(tp1).acquiredRecords());
    }

    @Test
    public void testMultipleConcurrentShareFetches() throws InterruptedException {

        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        Uuid barId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(barId, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(barId, new TopicPartition("bar", 1));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp2, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp3, PARTITION_MAX_BYTES);

        final Time time = new MockTime(0, System.currentTimeMillis(), 0);
        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp0),
                k -> new SharePartition(groupId, tp0, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                        RECORD_LOCK_DURATION_MS, mockTimer, time, persister, replicaManager));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp1),
                k -> new SharePartition(groupId, tp1, MAX_DELIVERY_COUNT,  MAX_IN_FLIGHT_MESSAGES,
                        RECORD_LOCK_DURATION_MS, mockTimer, time, persister, replicaManager));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp2),
                k -> new SharePartition(groupId, tp2, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                        RECORD_LOCK_DURATION_MS, mockTimer, time, persister, replicaManager));
        partitionCacheMap.computeIfAbsent(new SharePartitionManager.SharePartitionKey(groupId, tp3),
                k -> new SharePartition(groupId, tp3, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES,
                        RECORD_LOCK_DURATION_MS, mockTimer, time, persister, replicaManager));

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withTime(time).withReplicaManager(replicaManager).build();

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        SharePartition sp3 = Mockito.mock(SharePartition.class);

        when(sp0.nextFetchOffset()).thenReturn((long) 1, (long) 15, (long) 6, (long) 30, (long) 25);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 1, (long) 18, (long) 5);
        when(sp2.nextFetchOffset()).thenReturn((long) 10, (long) 25, (long) 26);
        when(sp3.nextFetchOffset()).thenReturn((long) 20, (long) 15, (long) 23, (long) 16);

        doAnswer(invocation -> {
            assertEquals(1, sp0.nextFetchOffset());
            assertEquals(4, sp1.nextFetchOffset());
            assertEquals(10, sp2.nextFetchOffset());
            assertEquals(20, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(15, sp0.nextFetchOffset());
            assertEquals(1, sp1.nextFetchOffset());
            assertEquals(25, sp2.nextFetchOffset());
            assertEquals(15, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(6, sp0.nextFetchOffset());
            assertEquals(18, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(23, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(30, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).doAnswer(invocation -> {
            assertEquals(25, sp0.nextFetchOffset());
            assertEquals(5, sp1.nextFetchOffset());
            assertEquals(26, sp2.nextFetchOffset());
            assertEquals(16, sp3.nextFetchOffset());
            sharePartitionManager.releaseFetchQueueAndPartitionsLock(groupId, partitionMaxBytes.keySet());
            return null;
        }).when(replicaManager).fetchMessages(any(), any(), any(ReplicaQuota.class), any());

        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

        try {
            for (int i = 0; i != threadCount; ++i) {
                executorService.submit(() -> {
                    sharePartitionManager.fetchMessages(groupId, memberId1.toString(), fetchParams, Arrays.asList(tp0, tp1, tp2, tp3), partitionMaxBytes);
                });
                // We are blocking the main thread at an interval of 10 threads so that the currently running executorService threads can complete.
                if (i % 10 == 0)
                    executorService.awaitTermination(50, TimeUnit.MILLISECONDS);
            }
        } finally {
            if (!executorService.awaitTermination(50, TimeUnit.MILLISECONDS))
                executorService.shutdown();
        }
        // We are checking the number of replicaManager fetchMessages() calls
        Mockito.verify(replicaManager, atMost(100)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
        Mockito.verify(replicaManager, atLeast(10)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
    }

    @Test
    public void testReplicaManagerFetchShouldNotProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        // Simulating the case where the SharePartition has reached the maximum in-flight records limit, and the
        // nextFetchOffset points to endOffset + 1
        when(sp0.nextFetchOffset()).thenReturn((long) 200);
        when(sp0.canAcquireMore()).thenReturn(false);
        when(sp0.endOffset()).thenReturn((long) 199);
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future =
                sharePartitionManager.fetchMessages(groupId, memberId.toString(), fetchParams, Collections.singletonList(tp0), partitionMaxBytes);
        // Since the nextFetchOffset points to endOffset + 1, i.e. none of the records in the cachedState are AVAILABLE,
        // and the maxInFlightMessages limit is exceeded, replicaManager.fetchMessages should not be called
        Mockito.verify(replicaManager, times(0)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = future.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testReplicaManagerFetchShouldProceed() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        FetchParams fetchParams = new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty());
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);

        SharePartition sp0 = Mockito.mock(SharePartition.class);
        when(sp0.maybeAcquireFetchLock()).thenReturn(true);
        // Simulating the case where the SharePartition has reached the maximum in-flight records limit, and the
        // nextFetchOffset does not point to endOffset + 1
        when(sp0.nextFetchOffset()).thenReturn((long) 100);
        when(sp0.canAcquireMore()).thenReturn(false);
        when(sp0.endOffset()).thenReturn((long) 199);
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        sharePartitionManager.fetchMessages(groupId, memberId.toString(), fetchParams, Collections.singletonList(tp0), partitionMaxBytes);
        // Since the nextFetchOffset does not point to endOffset + 1, i.e. some of the records in the cachedState are AVAILABLE,
        // even though the maxInFlightMessages limit is exceeded, replicaManager.fetchMessages should be called
        Mockito.verify(replicaManager, times(1)).fetchMessages(
                any(), any(), any(ReplicaQuota.class), any());
    }

    @Test
    public void testCachedTopicPartitionsForInvalidShareSession() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();

        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.cachedTopicIdPartitionsInShareSession("grp", Uuid.randomUuid()));
    }

    @Test
    public void testCachedTopicPartitionsForValidShareSessions() {
        SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withCache(cache).build();
        Map<Uuid, String> topicNames = new HashMap<>();
        Uuid tpId0 = Uuid.randomUuid();
        Uuid tpId1 = Uuid.randomUuid();
        topicNames.put(tpId0, "foo");
        topicNames.put(tpId1, "bar");
        TopicIdPartition tp0 = new TopicIdPartition(tpId0, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(tpId0, new TopicPartition("foo", 1));
        TopicIdPartition tp2 = new TopicIdPartition(tpId1, new TopicPartition("bar", 0));
        TopicIdPartition tp3 = new TopicIdPartition(tpId1, new TopicPartition("bar", 1));
        String groupId = "grp";
        Uuid memberId1 = Uuid.randomUuid();
        Uuid memberId2 = Uuid.randomUuid();

        // Create a new share session with an initial share fetch request.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData1 = new LinkedHashMap<>();
        reqData1.put(tp0, new ShareFetchRequest.SharePartitionData(tp0.topicId(), 100));
        reqData1.put(tp1, new ShareFetchRequest.SharePartitionData(tp1.topicId(), 100));

        ShareFetchMetadata reqMetadata1 = new ShareFetchMetadata(memberId1, ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context1 = sharePartitionManager.newContext(groupId, reqData1, emptyPartList, reqMetadata1);
        assertEquals(context1.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context1).isSubsequent());

        SharePartitionManager.ShareSessionKey shareSessionKey1 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata1.memberId());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData1 = new LinkedHashMap<>();
        respData1.put(tp0, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        respData1.put(tp1, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));

        ShareFetchResponse resp1 = context1.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData1);
        assertEquals(Errors.NONE, resp1.error());

        assertEquals(new HashSet<>(Arrays.asList(tp0, tp1)),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Create a new share session with an initial share fetch request.
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData2 = new LinkedHashMap<>();
        reqData2.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));

        ShareFetchMetadata reqMetadata2 = new ShareFetchMetadata(memberId2, ShareFetchMetadata.INITIAL_EPOCH);
        ShareFetchContext context2 = sharePartitionManager.newContext(groupId, reqData2, emptyPartList, reqMetadata2);
        assertEquals(context2.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertFalse(((SharePartitionManager.ShareSessionContext) context2).isSubsequent());

        SharePartitionManager.ShareSessionKey shareSessionKey2 = new SharePartitionManager.ShareSessionKey(groupId,
                reqMetadata2.memberId());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData2 = new LinkedHashMap<>();
        respData2.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));

        ShareFetchResponse resp2 = context2.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData2);
        assertEquals(Errors.NONE, resp2.error());

        assertEquals(Collections.singletonList(tp2), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Continue the first share session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData3 = new LinkedHashMap<>();
        reqData3.put(tp2, new ShareFetchRequest.SharePartitionData(tp2.topicId(), 100));
        ShareFetchContext context3 = sharePartitionManager.newContext(groupId, reqData3, emptyPartList,
                new ShareFetchMetadata(shareSessionKey1.memberId(), 1));
        assertEquals(context3.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context3).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData3 = new LinkedHashMap<>();
        respData3.put(tp2, new ShareFetchResponseData.PartitionData().setPartitionIndex(0));
        ShareFetchResponse resp3 = context3.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData3);
        assertEquals(Errors.NONE, resp3.error());

        assertEquals(new HashSet<>(Arrays.asList(tp0, tp1, tp2)),
                new HashSet<>(sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1)));

        // Continue the second session we created.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData4 = new LinkedHashMap<>();
        reqData4.put(tp3, new ShareFetchRequest.SharePartitionData(tp3.topicId(), 100));
        ShareFetchContext context4 = sharePartitionManager.newContext(groupId, reqData4, Collections.singletonList(tp2),
                new ShareFetchMetadata(shareSessionKey2.memberId(), 1));
        assertEquals(context4.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context4).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData4 = new LinkedHashMap<>();
        respData4.put(tp3, new ShareFetchResponseData.PartitionData().setPartitionIndex(1));
        ShareFetchResponse resp4 = context4.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData4);
        assertEquals(Errors.NONE, resp4.error());

        assertEquals(Collections.singletonList(tp3), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));

        // Close the first share session.
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData5 = new LinkedHashMap<>();
        ShareFetchContext context5 = sharePartitionManager.newContext(groupId, reqData5, emptyPartList,
                new ShareFetchMetadata(reqMetadata1.memberId(), ShareFetchMetadata.FINAL_EPOCH));
        assertEquals(context5.getClass(), SharePartitionManager.FinalContext.class);

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData5 = new LinkedHashMap<>();
        ShareFetchResponse resp5 = context5.updateAndGenerateResponseData(groupId, reqMetadata1.memberId(), respData5);
        assertEquals(Errors.NONE, resp5.error());

        assertThrows(ShareSessionNotFoundException.class, () -> sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId1));

        // Continue the second share session .
        LinkedHashMap<TopicIdPartition, ShareFetchRequest.SharePartitionData> reqData6 = new LinkedHashMap<>();
        ShareFetchContext context6 = sharePartitionManager.newContext(groupId, reqData6, Collections.singletonList(tp3),
                new ShareFetchMetadata(shareSessionKey2.memberId(), 2));
        assertEquals(context6.getClass(), SharePartitionManager.ShareSessionContext.class);
        assertTrue(((SharePartitionManager.ShareSessionContext) context6).isSubsequent());

        LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> respData6 = new LinkedHashMap<>();
        ShareFetchResponse resp6 = context6.updateAndGenerateResponseData(groupId, reqMetadata2.memberId(), respData6);
        assertEquals(Errors.NONE, resp6.error());

        assertEquals(Collections.emptyList(), sharePartitionManager.cachedTopicIdPartitionsInShareSession(groupId, memberId2));
    }

    @Test
    public void testReleaseAcquiredRecordsSuccess() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));
        TopicIdPartition tp3 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("baz", 4));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(
                Optional.of(new InvalidRecordStateException("Unable to release acquired records for the batch"))
        ));
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseAcquiredRecords(groupId, memberId, Arrays.asList(tp1, tp2, tp3));
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(3, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertTrue(result.containsKey(tp3));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.INVALID_RECORD_STATE.code(), result.get(tp2).errorCode());
        // tp3 is not a part of partitionCacheMap.
        assertEquals(4, result.get(tp3).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp3).errorCode());
    }

    @Test
    public void testReleaseAcquiredRecordsWithIncorrectGroupId() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();
        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseAcquiredRecords("grp-2", memberId, Collections.singletonList(tp1));
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(1, result.size());
        assertTrue(result.containsKey(tp1));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION.code(), result.get(tp1).errorCode());
    }

    @Test
    public void testReleaseAcquiredRecordsWithIncorrectMemberId() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseAcquiredRecords(groupId, memberId, Arrays.asList(tp1, tp2));
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(2, result.size());
        assertTrue(result.containsKey(tp1));
        assertTrue(result.containsKey(tp2));
        assertEquals(0, result.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp1).errorCode());
        assertEquals(2, result.get(tp2).partitionIndex());
        assertEquals(Errors.NONE.code(), result.get(tp2).errorCode());
    }

    @Test
    public void testReleaseAcquiredRecordsWithEmptyTopicPartitions() {
        String groupId = "grp";
        String memberId = Uuid.randomUuid().toString();

        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("foo", 0));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bar", 2));

        SharePartition sp1 = Mockito.mock(SharePartition.class);
        SharePartition sp2 = Mockito.mock(SharePartition.class);
        when(sp1.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(sp2.releaseAcquiredRecords(ArgumentMatchers.eq(memberId))).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp2), sp2);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).build();

        CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> resultFuture =
                sharePartitionManager.releaseAcquiredRecords(groupId, memberId, Collections.emptyList());
        Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = resultFuture.join();
        assertEquals(0, result.size());
    }

    @Test
    public void testCloseSharePartitionManager() throws Exception {
        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder().build();

        List<Integer> mockList = new ArrayList<>();
        sharePartitionManager.timer().add(createTimerTask(mockList));
        // Allowing first timer task to expire. The timer task will add an element to mockList.
        Thread.sleep(180);
        assertEquals(1, mockList.size());
        sharePartitionManager.timer().add(createTimerTask(mockList));
        // Closing the sharePartitionManager closes timer object in sharePartitionManager.
        sharePartitionManager.close();
        // Allowing second timer task to expire. It won't be able to add an element to mockList.
        Thread.sleep(180);
        assertEquals(1, mockList.size());
    }

    private TimerTask createTimerTask(List<Integer> mockList) {
        return new TimerTask(100) {
            @Override
            public void run() {
                mockList.add(0);
            }
        };
    }

    @Test
    public void testSharePersisterObjectCreationFailure() throws RuntimeException {
        mockUtils.when(() -> Utils.newInstance(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenThrow(new
                RuntimeException("Persister object creation failed"));
        assertThrows(KafkaException.class, () -> SharePartitionManagerBuilder.builder()
                .withShareGroupPersisterClassName("mockShareGroupPersisterClass").build());

        mockUtils.when(() -> Utils.newInstance(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenThrow(new
                ClassNotFoundException("Persister object creation failed"));
        assertThrows(ConfigException.class, () -> SharePartitionManagerBuilder.builder()
                .withShareGroupPersisterClassName("mockShareGroupPersisterClass").build());
    }

    @Test
    public void testSharePersisterObjectCreationSuccess() {
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));

        // Mocking the Persister object creation. We also need to mock readState() method to return a valid result since
        // it is called during SharePartition object creation.
        Persister persister = Mockito.mock(Persister.class);
        ReadShareGroupStateResult readShareGroupStateResult = Mockito.mock(ReadShareGroupStateResult.class);
        Mockito.when(readShareGroupStateResult.topicsData()).thenReturn(Collections.singletonList(
                new TopicData<>(tp0.topicId(), Collections.singletonList(
                        PartitionFactory.newPartitionAllData(0, 3, 5L, Errors.NONE.code(), Errors.NONE.message(),
                                Arrays.asList(
                                        new PersisterStateBatch(5L, 10L, SharePartition.RecordState.AVAILABLE.id, (short) 2),
                                        new PersisterStateBatch(11L, 15L, SharePartition.RecordState.ARCHIVED.id, (short) 3)))))));
        Mockito.when(persister.readState(Mockito.any())).thenReturn(CompletableFuture.completedFuture(readShareGroupStateResult));

        mockUtils.when(() -> Utils.newInstance(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(persister);
        assertDoesNotThrow(() -> SharePartitionManagerBuilder.builder().withShareGroupPersisterClassName("mockShareGroupPersisterClass").build());
    }

    @Test
    public void testProcessFetchResponseWithLsoMovementForTopicPartition() {
        String groupId = "grp";
        Uuid memberId = Uuid.randomUuid();
        Uuid fooId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(fooId, new TopicPartition("foo", 0));
        TopicIdPartition tp1 = new TopicIdPartition(fooId, new TopicPartition("foo", 1));

        Map<TopicIdPartition, Integer> partitionMaxBytes = new HashMap<>();
        partitionMaxBytes.put(tp0, PARTITION_MAX_BYTES);
        partitionMaxBytes.put(tp1, PARTITION_MAX_BYTES);

        ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        SharePartition sp0 = Mockito.mock(SharePartition.class);
        SharePartition sp1 = Mockito.mock(SharePartition.class);

        Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new ConcurrentHashMap<>();
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp0), sp0);
        partitionCacheMap.put(new SharePartitionManager.SharePartitionKey(groupId, tp1), sp1);

        SharePartitionManager sharePartitionManager = SharePartitionManagerBuilder.builder()
                .withPartitionCacheMap(partitionCacheMap).withReplicaManager(replicaManager).build();

        when(sp0.nextFetchOffset()).thenReturn((long) 0, (long) 5);
        when(sp1.nextFetchOffset()).thenReturn((long) 4, (long) 4);

        when(sp0.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.emptyList()),
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(0).setLastOffset(3).setDeliveryCount((short) 1))));
        when(sp1.acquire(any(), any())).thenReturn(
                CompletableFuture.completedFuture(Collections.singletonList(new ShareFetchResponseData.AcquiredRecords()
                        .setFirstOffset(100).setLastOffset(103).setDeliveryCount((short) 1))),
                CompletableFuture.completedFuture(Collections.emptyList()));

        doNothing().when(sp1).updateOffsetsOnLsoMovement();
        doNothing().when(sp0).updateOffsetsOnLsoMovement();

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        SharePartitionManager.ShareFetchPartitionData shareFetchPartitionData = new SharePartitionManager.ShareFetchPartitionData(
                new FetchParams(ApiKeys.SHARE_FETCH.latestVersion(), FetchRequest.ORDINARY_CONSUMER_ID, -1, 0,
                        1, 1024 * 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty()),
                groupId, memberId.toString(), Arrays.asList(tp0, tp1), future, partitionMaxBytes);

        MemoryRecords records1 = MemoryRecords.withRecords(CompressionType.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData1 = new ArrayList<>();
        responseData1.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.OFFSET_OUT_OF_RANGE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData1.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records1, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result1 =
                sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData1);

        assertTrue(result1.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData1 = result1.join();
        assertEquals(2, resultData1.size());
        assertTrue(resultData1.containsKey(tp0));
        assertTrue(resultData1.containsKey(tp1));
        assertEquals(0, resultData1.get(tp0).partitionIndex());
        assertEquals(1, resultData1.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData1.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData1.get(tp1).errorCode());

        Mockito.verify(sp0, times(1)).updateOffsetsOnLsoMovement();
        Mockito.verify(sp1, times(0)).updateOffsetsOnLsoMovement();

        MemoryRecords records2 = MemoryRecords.withRecords(100L, CompressionType.NONE,
                new SimpleRecord("0".getBytes(), "v".getBytes()),
                new SimpleRecord("1".getBytes(), "v".getBytes()),
                new SimpleRecord("2".getBytes(), "v".getBytes()),
                new SimpleRecord(null, "value".getBytes()));

        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData2 = new ArrayList<>();
        responseData2.add(new Tuple2<>(tp0, new FetchPartitionData(Errors.NONE, 0L, 0L,
                records2, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        responseData2.add(new Tuple2<>(tp1, new FetchPartitionData(Errors.NONE, 0L, 0L,
                MemoryRecords.EMPTY, Optional.empty(), OptionalLong.empty(), Optional.empty(),
                OptionalInt.empty(), false)));
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> result2 =
                sharePartitionManager.processFetchResponse(shareFetchPartitionData, responseData2);

        assertTrue(result2.isDone());
        Map<TopicIdPartition, ShareFetchResponseData.PartitionData> resultData2 = result2.join();
        assertEquals(2, resultData2.size());
        assertTrue(resultData2.containsKey(tp0));
        assertTrue(resultData2.containsKey(tp1));
        assertEquals(0, resultData2.get(tp0).partitionIndex());
        assertEquals(1, resultData2.get(tp1).partitionIndex());
        assertEquals(Errors.NONE.code(), resultData2.get(tp0).errorCode());
        assertEquals(Errors.NONE.code(), resultData2.get(tp1).errorCode());

        Mockito.verify(sp0, times(1)).updateOffsetsOnLsoMovement();
        Mockito.verify(sp1, times(0)).updateOffsetsOnLsoMovement();
    }

    private static class SharePartitionManagerBuilder {
        private ReplicaManager replicaManager = Mockito.mock(ReplicaManager.class);
        private Time time = new MockTime();
        private SharePartitionManager.ShareSessionCache cache = new SharePartitionManager.ShareSessionCache(10, 1000);
        private Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap = new HashMap<>();
        private String shareGroupPersisterClassName = "";

        private SharePartitionManagerBuilder withReplicaManager(ReplicaManager replicaManager) {
            this.replicaManager = replicaManager;
            return this;
        }

        private SharePartitionManagerBuilder withTime(Time time) {
            this.time = time;
            return this;
        }

        private SharePartitionManagerBuilder withCache(SharePartitionManager.ShareSessionCache cache) {
            this.cache = cache;
            return this;
        }

        private SharePartitionManagerBuilder withPartitionCacheMap(Map<SharePartitionManager.SharePartitionKey, SharePartition> partitionCacheMap) {
            this.partitionCacheMap = partitionCacheMap;
            return this;
        }

        private SharePartitionManagerBuilder withShareGroupPersisterClassName(String shareGroupPersisterClassName) {
            this.shareGroupPersisterClassName = shareGroupPersisterClassName;
            return this;
        }

        public static SharePartitionManagerBuilder builder() {
            return new SharePartitionManagerBuilder();
        }
        public SharePartitionManager build() {
            return new SharePartitionManager(replicaManager, time, cache, partitionCacheMap, RECORD_LOCK_DURATION_MS, MAX_DELIVERY_COUNT, MAX_IN_FLIGHT_MESSAGES, shareGroupPersisterClassName);
        }
    }
}

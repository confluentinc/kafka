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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeRequestData;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.TreeMap;

import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import org.slf4j.LoggerFactory;

public class SharePartitionManager {

    private final static Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    // TODO: May be use ImplicitLinkedHashCollection.
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;
    private final ReplicaManager replicaManager;
    private final Time time;
    private final ShareSessionCache cache;

    public SharePartitionManager(ReplicaManager replicaManager, Time time, ShareSessionCache cache) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        partitionCacheMap = new ConcurrentHashMap<>();
    }

    // TODO: Move some part in share session context and change method signature to accept share
    //  partition data along TopicIdPartition.
    public CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
            topicIdPartitions, groupId, fetchParams);
        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();

        synchronized (this) {
            Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new HashMap<>();
            topicIdPartitions.forEach(topicIdPartition -> {
                // TODO: Fetch inflight and delivery count from config.
                SharePartition sharePartition = partitionCacheMap.computeIfAbsent(sharePartitionKey(groupId, topicIdPartition), k -> new SharePartition(100, 5));
                topicPartitionData.put(topicIdPartition, new FetchRequest.PartitionData(
                    topicIdPartition.topicId(),
                    sharePartition.nextFetchOffset(),
                    -1,
                    Integer.MAX_VALUE,
                    Optional.empty()));
            });

            replicaManager.fetchMessages(
                fetchParams,
                CollectionConverters.asScala(
                    topicPartitionData.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue())).collect(
                        Collectors.toList())
                ),
                QuotaFactory.UnboundedQuota$.MODULE$,
                responsePartitionData -> {
                    List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = CollectionConverters.asJava(
                        responsePartitionData);
                    Map<TopicIdPartition, ShareFetchResponseData.PartitionData> result = new HashMap<>();
                    responseData.forEach(data -> {
                        TopicIdPartition topicIdPartition = data._1;
                        FetchPartitionData fetchPartitionData = data._2;

                        ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                            .setPartitionIndex(topicIdPartition.partition())
                            .setRecords(fetchPartitionData.records)
                            .setErrorCode(fetchPartitionData.error.code())
//                        .setAcquiredRecords(fetchPartitionData.records)
                            .setAcknowledgeErrorCode(Errors.NONE.code());

                        SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
                        sharePartition.update(memberId, fetchPartitionData);

                        result.put(topicIdPartition, partitionData);
                        future.complete(result);
                    });
                    return BoxedUnit.UNIT;
                });
        }
        return future;
    }

    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> acknowledge(
            String groupId,
            Map<TopicIdPartition, ShareAcknowledgeRequestData.AcknowledgePartition> acknowledgeTopics
    ) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    private SharePartition sharePartition(SharePartitionKey sharePartitionKey) {
        return partitionCacheMap.getOrDefault(sharePartitionKey, null);
    }

    // TODO: Function requires an in depth implementation in the future. For now, it returns a new share session everytime
    public ShareSession session(Time time, String groupId, Uuid memberId, ShareFetchRequest request) {
        return new ShareSession(shareSessionKey(groupId, memberId), new ImplicitLinkedHashCollection<>(),
                time.milliseconds(), time.milliseconds(), ShareFetchMetadata.nextEpoch(ShareFetchMetadata.INITIAL_EPOCH));
    }

    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> forgottenTopics,
                                        Map<Uuid, String> topicNames, ShareFetchMetadata reqMetadata) {
        ShareFetchContext context;
        if (reqMetadata.isFull()) {
            String removedFetchSessionStr = "";
            // TODO: We will handle the case of INVALID_MEMBER_ID once we have a clear definition for it
         //  if (!Objects.equals(reqMetadata.memberId(), ShareFetchMetadata.INVALID_MEMBER_ID)) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            if (cache.remove(key) != null)
                removedFetchSessionStr = "Removed share session with key " + key;

            String suffix = "";
            if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                suffix = " Will not try to create a new session.";
                context = new SessionlessShareFetchContext(shareFetchData);
            } else {
                context = new FullShareFetchContext(time, cache, reqMetadata, shareFetchData);
                log.debug("Created a new full ShareFetchContext with {} {}",
                        partitionsToLogString(shareFetchData.keySet()), removedFetchSessionStr);
            }
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such session ID found", key);
                    context = new ShareSessionErrorContext(Errors.SHARE_SESSION_ID_NOT_FOUND, reqMetadata);
                }
                else {
                    if (shareSession.epoch != reqMetadata.epoch()) {
                        log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                                shareSession.epoch, reqMetadata.epoch());
                        context = new ShareSessionErrorContext(Errors.INVALID_SHARE_SESSION_EPOCH, reqMetadata);
                    }
                    else {

                    }
                }
            }
        }
        return context;
    }

    String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    // TODO: Define share session class.
    public static class ShareSession {

        private final ShareSessionKey key;
        private final ImplicitLinkedHashCollection<SharePartitionManager.CachedPartition> partitionMap;
        private final long creationMs;
        private final long lastUsedMs;
        private final int epoch;

        // This is used by the ShareSessionCache to store the last known size of this session.
        // If this is -1, the Session is not in the cache.
        private int cachedSize = -1;

        /**
         * The share session.
         * Each share session is protected by its own lock, which must be taken before mutable
         * fields are read or modified.  This includes modification of the share session partition map.
         *
         * @param key                The share session key to identify the share session uniquely.
         * @param partitionMap       The CachedPartitionMap.
         * @param creationMs         The time in milliseconds when this share session was created.
         * @param lastUsedMs         The last used time in milliseconds. This should only be updated by
         *                           ShareSessionCache#touch.
         * @param epoch              The share session sequence number.
         */
        public ShareSession(ShareSessionKey key, ImplicitLinkedHashCollection<CachedPartition> partitionMap,
                            long creationMs, long lastUsedMs, int epoch) {
            this.key = key;
            this.partitionMap = partitionMap;
            this.creationMs = creationMs;
            this.lastUsedMs = lastUsedMs;
            this.epoch = epoch;
        }

        public int size() {
            synchronized (this) {
                return partitionMap.size();
            }
        }

        public Boolean isEmpty() {
            synchronized (this) {
                return partitionMap.isEmpty();
            }
        }

        public LastUsedKey lastUsedKey() {
            synchronized (this) {
                return new LastUsedKey(key, lastUsedMs);
            }
        }

        public EvictableKey evictableKey() {
            synchronized (this) {
                return new EvictableKey(key, cachedSize);
            }
        }

        // Update the cached partition data based on the request.
        public void update(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData,
                           List<TopicIdPartition> toForget) {
            List<TopicIdPartition> added = new ArrayList<>();
            List<TopicIdPartition> updated = new ArrayList<>();
            List<TopicIdPartition> removed = new ArrayList<>();
            shareFetchData.forEach((topicIdPartition, sharePartitionData) -> {
                CachedPartition cachedPartitionKey = new CachedPartition(topicIdPartition, sharePartitionData);
                CachedPartition cachedPart = partitionMap.find(cachedPartitionKey);
                if(cachedPart == null) {
                    partitionMap.mustAdd(cachedPartitionKey);
                    added.add(topicIdPartition);
                }
                else {
                    cachedPart.updateRequestParams(sharePartitionData);
                    updated.add(topicIdPartition);
                }
            });
            toForget.forEach(topicIdPartition -> {
                if (partitionMap.remove(new CachedPartition(topicIdPartition)))
                    removed.add(topicIdPartition);
            });
            return added, updated, removed;
        }

        public String toString() {
            return "ShareSession(" +
                    " key=" + key +
                    ", partitionMap=" + partitionMap +
                    ", creationMs=" + creationMs +
                    ", lastUsedMs=" + lastUsedMs +
                    ", epoch=" +
                    ")";
        }
    }

    /**
     * The share fetch context for a sessionless share fetch request.
     */
    public static class SessionlessShareFetchContext extends ShareFetchContext {
        private final Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        public SessionlessShareFetchContext(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.log = LoggerFactory.getLogger(SessionlessShareFetchContext.class);
            this.shareFetchData = shareFetchData;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Sessionless fetch context returning" + partitionsToLogString(updates.keySet()));
            return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            return shareFetchData;
        }
    }

    /**
     * The fetch context for a full share fetch request.
     */
    // TODO: Implement FullShareFetchContext when you have share sessions available
    public static class FullShareFetchContext extends ShareFetchContext {

        private Time time;
        private ShareSessionCache cache;
        private ShareFetchMetadata reqMetadata;
        private Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        /**
         * @param time               The clock to use.
         * @param cache              The share fetch session cache.
         * @param reqMetadata        The request metadata.
         * @param shareFetchData     The share partition data from the share fetch request.
         */
        public FullShareFetchContext(Time time, ShareSessionCache cache, ShareFetchMetadata reqMetadata,
                                     Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            this.log = LoggerFactory.getLogger(FullShareFetchContext.class);
            this.time = time;
            this.cache = cache;
            this.reqMetadata = reqMetadata;
            this.shareFetchData = shareFetchData;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    /**
     * The share fetch context for a share fetch request that had a session error.
     */
    // TODO: Implement ShareSessionErrorContext when you have share sessions available
    public static class ShareSessionErrorContext extends ShareFetchContext {
        private final Errors error;
        private final ShareFetchMetadata reqMetadata;

        public ShareSessionErrorContext(Errors error, ShareFetchMetadata reqMetadata) {
            this.error = error;
            this.reqMetadata = reqMetadata;
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            return ShareFetchResponse.sizeOf(version, Collections.emptyIterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Share session error fetch context returning " + error);
            return new ShareFetchResponse(ShareFetchResponse.toMessage(error, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            return new HashMap<>();
        }
    }

    /**
     * The share fetch context for an incremental share fetch request.
     */
    // TODO: Implement IncrementalFetchContext when you have share sessions available
    public static class IncrementalFetchContext extends ShareFetchContext {

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates,
                            short version) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            throw new UnsupportedOperationException("Not implemented yet");
        }

        @Override
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> cachedPartitions() {
            throw new UnsupportedOperationException("Not implemented yet");
        }
    }

    // Visible for testing
    static class SharePartitionKey {
        private final String groupId;
        private final TopicIdPartition topicIdPartition;

        public SharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
            this.groupId = Objects.requireNonNull(groupId);
            this.topicIdPartition = Objects.requireNonNull(topicIdPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, topicIdPartition);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            else if (obj == null || getClass() != obj.getClass())
                return false;
            else {
                SharePartitionKey that = (SharePartitionKey) obj;
                return groupId.equals(that.groupId) && Objects.equals(topicIdPartition, that.topicIdPartition);
            }
        }
    }

    // visible for testing
    static class ShareSessionKey {
        private final String groupId;
        private final Uuid memberId;

        public ShareSessionKey(String groupId, Uuid memberId) {
            this.groupId = Objects.requireNonNull(groupId);
            this.memberId = Objects.requireNonNull(memberId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupId, memberId);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            else if (obj == null || getClass() != obj.getClass())
                return false;
            else {
                ShareSessionKey that = (ShareSessionKey) obj;
                return groupId.equals(that.groupId) && Objects.equals(memberId, that.memberId);
            }
        }

        public String toString() {
            return "ShareSessionKey(" +
                    " groupId=" + groupId +
                    ", memberId=" + memberId +
                    ")";
        }
    }

    static class LastUsedKey implements Comparable<LastUsedKey> {
        private final ShareSessionKey key;
        private final long lastUsedMs;

        public LastUsedKey(ShareSessionKey key, long lastUsedMs) {
            this.key = key;
            this.lastUsedMs = lastUsedMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, lastUsedMs);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            LastUsedKey other = (LastUsedKey) obj;
            return lastUsedMs == other.lastUsedMs && Objects.equals(key, other.key);
        }

        //TODO: Complete this compareTo
        @Override
        public int compareTo(LastUsedKey other) {
            return 0;
        }
    }

    static class EvictableKey implements Comparable<EvictableKey> {
        private final ShareSessionKey key;
        private final int size;

        public EvictableKey(ShareSessionKey key, int size) {
            this.key = key;
            this.size = size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            EvictableKey other = (EvictableKey) obj;
            return size == other.size && Objects.equals(key, other.key);
        }

        //TODO: Complete this compareTo
        @Override
        public int compareTo(EvictableKey other) {
            return 0;
        }
    }

    /*
     * Caches share sessions.
     *
     * See tryEvict for an explanation of the cache eviction strategy.
     *
     * The ShareSessionCache is thread-safe because all of its methods are synchronized.
     * Note that individual share sessions have their own locks which are separate from the
     * ShareSessionCache lock.  In order to avoid deadlock, the ShareSessionCache lock
     * must never be acquired while an individual ShareSession lock is already held.
     */
    // TODO: Implement ShareSessionCache class
    public static class ShareSessionCache {
        private final int maxEntries;
        private final long evictionMs;
        private long numPartitions = 0;

        // A map of session key to ShareSession.
        private Map<ShareSessionKey, ShareSession> sessions = new HashMap<>();

        // Maps last used times to sessions.
        private Map<LastUsedKey, ShareSession> lastUsed = new TreeMap<>();

        // A map containing sessions which can be evicted by sessions on basis of size
        private Map<EvictableKey, ShareSession> evictable = new TreeMap<>();

        public ShareSessionCache(int maxEntries, long evictionMs) {
            this.maxEntries = maxEntries;
            this.evictionMs = evictionMs;
        }

        /**
         * Get a session by session key.
         *
         * @param key The share session key.
         * @return The session, or None if no such session was found.
         */
        public ShareSession get(ShareSessionKey key) {
            synchronized (this) {
                return sessions.getOrDefault(key, null);
            }
        }

        /**
         * Get the number of entries currently in the share session cache.
         */
        public int size() {
            synchronized (this) {
                return sessions.size();
            }
        }

        /**
         * Get the total number of cached partitions.
         */
        public long totalPartitions() {
            synchronized (this) {
                return numPartitions;
            }
        }

        public ShareSession remove(ShareSessionKey key) {
            synchronized (this) {
                ShareSession session = get(key);
                if (session != null)
                    return remove(session);
                return null;
            }
        }

        /**
         * Remove an entry from the session cache.
         *
         * @param session The session.
         * @return The removed session, or None if there was no such session.
         */
        public ShareSession remove(ShareSession session) {
            synchronized (this) {
                EvictableKey evictableKey;
                synchronized (session) {
                    lastUsed.remove(session.lastUsedKey());
                    evictableKey = session.evictableKey();
                }
                evictable.remove(evictableKey);
                ShareSession removeResult = sessions.remove(session.key);
                if (removeResult != null) {
                    numPartitions = numPartitions - session.cachedSize;
                }
                return removeResult;
            }
        }
    }

    /*
     * A cached partition.
     *
     * The broker maintains a set of these objects for each share fetch session.
     * When a share fetch request is made, any partitions which are not explicitly
     * enumerated in the fetch request are loaded from the cache.  Similarly, when an
     * share fetch response is being prepared, any partitions that have not changed and
     * do not have errors are left out of the response.
     *
     * We store many of these objects, so it is important for them to be memory-efficient.
     * That is why we store topic and partition separately rather than storing a TopicPartition
     * object. The TP object takes up more memory because it is a separate JVM object, and
     * because it stores the cached hash code in memory.
     *
     */
    public static class CachedPartition implements ImplicitLinkedHashCollection.Element {
        private String topic;
        private final Uuid topicId;
        private int partition, maxBytes;
        private Optional<Integer> leaderEpoch;

        private int cachedNext = ImplicitLinkedHashCollection.INVALID_INDEX;
        private int cachedPrev = ImplicitLinkedHashCollection.INVALID_INDEX;

        private CachedPartition(String topic, Uuid topicId, int partition, int maxBytes, Optional<Integer> leaderEpoch) {
            this.topic = topic;
            this.topicId = topicId;
            this.partition = partition;
            this.maxBytes = maxBytes;
            this.leaderEpoch = leaderEpoch;
        }

        private CachedPartition(String topic, Uuid topicId, int partition) {
            this(topic, topicId, partition, -1, Optional.empty());
        }

        public CachedPartition(TopicIdPartition topicIdPartition) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition());
        }

        public CachedPartition(TopicIdPartition topicIdPartition, ShareFetchRequest.SharePartitionData reqData) {
            this(topicIdPartition.topic(), topicIdPartition.topicId(), topicIdPartition.partition(), reqData.maxBytes,
                    reqData.currentLeaderEpoch);
        }

        public ShareFetchRequest.SharePartitionData reqData() {
            return new ShareFetchRequest.SharePartitionData(topicId, maxBytes, leaderEpoch);
        }

        public void updateRequestParams(ShareFetchRequest.SharePartitionData reqData) {
            // Update our cached request parameters.
            maxBytes = reqData.maxBytes;
            leaderEpoch = reqData.currentLeaderEpoch;
        }

        public void maybeResolveUnknownName(Map<Uuid, String> topicNames) {
            if (this.topic == null)
                this.topic = topicNames.get(this.topicId);
        }


        @Override
        public int prev() {
            return cachedPrev;
        }

        @Override
        public void setPrev(int prev) {
            cachedPrev = prev;
        }

        @Override
        public int next() {
            return cachedNext;
        }

        @Override
        public void setNext(int next) {
            cachedNext = next;
        }
    }
}

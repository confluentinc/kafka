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

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.FetchParams;
import org.apache.kafka.storage.internals.log.FetchPartitionData;

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

import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import org.slf4j.LoggerFactory;

public class SharePartitionManager {

    // TODO: May be use ImplicitLinkedHashCollection.
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;
    private final ReplicaManager replicaManager;
    private final Time time;
    private final ShareFetchSessionCache cache;

    public SharePartitionManager(ReplicaManager replicaManager, Time time, ShareFetchSessionCache cache) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        partitionCacheMap = new ConcurrentHashMap<>();
    }

    // TODO: Move some part in share session context and change method signature to accept share
    //  partition data along TopicIdPartition.
    public CompletableFuture<Map<TopicIdPartition, PartitionData>> fetchMessages(FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions, String groupId) {
        CompletableFuture<Map<TopicIdPartition, PartitionData>> future = new CompletableFuture<>();

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
                    Map<TopicIdPartition, PartitionData> result = new HashMap<>();
                    responseData.forEach(data -> {
                        TopicIdPartition topicIdPartition = data._1;
                        FetchPartitionData fetchPartitionData = data._2;

                        PartitionData partitionData = new PartitionData()
                            .setPartitionIndex(topicIdPartition.partition())
                            .setRecords(fetchPartitionData.records)
                            .setErrorCode(fetchPartitionData.error.code())
//                        .setAcquiredRecords(fetchPartitionData.records)
                            .setAcknowledgeErrorCode(Errors.NONE.code());

                        SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
                        sharePartition.update(fetchPartitionData);

                        result.put(topicIdPartition, partitionData);
                        future.complete(result);
                    });
                    return BoxedUnit.UNIT;
                });
        }
        return future;
    }

    public CompletableFuture<ShareAcknowledgeResponseData> acknowledge(ShareSession session, PartitionInfo partitionInfo) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    // TODO: Implement session. Initial implementation shall create new session on each call which can
    //  be used in further API calls.
    public ShareSession session() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public ShareFetchContext newContext(Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> forgottenTopics,
                                        Map<Uuid, String> topicNames, FetchMetadata reqMetadata) {
        ShareFetchContext context;
        if (reqMetadata.isFull()) {
            String removedFetchSessionStr = "";
            if (reqMetadata.sessionId() != FetchMetadata.INVALID_SESSION_ID) {
                // Any session specified in a FULL share fetch request will be closed.
                // TODO: Implement ShareFetchSessionCache and do a remove of share fetch session
                throw new UnsupportedOperationException("Not implemented yet");
            }
            String suffix = "";
            if (reqMetadata.epoch() == FetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                suffix = " Will not try to create a new session.";
                context = new SessionlessShareFetchContext(shareFetchData);
            } else {
                context = new FullShareFetchContext(time, cache, reqMetadata, shareFetchData);
            }
        } else {
            // TODO: Implement getting ShareFetchContext using ShareFetchSession cache
            throw new UnsupportedOperationException("Not implemented yet");
        }
        return context;
    }

    // TODO: Define share session class.
    public static class ShareSession {

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
        private ShareFetchSessionCache cache;
        private FetchMetadata reqMetadata;
        private Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData;

        /**
         * @param time               The clock to use.
         * @param cache              The share fetch session cache.
         * @param reqMetadata        The request metadata.
         * @param shareFetchData     The share partition data from the share fetch request.
         */
        public FullShareFetchContext(Time time, ShareFetchSessionCache cache, FetchMetadata reqMetadata,
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
            this.groupId = groupId;
            this.topicIdPartition = topicIdPartition;
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

    /*
     * Caches share fetch sessions.
     *
     * See tryEvict for an explanation of the cache eviction strategy.
     *
     * The ShareFetchSessionCache is thread-safe because all of its methods are synchronized.
     * Note that individual share fetch sessions have their own locks which are separate from the
     * ShareFetchSessionCache lock.  In order to avoid deadlock, the ShareFetchSessionCache lock
     * must never be acquired while an individual ShareFetchSession lock is already held.
     */
    // TODO: Implement ShareFetchSessionCache class
    public static class ShareFetchSessionCache {
        private final int maxEntries;
        private final long evictionMs;

        public ShareFetchSessionCache(int maxEntries, long evictionMs) {
            this.maxEntries = maxEntries;
            this.evictionMs = evictionMs;
        }
    }
}

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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ShareSessionNotFoundException;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ShareFetchMetadata;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.utils.ImplicitLinkedHashCollection;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.ShareSession;
import org.apache.kafka.server.share.ShareSession.ModifiedTopicIdPartitionType;
import org.apache.kafka.server.share.ShareSessionCache;
import org.apache.kafka.server.share.ShareSessionKey;
import org.apache.kafka.server.util.timer.SystemTimer;
import org.apache.kafka.server.util.timer.SystemTimerReaper;
import org.apache.kafka.server.util.timer.Timer;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.BoxedUnit;

import org.slf4j.LoggerFactory;

public class SharePartitionManager implements AutoCloseable {

    private final static Logger log = LoggerFactory.getLogger(SharePartitionManager.class);

    // TODO: May be use ImplicitLinkedHashCollection.
    private final Map<SharePartitionKey, SharePartition> partitionCacheMap;
    private final ReplicaManager replicaManager;
    private final Time time;
    private final ShareSessionCache cache;
    private final ConcurrentLinkedQueue<ShareFetchPartitionData> fetchQueue;
    private final AtomicBoolean processFetchQueueLock;
    private final int recordLockDurationMs;
    private final Timer timer;
    private final int maxInFlightMessages;
    private final int maxDeliveryCount;
    private final Persister persister;

    public SharePartitionManager(
            ReplicaManager replicaManager,
            Time time,
            ShareSessionCache cache,
            int recordLockDurationMs,
            int maxDeliveryCount,
            int maxInFlightMessages,
            Persister persister
    ) {
        this(replicaManager, time, cache, new ConcurrentHashMap<>(), recordLockDurationMs, maxDeliveryCount, maxInFlightMessages, persister);
    }

    // Visible for testing
    SharePartitionManager(ReplicaManager replicaManager, Time time, ShareSessionCache cache, Map<SharePartitionKey, SharePartition> partitionCacheMap,
                          int recordLockDurationMs, int maxDeliveryCount, int maxInFlightMessages, Persister persister) {
        this.replicaManager = replicaManager;
        this.time = time;
        this.cache = cache;
        this.partitionCacheMap = partitionCacheMap;
        this.fetchQueue = new ConcurrentLinkedQueue<>();
        this.processFetchQueueLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = new SystemTimerReaper("share-group-lock-timeout-reaper",
                new SystemTimer("share-group-lock-timeout"));
        this.maxDeliveryCount = maxDeliveryCount;
        this.maxInFlightMessages = maxInFlightMessages;
        this.persister = persister;
    }

    // TODO: Move some part in share session context and change method signature to accept share
    //  partition data along TopicIdPartition.
    public CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> fetchMessages(
        String groupId,
        String memberId,
        FetchParams fetchParams,
        List<TopicIdPartition> topicIdPartitions,
        Map<TopicIdPartition, Integer> partitionMaxBytes) {
        log.trace("Fetch request for topicIdPartitions: {} with groupId: {} fetch params: {}",
                topicIdPartitions, groupId, fetchParams);

        CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future = new CompletableFuture<>();
        ShareFetchPartitionData shareFetchPartitionData = new ShareFetchPartitionData(fetchParams, groupId, memberId,
                topicIdPartitions, future, partitionMaxBytes);
        fetchQueue.add(shareFetchPartitionData);
        maybeProcessFetchQueue();

        return future;
    }

    /**
     * Recursive function to process all the fetch requests present inside the fetch queue
     */
    private void maybeProcessFetchQueue() {
        if (!processFetchQueueLock.compareAndSet(false, true)) {
            // The queue is already being processed hence avoid re-triggering.
            return;
        }

        // Initialize the topic partitions for which the fetch should be attempted.
        Map<TopicIdPartition, FetchRequest.PartitionData> topicPartitionData = new LinkedHashMap<>();
        ShareFetchPartitionData shareFetchPartitionData = fetchQueue.poll();
        try {
            assert shareFetchPartitionData != null;
            shareFetchPartitionData.topicIdPartitions.forEach(topicIdPartition -> {
                SharePartitionKey sharePartitionKey = sharePartitionKey(
                        shareFetchPartitionData.groupId,
                        topicIdPartition
                );
                SharePartition sharePartition = partitionCacheMap.computeIfAbsent(sharePartitionKey,
                    k -> new SharePartition(shareFetchPartitionData.groupId, topicIdPartition, maxInFlightMessages, maxDeliveryCount,
                            recordLockDurationMs, timer, time, persister, replicaManager));
                int partitionMaxBytes = shareFetchPartitionData.partitionMaxBytes.getOrDefault(topicIdPartition, 0);
                // Add the share partition to the list of partitions to be fetched only if we can
                // acquire the fetch lock on it.
                if (sharePartition.maybeAcquireFetchLock()) {
                    // Fetching over a topic should be able to proceed if any one of the following 2 conditions are met:
                    // 1. The fetch is to happen somewhere in between the record states cached in the share partition.
                    //    This is because in this case we don't need to check for the partition limit for in flight messages
                    // 2. If condition 1 is not true, then that means we will be fetching new records which haven't been cached before.
                    //    In this case it is necessary to check if the partition limit for in flight messages has been reached.
                    if (sharePartition.nextFetchOffset() != (sharePartition.endOffset() + 1) || sharePartition.canAcquireMore()) {
                        topicPartitionData.put(
                            topicIdPartition,
                            new FetchRequest.PartitionData(
                                    topicIdPartition.topicId(),
                                    sharePartition.nextFetchOffset(),
                                    0,
                                    partitionMaxBytes,
                                    Optional.empty()
                            )
                        );
                    } else {
                        sharePartition.releaseFetchLock();
                        log.info("Record lock partition limit exceeded for SharePartition with key {}, " +
                                "cannot acquire more records", sharePartitionKey);
                    }
                }
            });

            if (topicPartitionData.isEmpty()) {
                // No locks for share partitions could be acquired, so we complete the request and
                // will re-fetch for the client in next poll.
                shareFetchPartitionData.future.complete(Collections.emptyMap());
                // Though if no partitions can be locked then there must be some other request which
                // is in-flight and should release the lock. But it's safe to release the lock as
                // the lock on share partition already exists which facilitates correct behaviour
                // with multiple requests from queue being processed.
                releaseProcessFetchQueueLock();
                return;
            }

            log.trace("Fetchable share partitions data: {} with groupId: {} fetch params: {}",
                topicPartitionData, shareFetchPartitionData.groupId, shareFetchPartitionData.fetchParams);

            replicaManager.fetchMessages(
                shareFetchPartitionData.fetchParams,
                CollectionConverters.asScala(
                    topicPartitionData.entrySet().stream().map(entry ->
                        new Tuple2<>(entry.getKey(), entry.getValue())).collect(Collectors.toList())
                ),
                QuotaFactory.UnboundedQuota$.MODULE$,
                responsePartitionData -> {
                    log.trace("Data successfully retrieved by replica manager: {}", responsePartitionData);
                    List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData = CollectionConverters.asJava(
                        responsePartitionData);
                    processFetchResponse(shareFetchPartitionData, responseData).whenComplete(
                        (result, throwable) -> {
                            if (throwable != null) {
                                log.error("Error processing fetch response for share partitions", throwable);
                                shareFetchPartitionData.future.completeExceptionally(throwable);
                            } else {
                                shareFetchPartitionData.future.complete(result);
                            }
                            // Releasing the lock to move ahead with the next request in queue.
                            releaseFetchQueueAndPartitionsLock(shareFetchPartitionData.groupId, topicPartitionData.keySet());
                            if (!fetchQueue.isEmpty())
                                maybeProcessFetchQueue();
                        });
                    return BoxedUnit.UNIT;
                });
        } catch (Exception e) {
            // In case exception occurs then release the locks so queue can be further processed.
            log.error("Error processing fetch queue for share partitions", e);
            releaseFetchQueueAndPartitionsLock(shareFetchPartitionData.groupId, topicPartitionData.keySet());
        }
    }

    // Visible for testing.
    CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> processFetchResponse(
        ShareFetchPartitionData shareFetchPartitionData,
        List<Tuple2<TopicIdPartition, FetchPartitionData>> responseData
    ) {
        Map<TopicIdPartition, CompletableFuture<PartitionData>> futures = new HashMap<>();
        responseData.forEach(data -> {
            TopicIdPartition topicIdPartition = data._1;
            FetchPartitionData fetchPartitionData = data._2;

            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(shareFetchPartitionData.groupId, topicIdPartition));
            futures.put(topicIdPartition, sharePartition.acquire(shareFetchPartitionData.memberId, fetchPartitionData)
                .handle((acquiredRecords, throwable) -> {
                    log.trace("Acquired records for topicIdPartition: {} with share fetch data: {}, records: {}",
                        topicIdPartition, shareFetchPartitionData, acquiredRecords);
                    ShareFetchResponseData.PartitionData partitionData = new ShareFetchResponseData.PartitionData()
                        .setPartitionIndex(topicIdPartition.partition());

                    if (throwable != null) {
                        partitionData.setErrorCode(Errors.forException(throwable).code());
                    } else if (fetchPartitionData.error.code() == Errors.OFFSET_OUT_OF_RANGE.code()) {
                        // In case we get OFFSET_OUT_OF_RANGE error, that's because the LSO is later than the fetch offset.
                        // So, we would update the start and end offset of the share partition and still return an empty
                        // response and let the client retry the fetch. This way we do not lose out on the data that
                        // would be returned for other share partitions in the fetch request.
                        sharePartition.updateOffsetsOnLsoMovement();
                        partitionData
                                .setPartitionIndex(topicIdPartition.partition())
                                .setRecords(null)
                                .setErrorCode(Errors.NONE.code())
                                .setAcquiredRecords(Collections.emptyList())
                                .setAcknowledgeErrorCode(Errors.NONE.code());
                    } else {
                        // Maybe check if no records are acquired and we want to retry replica
                        // manager fetch. Depends on the share partition manager implementation,
                        // if we want parallel requests for the same share partition or not.
                        partitionData
                            .setPartitionIndex(topicIdPartition.partition())
                            .setRecords(fetchPartitionData.records)
                            .setErrorCode(fetchPartitionData.error.code())
                            .setAcquiredRecords(acquiredRecords)
                            .setAcknowledgeErrorCode(Errors.NONE.code());
                    }
                    return partitionData;
                }));
        });

        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).thenApply(v -> {
            Map<TopicIdPartition, ShareFetchResponseData.PartitionData> processedResult = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> processedResult.put(topicIdPartition, future.join()));
            return processedResult;
        });
    }

    // Visible for testing.
    void releaseFetchQueueAndPartitionsLock(String groupId, Set<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.forEach(tp -> partitionCacheMap.get(sharePartitionKey(groupId, tp)).releaseFetchLock());
        releaseProcessFetchQueueLock();
    }

    private void releaseProcessFetchQueueLock() {
        processFetchQueueLock.set(false);
    }

    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> acknowledge(
            String memberId,
            String groupId,
            Map<TopicIdPartition, List<ShareAcknowledgementBatch>> acknowledgeTopics
    ) {
        log.debug("Acknowledge request for topicIdPartitions: {} with groupId: {}",
                acknowledgeTopics.keySet(), groupId);
        Map<TopicIdPartition, CompletableFuture<Errors>> futures = new HashMap<>();
        acknowledgeTopics.forEach((topicIdPartition, acknowledgePartitionBatches) -> {
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
            if (sharePartition != null) {
                CompletableFuture<Errors> future = sharePartition.acknowledge(memberId, acknowledgePartitionBatches).thenApply(throwable -> {
                    if (throwable.isPresent()) {
                        return Errors.forException(throwable.get());
                    } else {
                        return Errors.NONE;
                    }

                });
                futures.put(topicIdPartition, future);
            } else {
                futures.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION));
            }
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.values().toArray(new CompletableFuture[0]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> {
                result.put(topicIdPartition, new ShareAcknowledgeResponseData.PartitionData()
                                .setPartitionIndex(topicIdPartition.partition())
                                .setErrorCode(future.join().code()));
            });
            return result;
        });
    }

    private SharePartitionKey sharePartitionKey(String groupId, TopicIdPartition topicIdPartition) {
        return new SharePartitionKey(groupId, topicIdPartition);
    }

    private ShareSessionKey shareSessionKey(String groupId, Uuid memberId) {
        return new ShareSessionKey(groupId, memberId);
    }

    public void acknowledgeShareSessionCacheUpdate(String groupId, Uuid memberId, int reqEpoch) {
        // If the request is a Final Fetch Request, the share session would have already been deleted from cache, i.e. there is no need to update the cache
        if (reqEpoch == ShareFetchMetadata.FINAL_EPOCH) {
            return;
        }
        ShareSession shareSession = cache.get(new ShareSessionKey(groupId, memberId));
        // Update the session's position in the cache for both piggybacked (to guard against the entry disappearing
        // from the cache between the ack and the fetch) and standalone acknowledgments.
        cache.touch(shareSession, time.milliseconds());
        // If acknowledgement is piggybacked on fetch, newContext function takes care of updating the share session epoch
        // and share session cache. However, if the acknowledgement is standalone, the updates are handled in the if block
    }

    public List<TopicIdPartition> cachedTopicIdPartitionsInShareSession(String groupId, Uuid memberId) {
        ShareSessionKey key = shareSessionKey(groupId, memberId);
        ShareSession shareSession = cache.get(key);
        if (shareSession == null) {
            throw new ShareSessionNotFoundException("Share session not found in cache");
        }
        List<TopicIdPartition> cachedTopicIdPartitions = new ArrayList<>();
        shareSession.partitionMap().forEach(cachedSharePartition -> cachedTopicIdPartitions.add(
                new TopicIdPartition(cachedSharePartition.topicId(), new TopicPartition(cachedSharePartition.topic(), cachedSharePartition.partition()
        ))));
        return cachedTopicIdPartitions;
    }

    public CompletableFuture<Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData>> releaseAcquiredRecords(
            String groupId, String memberId, List<TopicIdPartition> topicIdPartitions) {
        Map<TopicIdPartition, CompletableFuture<Errors>> futures = new HashMap<>();
        topicIdPartitions.forEach(topicIdPartition -> {
            SharePartition sharePartition = partitionCacheMap.get(sharePartitionKey(groupId, topicIdPartition));
            if (sharePartition == null) {
                log.debug("No share partition found for groupId {} topicPartition {} while releasing acquired topic partitions", groupId, topicIdPartition);
                futures.put(topicIdPartition, CompletableFuture.completedFuture(Errors.UNKNOWN_TOPIC_OR_PARTITION));
            } else {
                CompletableFuture<Errors> future = sharePartition.releaseAcquiredRecords(memberId).thenApply(throwable -> {
                    if (throwable.isPresent()) {
                        return Errors.forException(throwable.get());
                    } else {
                        return Errors.NONE;
                    }
                });
                futures.put(topicIdPartition, future);
            }
        });

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.values().toArray(new CompletableFuture[futures.size()]));
        return allFutures.thenApply(v -> {
            Map<TopicIdPartition, ShareAcknowledgeResponseData.PartitionData> result = new HashMap<>();
            futures.forEach((topicIdPartition, future) -> result.put(topicIdPartition, new ShareAcknowledgeResponseData.PartitionData()
                    .setPartitionIndex(topicIdPartition.partition())
                    .setErrorCode(future.join().code())));
            return result;
        });
    }

    public ShareFetchContext newContext(String groupId, Map<TopicIdPartition,
            ShareFetchRequest.SharePartitionData> shareFetchData, List<TopicIdPartition> toForget, ShareFetchMetadata reqMetadata) {
        ShareFetchContext context;
        // TopicPartition with maxBytes as 0 should not be added in the cachedPartitions
        Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchDataWithMaxBytes = new HashMap<>();
        shareFetchData.forEach((tp, sharePartitionData) -> {
            if (sharePartitionData.maxBytes > 0) shareFetchDataWithMaxBytes.put(tp, sharePartitionData);
        });
        if (reqMetadata.isFull()) {
            // TODO: We will handle the case of INVALID_MEMBER_ID once we have a clear definition for it
         //  if (!Objects.equals(reqMetadata.memberId(), ShareFetchMetadata.INVALID_MEMBER_ID)) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            String removedFetchSessionStr = "";
            if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
                // If the epoch is FINAL_EPOCH, don't try to create a new session.
                if (!shareFetchDataWithMaxBytes.isEmpty()) {
                    throw Errors.INVALID_REQUEST.exception();
                }
                context = new FinalContext();
                if (cache.remove(key) != null) {
                    removedFetchSessionStr = "Removed share session with key " + key;
                    log.debug(removedFetchSessionStr);
                }
            } else {
                // Initialize a new share session.
                if (cache.remove(key) != null) {
                    removedFetchSessionStr = "Removed share session with key " + key;
                    log.debug(removedFetchSessionStr);
                }
                ImplicitLinkedHashCollection<CachedSharePartition> cachedSharePartitions = new
                        ImplicitLinkedHashCollection<>(shareFetchDataWithMaxBytes.size());
                shareFetchDataWithMaxBytes.forEach((topicIdPartition, reqData) -> {
                    cachedSharePartitions.mustAdd(new CachedSharePartition(topicIdPartition, reqData, false));
                });
                ShareSessionKey responseShareSessionKey = cache.maybeCreateSession(groupId, reqMetadata.memberId(),
                        time.milliseconds(), shareFetchDataWithMaxBytes.size(), cachedSharePartitions);
                log.debug("Share session context with key {} isSubsequent {} returning {}", responseShareSessionKey,
                        false, partitionsToLogString(shareFetchDataWithMaxBytes.keySet()));

                context = new ShareSessionContext(time, cache, reqMetadata, shareFetchDataWithMaxBytes);
                log.debug("Created a new ShareSessionContext with {}. A new share session will be started.",
                        partitionsToLogString(shareFetchDataWithMaxBytes.keySet()));
            }
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                } else {
                    if (shareSession.epoch != reqMetadata.epoch()) {
                        log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                                shareSession.epoch, reqMetadata.epoch());
                        throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                    } else {
                        Map<ModifiedTopicIdPartitionType, List<TopicIdPartition>> modifiedTopicIdPartitions = shareSession.update(
                                shareFetchDataWithMaxBytes, toForget);
                        cache.touch(shareSession, time.milliseconds());
                        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
                        log.debug("Created a new ShareSessionContext for session key {}, epoch {}: " +
                                        "added {}, updated {}, removed {}", shareSession.key(), shareSession.epoch,
                                partitionsToLogString(modifiedTopicIdPartitions.get(
                                    ModifiedTopicIdPartitionType.ADDED)),
                                partitionsToLogString(modifiedTopicIdPartitions.get(ModifiedTopicIdPartitionType.UPDATED)),
                                partitionsToLogString(modifiedTopicIdPartitions.get(ModifiedTopicIdPartitionType.REMOVED))
                        );
                        context = new ShareSessionContext(time, reqMetadata, shareSession);
                    }
                }
            }
        }
        return context;
    }

    public void acknowledgeSessionUpdate(String groupId, ShareFetchMetadata reqMetadata) {
        if (reqMetadata.epoch() == ShareFetchMetadata.INITIAL_EPOCH) {
            // ShareAcknowledge Request cannot have epoch as INITIAL_EPOCH (0)
            throw Errors.INVALID_SHARE_SESSION_EPOCH.exception();
        } else if (reqMetadata.epoch() == ShareFetchMetadata.FINAL_EPOCH) {
            ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
            if (cache.remove(key) != null) {
                log.debug("Removed share session with key " + key);
            }
        } else {
            synchronized (cache) {
                ShareSessionKey key = shareSessionKey(groupId, reqMetadata.memberId());
                ShareSession shareSession = cache.get(key);
                if (shareSession == null) {
                    log.debug("Share session error for {}: no such share session found", key);
                    throw Errors.SHARE_SESSION_NOT_FOUND.exception();
                } else {
                    if (shareSession.epoch != reqMetadata.epoch()) {
                        log.debug("Share session error for {}: expected epoch {}, but got {} instead", key,
                                shareSession.epoch, reqMetadata.epoch());
                        throw  Errors.INVALID_SHARE_SESSION_EPOCH.exception();
                    } else {
                        cache.touch(shareSession, time.milliseconds());
                        shareSession.epoch = ShareFetchMetadata.nextEpoch(shareSession.epoch);
                    }
                }
            }
        }
    }

    String partitionsToLogString(Collection<TopicIdPartition> partitions) {
        return FetchSession.partitionsToLogString(partitions, log.isTraceEnabled());
    }

    // Visible for testing.
    Timer timer() {
        return timer;
    }

    @Override
    public void close() throws Exception {
        timer.close();
        this.persister.stop();
    }

    // Helper class to return the erroneous partitions and valid partition data
    public static class ErroneousAndValidPartitionData {
        private final List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous;
        private final List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions;

        public ErroneousAndValidPartitionData(List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous,
                                              List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions) {
            this.erroneous = erroneous;
            this.validTopicIdPartitions = validTopicIdPartitions;
        }

        public ErroneousAndValidPartitionData(Map<TopicIdPartition, ShareFetchRequest.SharePartitionData> shareFetchData) {
            erroneous = new ArrayList<>();
            validTopicIdPartitions = new ArrayList<>();
            shareFetchData.forEach((topicIdPartition, sharePartitionData) -> {
                if (topicIdPartition.topic() == null) {
                    erroneous.add(new Tuple2<>(topicIdPartition, ShareFetchResponse.partitionResponse(topicIdPartition, Errors.UNKNOWN_TOPIC_ID)));
                } else {
                    validTopicIdPartitions.add(new Tuple2<>(topicIdPartition, sharePartitionData));
                }
            });
        }

        public ErroneousAndValidPartitionData() {
            this.erroneous = new ArrayList<>();
            this.validTopicIdPartitions = new ArrayList<>();
        }

        public List<Tuple2<TopicIdPartition, ShareFetchResponseData.PartitionData>> erroneous() {
            return erroneous;
        }

        public List<Tuple2<TopicIdPartition, ShareFetchRequest.SharePartitionData>> validTopicIdPartitions() {
            return validTopicIdPartitions;
        }
    }

    /**
     * The share fetch context for a final share fetch request.
     */
    public static class FinalContext extends ShareFetchContext {

        public FinalContext() {
            this.log = LoggerFactory.getLogger(FinalContext.class);
        }

        @Override
        int responseSize(LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates, short version) {
            return ShareFetchResponse.sizeOf(version, updates.entrySet().iterator());
        }

        @Override
        ShareFetchResponse updateAndGenerateResponseData(String groupId, Uuid memberId,
                                                         LinkedHashMap<TopicIdPartition, ShareFetchResponseData.PartitionData> updates) {
            log.debug("Final context returning" + partitionsToLogString(updates.keySet()));
            return new ShareFetchResponse(ShareFetchResponse.toMessage(Errors.NONE, 0,
                    updates.entrySet().iterator(), Collections.emptyList()));
        }

        @Override
        ErroneousAndValidPartitionData getErroneousAndValidTopicIdPartitions() {
            return new ErroneousAndValidPartitionData();
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

    // Visible for testing.
    static class ShareFetchPartitionData {
        private final FetchParams fetchParams;
        private final String groupId;
        private final String memberId;
        private final List<TopicIdPartition> topicIdPartitions;
        private final CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future;
        private final Map<TopicIdPartition, Integer> partitionMaxBytes;

        public ShareFetchPartitionData(FetchParams fetchParams, String groupId, String memberId,
                                       List<TopicIdPartition> topicIdPartitions,
                                       CompletableFuture<Map<TopicIdPartition, ShareFetchResponseData.PartitionData>> future,
                                       Map<TopicIdPartition, Integer> partitionMaxBytes) {
            this.fetchParams = fetchParams;
            this.groupId = groupId;
            this.memberId = memberId;
            this.topicIdPartitions = topicIdPartitions;
            this.future = future;
            this.partitionMaxBytes = partitionMaxBytes;
        }
    }
}

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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.group.share.GroupTopicPartitionData;
import org.apache.kafka.server.group.share.PartitionAllData;
import org.apache.kafka.server.group.share.PartitionErrorData;
import org.apache.kafka.server.group.share.PartitionFactory;
import org.apache.kafka.server.group.share.PartitionIdData;
import org.apache.kafka.server.group.share.PartitionStateBatchData;
import org.apache.kafka.server.group.share.Persister;
import org.apache.kafka.server.group.share.PersisterStateBatch;
import org.apache.kafka.server.group.share.ReadShareGroupStateParameters;
import org.apache.kafka.server.group.share.ReadShareGroupStateResult;
import org.apache.kafka.server.group.share.TopicData;
import org.apache.kafka.server.group.share.WriteShareGroupStateParameters;
import org.apache.kafka.server.group.share.WriteShareGroupStateResult;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;
import org.apache.kafka.storage.internals.log.FetchPartitionData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The SharePartition is used to track the state of a partition that is shared between multiple
 * consumers. The class maintains the state of the records that have been fetched from the leader
 * and are in-flight.
 */
public class SharePartition {

    private final static Logger log = LoggerFactory.getLogger(SharePartition.class);

    /**
     * The RecordState is used to track the state of a record that has been fetched from the leader.
     * The state of the records determines if the records should be re-delivered, move the next fetch
     * offset, or be state persisted to disk.
     */
    public enum RecordState {
        AVAILABLE((byte) 0),
        ACQUIRED((byte) 1),
        ACKNOWLEDGED((byte) 2),
        ARCHIVED((byte) 4);

        public final byte id;
        RecordState(byte id) {
            this.id = id;
        }

        /**
         * Validates that the <code>newState</code> is one of the valid transition from the current
         * {@code RecordState}.
         *
         * @param newState State into which requesting to transition; must be non-<code>null</code>
         * @return {@code RecordState} <code>newState</code> if validation succeeds. Returning
         * <code>newState</code> helps state assignment chaining.
         * @throws IllegalStateException if the state transition validation fails.
         */

        public RecordState validateTransition(RecordState newState) {
            Objects.requireNonNull(newState, "newState cannot be null");
            if (this == newState) {
                throw new IllegalStateException("The state transition is invalid as the new state is"
                    + "the same as the current state");
            }

            if (this == ACKNOWLEDGED || this == ARCHIVED) {
                throw new IllegalStateException("The state transition is invalid from the current state: " + this);
            }

            if (this == AVAILABLE && newState != ACQUIRED) {
                throw new IllegalStateException("The state can only be transitioned to ACQUIRED from AVAILABLE");
            }

            // Either the transition is from Available -> Acquired or from Acquired -> Available/
            // Acknowledged/Archived.
            return newState;
        }

        public static RecordState forId(byte id) {
            switch (id) {
                case 0:
                    return AVAILABLE;
                case 1:
                    return ACQUIRED;
                case 2:
                    return ACKNOWLEDGED;
                case 4:
                    return ARCHIVED;
                default:
                    throw new IllegalArgumentException("Unknown record state id: " + id);
            }
        }
    }

    /**
     * The group id of the share partition belongs to.
     */
    private final String groupId;

    /**
     * The topic id partition of the share partition.
     */
    private final TopicIdPartition topicIdPartition;

    /**
     * The in-flight record is used to track the state of a record that has been fetched from the
     * leader. The state of the record is used to determine if the record should be re-fetched or if it
     * can be acknowledged or archived. Once share partition start offset is moved then the in-flight
     * records prior to the start offset are removed from the cache. The cache holds data against the
     * first offset of the in-flight batch.
     */
    private final NavigableMap<Long, InFlightBatch> cachedState;

    /**
     * The lock is used to synchronize access to the in-flight records. The lock is used to ensure that
     * the in-flight records are accessed in a thread-safe manner.
     */
    private final ReadWriteLock lock;

    /**
     * The find next fetch offset is used to indicate if the next fetch offset should be recomputed.
     */
    private final AtomicBoolean findNextFetchOffset;

    /**
     * The lock to ensure that the same share partition does not enter a fetch queue
     * while another one is being fetched within the queue.
     */
    private final AtomicBoolean fetchLock;

    /**
     * The max in-flight messages is used to limit the number of records that can be in-flight at any
     * given time. The max in-flight messages is used to prevent the consumer from fetching too many
     * records from the leader and running out of memory.
     */
    private final int maxInFlightMessages;

    /**
     * The max delivery count is used to limit the number of times a record can be delivered to the
     * consumer. The max delivery count is used to prevent the consumer re-delivering the same record
     * indefinitely.
     */
    private final int maxDeliveryCount;

    /**
     * The record lock duration is used to limit the duration for which a consumer can acquire a record.
     * Once this time period is elapsed, the record will be made available or archived depending on the delivery count.
     */
    private final int recordLockDurationMs;

    /**
     * Timer is used to implement acquisition lock on records that guarantees the movement of records from
     * acquired to available/archived state upon timeout
     */
    private final Timer timer;

    /**
     * Time is used to get the currentTime.
     */
    private final Time time;

    /**
     * The persister is used to persist the state of the share partition to disk.
     */
    private Persister persister;

    /**
     * The share partition start offset specifies the partition start offset from which the records
     * are cached in the cachedState of the sharePartition.
     */
    private long startOffset;

    /**
     * The share partition end offset specifies the partition end offset from which the records
     * are already fetched.
     */
    private long endOffset;

    /**
     * The next fetch offset specifies the partition offset which should be fetched from the leader
     * for the share partition.
     */
    private long nextFetchOffset;

    /**
     * The state epoch is used to track the version of the state of the share partition.
     */
    private int stateEpoch;

    SharePartition(String groupId, TopicIdPartition topicIdPartition, int maxInFlightMessages, int maxDeliveryCount,
                   int recordLockDurationMs, Timer timer, Time time, Persister persister) {
        this.groupId = groupId;
        this.topicIdPartition = topicIdPartition;
        this.maxInFlightMessages = maxInFlightMessages;
        this.maxDeliveryCount = maxDeliveryCount;
        this.cachedState = new ConcurrentSkipListMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.findNextFetchOffset = new AtomicBoolean(false);
        this.fetchLock = new AtomicBoolean(false);
        this.recordLockDurationMs = recordLockDurationMs;
        this.timer = timer;
        this.time = time;
        this.persister = persister;
        // Initialize the partition.
        initialize();
    }

    long startOffset() {
        long startOffset;
        lock.readLock().lock();
        startOffset = this.startOffset;
        lock.readLock().unlock();
        return startOffset;
    }

    long endOffset() {
        long endOffset;
        lock.readLock().lock();
        endOffset = this.endOffset;
        lock.readLock().unlock();
        return endOffset;
    }

    /**
     * The next fetch offset is used to determine the next offset that should be fetched from the leader.
     * The offset should be the next offset after the last fetched batch but there could be batches/
     * offsets that are either released by acknowledgement API or lock timed out hence the next fetch
     * offset might be different from the last batch next offset. Hence, method checks if the next
     * fetch offset should be recomputed else returns the last computed next fetch offset.
     *
     * @return The next fetch offset that should be fetched from the leader.
     */
    public long nextFetchOffset() {
        if (findNextFetchOffset.compareAndSet(true, false)) {
            lock.writeLock().lock();
            try {
                // Find the next fetch offset.
                long firstOffset = nextFetchOffset;
                // If the re-computation is required then there occurred an overlap with the existing
                // in-flight records, hence find the batch from which the last offsets were acquired.
                Map.Entry<Long, InFlightBatch> floorEntry = cachedState.floorEntry(nextFetchOffset);
                if (floorEntry != null) {
                    firstOffset = floorEntry.getKey();
                }
                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(firstOffset, true, endOffset, true);
                if (subMap.isEmpty()) {
                    // The in-flight batches are removed, might be due to the start offset move. Hence,
                    // the next fetch offset should be the partition end offset.
                    nextFetchOffset = endOffset;
                } else {
                    boolean updated = false;
                    for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                        // Check if the state is maintained per offset or batch. If the offsetState
                        // is not maintained then the batch state is used to determine the offsets state.
                        if (entry.getValue().offsetState() == null) {
                            if (entry.getValue().batchState() == RecordState.AVAILABLE) {
                                nextFetchOffset = entry.getValue().firstOffset();
                                updated = true;
                                break;
                            }
                        } else {
                            // The offset state is maintained hence find the next available offset.
                            for (Map.Entry<Long, InFlightState> offsetState : entry.getValue().offsetState().entrySet()) {
                                if (offsetState.getValue().state == RecordState.AVAILABLE) {
                                    nextFetchOffset = offsetState.getKey();
                                    updated = true;
                                    break;
                                }
                            }
                            // Break from the outer loop if updated.
                            if (updated) {
                                break;
                            }
                        }
                    }
                    // No available records found in the fetched batches.
                    if (!updated) {
                        nextFetchOffset = endOffset + 1;
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        lock.readLock().lock();
        try {
            return nextFetchOffset;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void releaseFetchLock() {
        fetchLock.set(false);
    }

    public boolean maybeAcquireFetchLock() {
        return fetchLock.compareAndSet(false, true);
    }

    /**
     * Acquire the fetched records for the share partition. The acquired records are added to the
     * in-flight records and the next fetch offset is updated to the next offset that should be
     * fetched from the leader.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param fetchPartitionData The fetched records for the share partition.
     *
     * @return A future which is completed when the records are acquired.
     */
    public CompletableFuture<List<AcquiredRecords>> acquire(String memberId, FetchPartitionData fetchPartitionData) {
        log.trace("Received acquire request for share partition: {}-{}", memberId, fetchPartitionData);
        RecordBatch lastBatch = fetchPartitionData.records.lastBatch().orElse(null);
        if (lastBatch == null) {
            // Nothing to acquire.
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        // We require the first batch of records to get the base offset. Stop parsing further
        // batches.
        RecordBatch firstBatch = fetchPartitionData.records.batches().iterator().next();
        lock.writeLock().lock();
        try {
            long baseOffset = firstBatch.baseOffset();
            // Find the floor batch record for the request batch. The request batch could be
            // for a subset of the in-flight batch i.e. cached batch of offset 10-14 and request batch
            // of 12-13. Hence, floor entry is fetched to find the sub-map.
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(baseOffset);
            // We might find a batch with floor entry but not necessarily that batch has an overlap,
            // if the request batch base offset is ahead of last offset from floor entry i.e. cached
            // batch of 10-14 and request batch of 15-18, though floor entry is found but no overlap.
            if (floorOffset != null && floorOffset.getValue().lastOffset >= baseOffset) {
                baseOffset = floorOffset.getKey();
            }
            // Validate if the fetch records are already part of existing batches and if available.
            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(baseOffset, true, lastBatch.lastOffset(), true);
            // No overlap with request offsets in the cache for in-flight records. Acquire the complete
            // batch.
            if (subMap.isEmpty()) {
                log.trace("No cached data exists for the share partition for requested fetch batch: {}-{}",
                    groupId, topicIdPartition);
                return CompletableFuture.completedFuture(Collections.singletonList(
                            acquireNewBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset(),
                            lastBatch.nextOffset())));
            }

            log.trace("Overlap exists with in-flight records. Acquire the records if available for"
                + " the share group: {}-{}", groupId, topicIdPartition);
            List<AcquiredRecords> result = new ArrayList<>();
            // The fetched records are already part of the in-flight records. The records might
            // be available for re-delivery hence try acquiring same. The request batches could
            // be an exact match, subset or span over multiple already fetched batches.
            for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();
                // Compute if the batch is a full match.
                boolean fullMatch = inFlightBatch.firstOffset() >= firstBatch.baseOffset()
                    && inFlightBatch.lastOffset() <= lastBatch.lastOffset();

                if (!fullMatch || inFlightBatch.offsetState != null) {
                    log.trace("Subset or offset tracked batch record found for share partition,"
                            + " batch: {} request offsets - first: {}, last: {} for the share"
                            + " group: {}-{}", inFlightBatch, firstBatch.baseOffset(),
                            lastBatch.lastOffset(), groupId, topicIdPartition);
                    if (inFlightBatch.offsetState == null) {
                        // Though the request is a subset of in-flight batch but the offset
                        // tracking has not been initialized yet which means that we could only
                        // acquire subset of offsets from the in-flight batch but only if the
                        // complete batch is available yet. Hence, do a pre-check to avoid exploding
                        // the in-flight offset tracking unnecessarily.
                        if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                            log.trace("The batch is not available to acquire in share group: {}-{}, skipping: {}"
                                    + " skipping offset tracking for batch as well.", groupId,
                                topicIdPartition, inFlightBatch);
                            continue;
                        }
                        // The request batch is a subset or per offset state is managed hence update
                        // the offsets state in the in-flight batch.
                        inFlightBatch.maybeInitializeOffsetStateUpdate();
                    }
                    acquireSubsetBatchRecords(memberId, firstBatch.baseOffset(), lastBatch.lastOffset(), inFlightBatch, result);
                    continue;
                }

                // The in-flight batch is a full match hence change the state of the complete.
                if (inFlightBatch.batchState() != RecordState.AVAILABLE) {
                    log.trace("The batch is not available to acquire in share group: {}-{}, skipping: {}",
                        groupId, topicIdPartition, inFlightBatch);
                    continue;
                }

                InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.ACQUIRED, true);
                if (updateResult == null) {
                    log.info("Unable to acquire records for the batch: {} in share group: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);
                    continue;
                }
                // Update the member id of the in-flight batch since the member acquiring could be different.
                updateResult.memberId = memberId;
                // Schedule acquisition lock timeout for the batch.
                TimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, inFlightBatch.firstOffset(), inFlightBatch.lastOffset());
                // Set the acquisition lock timeout task for the batch.
                inFlightBatch.updateAcquisitionLockTimeout(acquisitionLockTimeoutTask);

                findNextFetchOffset.set(true);
                result.add(new AcquiredRecords()
                    .setFirstOffset(inFlightBatch.firstOffset())
                    .setLastOffset(inFlightBatch.lastOffset())
                    .setDeliveryCount((short) inFlightBatch.batchDeliveryCount()));
            }

            // Some of the request offsets are not found in the fetched batches. Acquire the
            // missing records as well.
            if (subMap.lastEntry().getValue().lastOffset < lastBatch.lastOffset()) {
                log.trace("There exists another batch which needs to be acquired as well");
                result.add(acquireNewBatchRecords(memberId, subMap.lastEntry().getValue().lastOffset() + 1,
                    lastBatch.lastOffset(), lastBatch.nextOffset()));
            }
            return CompletableFuture.completedFuture(result);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Acknowledge the fetched records for the share partition. The accepted batches are removed
     * from the in-flight records once persisted. The next fetch offset is updated to the next offset
     * that should be fetched from the leader, if required.
     *
     * @param memberId The member id of the client that is fetching the record.
     * @param acknowledgementBatch The acknowledgement batch list for the share partition.
     *
     * @return A future which is completed when the records are acknowledged.
     */
    public CompletableFuture<Optional<Throwable>> acknowledge(String memberId, List<AcknowledgementBatch> acknowledgementBatch) {
        log.trace("Acknowledgement batch request for share partition: {}-{}", groupId, topicIdPartition);

        Throwable throwable = null;
        lock.writeLock().lock();
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();
        try {
            long localNextFetchOffset = nextFetchOffset;
            // Avoided using enhanced for loop as need to check if the last batch have offsets
            // in the range.
            for (int i = 0; i < acknowledgementBatch.size(); i++) {
                AcknowledgementBatch batch = acknowledgementBatch.get(i);

                RecordState recordState;
                try {
                    recordState = fetchRecordState(batch.acknowledgeType);
                } catch (IllegalArgumentException e) {
                    log.debug("Invalid acknowledge type: {} for share partition: {}-{}",
                        batch.acknowledgeType, groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Invalid acknowledge type: " + batch.acknowledgeType);
                    break;
                }

                // Find the floor batch record for the request batch. The request batch could be
                // for a subset of the batch i.e. cached batch of offset 10-14 and request batch
                // of 12-13. Hence, floor entry is fetched to find the sub-map.
                Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(batch.firstOffset);
                if (floorOffset == null) {
                    log.debug("Batch record {} not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                    throwable = new InvalidRecordStateException("Batch record not found. The base offset is not found in the cache.");
                    break;
                }

                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, batch.lastOffset, true);

                // Validate if the request batch has the last offset greater than the last offset of
                // the last fetched cached batch, then there will be offsets in the request than cannot
                // be found in the fetched batches.
                if (i == acknowledgementBatch.size() - 1 && batch.lastOffset > subMap.lastEntry().getValue().lastOffset) {
                    log.debug("Request batch: {} has offsets which are not found for share partition: {}-{}", batch, groupId, topicIdPartition);
                    throwable = new InvalidRequestException("Batch record not found. The last offset in request is past acquired records.");
                    break;
                }

                // Add the gap offsets. There is no need to roll back the gap offsets for any
                // in-flight batch if the acknowledgement request for any batch fails as once
                // identified gap offset should remain the gap offset in future requests.
                Set<Long> gapOffsetsSet = null;
                if (batch.gapOffsets != null && !batch.gapOffsets.isEmpty()) {
                    // Keep a set to easily check if the gap offset is part of the iterated in-flight
                    // batch.
                    gapOffsetsSet = new HashSet<>(batch.gapOffsets);
                }

                boolean updateNextFetchOffset = batch.acknowledgeType == AcknowledgeType.RELEASE;
                // The acknowledgement batch either is exact fetch equivalent batch (mostly), subset
                // or spans over multiple fetched batches. The state can vary per offset itself from
                // the fetched batch in case of subset.
                for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                    InFlightBatch inFlightBatch = entry.getValue();

                    if (inFlightBatch.offsetState == null && !inFlightBatch.inFlightState.memberId().equals(memberId)) {
                        log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}",
                            memberId, inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("Member is not the owner of batch record");
                        break;
                    }

                    boolean fullMatch = inFlightBatch.firstOffset() >= batch.firstOffset
                        && inFlightBatch.lastOffset() <= batch.lastOffset;

                    if (!fullMatch || inFlightBatch.offsetState != null) {
                        log.debug("Subset or offset tracked batch record found for acknowledgement,"
                                + " batch: {} request offsets - first: {}, last: {} for the share"
                                + " partition: {}-{}", inFlightBatch, batch.firstOffset, batch.lastOffset,
                                groupId, topicIdPartition);
                        if (inFlightBatch.offsetState == null) {
                            // Though the request is a subset of in-flight batch but the offset
                            // tracking has not been initialized yet which means that we could only
                            // acknowledge subset of offsets from the in-flight batch but only if the
                            // complete batch is acquired yet. Hence, do a pre-check to avoid exploding
                            // the in-flight offset tracking unnecessarily.
                            if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                                log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                                    inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The subset batch is not in the acquired state.");
                                break;
                            }
                            // The request batch is a subset or per offset state is managed hence update
                            // the offsets state in the in-flight batch.
                            inFlightBatch.maybeInitializeOffsetStateUpdate();
                        }
                        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {

                            // For the first batch which might have offsets prior to the request base
                            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                            if (offsetState.getKey() < batch.firstOffset) {
                                continue;
                            }

                            if (offsetState.getKey() > batch.lastOffset) {
                                // No further offsets to process.
                                break;
                            }

                            // Add the gap offsets.
                            if (gapOffsetsSet != null && gapOffsetsSet.contains(offsetState.getKey())) {
                                inFlightBatch.addGapOffsets(offsetState.getKey());
                                // Force the state of the offset to Archived for the gap offset
                                // irrespectively.
                                offsetState.getValue().state = RecordState.ARCHIVED;
                                // Cancel and clear the acquisition lock timeout task for the state since it is moved to ARCHIVED state.
                                offsetState.getValue().cancelAndClearAcquisitionLockTimeoutTask();
                                continue;
                            }

                            // Check if member id is the owner of the offset.
                            if (!offsetState.getValue().memberId.equals(memberId)) {
                                log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                                        + " partition: {}-{}", memberId, offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("Member is not the owner of offset");
                                break;
                            }

                            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                                log.debug("The offset is not acquired, offset: {} batch: {} for the share"
                                    + " partition: {}-{}", offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The offset is not acquired.");
                                break;
                            }

                            InFlightState updateResult =  offsetState.getValue().startStateTransition(
                                    recordState,
                                    false,
                                    this.maxDeliveryCount
                            );
                            if (updateResult == null) {
                                log.debug("Unable to acknowledge records for the offset: {} in batch: {}"
                                        + " for the share partition: {}-{}", offsetState.getKey(),
                                    inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("Unable to acknowledge records for the batch");
                                break;
                            }
                            // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                            // This should not change the nextFetchOffset because the record is not available for acquisition
                            // Successfully updated the state of the offset.
                            updatedStates.add(updateResult);
                            stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                                    updateResult.state.id, (short) updateResult.deliveryCount));
                            if (updateNextFetchOffset && updateResult.state != RecordState.ARCHIVED) {
                                localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                            }
                        }
                        continue;
                    }

                    // The in-flight batch is a full match hence change the state of the complete.
                    log.trace("Acknowledging complete batch record {} for the share partition: {}-{}",
                        batch, groupId, topicIdPartition);
                    if (inFlightBatch.batchState() != RecordState.ACQUIRED) {
                        log.debug("The batch is not in the acquired state: {} for share partition: {}-{}",
                            inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("The batch cannot be acknowledged. The batch is not in the acquired state.");
                        break;
                    }

                    InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                            recordState,
                            false,
                            this.maxDeliveryCount
                    );
                    if (updateResult == null) {
                        log.debug("Unable to acknowledge records for the batch: {} with state: {}"
                            + " for the share partition: {}-{}", inFlightBatch, recordState, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("Unable to acknowledge records for the batch");
                        break;
                    }
                    // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                    // This should not change the nextFetchOffset because the record is not available for acquisition
                    // Add the gap offsets.
                    if (batch.gapOffsets != null && !batch.gapOffsets.isEmpty()) {
                        for (Long gapOffset : batch.gapOffsets) {
                            if (inFlightBatch.firstOffset() <= gapOffset && inFlightBatch.lastOffset() >= gapOffset) {
                                inFlightBatch.addGapOffsets(gapOffset);
                                // Just track the gap offset, no need to change the state of the offset
                                // as the offset tracking is not maintained for the batch.
                            }
                        }
                    }

                    // Successfully updated the state of the batch.
                    updatedStates.add(updateResult);
                    stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset, inFlightBatch.lastOffset,
                            updateResult.state.id, (short) updateResult.deliveryCount));
                    if (updateNextFetchOffset && updateResult.state != RecordState.ARCHIVED) {
                        localNextFetchOffset = Math.min(inFlightBatch.firstOffset, localNextFetchOffset);
                    }
                }

                if (throwable != null) {
                    break;
                }
            }

            if (throwable != null || !isWriteShareGroupStateSuccessful(stateBatches)) {
                // the log should be DEBUG to avoid flooding of logs for a faulty client
                log.debug("Acknowledgement batch request failed for share partition, rollback any changed state"
                    + " for the share partition: {}-{}", groupId, topicIdPartition);
                updatedStates.forEach(state -> state.completeStateTransition(false));
            } else {
                log.trace("Acknowledgement batch request successful for share partition: {}-{}",
                    groupId, topicIdPartition);
                updatedStates.forEach(state -> {
                    state.completeStateTransition(true);
                    // Cancel the acquisition lock timeout task for the state since it is acknowledged successfully.
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });

                nextFetchOffset = localNextFetchOffset;

                maybeUpdateCachedStateAndOffsets();
            }
        } finally {
            lock.writeLock().unlock();
        }

        return CompletableFuture.completedFuture(Optional.ofNullable(throwable));
    }

    /**
     * Checks if the number of records between startOffset and endOffset does not exceed the record lock partition limit
     *
     * @return A boolean which indicates whether more records can be acquired or not
     */
    public boolean canAcquireMore() {
        lock.readLock().lock();
        long numRecords;
        try {
            if (cachedState.isEmpty()) {
                numRecords = 0;
            } else {
                numRecords = this.endOffset - this.startOffset + 1;
            }
        } finally {
            lock.readLock().unlock();
        }
        return numRecords < maxInFlightMessages;
    }

    /**
     * Release the acquired records for the share partition. The next fetch offset is updated to the next offset
     * that should be fetched from the leader.
     *
     * @param memberId The member id of the client that is fetching/acknowledging the record.
     *
     * @return A future which is completed when the records are released.
     */
    public CompletableFuture<Optional<Throwable>> releaseAcquiredRecords(String memberId) {
        log.trace("Release acquired records request for share partition: {}-{}-{}", groupId, memberId, topicIdPartition);

        Throwable throwable = null;
        lock.writeLock().lock();
        List<InFlightState> updatedStates = new ArrayList<>();
        List<PersisterStateBatch> stateBatches = new ArrayList<>();
        try {
            long localNextFetchOffset = nextFetchOffset;
            RecordState recordState = RecordState.AVAILABLE;

            // Iterate over multiple fetched batches. The state can vary per offset entry
            for (Map.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                InFlightBatch inFlightBatch = entry.getValue();

                if (inFlightBatch.offsetState != null) {
                    log.trace("Offset tracked batch record found, batch: {} for the share partition: {}-{}", inFlightBatch,
                            groupId, topicIdPartition);
                    for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                        // Check if member id is the owner of the offset.
                        if (!offsetState.getValue().memberId.equals(memberId)) {
                            log.debug("Member {} is not the owner of offset: {} in batch: {} for the share"
                                    + " partition: {}-{}", memberId, offsetState.getKey(), inFlightBatch, groupId, topicIdPartition);
                            continue;
                        }
                        if (offsetState.getValue().state == RecordState.ACQUIRED) {
                            InFlightState updateResult = offsetState.getValue().startStateTransition(
                                    recordState,
                                    false,
                                    this.maxDeliveryCount
                            );
                            if (updateResult == null) {
                                log.debug("Unable to release records from acquired state for the offset: {} in batch: {}"
                                                + " for the share partition: {}-{}", offsetState.getKey(),
                                        inFlightBatch, groupId, topicIdPartition);
                                throwable = new InvalidRecordStateException("Unable to release acquired records for the batch");
                                break;
                            }
                            // Successfully updated the state of the offset.
                            updatedStates.add(updateResult);
                            stateBatches.add(new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                                    updateResult.state.id, (short) updateResult.deliveryCount));
                            // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                            // This should not change the nextFetchOffset because the record is not available for acquisition
                            if (updateResult.state != RecordState.ARCHIVED) {
                                localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                            }
                        }
                    }
                    if (throwable != null)
                        break;
                    continue;
                }

                // Check if member id is the owner of the batch.
                if (!inFlightBatch.inFlightState.memberId().equals(memberId)) {
                    log.debug("Member {} is not the owner of batch record {} for share partition: {}-{}",
                            memberId, inFlightBatch, groupId, topicIdPartition);
                    continue;
                }
                // Change the state of complete batch since the same state exists for the entire inFlight batch
                log.trace("Releasing acquired records for complete batch {} for the share partition: {}-{}",
                        inFlightBatch, groupId, topicIdPartition);

                if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
                    InFlightState updateResult = inFlightBatch.startBatchStateTransition(
                            recordState,
                            false,
                            this.maxDeliveryCount
                    );
                    if (updateResult == null) {
                        log.debug("Unable to release records from acquired state for the batch: {}"
                                        + " for the share partition: {}-{}", inFlightBatch, groupId, topicIdPartition);
                        throwable = new InvalidRecordStateException("Unable to release acquired records for the batch");
                        break;
                    }
                    // Successfully updated the state of the batch.
                    updatedStates.add(updateResult);
                    stateBatches.add(new PersisterStateBatch(inFlightBatch.firstOffset, inFlightBatch.lastOffset,
                            updateResult.state.id, (short) updateResult.deliveryCount));
                    // If the maxDeliveryCount limit has been exceeded, the record will be transitioned to ARCHIVED state.
                    // This should not change the nextFetchOffset because the record is not available for acquisition
                    if (updateResult.state != RecordState.ARCHIVED) {
                        localNextFetchOffset = Math.min(inFlightBatch.firstOffset, localNextFetchOffset);
                    }
                }
            }

            if (throwable != null || !isWriteShareGroupStateSuccessful(stateBatches)) {
                log.debug("Release records from acquired state failed for share partition, rollback any changed state"
                        + " for the share partition: {}-{}", groupId, topicIdPartition);
                updatedStates.forEach(state -> state.completeStateTransition(false));
            } else {
                log.trace("Release records from acquired state successful for share partition: {}-{}",
                        groupId, topicIdPartition);
                updatedStates.forEach(state -> {
                    state.completeStateTransition(true);
                    // Cancel the acquisition lock timeout task for the state since it is released successfully.
                    state.cancelAndClearAcquisitionLockTimeoutTask();
                });
                nextFetchOffset = localNextFetchOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }
        return CompletableFuture.completedFuture(Optional.ofNullable(throwable));
    }

    private void initialize() {
        // Initialize the partition.
        log.debug("Initializing share partition: {}-{}", groupId, topicIdPartition);
        // Persister class can be null during active development and shall be driven by temporary config.
        if (persister == null) {
            // Set the default states so that the partition can be used without the persister.
            startOffset = 0;
            endOffset = 0;
            nextFetchOffset = 0;
            stateEpoch = 0;
            return;
        }

        // Initialize the partition by issuing an initialize RPC call to persister.
        ReadShareGroupStateResult response = null;
        try {
            response = persister.readState(new ReadShareGroupStateParameters.Builder()
                .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionIdData>()
                    .setGroupId(this.groupId)
                    .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                        Collections.singletonList(PartitionFactory.newPartitionIdData(topicIdPartition.partition())))))
                    .build())
                .build()
            ).get();

            if (response == null || response.topicsData() == null || response.topicsData().size() != 1) {
                log.error("Failed to initialize the share partition: {}-{}. Invalid state found: {}.",
                    groupId, topicIdPartition, response);
                throw new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
            }

            TopicData<PartitionAllData> state = response.topicsData().get(0);
            if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1
                || state.partitions().get(0).partition() != topicIdPartition.partition()) {
                log.error("Failed to initialize the share partition: {}-{}. Invalid topic partition response: {}.",
                    groupId, topicIdPartition, response);
                throw new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
            }

            PartitionAllData partitionData = state.partitions().get(0);
            // Set the state epoch, next fetch offset and end offset from the persisted state.
            startOffset = partitionData.startOffset();
            nextFetchOffset = partitionData.startOffset();
            stateEpoch = partitionData.stateEpoch();

            List<PersisterStateBatch> stateBatches = partitionData.stateBatches();
            for (PersisterStateBatch stateBatch : stateBatches) {
                // TODO: Fix memberId passed as when messages are released then the member id should be
                //  either null or blank. Though to prevent NPE passed blank, maybe keep memberId
                //  as Optional<String> in the InFlightState. Also the check in acknowledgment
                //  happens for memberId prior state hence in Available state with blank/null
                //  memberId the error could be incorrect. Similar change required for timerTask.
                if (stateBatch.firstOffset() < startOffset) {
                    log.error("Invalid state batch found for the share partition: {}-{}. The base offset: {}"
                                + " is less than the start offset: {}.", groupId, topicIdPartition,
                    stateBatch.firstOffset(), startOffset);
                    throw new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition));
                }
                InFlightBatch inFlightBatch = new InFlightBatch("", stateBatch.firstOffset(),
                    stateBatch.lastOffset(), RecordState.forId(stateBatch.deliveryState()), stateBatch.deliveryCount(), null);
                cachedState.put(stateBatch.firstOffset(), inFlightBatch);
            }
            // Update the endOffset of the partition.
            if (!cachedState.isEmpty()) {
                endOffset = cachedState.lastEntry().getValue().lastOffset();
            } else {
                endOffset = partitionData.startOffset();
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to initialize the share partition: {}-{}", groupId, topicIdPartition, e);
            throw new IllegalStateException(String.format("Failed to initialize the share partition %s-%s", groupId, topicIdPartition), e);
        }
    }

    private void maybeUpdateCachedStateAndOffsets() {
        lock.writeLock().lock();
        try {
            boolean canMoveStartOffset = false;
            // The Share Partition Start Offset can be moved after acknowledgement is complete when the following 2 conditions are true -
            // 1. When the cachedState is not empty
            // 2. When the acknowledgement type for the startOffset is either ACCEPT or REJECT
            if (!cachedState.isEmpty()) {
                RecordState startOffsetState = cachedState.floorEntry(startOffset).getValue().offsetState == null ?
                        cachedState.floorEntry(startOffset).getValue().inFlightState.state :
                        cachedState.floorEntry(startOffset).getValue().offsetState.get(startOffset).state;
                if (startOffsetState == RecordState.ACKNOWLEDGED || startOffsetState == RecordState.ARCHIVED) {
                    canMoveStartOffset = true;
                }
            }
            if (canMoveStartOffset) {
                // This will help to find the next position for the startOffset.
                // The new position of startOffset will be lastOffsetAcknowledged + 1
                long lastOffsetAcknowledged = -1;
                for (NavigableMap.Entry<Long, InFlightBatch> entry : cachedState.entrySet()) {
                    InFlightBatch inFlightBatch = entry.getValue();
                    if (inFlightBatch.offsetState == null) {
                        if (inFlightBatch.inFlightState.state == RecordState.ACKNOWLEDGED ||
                                inFlightBatch.inFlightState.state == RecordState.ARCHIVED) {
                            lastOffsetAcknowledged = inFlightBatch.lastOffset;
                        } else {
                            break;
                        }
                    } else {
                        boolean toBreak = false;
                        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                            if (offsetState.getValue().state == RecordState.ACKNOWLEDGED ||
                                    offsetState.getValue().state == RecordState.ARCHIVED) {
                                lastOffsetAcknowledged = offsetState.getKey();
                            } else {
                                toBreak = true;
                                break;
                            }
                        }
                        if (toBreak) break;
                    }
                }

                // If lastOffsetAcknowledged is -1, this means we cannot move out startOffset ahead
                if (lastOffsetAcknowledged != -1) {
                    // This is true if all records in the cachedState have been acknowledged (either Accept or Reject).
                    // The resulting action should be to empty the cachedState altogether
                    if (lastOffsetAcknowledged == cachedState.lastEntry().getValue().lastOffset()) {
                        startOffset = cachedState.lastEntry().getValue().lastOffset() + 1; // The next offset that will be fetched and acquired in the share partition
                        endOffset = cachedState.lastEntry().getValue().lastOffset() + 1;
                        cachedState.clear();
                    } else {
                        // If the code arrives in this else block, then the cachedState contains some records that are
                        // yet to be acknowledged, and thus should not be removed. Only a subMap will be removed from the
                        // cachedState. The logic to remove batches from cachedState is as follows -
                        //    Only full batches can be removed from the cachedState, For example if there is batch (0-99)
                        //    and 0-49 records are acknowledged (ACCEPT or REJECT), the first 50 records will not be removed
                        //    from the cachedState. Instead, the startOffset will be moved to 50, but the batch will only
                        //    be removed once all the messages (0-99) are acknowledged (ACCEPT or REJECT)

                        // Since only a subMap will be removed, we need to find the first and last keys of that subMap
                        long firstKeyToRemove = cachedState.firstKey();
                        long lastKeyToRemove;
                        NavigableMap.Entry<Long, InFlightBatch> entry = cachedState.floorEntry(lastOffsetAcknowledged);
                        if (lastOffsetAcknowledged == entry.getValue().lastOffset) {
                            startOffset = cachedState.higherKey(lastOffsetAcknowledged);
                            lastKeyToRemove = entry.getKey();
                        } else {
                            startOffset = lastOffsetAcknowledged + 1;
                            if (entry.getKey().equals(cachedState.firstKey())) {
                                // If the first batch in cachedState has some records yet to be acknowledged,
                                // then nothing should be removed from cachedState
                                lastKeyToRemove = -1;
                            } else {
                                lastKeyToRemove = cachedState.lowerKey(entry.getKey());
                            }
                        }

                        if (lastKeyToRemove != -1) {
                            NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(firstKeyToRemove, true, lastKeyToRemove, true);
                            for (Long key : subMap.keySet()) {
                                cachedState.remove(key);
                            }
                        }
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private AcquiredRecords acquireNewBatchRecords(String memberId, long firstOffset, long lastOffset, long nextOffset) {
        lock.writeLock().lock();
        try {
            // Schedule acquisition lock timeout for the batch.
            TimerTask timerTask = scheduleAcquisitionLockTimeout(memberId, firstOffset, lastOffset);
            // Add the new batch to the in-flight records along with the acquisition lock timeout task for the batch.
            cachedState.put(firstOffset, new InFlightBatch(
                memberId,
                firstOffset,
                lastOffset,
                RecordState.ACQUIRED,
                1,
                timerTask));
            // if the cachedState was empty before acquiring the new batches then startOffset needs to be updated
            if (cachedState.firstKey() == firstOffset)  {
                startOffset = firstOffset;
            }
            endOffset = lastOffset;
            nextFetchOffset = nextOffset;
            return new AcquiredRecords()
                .setFirstOffset(firstOffset)
                .setLastOffset(lastOffset)
                .setDeliveryCount((short) 1);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void acquireSubsetBatchRecords(String memberId, long requestFirstOffset, long requestLastOffset,
        InFlightBatch inFlightBatch, List<AcquiredRecords> result) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                // For the first batch which might have offsets prior to the request base
                // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                if (offsetState.getKey() < requestFirstOffset) {
                    continue;
                }

                if (offsetState.getKey() > requestLastOffset) {
                    // No further offsets to process.
                    break;
                }

                if (offsetState.getValue().state != RecordState.AVAILABLE) {
                    log.trace("The offset is not available skipping, offset: {} batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }

                InFlightState updateResult =  offsetState.getValue().tryUpdateState(RecordState.ACQUIRED, true);
                if (updateResult == null) {
                    log.trace("Unable to acquire records for the offset: {} in batch: {}"
                            + " for the share group: {}-{}", offsetState.getKey(), inFlightBatch,
                        groupId, topicIdPartition);
                    continue;
                }
                // Update the member id of the offset since the member acquiring could be different.
                updateResult.memberId = memberId;
                // Schedule acquisition lock timeout for the offset.
                TimerTask acquisitionLockTimeoutTask = scheduleAcquisitionLockTimeout(memberId, offsetState.getKey(), offsetState.getKey());
                // Update acquisition lock timeout task for the offset.
                offsetState.getValue().updateAcquisitionLockTimeoutTask(acquisitionLockTimeoutTask);

                findNextFetchOffset.set(true);
                // TODO: Maybe we can club the continuous offsets here.
                result.add(new AcquiredRecords()
                    .setFirstOffset(offsetState.getKey())
                    .setLastOffset(offsetState.getKey())
                    .setDeliveryCount((short) offsetState.getValue().deliveryCount));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static RecordState fetchRecordState(AcknowledgeType acknowledgeType) {
        switch (acknowledgeType) {
            case ACCEPT:
                return RecordState.ACKNOWLEDGED;
            case RELEASE:
                return RecordState.AVAILABLE;
            case REJECT:
                return RecordState.ARCHIVED;
            default:
                throw new IllegalArgumentException("Invalid acknowledge type: " + acknowledgeType);
        }
    }

    private TimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset) {
        return scheduleAcquisitionLockTimeout(memberId, firstOffset, lastOffset, recordLockDurationMs);
    }

    // TODO: maxDeliveryCount should be utilized here once it is implemented
    /**
     * Apply acquisition lock to acquired records.
     * @param memberId The member id of the client that is putting the acquisition lock.
     * @param firstOffset The first offset of the acquired records.
     * @param lastOffset The last offset of the acquired records.
     */
    private TimerTask scheduleAcquisitionLockTimeout(String memberId, long firstOffset, long lastOffset, long delayMs) {
        TimerTask acquistionLockTimerTask = acquisitionLockTimerTask(memberId, firstOffset, lastOffset, delayMs);
        timer.add(acquistionLockTimerTask);
        return acquistionLockTimerTask;
    }

    private TimerTask acquisitionLockTimerTask(String memberId, long firstOffset, long lastOffset, long delayMs) {
        return new AcquisitionLockTimerTask(delayMs, memberId, firstOffset, lastOffset);
    }

    private final class AcquisitionLockTimerTask extends TimerTask {
        private final long expirationMs;
        private final String memberId;
        private final long firstOffset, lastOffset;

        AcquisitionLockTimerTask(long delayMs, String memberId, long firstOffset, long lastOffset) {
            super(delayMs);
            this.expirationMs = time.hiResClockMs() + delayMs;
            this.memberId = memberId;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

        long expirationMs() {
            return expirationMs;
        }

        /**
         * The task is executed when the acquisition lock timeout is reached. The task releases the acquired records.
         */
        @Override
        public void run() {
            releaseAcquisitionLockOnTimeout(memberId, firstOffset, lastOffset);
        }
    }

    private void releaseAcquisitionLockOnTimeout(String memberId, long firstOffset, long lastOffset) {
        lock.writeLock().lock();
        try {
            Map.Entry<Long, InFlightBatch> floorOffset = cachedState.floorEntry(firstOffset);
            if (floorOffset == null) {
                log.debug("Base Offset {} not found for share partition: {}-{}", firstOffset, groupId, topicIdPartition);
            } else {
                NavigableMap<Long, InFlightBatch> subMap = cachedState.subMap(floorOffset.getKey(), true, lastOffset, true);
                long localNextFetchOffset = nextFetchOffset;
                for (Map.Entry<Long, InFlightBatch> entry : subMap.entrySet()) {
                    InFlightBatch inFlightBatch = entry.getValue();
                    // Case when the state of complete batch is valid
                    if (inFlightBatch.offsetState == null) {
                        if (inFlightBatch.batchState() == RecordState.ACQUIRED) {
                            InFlightState updateResult = inFlightBatch.tryUpdateBatchState(RecordState.AVAILABLE, false);
                            if (updateResult == null) {
                                log.debug("Unable to release acquisition lock on timeout for the batch: {}"
                                        + " for the share partition: {}-{}-{}", inFlightBatch, groupId, memberId, topicIdPartition);
                            } else {
                                // Even if write share group state RPC call fails, we will still go ahead with the state transition.
                                isWriteShareGroupStateSuccessful(Collections.singletonList(
                                    new PersisterStateBatch(inFlightBatch.firstOffset(), inFlightBatch.lastOffset(),
                                        updateResult.state.id, (short) updateResult.deliveryCount)));
                                // Update acquisition lock timeout task for the batch to null since it is completed now.
                                updateResult.updateAcquisitionLockTimeoutTask(null);
                                localNextFetchOffset = Math.min(entry.getKey(), localNextFetchOffset);
                            }
                        } else {
                            log.debug("The batch is not in acquired state while release of acquisition lock on timeout, skipping, batch: {}"
                                    + " for the share group: {}-{}-{}", inFlightBatch, groupId, memberId, topicIdPartition);
                        }
                    } else { // Case when batch has a valid offset state map.
                        for (Map.Entry<Long, InFlightState> offsetState : inFlightBatch.offsetState.entrySet()) {
                            // For the first batch which might have offsets prior to the request base
                            // offset i.e. cached batch of 10-14 offsets and request batch of 12-13.
                            if (offsetState.getKey() < firstOffset) {
                                continue;
                            }

                            if (offsetState.getKey() > lastOffset) {
                                // No further offsets to process.
                                break;
                            }

                            if (offsetState.getValue().state != RecordState.ACQUIRED) {
                                log.debug("The offset is not in acquired state while release of acquisition lock on timeout, skipping, offset: {} batch: {}"
                                                + " for the share group: {}-{}-{}", offsetState.getKey(), inFlightBatch,
                                        groupId, memberId, topicIdPartition);
                                continue;
                            }
                            InFlightState updateResult = offsetState.getValue().tryUpdateState(RecordState.AVAILABLE, false);
                            if (updateResult == null) {
                                log.debug("Unable to release acquisition lock on timeout for the offset: {} in batch: {}"
                                                + " for the share group: {}-{}-{}", offsetState.getKey(), inFlightBatch,
                                        groupId, memberId, topicIdPartition);
                                continue;
                            }
                            // Even if write share group state RPC call fails, we will still go ahead with the state transition.
                            isWriteShareGroupStateSuccessful(Collections.singletonList(
                                new PersisterStateBatch(offsetState.getKey(), offsetState.getKey(),
                                    updateResult.state.id, (short) updateResult.deliveryCount)));
                            // Update acquisition lock timeout task for the offset to null since it is completed now.
                            updateResult.updateAcquisitionLockTimeoutTask(null);
                            localNextFetchOffset = Math.min(offsetState.getKey(), localNextFetchOffset);
                        }
                    }
                }
                nextFetchOffset = localNextFetchOffset;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }


    // Visible for testing
     boolean isWriteShareGroupStateSuccessful(List<PersisterStateBatch> stateBatches) {
        try {
            // Persister class can be null during active development and shall be driven by temporary config.
            if (persister == null)
                return true;
            WriteShareGroupStateResult response = persister.writeState(new WriteShareGroupStateParameters.Builder()
                .setGroupTopicPartitionData(new GroupTopicPartitionData.Builder<PartitionStateBatchData>()
                    .setGroupId(this.groupId)
                    .setTopicsData(Collections.singletonList(new TopicData<>(topicIdPartition.topicId(),
                        Collections.singletonList(PartitionFactory.newPartitionStateBatchData(
                            topicIdPartition.partition(), stateEpoch, startOffset, stateBatches))))
                    ).build()).build()).get();

            if (response == null || response.topicsData() == null || response.topicsData().size() != 1) {
                log.error("Failed to write the share group state for share partition: {}-{}. Invalid state found: {}",
                        groupId, topicIdPartition, response);
                throw new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition));
            }

            TopicData<PartitionErrorData> state = response.topicsData().get(0);
            if (state.topicId() != topicIdPartition.topicId() || state.partitions().size() != 1
                    || state.partitions().get(0).partition() != topicIdPartition.partition()) {
                log.error("Failed to write the share group state for share partition: {}-{}. Invalid topic partition response: {}",
                        groupId, topicIdPartition, response);
                throw new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                        groupId, topicIdPartition));
            }

            PartitionErrorData partitionData = state.partitions().get(0);
            if (partitionData.errorCode() != Errors.NONE.code()) {
                Exception exception = Errors.forCode(partitionData.errorCode()).exception();
                log.error("Failed to write the share group state for share partition: {}-{} due to exception",
                        groupId, topicIdPartition, exception);
                return false;
            }
            return true;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to write the share group state for share partition: {}-{}", groupId, topicIdPartition, e);
            throw new IllegalStateException(String.format("Failed to write the share group state for share partition %s-%s",
                    groupId, topicIdPartition), e);
        }
    }

    // Visible for testing. Should only be used for testing purposes.
    NavigableMap<Long, InFlightBatch> cachedState() {
        return new ConcurrentSkipListMap<>(cachedState);
    }

    // Visible for testing.
    Timer timer() {
        return timer;
    }

    // Visible for testing.
    int stateEpoch() {
        return stateEpoch;
    }

    /**
     * The InFlightBatch maintains the in-memory state of the fetched records i.e. in-flight records.
     */
    class InFlightBatch {
        // The offset of the first record in the batch that is fetched from the log.
        private final long firstOffset;
        // The last offset of the batch that is fetched from the log.
        private final long lastOffset;

        // The in-flight state of the fetched records. If the offset state map is empty then inflightState
        // determines the state of the complete batch else individual offset determines the state of
        // the respective records.
        private InFlightState inFlightState;
        // Must be null for most of the records hence lazily-initialize. Should hold the gap offsets for
        // the batch that are reported by the client.
        private Set<Long> gapOffsets;
        // The subset of offsets within the batch that holds different record states within the parent
        // fetched batch. This is used to maintain the state of the records per offset, it is complex
        // to maintain the subset batch (InFlightBatch) within the parent batch itself as the nesting
        // can be deep and the state of the records can be different per offset, not always though.
        private NavigableMap<Long, InFlightState> offsetState;

        InFlightBatch(String memberId, long firstOffset, long lastOffset, RecordState state, int deliveryCount, TimerTask acquisitionLockTimeoutTask) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.inFlightState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
        }

        // Visible for testing.
        long firstOffset() {
            return firstOffset;
        }

        // Visible for testing.
        long lastOffset() {
            return lastOffset;
        }

        // Visible for testing.
        RecordState batchState() {
            if (inFlightState == null) {
                  throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return inFlightState.state;
        }

        // Visible for testing.
        String batchMemberId() {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch member id is not available as the offset state is maintained");
            }
            return inFlightState.memberId;
        }

        // Visible for testing.
        int batchDeliveryCount() {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch delivery count is not available as the offset state is maintained");
            }
            return inFlightState.deliveryCount;
        }

        // Visible for testing.
        Set<Long> gapOffsets() {
            return gapOffsets;
        }

        // Visible for testing.
        NavigableMap<Long, InFlightState> offsetState() {
            return offsetState;
        }

        private InFlightState tryUpdateBatchState(RecordState newState, boolean incrementDeliveryCount) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return inFlightState.tryUpdateState(newState, incrementDeliveryCount);
        }

        private InFlightState startBatchStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state update is not available as the offset state is maintained");
            }
            return inFlightState.startStateTransition(newState, incrementDeliveryCount, maxDeliveryCount);
        }

        private void addGapOffsets(Long gapOffset) {
            if (this.gapOffsets == null) {
                this.gapOffsets = new HashSet<>();
            }
            this.gapOffsets.add(gapOffset);
        }

        private void addOffsetState(long firstOffset, long lastOffset, RecordState state, boolean incrementDeliveryCount) {
            maybeInitializeOffsetStateUpdate();
            // The offset state map is already initialized hence update the state of the offsets.
            for (long offset = firstOffset; offset <= lastOffset; offset++) {
                offsetState.get(offset).tryUpdateState(state, incrementDeliveryCount);
            }
        }

        private void addOffsetState(Map<Long, RecordState> stateMap, boolean incrementDeliveryCount) {
            maybeInitializeOffsetStateUpdate();
            // The offset state map is already initialized hence update the state of the offsets.
            stateMap.forEach((offset, state) -> offsetState.get(offset).tryUpdateState(state, incrementDeliveryCount));
        }

        private void maybeInitializeOffsetStateUpdate() {
            if (offsetState == null) {
                offsetState = new ConcurrentSkipListMap<>();
                // The offset state map is not initialized hence initialize the state of the offsets
                // from the first offset to the last offset. Mark the batch inflightState to null as
                // the state of the records is maintained in the offset state map now.
                for (long offset = this.firstOffset; offset <= this.lastOffset; offset++) {
                    if (gapOffsets != null && gapOffsets.contains(offset)) {
                        // Directly move the record to archived if gap offset is already known.
                        offsetState.put(offset, new InFlightState(RecordState.ARCHIVED, 0, inFlightState.memberId));
                        continue;
                    }
                    if (inFlightState.acquisitionLockTimeoutTask != null) {
                        // The acquisition lock timeout task is already scheduled for the batch, hence we need to schedule
                        // the acquisition lock timeout task for the offset as well.
                        long delayMs = ((AcquisitionLockTimerTask) inFlightState.acquisitionLockTimeoutTask).expirationMs() - time.hiResClockMs();
                        TimerTask timerTask = scheduleAcquisitionLockTimeout(inFlightState.memberId, offset, offset, delayMs);
                        offsetState.put(offset, new InFlightState(inFlightState.state, inFlightState.deliveryCount, inFlightState.memberId, timerTask));
                        timer.add(timerTask);
                    } else {
                        offsetState.put(offset, new InFlightState(inFlightState.state, inFlightState.deliveryCount, inFlightState.memberId));
                    }
                }
                // Cancel the acquisition lock timeout task for the batch as the offset state is maintained.
                if (inFlightState.acquisitionLockTimeoutTask != null) {
                    inFlightState.cancelAndClearAcquisitionLockTimeoutTask();
                }
                inFlightState = null;
            }
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            return inFlightState.acquisitionLockTimeoutTask;
        }

        private void updateAcquisitionLockTimeout(TimerTask acquisitionLockTimeoutTask) {
            if (inFlightState == null) {
                throw new IllegalStateException("The batch state is not available as the offset state is maintained");
            }
            inFlightState.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        @Override
        public String toString() {
            return "InFlightBatch(" +
                " firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", inFlightState=" + inFlightState +
                ", gapOffsets=" + ((gapOffsets == null) ? "null" : gapOffsets) +
                ", offsetState=" + ((offsetState == null) ? "null" : offsetState) +
                ")";
        }
    }

    /**
     * The InFlightState is used to track the state and delivery count of a record that has been
     * fetched from the leader. The state of the record is used to determine if the record should
     * be re-deliver or if it can be acknowledged or archived.
     */
    static class InFlightState {

        // The state of the fetch batch records.
        private RecordState state;
        // The number of times the records has been delivered to the client.
        private int deliveryCount;
        // The member id of the client that is fetching/acknowledging the record.
        private String memberId;
        // The state of the records before the transition. In case we need to revert an in-flight state, we revert the above
        // attributes of InFlightState to this state, namely - state, deliveryCount and memberId.
        private InFlightState rollbackState;
        // The timer task for the acquisition lock timeout.
        private TimerTask acquisitionLockTimeoutTask;


        InFlightState(RecordState state, int deliveryCount, String memberId) {
            this(state, deliveryCount, memberId, null);
        }

        InFlightState(RecordState state, int deliveryCount, String memberId, TimerTask acquisitionLockTimeoutTask) {
          this.state = state;
          this.deliveryCount = deliveryCount;
          this.memberId = memberId;
          this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        // Visible for testing.
        RecordState state() {
            return state;
        }

        // Visible for testing.
        int deliveryCount() {
            return deliveryCount;
        }

        String memberId() {
            return memberId;
        }

        // Visible for testing.
        TimerTask acquisitionLockTimeoutTask() {
            return acquisitionLockTimeoutTask;
        }

        void updateAcquisitionLockTimeoutTask(TimerTask acquisitionLockTimeoutTask) {
            this.acquisitionLockTimeoutTask = acquisitionLockTimeoutTask;
        }

        void cancelAndClearAcquisitionLockTimeoutTask() {
            acquisitionLockTimeoutTask.cancel();
            acquisitionLockTimeoutTask = null;
        }

        /**
        * Try to update the state of the records. The state of the records can only be updated if the
        * new state is allowed to be transitioned from old state. The delivery count is not incremented
        * if the state update is unsuccessful.
        *
        * @param newState The new state of the records.
        * @param incrementDeliveryCount Whether to increment the delivery count.
        *
        * @return {@code InFlightState} if update succeeds, null otherwise. Returning state
        *         helps update chaining.
        */
        private InFlightState tryUpdateState(RecordState newState, boolean incrementDeliveryCount) {
            try {
                state = state.validateTransition(newState);
                if (incrementDeliveryCount) {
                    deliveryCount++;
                }
                return this;
            } catch (IllegalStateException e) {
                log.info("Failed to update state of the records", e);
                return null;
            }
        }

        private InFlightState startStateTransition(RecordState newState, boolean incrementDeliveryCount, int maxDeliveryCount) {
            try {
                rollbackState = new InFlightState(state, deliveryCount, memberId, acquisitionLockTimeoutTask);
                if (newState == RecordState.AVAILABLE && deliveryCount >= maxDeliveryCount) {
                    newState = RecordState.ARCHIVED;
                }
                state = state.validateTransition(newState);
                if (incrementDeliveryCount && newState != RecordState.ARCHIVED) {
                    deliveryCount++;
                }
                return this;
            } catch (IllegalStateException e) {
                log.info("Failed to start state transition of the records", e);
                return null;
            }
        }

        private void completeStateTransition(boolean commit) {
            if (commit) {
                rollbackState = null;
                return;
            }
            state = rollbackState.state;
            deliveryCount = rollbackState.deliveryCount;
            memberId = rollbackState.memberId;
            rollbackState = null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, deliveryCount, memberId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InFlightState that = (InFlightState) o;
            return state == that.state && deliveryCount == that.deliveryCount && memberId.equals(that.memberId);
        }

        @Override
        public String toString() {
            return "InFlightState(" +
                " state=" + state.toString() +
                ", deliveryCount=" + deliveryCount +
                ", memberId=" + memberId +
                ")";
        }
    }

    /**
     * The AcknowledgementBatch containing the fields required to acknowledge the fetched records.
     */
    public static class AcknowledgementBatch {

        private final long firstOffset;
        private final long lastOffset;
        private final List<Long> gapOffsets;
        private final AcknowledgeType acknowledgeType;

        public AcknowledgementBatch(long firstOffset, long lastOffset, List<Long> gapOffsets, AcknowledgeType acknowledgeType) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.gapOffsets = gapOffsets;
            this.acknowledgeType = Objects.requireNonNull(acknowledgeType);
        }

        public long firstOffset() {
            return firstOffset;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public List<Long> gapOffsets() {
            return gapOffsets;
        }

        public AcknowledgeType acknowledgeType() {
            return acknowledgeType;
        }

        @Override
        public String toString() {
            return "AcknowledgementBatch(" +
                " firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", gapOffsets=" + ((gapOffsets == null) ? "" : gapOffsets) +
                ", acknowledgeType=" + acknowledgeType +
                ")";
        }
    }
}

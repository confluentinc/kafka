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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.ExternalKRaftMetrics;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.ReplicatedLog;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.SnapshotReader;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * The KRaft state machine for tracking control records in the topic partition.
 *
 * This type keeps track of changes to the finalized kraft.version and the sets of voters between
 * the latest snapshot and the log end offset.
 *
 * There are two type of actors/threads accessing this type. One is the KRaft driver which indirectly call a lot of
 * the public methods. The other actors/threads are the callers of {@code RaftClient.createSnapshot} which
 * indirectly call {@code voterSetAtOffset} and {@code kraftVersionAtOffset} when freezing a snapshot.
 */
public final class KRaftControlRecordStateMachine {
    private static final long STARTING_NEXT_OFFSET = -1;
    private static final long SMALLEST_LOG_OFFSET = 0;

    private final LogContext logContext;
    private final ReplicatedLog log;
    private final RecordSerde<?> serde;
    private final BufferSupplier bufferSupplier;
    private final Logger logger;
    private final int maxBatchSizeBytes;

    // These objects are synchronized using their respective object monitor. The two actors
    // are the KRaft driver when calling updateState and the RaftClient callers when freezing
    // snapshots
    private final VoterSetHistory voterSetHistory;
    private final LogHistory<KRaftVersion> kraftVersionHistory = new TreeMapLogHistory<>();

    // This synchronization is enough because
    // 1. The write operation updateState only sets the value without reading it and updates to
    // voterSetHistory or kraftVersionHistory are done before setting the nextOffset
    //
    // 2. The read operations lastVoterSet, voterSetAtOffset and kraftVersionAtOffset read
    // the nextOffset first before reading voterSetHistory or kraftVersionHistory
    private volatile long nextOffset = STARTING_NEXT_OFFSET;
    private final KafkaRaftMetrics kafkaRaftMetrics;
    private final ExternalKRaftMetrics externalKRaftMetrics;
    private final VoterSet staticVoterSet;

    /**
     * Constructs an internal log listener
     *
     * @param staticVoterSet the set of voter statically configured
     * @param log the on disk topic partition
     * @param serde the record decoder for data records
     * @param bufferSupplier the supplier of byte buffers
     * @param maxBatchSizeBytes the maximum size of record batch
     * @param logContext the log context
     */
    public KRaftControlRecordStateMachine(
        VoterSet staticVoterSet,
        ReplicatedLog log,
        RecordSerde<?> serde,
        BufferSupplier bufferSupplier,
        int maxBatchSizeBytes,
        LogContext logContext,
        KafkaRaftMetrics kafkaRaftMetrics,
        ExternalKRaftMetrics externalKRaftMetrics
    ) {
        this.logContext = logContext;
        this.log = log;
        this.voterSetHistory = new VoterSetHistory(staticVoterSet, logContext);
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
        this.maxBatchSizeBytes = maxBatchSizeBytes;
        this.logger = logContext.logger(getClass());
        this.kafkaRaftMetrics = kafkaRaftMetrics;
        this.externalKRaftMetrics = externalKRaftMetrics;
        this.staticVoterSet = staticVoterSet;

        kafkaRaftMetrics.updateNumVoters(staticVoterSet.size());
    }

    /**
     * Must be called whenever the {@code log} has changed.
     */
    public void updateState() {
        maybeLoadSnapshot();
        maybeLoadLog();
    }

    /**
     * Remove the head of the log until the given offset.
     *
     * @param endOffset the end offset (exclusive)
     */
    public void truncateNewEntries(long endOffset) {
        synchronized (voterSetHistory) {
            voterSetHistory.truncateNewEntries(endOffset);
        }
        synchronized (kraftVersionHistory) {
            kraftVersionHistory.truncateNewEntries(endOffset);
        }

        kafkaRaftMetrics.updateNumVoters(voterSetHistory.lastValue().size());
        if (!staticVoterSet.isEmpty() && voterSetHistory.lastEntry().isEmpty()) {
            externalKRaftMetrics.setIgnoredStaticVoters(false);
        }
    }

    /**
     * Remove the tail of the log until the given offset.
     *
     * @param startOffset the start offset (inclusive)
     */
    public void truncateOldEntries(long startOffset) {
        synchronized (voterSetHistory) {
            voterSetHistory.truncateOldEntries(startOffset);
        }
        synchronized (kraftVersionHistory) {
            kraftVersionHistory.truncateOldEntries(startOffset);
        }
    }

    /**
     * Returns the last voter set.
     */
    public VoterSet lastVoterSet() {
        synchronized (voterSetHistory) {
            return voterSetHistory.lastValue();
        }
    }

    /**
     * Return the latest entry for the set of voters.
     */
    public Optional<LogHistory.Entry<VoterSet>> lastVoterSetEntry() {
        synchronized (voterSetHistory) {
            return voterSetHistory.lastEntry();
        }
    }

    /**
     * Returns the offset of the last voter set.
     */
    public OptionalLong lastVoterSetOffset() {
        synchronized (voterSetHistory) {
            return voterSetHistory.lastVoterSetOffset();
        }
    }

    /**
     * Returns the last kraft version.
     */
    public KRaftVersion lastKraftVersion() {
        synchronized (kraftVersionHistory) {
            return kraftVersionHistory.lastEntry().map(LogHistory.Entry::value).
                orElse(KRaftVersion.KRAFT_VERSION_0);
        }
    }

    /**
     * Returns the voter set at a given offset.
     *
     * @param offset the offset (inclusive)
     * @return the voter set if one exist, otherwise {@code Optional.empty()}
     */
    public Optional<VoterSet> voterSetAtOffset(long offset) {
        checkOffsetIsValid(offset);

        synchronized (voterSetHistory) {
            return voterSetHistory.valueAtOrBefore(offset);
        }
    }

    /**
     * Returns the finalized kraft version at a given offset.
     *
     * @param offset the offset (inclusive)
     * @return the finalized kraft version if one exist, otherwise 0
     */
    public KRaftVersion kraftVersionAtOffset(long offset) {
        checkOffsetIsValid(offset);

        synchronized (kraftVersionHistory) {
            return kraftVersionHistory.valueAtOrBefore(offset).
                orElse(KRaftVersion.KRAFT_VERSION_0);
        }
    }

    private void checkOffsetIsValid(long offset) {
        long fixedNextOffset = nextOffset;
        if (offset >= fixedNextOffset) {
            throw new IllegalArgumentException(
                String.format(
                    "Attempting the read a value at an offset (%d) which is greater than or " +
                    "equal to the largest known offset (%d)",
                    offset,
                    fixedNextOffset - 1
                )
            );
        }
    }

    private void maybeLoadLog() {
        while (log.endOffset().offset() > nextOffset) {
            LogFetchInfo info = log.read(nextOffset, Isolation.UNCOMMITTED);
            try (RecordsIterator<?> iterator = new RecordsIterator<>(
                    info.records,
                    serde,
                    bufferSupplier,
                    maxBatchSizeBytes,
                    true, // Validate batch CRC
                    logContext
                )
            ) {
                while (iterator.hasNext()) {
                    Batch<?> batch = iterator.next();
                    handleBatch(batch, OptionalLong.empty());
                    nextOffset = batch.lastOffset() + 1;
                }
            }
        }
    }

    private void maybeLoadSnapshot() {
        if ((nextOffset == STARTING_NEXT_OFFSET || nextOffset < log.startOffset()) && log.latestSnapshot().isPresent()) {
            RawSnapshotReader rawSnapshot = log.latestSnapshot().get();
            // Clear the current state
            synchronized (kraftVersionHistory) {
                kraftVersionHistory.clear();
            }
            synchronized (voterSetHistory) {
                voterSetHistory.clear();
            }

            // Load the snapshot since the listener is at the start of the log or the log doesn't have the next entry.
            try (SnapshotReader<?> reader = RecordsSnapshotReader.of(
                    rawSnapshot,
                    serde,
                    bufferSupplier,
                    maxBatchSizeBytes,
                    true, // Validate batch CRC
                    logContext
                )
            ) {
                logger.info(
                    "Loading snapshot ({}) since log start offset ({}) is greater than the internal listener's next offset ({})",
                    reader.snapshotId(),
                    log.startOffset(),
                    nextOffset
                );
                OptionalLong currentOffset = OptionalLong.of(reader.lastContainedLogOffset());
                while (reader.hasNext()) {
                    Batch<?> batch = reader.next();
                    handleBatch(batch, currentOffset);
                }

                nextOffset = reader.lastContainedLogOffset() + 1;
            }
        } else if (nextOffset == STARTING_NEXT_OFFSET) {
            // Listener just started and there are no snapshots; set the nextOffset to
            // 0 to start reading the log
            nextOffset = SMALLEST_LOG_OFFSET;
        }
    }

    private void handleBatch(Batch<?> batch, OptionalLong overrideOffset) {
        int offsetDelta = 0;
        for (ControlRecord record : batch.controlRecords()) {
            long currentOffset = overrideOffset.orElse(batch.baseOffset() + offsetDelta);
            switch (record.type()) {
                case KRAFT_VOTERS:
                    VoterSet voters = VoterSet.fromVotersRecord((VotersRecord) record.message());
                    kafkaRaftMetrics.updateNumVoters(voters.size());
                    if (!staticVoterSet.isEmpty()) {
                        externalKRaftMetrics.setIgnoredStaticVoters(true);
                    }
                    logger.info("Latest set of voters is {} at offset {}", voters, currentOffset);
                    synchronized (voterSetHistory) {
                        voterSetHistory.addAt(currentOffset, voters);
                    }
                    break;

                case KRAFT_VERSION:
                    KRaftVersion kraftVersion = KRaftVersion.fromFeatureLevel(
                        ((KRaftVersionRecord) record.message()).kRaftVersion()
                    );
                    logger.info(
                        "Latest {} is {} at offset {}",
                        KRaftVersion.FEATURE_NAME,
                        kraftVersion,
                        currentOffset
                    );
                    synchronized (kraftVersionHistory) {
                        kraftVersionHistory.addAt(currentOffset, kraftVersion);
                    }
                    break;

                default:
                    // Skip the rest of the control records
                    break;
            }
            ++offsetDelta;
        }
    }
}

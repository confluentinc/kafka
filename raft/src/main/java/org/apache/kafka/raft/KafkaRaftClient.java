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
package org.apache.kafka.raft;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchQuorumRecordsRequestData;
import org.apache.kafka.common.message.FetchQuorumRecordsResponseData;
import org.apache.kafka.common.message.FindQuorumRequestData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.LeaderChangeMessage.Voter;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.ConnectionCache.ConnectionState;
import org.apache.kafka.raft.ConnectionCache.HostInfo;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class implements a Kafkaesque version of the Raft protocol. Leader election
 * is more or less pure Raft, but replication is driven by replica fetching and we use Kafka's
 * log reconciliation protocol to truncate the log to a common point following each leader
 * election.
 *
 * Like Zookeeper, this protocol distinguishes between voters and observers. Voters are
 * the only ones who are eligible to handle protocol requests and they are the only ones
 * who take part in elections. The protocol does not yet support dynamic quorum changes.
 *
 * These are the APIs in this protocol:
 *
 * 1) {@link VoteRequestData}: Sent by valid voters when their election timeout expires and they
 *    become a candidate. This request includes the last offset in the log which electors use
 *    to tell whether or not to grant the vote.
 *
 * 2) {@link BeginQuorumEpochRequestData}: Sent by the leader of an epoch only to valid voters to
 *    assert its leadership of the new epoch. This request will be retried indefinitely for
 *    each voter until it acknowledges the request or a new election occurs.
 *
 *    This is not needed in usual Raft because the leader can use an empty data push
 *    to achieve the same purpose. The Kafka Raft implementation, however, is driven by
 *    fetch requests from followers, so there must be a way to find the new leader after
 *    an election has completed.
 *
 *    We might consider replacing this API and let followers use FindLeader even if they
 *    are voters.
 *
 * 3) {@link EndQuorumEpochRequestData}: Sent by the leader of an epoch to valid voters in order to
 *    gracefully resign from the current epoch. This causes remaining voters to immediately
 *    begin a new election.
 *
 * 4) {@link FetchQuorumRecordsRequestData}: This is basically the same as the usual Fetch API in
 *    Kafka, however the protocol implements it as a separate request type because there
 *    is additional metadata which we need to piggyback on responses. Unlike partition replication,
 *    we also piggyback truncation detection on this API rather than through a separate state.
 *
 * 5) {@link FindQuorumRequestData}: Sent by observers in order to find the leader. The leader
 *    is responsible for pushing BeginQuorumEpoch requests to other votes, but it is the responsibility
 *    of observers to find the current leader themselves. We could probably use one of the Fetch
 *    APIs for the same purpose, but we separated it initially for clarity.
 *
 */
public class KafkaRaftClient implements RaftClient {
    private final AtomicReference<GracefulShutdown> shutdown = new AtomicReference<>();
    private final Logger logger;
    private final Time time;
    private final Timer electionTimer;
    private final int electionTimeoutMs;
    private final int electionJitterMs;
    private final int requestTimeoutMs;
    private final long bootTimestamp;
    private final InetSocketAddress advertisedListener;
    private final NetworkChannel channel;
    private final ReplicatedLog log;
    private final QuorumState quorum;
    private final Random random;
    private final ConnectionCache connections;

    private BlockingQueue<PendingAppendRequest> unsentAppends;
    private DistributedStateMachine stateMachine;

    public KafkaRaftClient(NetworkChannel channel,
                           ReplicatedLog log,
                           QuorumState quorum,
                           Time time,
                           InetSocketAddress advertisedListener,
                           List<InetSocketAddress> bootstrapServers,
                           int electionTimeoutMs,
                           int electionJitterMs,
                           int retryBackoffMs,
                           int requestTimeoutMs,
                           LogContext logContext,
                           Random random) {
        this.channel = channel;
        Objects.requireNonNull(log, "Log instance cannot be null");
        this.log = log;
        this.quorum = quorum;
        this.time = time;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionJitterMs = electionJitterMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.bootTimestamp = time.milliseconds();
        this.advertisedListener = advertisedListener;
        this.logger = logContext.logger(KafkaRaftClient.class);
        this.random = random;
        this.connections = new ConnectionCache(channel, bootstrapServers,
            retryBackoffMs, requestTimeoutMs, logContext);
        this.unsentAppends = new ArrayBlockingQueue<>(10);
        this.connections.maybeUpdate(quorum.localId, new HostInfo(advertisedListener, bootTimestamp));
    }

    private void applyCommittedRecordsToStateMachine() {
        quorum.highWatermark().ifPresent(highWatermark -> {
            while (stateMachine.position().offset < highWatermark && shutdown.get() == null) {
                OffsetAndEpoch position = stateMachine.position();
                Records records = readCommitted(position, highWatermark);
                stateMachine.apply(records);
                logger.trace("Applied committed records at {} to the state machine; position " +
                    "updated to {}", position, stateMachine.position());
            }
        });
    }

    private void updateFollowerHighWatermark(FollowerState state, OptionalLong highWatermarkOpt) {
        highWatermarkOpt.ifPresent(highWatermark -> {
            long newHighWatermark = Math.min(endOffset().offset, highWatermark);
            state.updateHighWatermark(OptionalLong.of(newHighWatermark));
            highWatermarkOpt.ifPresent(log::updateHighWatermark);
            logger.trace("Follower high watermark updated to {}", newHighWatermark);
            applyCommittedRecordsToStateMachine();
        });
    }

    private void updateLeaderEndOffset(LeaderState state) {
        if (state.updateLocalEndOffset(log.endOffset())) {
            logger.trace("Leader high watermark updated to {} after end offset updated to {}",
                    state.highWatermark(), log.endOffset());
            applyCommittedRecordsToStateMachine();
        }
    }

    private void updateReplicaEndOffset(LeaderState state, int replicaId, long endOffset) {
        if (state.updateEndOffset(replicaId, endOffset)) {
            logger.trace("Leader high watermark updated to {} after replica {} end offset updated to {}",
                    state.highWatermark(), replicaId, endOffset);
            applyCommittedRecordsToStateMachine();
        }
    }

    @Override
    public void initialize(DistributedStateMachine stateMachine) throws IOException {
        this.stateMachine = stateMachine;
        quorum.initialize(new OffsetAndEpoch(log.endOffset(), log.lastFetchedEpoch()));

        if (quorum.isLeader()) {
            electionTimer.reset(Long.MAX_VALUE);
            onBecomeLeader(quorum.leaderStateOrThrow());
        } else if (quorum.isCandidate()) {
            // If the quorum consists of a single node, we can become leader immediately
            electionTimer.reset(electionTimeoutMs);
            maybeBecomeLeader(quorum.candidateStateOrThrow());
        } else if (quorum.isFollower()) {
            FollowerState state = quorum.followerStateOrThrow();
            if (quorum.isVoter() && quorum.epoch() == 0) {
                // If we're initializing for the first time, become a candidate immediately
                becomeCandidate();
            } else {
                electionTimer.reset(electionTimeoutMs);
                if (state.hasLeader())
                    onBecomeFollowerOfElectedLeader(quorum.followerStateOrThrow());
            }
        }
    }

    private OffsetAndEpoch endOffset() {
        return new OffsetAndEpoch(log.endOffset(), log.lastFetchedEpoch());
    }

    private void resetConnections() {
        connections.resetAll();
    }

    private void onBecomeLeader(LeaderState state) {
        stateMachine.becomeLeader(quorum.epoch());
        updateLeaderEndOffset(state);

        // Add a control message for faster high watermark advance.
        appendControlRecord(MemoryRecords.withLeaderChangeMessage(
            time.milliseconds(),
            quorum.epoch(),
            new LeaderChangeMessage()
                .setLeaderId(state.election().leaderId())
                .setVoters(
                    state.followers().stream().map(
                        follower -> new Voter().setVoterId(follower)).collect(Collectors.toList())))
        );

        log.assignEpochStartOffset(quorum.epoch(), log.endOffset());
        electionTimer.reset(Long.MAX_VALUE);
        resetConnections();
    }

    private void appendControlRecord(Records controlRecord) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");
        log.appendAsLeader(controlRecord, quorum.epoch());
    }

    private void maybeBecomeLeader(CandidateState state) throws IOException {
        if (state.isVoteGranted()) {
            long endOffset = log.endOffset();
            LeaderState leaderState = quorum.becomeLeader(endOffset);
            onBecomeLeader(leaderState);
        }
    }

    private void becomeCandidate() throws IOException {
        electionTimer.reset(electionTimeoutMs + randomElectionJitterMs());
        CandidateState state = quorum.becomeCandidate();
        maybeBecomeLeader(state);
        resetConnections();
    }

    private void becomeUnattachedFollower(int epoch) throws IOException {
        boolean isLeader = quorum.isLeader();
        if (quorum.becomeUnattachedFollower(epoch)) {
            resetConnections();

            if (isLeader) {
                electionTimer.reset(electionTimeoutMs);
            }
        }
    }

    private void becomeVotedFollower(int candidateId, int epoch) throws IOException {
        if (quorum.becomeVotedFollower(epoch, candidateId)) {
            electionTimer.reset(electionTimeoutMs);
            resetConnections();
        }
    }

    private void onBecomeFollowerOfElectedLeader(FollowerState state) {
        stateMachine.becomeFollower(state.epoch());
        electionTimer.reset(electionTimeoutMs);
        resetConnections();
    }

    private void becomeFetchingFollower(int leaderId, int epoch) throws IOException {
        if (quorum.becomeFetchingFollower(epoch, leaderId)) {
            onBecomeFollowerOfElectedLeader(quorum.followerStateOrThrow());
        }
    }

    private void becomeFollower(OptionalInt leaderId, int epoch) throws IOException {
        if (leaderId.isPresent()) {
            becomeFetchingFollower(leaderId.getAsInt(), epoch);
        } else {
            becomeUnattachedFollower(epoch);
        }
    }

    private VoteResponseData buildVoteResponse(Errors error, boolean voteGranted) {
        return new VoteResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil())
                .setVoteGranted(voteGranted);
    }

    /**
     * Handle a Vote request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#INVALID_REQUEST} if the last epoch or offset are invalid
     */
    private VoteResponseData handleVoteRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        VoteRequestData request = (VoteRequestData) requestMetadata.data;
        Optional<Errors> errorOpt = validateVoterOnlyRequest(request.candidateId(), request.candidateEpoch());
        if (errorOpt.isPresent()) {
            return buildVoteResponse(errorOpt.get(), false);
        }

        if (quorum.isLeader()) {
            logger.debug("Ignoring vote request {} since we are the leader of epoch {}",
                    request, quorum.epoch());
            return buildVoteResponse(Errors.NONE, false);
        } else if (quorum.isCandidate()) {
            logger.debug("Ignoring vote request {} since we are a candidate of epoch {}",
                    request, quorum.epoch());
            return buildVoteResponse(Errors.NONE, false);
        } else {
            FollowerState state = quorum.followerStateOrThrow();
            int candidateId = request.candidateId();
            final boolean voteGranted;
            if (state.hasLeader()) {
                voteGranted = false;
            } else if (state.hasVoted()) {
                voteGranted = state.isVotedCandidate(candidateId);
            } else {
                int lastEpoch = request.lastEpoch();
                long lastEpochEndOffset = request.lastEpochEndOffset();
                if (lastEpochEndOffset < 0 || lastEpoch < 0 || lastEpoch >= request.candidateEpoch()) {
                    return buildVoteResponse(Errors.INVALID_REQUEST, false);
                }

                OffsetAndEpoch lastEpochEndOffsetAndEpoch = new OffsetAndEpoch(
                    lastEpochEndOffset, lastEpoch);
                voteGranted = lastEpochEndOffsetAndEpoch.compareTo(endOffset()) >= 0;
            }

            if (voteGranted)
                becomeVotedFollower(candidateId, request.candidateEpoch());
            return buildVoteResponse(Errors.NONE, voteGranted);
        }
    }

    /**
     * Handle a received Vote response.
     *
     * @param responseMetadata The inbound response
     * @return true if the response was handled successfully and does not need to be retried, false otherwise
     */
    private boolean handleVoteResponse(
        RaftResponse.Inbound responseMetadata
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        VoteResponseData response = (VoteResponseData) responseMetadata.data;
        Errors error = Errors.forCode(response.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(response.leaderId());
        int responseEpoch = response.leaderEpoch();

        Optional<Boolean> handled = maybeHandleStandardError(error, responseLeaderId, responseEpoch);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            if (quorum.isLeader()) {
                logger.debug("Ignoring vote response {} since we already became leader for epoch {}",
                    response, quorum.epoch());
            } else if (quorum.isFollower()) {
                logger.debug("Ignoring vote response {} since we are now a follower for epoch {}",
                    response, quorum.epoch());
            } else {
                CandidateState state = quorum.candidateStateOrThrow();
                if (response.voteGranted()) {
                    state.recordGrantedVote(remoteNodeId);
                    maybeBecomeLeader(state);
                } else {
                    state.recordRejectedVote(remoteNodeId);
                    if (state.isVoteRejected()) {
                        logger.info("Insufficient remaining votes to become leader (rejected by {}). " +
                                "We will await expiration of election timeout before retrying",
                            state.rejectingVoters());
                    }
                }
            }
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private int randomElectionJitterMs() {
        if (electionJitterMs == 0)
            return 0;
        return random.nextInt(electionJitterMs);
    }

    private BeginQuorumEpochResponseData buildBeginQuorumEpochResponse(Errors error) {
        return new BeginQuorumEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    /**
     * Handle a BeginEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private BeginQuorumEpochResponseData handleBeginQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        BeginQuorumEpochRequestData request = (BeginQuorumEpochRequestData) requestMetadata.data;
        Optional<Errors> errorOpt = validateVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildBeginQuorumEpochResponse(errorOpt.get());
        }

        int requestLeaderId = request.leaderId();
        int requestEpoch = request.leaderEpoch();
        becomeFetchingFollower(requestLeaderId, requestEpoch);
        return buildBeginQuorumEpochResponse(Errors.NONE);
    }

    /**
     * Handle a received BeginQuorumEpoch response.
     *
     * @param responseMetadata The inbound response
     * @return true if the response was handled successfully and does not need to be retried, false otherwise
     */
    private boolean handleBeginQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata
    ) throws IOException {
        int remoteNodeId = responseMetadata.sourceId();
        BeginQuorumEpochResponseData response = (BeginQuorumEpochResponseData) responseMetadata.data;
        Errors error = Errors.forCode(response.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(response.leaderId());
        int responseEpoch = response.leaderEpoch();

        Optional<Boolean> handled = maybeHandleStandardError(error, responseLeaderId, responseEpoch);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            LeaderState state = quorum.leaderStateOrThrow();
            state.addEndorsementFrom(remoteNodeId);
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private EndQuorumEpochResponseData buildEndQuorumEpochResponse(Errors error) {
        return new EndQuorumEpochResponseData()
                .setErrorCode(error.code())
                .setLeaderEpoch(quorum.epoch())
                .setLeaderId(quorum.leaderIdOrNil());
    }

    /**
     * Handle an EndEpoch request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#INCONSISTENT_VOTER_SET} if the request suggests inconsistent voter membership (e.g.
     *      if this node or the sender is not one of the current known voters)
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     */
    private EndQuorumEpochResponseData handleEndQuorumEpochRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        EndQuorumEpochRequestData request = (EndQuorumEpochRequestData) requestMetadata.data;
        Optional<Errors> errorOpt = validateVoterOnlyRequest(request.leaderId(), request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEndQuorumEpochResponse(errorOpt.get());
        }

        // Regardless of our current state, we will become a candidate.
        // Do not become a candidate immediately though.
        electionTimer.reset(randomElectionJitterMs());
        return buildEndQuorumEpochResponse(Errors.NONE);
    }

    /**
     * Handle a received EndQuorumEpoch response.
     *
     * @param responseMetadata The inbound response
     * @return true if the response was handled successfully and does not need to be retried, false otherwise
     */
    private boolean handleEndQuorumEpochResponse(
        RaftResponse.Inbound responseMetadata
    ) throws IOException {
        EndQuorumEpochResponseData response = (EndQuorumEpochResponseData) responseMetadata.data;
        Errors error = Errors.forCode(response.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(response.leaderId());
        int responseEpoch = response.leaderEpoch();

        Optional<Boolean> handled = maybeHandleStandardError(error, responseLeaderId, responseEpoch);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private FetchQuorumRecordsResponseData buildFetchQuorumRecordsResponse(
        Errors error,
        Records records,
        OptionalLong highWatermark
    ) throws IOException {
        return buildEmptyFetchQuorumRecordsResponse(error, highWatermark)
            .setRecords(RaftUtil.serializeRecords(records));
    }

    private FetchQuorumRecordsResponseData buildEmptyFetchQuorumRecordsResponse(
        Errors error,
        OptionalLong highWatermark
    ) {
        return new FetchQuorumRecordsResponseData()
            .setErrorCode(error.code())
            .setLeaderEpoch(quorum.epoch())
            .setLeaderId(quorum.leaderIdOrNil())
            .setRecords(ByteBuffer.wrap(new byte[0]))
            .setHighWatermark(highWatermark.orElse(-1L));
    }

    /**
     * Handle a FetchQuorumRecords request. This API may return the following errors:
     *
     * - {@link Errors#BROKER_NOT_AVAILABLE} if this node is currently shutting down
     * - {@link Errors#FENCED_LEADER_EPOCH} if the epoch is smaller than this node's epoch
     * - {@link Errors#INVALID_REQUEST} if the request epoch is larger than the leader's current epoch
     *     or if either the fetch offset or the last fetched epoch is invalid
     */
    private FetchQuorumRecordsResponseData handleFetchQuorumRecordsRequest(
        RaftRequest.Inbound requestMetadata
    ) throws IOException {
        FetchQuorumRecordsRequestData request = (FetchQuorumRecordsRequestData) requestMetadata.data;
        Optional<Errors> errorOpt = validateLeaderOnlyRequest(request.leaderEpoch());
        if (errorOpt.isPresent()) {
            return buildEmptyFetchQuorumRecordsResponse(errorOpt.get(), OptionalLong.empty());
        }

        int replicaId = request.replicaId();
        long fetchOffset = request.fetchOffset();
        int lastFetchedEpoch = request.lastFetchedEpoch();

        if (fetchOffset < 0 || lastFetchedEpoch < 0 || lastFetchedEpoch > request.leaderEpoch()) {
            return buildEmptyFetchQuorumRecordsResponse(Errors.INVALID_REQUEST, OptionalLong.empty());
        }

        LeaderState state = quorum.leaderStateOrThrow();
        OptionalLong highWatermark = state.highWatermark();
        Optional<OffsetAndEpoch> nextOffsetOpt = validateFetchOffsetAndEpoch(fetchOffset, lastFetchedEpoch);

        if (nextOffsetOpt.isPresent()) {
            OffsetAndEpoch nextOffsetAndEpoch = nextOffsetOpt.get();
            return buildEmptyFetchQuorumRecordsResponse(Errors.NONE, highWatermark)
                .setNextFetchOffset(nextOffsetAndEpoch.offset)
                .setNextFetchOffsetEpoch(nextOffsetAndEpoch.epoch);
        } else if (quorum.isVoter(replicaId)) {
            // Voters can read to the end of the log
            updateReplicaEndOffset(state, replicaId, fetchOffset);
            state.highWatermark().ifPresent(log::updateHighWatermark);
            Records records = log.read(fetchOffset, OptionalLong.empty());
            return buildFetchQuorumRecordsResponse(Errors.NONE, records, highWatermark);
        } else {
            Records records = highWatermark.isPresent() ?
                    log.read(fetchOffset, highWatermark) :
                    MemoryRecords.EMPTY;
            return buildFetchQuorumRecordsResponse(Errors.NONE, records, highWatermark);
        }
    }

    /**
     * Check whether a fetch offset and epoch is valid. Return the offset and epoch to truncate
     * to in case it is not.
     */
    private Optional<OffsetAndEpoch> validateFetchOffsetAndEpoch(long fetchOffset, int lastFetchedEpoch) {
        OffsetAndEpoch endOffsetAndEpoch = log.endOffsetForEpoch(lastFetchedEpoch)
            .orElse(new OffsetAndEpoch(-1L, -1));
        if (endOffsetAndEpoch.epoch != lastFetchedEpoch || endOffsetAndEpoch.offset < fetchOffset) {
            return Optional.of(endOffsetAndEpoch);
        } else {
            return Optional.empty();
        }
    }

    private OptionalInt optionalLeaderId(int leaderIdOrNil) {
        if (leaderIdOrNil < 0)
            return OptionalInt.empty();
        return OptionalInt.of(leaderIdOrNil);
    }

    /**
     * Handle a received FetchQuorumRecords response.
     *
     * @param responseMetadata The inbound response
     * @return true if the response was handled successfully and does not need to be retried, false otherwise
     */
    private boolean handleFetchQuorumRecordsResponse(
        RaftResponse.Inbound responseMetadata
    ) throws IOException {
        FetchQuorumRecordsResponseData response = (FetchQuorumRecordsResponseData) responseMetadata.data;
        Errors error = Errors.forCode(response.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(response.leaderId());
        int responseEpoch = response.leaderEpoch();

        Optional<Boolean> handled = maybeHandleStandardError(error, responseLeaderId, responseEpoch);
        if (handled.isPresent()) {
            return handled.get();
        }

        FollowerState state = quorum.followerStateOrThrow();
        if (error == Errors.NONE) {
            if (response.nextFetchOffset() > 0) {
                // The leader is asking us to truncate before continuing
                OffsetAndEpoch nextFetchOffsetAndEpoch = new OffsetAndEpoch(
                    response.nextFetchOffset(), response.nextFetchOffsetEpoch());

                log.truncateToEndOffset(nextFetchOffsetAndEpoch).ifPresent(truncationOffset -> {
                    logger.info("Truncated to offset {} after out of range error from leader {}",
                        truncationOffset, quorum.leaderIdOrNil());
                });
            } else {
                ByteBuffer recordsBuffer = response.records();
                log.appendAsFollower(MemoryRecords.readableRecords(recordsBuffer));

                OptionalLong highWatermark = response.highWatermark() < 0 ?
                    OptionalLong.empty() : OptionalLong.of(response.highWatermark());
                updateFollowerHighWatermark(state, highWatermark);
                logger.trace("Follower end offset updated to {} after append", endOffset());
            }

            electionTimer.reset(electionTimeoutMs);
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private OptionalLong maybeAppendAsLeader(LeaderState state, Records records) {
        if (state.highWatermark().isPresent() && stateMachine.accept(records)) {
            Long baseOffset = log.appendAsLeader(records, quorum.epoch());
            updateLeaderEndOffset(state);
            logger.trace("Leader appended records at base offset {}, new end offset is {}", baseOffset, endOffset());
            return OptionalLong.of(baseOffset);
        }
        return OptionalLong.empty();
    }

    /**
     * Handle a FindQuorum request. Currently this API does not return any application errors.
     */
    private FindQuorumResponseData handleFindQuorumRequest(
        RaftRequest.Inbound requestMetadata
    ) {
        FindQuorumRequestData request = (FindQuorumRequestData) requestMetadata.data;
        HostInfo hostInfo = new HostInfo(
            new InetSocketAddress(request.host(), request.port()),
            request.bootTimestamp()
        );
        if (quorum.isVoter(request.replicaId())) {
            connections.maybeUpdate(request.replicaId(), hostInfo);
        }

        List<FindQuorumResponseData.Voter> voters = new ArrayList<>();
        for (Map.Entry<Integer, Optional<HostInfo>> voterEntry : connections.allVoters().entrySet()) {
            FindQuorumResponseData.Voter voter = new FindQuorumResponseData.Voter();
            voter.setVoterId(voterEntry.getKey());
            voterEntry.getValue().ifPresent(voterHostInfo -> {
                voter.setHost(voterHostInfo.address.getHostString())
                    .setPort(voterHostInfo.address.getPort())
                    .setBootTimestamp(voterHostInfo.bootTimestamp);
            });
            voters.add(voter);
        }

        return new FindQuorumResponseData()
            .setErrorCode(Errors.NONE.code())
            .setLeaderEpoch(quorum.epoch())
            .setLeaderId(quorum.leaderIdOrNil())
            .setVoters(voters);
    }

    /**
     * Handle a received FindQuorum response.
     *
     * @param responseMetadata The inbound response
     * @return true if the response was handled successfully and does not need to be retried, false otherwise
     */
    private boolean handleFindQuorumResponse(
        RaftResponse.Inbound responseMetadata
    ) throws IOException {
        FindQuorumResponseData response = (FindQuorumResponseData) responseMetadata.data;
        Errors error = Errors.forCode(response.errorCode());
        OptionalInt responseLeaderId = optionalLeaderId(response.leaderId());
        int responseEpoch = response.leaderEpoch();

        // Always update voter connections if they are present, regardless of errors
        updateVoterConnections(response);

        Optional<Boolean> handled = maybeHandleStandardError(error, responseLeaderId, responseEpoch);
        if (handled.isPresent()) {
            return handled.get();
        } else if (error == Errors.NONE) {
            maybeBecomeFollower(responseLeaderId, responseEpoch);
            return true;
        } else {
            return handleUnexpectedError(error, responseMetadata);
        }
    }

    private void updateVoterConnections(FindQuorumResponseData response) {
        for (FindQuorumResponseData.Voter voter : response.voters()) {
            if (voter.host() == null || voter.host().isEmpty())
                continue;
            InetSocketAddress voterAddress = new InetSocketAddress(voter.host(), voter.port());
            connections.maybeUpdate(voter.voterId(),
                new HostInfo(voterAddress, voter.bootTimestamp()));
        }
    }

    private boolean hasConsistentLeader(int epoch, OptionalInt leaderId) {
        // Only elected leaders are sent in the request/response header, so if we have an elected
        // leaderId, it should be consistent with what is in the message.
        if (leaderId.isPresent() && leaderId.getAsInt() == quorum.localId) {
            // The response indicates that we should be the leader, so we verify that is the case
            return quorum.isLeader();
        } else if (epoch != quorum.epoch()) {
            return true;
        } else {
            if (!leaderId.isPresent())
                return true;
            if (!quorum.leaderId().isPresent())
                return true;
            return leaderId.equals(quorum.leaderId());
        }
    }

    /**
     * Handle response errors that are common across request types.
     *
     * @param error Error from the received response
     * @param leaderId Optional leaderId from the response
     * @param epoch Epoch received from the response
     * @return Optional value indicating whether the error was handled here. True indicates
     *   that the response need not be retried, false otherwise.
     */
    private Optional<Boolean> maybeHandleStandardError(
        Errors error,
        OptionalInt leaderId,
        int epoch
    ) throws IOException {
        if (epoch < quorum.epoch()) {
            // We have a larger epoch, so the response is no longer relevant
            return Optional.of(true);
        } else if (error == Errors.FENCED_LEADER_EPOCH) {
            // The response indicates that the request had a stale epoch, but we need
            // to validate the epoch from the response against our current state.
            maybeBecomeFollower(leaderId, epoch);
            return Optional.of(true);
        } else if (error == Errors.BROKER_NOT_AVAILABLE) {
            if (quorum.isObserver()) {
                becomeUnattachedFollower(epoch);
                return Optional.of(true);
            } else {
                return Optional.of(false);
            }
        } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL) {
            // For now we treat this as a fatal error. Once we have support for quorum
            // reassignment, this error could suggest that either we or the recipient of
            // the request just has stale voter information, which means we can retry
            // after backing off.
            throw new IllegalStateException("Received error indicating inconsistent voter sets");
        } else if (error == Errors.INVALID_REQUEST) {
            throw new IllegalStateException("Received unexpected invalid request error");
        }

        return Optional.empty();
    }

    private void maybeBecomeFollower(
        OptionalInt leaderId,
        int epoch
    ) throws IOException {
        if (!hasConsistentLeader(epoch, leaderId)) {
            throw new IllegalStateException("Received response with leader " + leaderId +
                " and epoch " + epoch + " which is inconsistent with current leader " +
                quorum.leaderId() + " and epoch " + quorum.epoch());
        } else if (epoch > quorum.epoch()) {
            // For any request type, if the response indicates a higher epoch, we bump our local
            // epoch and become a follower. Responses only include elected leaders.
            GracefulShutdown gracefulShutdown = shutdown.get();
            if (gracefulShutdown != null) {
                gracefulShutdown.onEpochUpdate(epoch);
            } else {
                becomeFollower(leaderId, epoch);
            }
        } else if (leaderId.isPresent() && !quorum.leaderId().isPresent()) {
            // The response indicates the leader of the current epoch, which is currently unknown
            becomeFetchingFollower(leaderId.getAsInt(), epoch);
        }
    }


    private boolean handleUnexpectedError(Errors error, RaftResponse.Inbound response) {
        logger.error("Unexpected error {} in {} response: {}",
            error, response.data.apiKey(), response);
        return false;
    }

    private void handleResponse(RaftResponse.Inbound response, long currentTimeMs) throws IOException {
        // The response epoch matches the local epoch, so we can handle the response
        ApiKeys apiKey = ApiKeys.forId(response.data.apiKey());
        final boolean handledSuccessfully;

        switch (apiKey) {
            case FETCH_QUORUM_RECORDS:
                handledSuccessfully = handleFetchQuorumRecordsResponse(response);
                break;

            case VOTE:
                handledSuccessfully = handleVoteResponse(response);
                break;

            case BEGIN_QUORUM_EPOCH:
                handledSuccessfully = handleBeginQuorumEpochResponse(response);
                break;

            case END_QUORUM_EPOCH:
                handledSuccessfully = handleEndQuorumEpochResponse(response);
                break;

            case FIND_QUORUM:
                handledSuccessfully = handleFindQuorumResponse(response);
                break;

            default:
                throw new IllegalArgumentException("Received unexpected response type: " + apiKey);
        }

        ConnectionState connection = connections.getOrCreate(response.sourceId());
        if (handledSuccessfully) {
            connection.onResponseReceived(response.correlationId);
        } else {
            connection.onResponseError(response.correlationId, currentTimeMs);
        }
    }

    /**
     * Validate a request which is only valid between voters.
     */
    private Optional<Errors> validateVoterOnlyRequest(int remoteNodeId, int requestEpoch) throws IOException {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (quorum.isObserver() || !quorum.isVoter(remoteNodeId)) {
            return Optional.of(Errors.INCONSISTENT_VOTER_SET);
        } else if (shutdown.get() != null) {
            shutdown.get().onEpochUpdate(requestEpoch);
            return Optional.of(Errors.BROKER_NOT_AVAILABLE);
        }

        // The request is from a valid voter and indicates a larger epoch,
        // so we transition to follower status before handling the request
        if (requestEpoch > quorum.epoch()) {
            becomeUnattachedFollower(requestEpoch);
        }
        return Optional.empty();
    }

    /**
     * Validate a request which is intended for the current quorum leader.
     */
    private Optional<Errors> validateLeaderOnlyRequest(int requestEpoch) {
        if (requestEpoch < quorum.epoch()) {
            return Optional.of(Errors.FENCED_LEADER_EPOCH);
        } else if (requestEpoch > quorum.epoch() || !quorum.isLeader()) {
            // If the epoch in the request is larger than our own epoch or
            // it matches our epoch and we are not a leader, then the request
            // is invalid. Unlike with Kafka topic partitions, leaders will
            // always be the first to know of their status.
            return Optional.of(Errors.INVALID_REQUEST);
        } else if (shutdown.get() != null) {
            return Optional.of(Errors.BROKER_NOT_AVAILABLE);
        }
        return Optional.empty();
    }

    private void handleRequest(RaftRequest.Inbound request) throws IOException {
        ApiKeys apiKey = ApiKeys.forId(request.data.apiKey());
        final ApiMessage responseData;

        switch (apiKey) {
            case FETCH_QUORUM_RECORDS:
                responseData = handleFetchQuorumRecordsRequest(request);
                break;

            case VOTE:
                responseData = handleVoteRequest(request);
                break;

            case BEGIN_QUORUM_EPOCH:
                responseData = handleBeginQuorumEpochRequest(request);
                break;

            case END_QUORUM_EPOCH:
                responseData = handleEndQuorumEpochRequest(request);
                break;

            case FIND_QUORUM:
                responseData = handleFindQuorumRequest(request);
                break;

            default:
                throw new IllegalArgumentException("Unexpected request type " + apiKey);
        }
        sendOutboundMessage(new RaftResponse.Outbound(request.correlationId(), responseData));
    }

    private void handleInboundMessage(RaftMessage message, long currentTimeMs) throws IOException {
        logger.trace("Received inbound message {}", message);

        if (message instanceof RaftRequest.Inbound) {
            RaftRequest.Inbound request = (RaftRequest.Inbound) message;
            handleRequest(request);
        } else if (message instanceof RaftResponse.Inbound) {
            RaftResponse.Inbound response = (RaftResponse.Inbound) message;
            handleResponse(response, currentTimeMs);
        } else {
            throw new IllegalArgumentException("Unexpected message " + message);
        }
    }

    private void sendOutboundMessage(RaftMessage message) {
        channel.send(message);
        logger.trace("Sent outbound message: {}", message);
    }

    private OptionalInt maybeSendRequest(long currentTimeMs,
                                         int destinationId,
                                         Supplier<ApiMessage> requestData) throws IOException {
        ConnectionState connection = connections.getOrCreate(destinationId);
        if (quorum.isObserver() && connection.hasRequestTimedOut(currentTimeMs)) {
            // Observers need to proactively find the leader if there is a request timeout
            becomeUnattachedFollower(quorum.epoch());
        } else if (connection.isReady(currentTimeMs)) {
            int correlationId = channel.newCorrelationId();
            ApiMessage request = requestData.get();
            sendOutboundMessage(new RaftRequest.Outbound(correlationId, request, destinationId, currentTimeMs));
            connection.onRequestSent(correlationId, time.milliseconds());
            return OptionalInt.of(correlationId);
        } else {
            logger.trace("Connection {} is not ready for sending", connection);
        }
        return OptionalInt.empty();
    }

    private EndQuorumEpochRequestData buildEndQuorumEpochRequest() {
        return new EndQuorumEpochRequestData()
                .setReplicaId(quorum.localId)
                .setLeaderId(quorum.leaderIdOrNil())
                .setLeaderEpoch(quorum.epoch());
    }

    private void maybeSendEndQuorumEpoch(long currentTimeMs) throws IOException {
        for (Integer voterId : quorum.remoteVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildEndQuorumEpochRequest);
        }
    }

    private BeginQuorumEpochRequestData buildBeginQuorumEpochRequest() {
        return new BeginQuorumEpochRequestData()
                .setLeaderId(quorum.localId)
                .setLeaderEpoch(quorum.epoch());
    }

    private void maybeSendBeginQuorumEpochToFollowers(long currentTimeMs, LeaderState state) throws IOException {
        for (Integer followerId : state.nonEndorsingFollowers()) {
            maybeSendRequest(currentTimeMs, followerId, this::buildBeginQuorumEpochRequest);
        }
    }

    private VoteRequestData buildVoteRequest() {
        OffsetAndEpoch endOffset = endOffset();
        return new VoteRequestData()
                .setCandidateEpoch(quorum.epoch())
                .setCandidateId(quorum.localId)
                .setLastEpoch(endOffset.epoch)
                .setLastEpochEndOffset(endOffset.offset);
    }

    private void maybeSendVoteRequestToVoters(long currentTimeMs, CandidateState state) throws IOException {
        for (Integer voterId : state.unrecordedVoters()) {
            maybeSendRequest(currentTimeMs, voterId, this::buildVoteRequest);
        }
    }

    private FetchQuorumRecordsRequestData buildFetchQuorumRecordsRequest() {
        return new FetchQuorumRecordsRequestData()
            .setLeaderEpoch(quorum.epoch())
            .setReplicaId(quorum.localId)
            .setFetchOffset(log.endOffset())
            .setLastFetchedEpoch(log.lastFetchedEpoch());
    }

    private void maybeSendFetchQuorumRecords(long currentTimeMs, int leaderId) throws IOException {
        maybeSendRequest(currentTimeMs, leaderId, this::buildFetchQuorumRecordsRequest);
    }

    private FindQuorumRequestData buildFindQuorumRequest() {
        return new FindQuorumRequestData()
            .setReplicaId(quorum.localId)
            .setHost(advertisedListener.getHostString())
            .setPort(advertisedListener.getPort())
            .setBootTimestamp(bootTimestamp);
    }

    private void maybeSendFindQuorum(long currentTimeMs) throws IOException {
        // Find a ready member of the quorum to send FindLeader to. Any voter can receive the
        // FindQuorum, but we only send one request at a time.
        OptionalInt readyNodeIdOpt = connections.findReadyBootstrapServer(currentTimeMs);
        if (readyNodeIdOpt.isPresent()) {
            maybeSendRequest(currentTimeMs, readyNodeIdOpt.getAsInt(), this::buildFindQuorumRequest);
        }
    }

    private void maybeSendRequests(long currentTimeMs) throws IOException {
        if (quorum.isLeader()) {
            LeaderState state = quorum.leaderStateOrThrow();
            maybeSendBeginQuorumEpochToFollowers(currentTimeMs, state);
        } else if (quorum.isCandidate()) {
            CandidateState state = quorum.candidateStateOrThrow();
            maybeSendVoteRequestToVoters(currentTimeMs, state);
        } else {
            FollowerState state = quorum.followerStateOrThrow();
            if (quorum.isObserver() && (!state.hasLeader() || electionTimer.isExpired())) {
                maybeSendFindQuorum(currentTimeMs);
            } else if (state.hasLeader()) {
                int leaderId = state.leaderId();
                maybeSendFetchQuorumRecords(currentTimeMs, leaderId);
            }
        }

        if (connections.hasUnknownVoterEndpoints()) {
            maybeSendFindQuorum(currentTimeMs);
        }
    }

    public boolean isRunning() {
        GracefulShutdown gracefulShutdown = shutdown.get();
        return gracefulShutdown == null || !gracefulShutdown.isFinished();
    }

    private void pollShutdown(GracefulShutdown shutdown) throws IOException {
        // Graceful shutdown allows a leader or candidate to resign its leadership without
        // awaiting expiration of the election timeout. During shutdown, we no longer update
        // quorum state. All we do is check for epoch updates and try to send EndQuorumEpoch request
        // to finish our term. We consider the term finished if we are a follower or if one of
        // the remaining voters bumps the existing epoch.

        shutdown.timer.update();

        if (quorum.isFollower() || quorum.remoteVoters().isEmpty()) {
            // Shutdown immediately if we are a follower or we are the only voter
            shutdown.finished.set(true);
        } else if (!shutdown.isFinished()) {
            long currentTimeMs = shutdown.timer.currentTimeMs();
            maybeSendEndQuorumEpoch(currentTimeMs);

            List<RaftMessage> inboundMessages = channel.receive(shutdown.timer.remainingMs());
            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message, currentTimeMs);
        }
    }

    public void poll() throws IOException {
        GracefulShutdown gracefulShutdown = shutdown.get();
        if (gracefulShutdown != null) {
            pollShutdown(gracefulShutdown);
        } else {
            electionTimer.update();

            if (quorum.isVoter() && electionTimer.isExpired()) {
                logger.debug("Become candidate due to election timeout");
                becomeCandidate();
            }

            maybeSendRequests(electionTimer.currentTimeMs());
            maybeSendOrHandleAppendRequest(electionTimer.currentTimeMs());

            // TODO: Receive time needs to take into account backing off operations that still need doing
            List<RaftMessage> inboundMessages = channel.receive(electionTimer.remainingMs());
            electionTimer.update();

            for (RaftMessage message : inboundMessages)
                handleInboundMessage(message, electionTimer.currentTimeMs());
        }
    }

    private void maybeSendOrHandleAppendRequest(long currentTimeMs) {
        PendingAppendRequest unsentAppend = unsentAppends.poll();
        if (unsentAppend == null || unsentAppend.isCancelled())
            return;

        if (quorum.isLeader()) {
            if (unsentAppend.isTimedOut(currentTimeMs)) {
                unsentAppend.fail(new TimeoutException());
            } else {
                LeaderState leaderState = quorum.leaderStateOrThrow();
                int epoch = quorum.epoch();
                OptionalLong baseOffsetOpt = maybeAppendAsLeader(leaderState, unsentAppend.records);
                if (baseOffsetOpt.isPresent()) {
                    unsentAppend.complete(new OffsetAndEpoch(baseOffsetOpt.getAsLong(), epoch));
                } else {
                    unsentAppend.fail(new InvalidRequestException("Leader refused the append"));
                }
            }
        } else {
            unsentAppend.fail(new NotLeaderForPartitionException("Append refused since this node is no longer " +
                "the leader"));
        }
    }

    /**
     * Append a set of records to the log. Successful completion of the future indicates a success of
     * the append, with the uncommitted base offset and epoch.
     *
     * @param records The records to write to the log
     * @return The uncommitted base offset and epoch of the appended records
     */
    @Override
    public CompletableFuture<OffsetAndEpoch> append(Records records) {
        if (shutdown.get() != null)
            throw new IllegalStateException("Cannot append records while we are shutting down");

        CompletableFuture<OffsetAndEpoch> future = new CompletableFuture<>();
        PendingAppendRequest pendingAppendRequest = new PendingAppendRequest(
            records, future, time.milliseconds(), requestTimeoutMs);

        if (!unsentAppends.offer(pendingAppendRequest)) {
            future.completeExceptionally(new KafkaException("Failed to append records since the unsent " +
                "append queue is full"));
        }
        return future;
    }

    /**
     * Read from the local log. This will only return records which have been committed to the quorum.
     * @param offsetAndEpoch The first offset to read from and the previous consumed epoch
     * @param highWatermark The current high watermark
     * @return A set of records beginning at the request offset
     */
    private Records readCommitted(OffsetAndEpoch offsetAndEpoch, long highWatermark) {
        Optional<OffsetAndEpoch> endOffset = log.endOffsetForEpoch(offsetAndEpoch.epoch);
        if (!endOffset.isPresent() || offsetAndEpoch.offset > endOffset.get().offset) {
            throw new LogTruncationException("The requested offset and epoch " + offsetAndEpoch +
                    " are not in range. The closest offset we found is " + endOffset + ".");
        }
        return log.read(offsetAndEpoch.offset, OptionalLong.of(highWatermark));
    }

    @Override
    public void shutdown(int timeoutMs) {
        // TODO: Safe to access epoch? Need to reset connections to be able to send EndQuorumEpoch? Block until shutdown completes?
        shutdown.set(new GracefulShutdown(timeoutMs, quorum.epoch()));
        channel.wakeup();
    }

    public OptionalLong highWatermark() {
        return quorum.highWatermark();
    }

    private class GracefulShutdown {
        final int epoch;
        final Timer timer;
        final AtomicBoolean finished = new AtomicBoolean(false);

        public GracefulShutdown(int shutdownTimeoutMs, int epoch) {
            this.timer = time.timer(shutdownTimeoutMs);
            this.epoch = epoch;
        }

        public void onEpochUpdate(int epoch) {
            // Shutdown is complete once the epoch has been bumped, which indicates
            // that a new election has been started.

            if (epoch > this.epoch)
                finished.set(true);
        }

        public boolean isFinished() {
            return succeeded() || failed();
        }

        public boolean succeeded() {
            return finished.get();
        }

        public boolean failed() {
            return timer.isExpired();
        }

    }

    private static class PendingAppendRequest {
        private final Records records;
        private final CompletableFuture<OffsetAndEpoch> future;
        private final long createTimeMs;
        private final long requestTimeoutMs;

        private PendingAppendRequest(Records records,
                                     CompletableFuture<OffsetAndEpoch> future,
                                     long createTimeMs,
                                     long requestTimeoutMs) {
            this.records = records;
            this.future = future;
            this.createTimeMs = createTimeMs;
            this.requestTimeoutMs = requestTimeoutMs;
        }

        public void complete(OffsetAndEpoch offsetAndEpoch) {
            future.complete(offsetAndEpoch);
        }

        public void fail(Exception e) {
            future.completeExceptionally(e);
        }

        public boolean isTimedOut(long currentTimeMs) {
            return currentTimeMs > createTimeMs + requestTimeoutMs;
        }

        public boolean isCancelled() {
            return future.isCancelled();
        }
    }

}

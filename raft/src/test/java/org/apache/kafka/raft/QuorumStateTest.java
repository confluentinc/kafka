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

import org.apache.kafka.common.errors.KafkaRaftException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class QuorumStateTest {
    private final int localId = 0;
    private final int logEndEpoch = 0;
    private final MockQuorumStateStore store = new MockQuorumStateStore();

    @Test
    public void testInitializePrimordialEpoch() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters, store);
        assertTrue(state.isFollower());
        assertEquals(0, state.epoch());
        state.becomeCandidate();
        CandidateState candidateState = state.candidateStateOrThrow();
        assertTrue(candidateState.isVoteGranted());
        assertEquals(1, candidateState.epoch());
    }

    @Test
    public void testInitializeAsFollowerWithElectedLeader() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, node1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasLeader());
        assertEquals(epoch, followerState.epoch());
        assertEquals(node1, followerState.leaderId());
    }

    @Test
    public void testInitializeAsFollowerWithVotedCandidate() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, node1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());

        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertEquals(epoch, state.epoch());

        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertEquals(epoch, followerState.epoch());
        assertTrue(followerState.hasVotedFor(node1));
    }

    @Test
    public void testInitializeAsFormerCandidate() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, localId, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isCandidate());
        assertEquals(epoch, state.epoch());

        CandidateState candidateState = state.candidateStateOrThrow();
        assertEquals(epoch, candidateState.epoch());
        assertEquals(Utils.mkSet(node1, node2), candidateState.unrecordedVoters());
    }

    @Test
    public void testInitializeAsFormerLeader() {
        int node1 = 1;
        int node2 = 2;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isLeader());
        assertEquals(epoch, state.epoch());

        LeaderState leaderState = state.leaderStateOrThrow();
        assertEquals(epoch, leaderState.epoch());
        assertEquals(Utils.mkSet(node1, node2), leaderState.nonEndorsingFollowers());
    }

    @Test
    public void testBecomeLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        QuorumState state = initializeEmptyState(voters, store);
        state.becomeCandidate();
        assertTrue(state.isCandidate());

        LeaderState leaderState = state.becomeLeader(0L);

        assertTrue(state.hasLeader());
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(logEndEpoch + 1));
        assertTrue(state.isLeader());
        assertEquals(1, leaderState.epoch());
        assertEquals(OptionalLong.empty(), leaderState.highWatermark());
        assertEquals(OptionalLong.empty(), state.highWatermark());
    }

    @Test
    public void testCannotBecomeLeaderIfAlreadyLeader() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());

        store.writeElectionState(ElectionState.withUnknownLeader(1, voters));

        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        assertFalse(state.hasLeader());

        state.becomeLeader(0L);

        assertTrue(state.hasLeader());
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
        assertTrue(state.isLeader());
    }

    @Test
    public void testCannotBecomeFollowerOfSelf() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        assertNull(store.readElectionState());
        QuorumState state = initializeEmptyState(voters, store);

        assertThrows(KafkaRaftException.class, () -> state.becomeFetchingFollower(localId, 0));
        assertThrows(KafkaRaftException.class, () -> state.becomeVotedFollower(localId, 0));
    }

    @Test
    public void testCannotBecomeLeaderIfCurrentlyFollowing() {
        int leaderId = 1;
        int epoch = 5;
        Set<Integer> voters = Utils.mkSet(localId, leaderId);
        store.writeElectionState(ElectionState.withVotedCandidate(epoch, leaderId, voters));
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
    }

    @Test
    public void testCannotBecomeCandidateIfCurrentlyLeading() throws IOException {
        Set<Integer> voters = Utils.mkSet(localId);
        QuorumState state = initializeEmptyState(voters, store);
        state.becomeCandidate();
        state.becomeLeader(0L);
        assertTrue(state.isLeader());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
    }

    @Test
    public void testCannotBecomeLeaderWithoutGrantedVote() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);

        assertEquals(Collections.singleton(otherNodeId), state.remoteVoters());

        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        assertFalse(state.candidateStateOrThrow().isVoteGranted());
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
        assertThrows(IllegalStateException.class, state::leaderStateOrThrow);
        assertThrows(IllegalStateException.class, state::followerStateOrThrow);

        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        assertTrue(state.candidateStateOrThrow().isVoteGranted());
        state.becomeLeader(0L);
        assertTrue(state.isLeader());
    }

    @Test
    public void testLeaderToFollowerOfElectedLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);

        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertTrue(state.becomeFetchingFollower(otherNodeId, 5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    private QuorumState initializeEmptyState(Set<Integer> voters,
                                             MockQuorumStateStore store) {
        QuorumState state = new QuorumState(localId, voters, store, new LogContext());
        store.writeElectionState(ElectionState.withUnknownLeader(0, voters));
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        return state;
    }

    @Test
    public void testLeaderToUnattachedFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);

        assertTrue(state.hasLeader());
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(logEndEpoch + 1));
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testLeaderToFollowerOfVotedCandidate() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);
        assertTrue(state.becomeVotedFollower(otherNodeId, 5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToFollowerOfElectedLeader() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeFetchingFollower(otherNodeId, 5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.of(otherNodeId), state.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToUnattachedFollower() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeUnattachedFollower(5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCandidateToFollowerOfVotedCandidate() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.becomeVotedFollower(otherNodeId, 5));
        assertEquals(5, state.epoch());
        assertEquals(OptionalInt.empty(), state.leaderId());
        FollowerState followerState = state.followerStateOrThrow();
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateSameEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(otherNodeId, 5);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(5, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testUnattachedFollowerToFollowerOfVotedCandidateHigherEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeVotedFollower(otherNodeId, 8);
        assertEquals(-1, state.leaderIdOrNil());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(otherNodeId));
        assertEquals(ElectionState.withVotedCandidate(8, otherNodeId, voters), store.readElectionState());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeVotedFollower(node1, 5);
        state.becomeFetchingFollower(node2, 5);
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(5, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(5, node2, voters), store.readElectionState());
    }

    @Test
    public void testVotedFollowerToFollowerOfElectedLeaderHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);

        QuorumState state = initializeEmptyState(voters, store);
        assertEquals(Utils.mkSet(node1, node2), state.remoteVoters());

        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertEquals(-1, state.leaderIdOrNil());

        state.becomeVotedFollower(node1, 5);
        assertEquals(-1, state.leaderIdOrNil());

        state.becomeFetchingFollower(node2, 8);
        assertEquals(node2, state.leaderIdOrNil());

        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeVotesInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeVotedFollower(node1, 5);
        assertThrows(KafkaRaftException.class, () -> state.becomeVotedFollower(node2, 5));
        FollowerState followerState = state.followerStateOrThrow();
        assertFalse(followerState.hasLeader());
        assertTrue(followerState.hasVoted());
        assertTrue(followerState.hasVotedFor(node1));
        assertEquals(ElectionState.withVotedCandidate(5, node1, voters), store.readElectionState());
    }

    @Test
    public void testFollowerCannotChangeLeadersInSameEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeFetchingFollower(node2, 8);
        assertThrows(KafkaRaftException.class, () -> state.becomeFetchingFollower(node1, 8));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testFollowerOfElectedLeaderHigherEpoch() throws IOException {
        int node1 = 1;
        int node2 = 2;
        Set<Integer> voters = Utils.mkSet(localId, node1, node2);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeFetchingFollower(node2, 8);
        assertThrows(KafkaRaftException.class, () -> state.becomeFetchingFollower(node1, 8));
        FollowerState followerState = state.followerStateOrThrow();
        assertEquals(8, followerState.epoch());
        assertTrue(followerState.hasLeader());
        assertFalse(followerState.hasVoted());
        assertEquals(node2, followerState.leaderId());
        assertEquals(ElectionState.withElectedLeader(8, node2, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromFollowerToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(otherNodeId, 4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(otherNodeId, 4));
        assertEquals(5, state.epoch());
        assertEquals(ElectionState.withUnknownLeader(5, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromCandidateToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(otherNodeId, 4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(otherNodeId, 4));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withVotedCandidate(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotTransitionFromLeaderToLowerEpoch() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        state.becomeUnattachedFollower(5);
        state.becomeCandidate();
        state.candidateStateOrThrow().recordGrantedVote(otherNodeId);
        state.becomeLeader(0L);

        assertEquals(localId, state.leaderIdOrNil());
        assertThrows(IllegalArgumentException.class, () -> state.becomeUnattachedFollower(4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeVotedFollower(otherNodeId, 4));
        assertThrows(IllegalArgumentException.class, () -> state.becomeFetchingFollower(otherNodeId, 4));
        assertEquals(6, state.epoch());
        assertEquals(ElectionState.withElectedLeader(6, localId, voters), store.readElectionState());
    }

    @Test
    public void testCannotBecomeFollowerOfNonVoter() {
        int otherNodeId = 1;
        int nonVoterId = 2;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertThrows(KafkaRaftException.class, () -> state.becomeVotedFollower(nonVoterId, 4));
        assertThrows(KafkaRaftException.class, () -> state.becomeFetchingFollower(nonVoterId, 4));
    }

    @Test
    public void testObserverCannotBecomeCandidateCandidateOrLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertTrue(state.isFollower());
        assertThrows(IllegalStateException.class, state::becomeCandidate);
        assertThrows(IllegalStateException.class, () -> state.becomeLeader(0L));
    }

    @Test
    public void testObserverDetachLeader() {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(otherNodeId);
        QuorumState state = initializeEmptyState(voters, store);
        state.initialize(new OffsetAndEpoch(0L, logEndEpoch));
        assertTrue(state.isObserver());
        assertTrue(state.isFollower());
        state.becomeFetchingFollower(otherNodeId, 1);
        assertEquals(1, state.epoch());
        // If we disconnect from the leader, we may become an unattached follower with the
        // current epoch so that we can discover the new leader.
        state.becomeUnattachedFollower(1);
        assertEquals(1, state.epoch());
    }

    @Test
    public void testInitializeWithCorruptedStore() {
        MockQuorumStateStore stateStore = new MockQuorumStateStore() {
            @Override
            public ElectionState readElectionState() throws IOException {
                throw new IOException("Could not read corrupted state");
            }
        };

        QuorumState state = new QuorumState(localId, Utils.mkSet(1), stateStore, new LogContext());

        int epoch = 2;
        state.initialize(new OffsetAndEpoch(0L, epoch));
        assertEquals(epoch, state.epoch());
    }

    @Test
    public void testInconsistentVotersBetweenConfigAndState() throws IOException {
        int otherNodeId = 1;
        Set<Integer> voters = Utils.mkSet(localId, otherNodeId);

        QuorumState state = initializeEmptyState(voters, store);

        int unknownVoterId = 2;
        Set<Integer> stateVoters = Utils.mkSet(localId, otherNodeId, unknownVoterId);

        int epoch = 5;
        store.writeElectionState(ElectionState.withElectedLeader(epoch, localId, stateVoters));
        assertThrows(IllegalStateException.class,
            () -> state.initialize(new OffsetAndEpoch(0L, logEndEpoch)));
    }
}

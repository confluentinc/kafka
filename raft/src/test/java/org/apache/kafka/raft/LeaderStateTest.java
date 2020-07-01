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

import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;


public class LeaderStateTest {
    private final int localId = 0;
    private final int epoch = 5;

    @Test
    public void testFollowerEndorsement() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 0L, Utils.mkSet(localId, node1, node2));
        assertEquals(Utils.mkSet(node1, node2), state.nonEndorsingFollowers());
        state.addEndorsementFrom(node1);
        assertEquals(Collections.singleton(node2), state.nonEndorsingFollowers());
        state.addEndorsementFrom(node2);
        assertEquals(Collections.emptySet(), state.nonEndorsingFollowers());
    }

    @Test
    public void testNonFollowerEndorsement() {
        int nonVoterId = 1;
        LeaderState state = new LeaderState(localId, epoch, 0L, Collections.singleton(localId));
        assertThrows(IllegalArgumentException.class, () -> state.addEndorsementFrom(nonVoterId));
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeOne() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testNonMonotonicEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        assertThrows(IllegalArgumentException.class,
            () -> state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(14L)));
    }

    @Test
    public void testIdempotentEndOffsetUpdate() {
        LeaderState state = new LeaderState(localId, epoch, 15L, Collections.singleton(localId));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeTwo() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 10L, Utils.mkSet(localId, otherNodeId));
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(otherNodeId, 0, lastFetchTime -> { }, new LogOffsetMetadata(10L));
        assertEquals(Collections.emptySet(), state.nonEndorsingFollowers());
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
        state.updateReplicaState(otherNodeId, 0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testHighWatermarkUnknownUntilStartOfLeaderEpoch() {
        int otherNodeId = 1;
        LeaderState state = new LeaderState(localId, epoch, 15L, Utils.mkSet(localId, otherNodeId));
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(20L));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(otherNodeId, 0, lastFetchTime -> { }, new LogOffsetMetadata(10L));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(otherNodeId, 0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
    }

    @Test
    public void testUpdateHighWatermarkQuorumSizeThree() {
        int node1 = 1;
        int node2 = 2;
        LeaderState state = new LeaderState(localId, epoch, 10L, Utils.mkSet(localId, node1, node2));
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(node1, 0, lastFetchTime -> { }, new LogOffsetMetadata(10L));
        assertEquals(Collections.singleton(node2), state.nonEndorsingFollowers());
        assertEquals(Optional.of(new LogOffsetMetadata(10L)), state.highWatermark());
        state.updateReplicaState(node2, 0, lastFetchTime -> { }, new LogOffsetMetadata(15L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(20L));
        assertEquals(Optional.of(new LogOffsetMetadata(15L)), state.highWatermark());
        state.updateReplicaState(node2, 0, lastFetchTime -> { }, new LogOffsetMetadata(20L));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
        state.updateReplicaState(node1, 0, lastFetchTime -> { }, new LogOffsetMetadata(20L));
        assertEquals(Optional.of(new LogOffsetMetadata(20L)), state.highWatermark());
    }

    @Test
    public void testGetNonLeaderFollowersByFetchOffsetDescending() {
        int node1 = 1;
        int node2 = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState state = setUpLeaderAndFollowers(node1, node2, leaderStartOffset, leaderEndOffset);

        // Leader should not be included; the follower with larger offset should be prioritized.
        assertEquals(Arrays.asList(node2, node1), state.nonLeaderVotersByDescendingFetchOffset());
    }

    @Test
    public void testGetVoterStates() {
        int node1 = 1;
        int node2 = 2;
        long leaderStartOffset = 10L;
        long leaderEndOffset = 15L;

        LeaderState state = setUpLeaderAndFollowers(node1, node2, leaderStartOffset, leaderEndOffset);

        assertEquals(Arrays.asList(
            new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(localId)
                .setLogEndOffset(leaderEndOffset),
            new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(node1)
                .setLogEndOffset(leaderStartOffset),
            new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(node2)
                .setLogEndOffset(leaderEndOffset)
        ), state.getVoterStates());
    }

    private LeaderState setUpLeaderAndFollowers(int follower1,
                                                int follower2,
                                                long leaderStartOffset,
                                                long leaderEndOffset) {
        LeaderState state = new LeaderState(localId, epoch, leaderStartOffset, Utils.mkSet(localId, follower1, follower2));
        state.updateLocalState(0, lastFetchTime -> { }, new LogOffsetMetadata(leaderEndOffset));
        assertEquals(Optional.empty(), state.highWatermark());
        state.updateReplicaState(follower1, 0, lastFetchTime -> { }, new LogOffsetMetadata(leaderStartOffset));
        state.updateReplicaState(follower2, 0, lastFetchTime -> { }, new LogOffsetMetadata(leaderEndOffset));
        return state;
    }

    @Test
    public void testFetchTimestampUpdated() {
        long endOffset = 10L;

        LeaderState state = new LeaderState(localId, epoch, endOffset, Utils.mkSet(localId));
        long timestamp = 20L;
        AtomicLong latestFetchTime = new AtomicLong(-1L);
        assertTrue(state.updateLocalState(timestamp, latestFetchTime::set, new LogOffsetMetadata(endOffset)));

        assertEquals(timestamp, latestFetchTime.get());
    }

    @Test
    public void testGetObserverStatesWithObserver() {
        int observerId = 10;
        long endOffset = 10L;

        LeaderState state = new LeaderState(localId, epoch, endOffset, Utils.mkSet(localId));
        long timestamp = 20L;
        assertFalse(state.updateReplicaState(observerId, timestamp, lastFetchTime -> { }, new LogOffsetMetadata(endOffset)));

        assertEquals(Collections.singletonList(
            new DescribeQuorumResponseData.ReplicaState()
                .setReplicaId(observerId)
                .setLogEndOffset(endOffset)
        ), state.getObserverStates(timestamp));
    }

    @Test
    public void testNoOpForNegativeRemoteNodeId() {
        int observerId = -1;
        long endOffset = 10L;

        LeaderState state = new LeaderState(localId, epoch, endOffset, Utils.mkSet(localId));
        assertFalse(state.updateReplicaState(observerId, 0, lastFetchTime -> { }, new LogOffsetMetadata(endOffset)));

        assertEquals(Collections.emptyList(), state.getObserverStates(10));
    }
}

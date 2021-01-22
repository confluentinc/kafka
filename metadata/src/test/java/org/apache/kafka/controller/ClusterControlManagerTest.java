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

package org.apache.kafka.controller;

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class ClusterControlManagerTest {
    @Test
    public void testReplay() {
        MockTime time = new MockTime(0, 0, 0);

        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(-1);
        ClusterControlManager clusterControl = new ClusterControlManager(
            new LogContext(), time, snapshotRegistry, 1000,
                new SimpleReplicaPlacementPolicy(new Random()));
        clusterControl.activate();
        assertFalse(clusterControl.unfenced(0));

        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(1);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092).
            setName("PLAINTEXT").
            setHost("example.com"));
        clusterControl.replay(brokerRecord);
        assertFalse(clusterControl.unfenced(0));
        assertFalse(clusterControl.unfenced(1));

        UnfenceBrokerRecord unfenceBrokerRecord =
            new UnfenceBrokerRecord().setId(1).setEpoch(100);
        clusterControl.replay(unfenceBrokerRecord);
        assertFalse(clusterControl.unfenced(0));
        assertTrue(clusterControl.unfenced(1));
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 10})
    public void testChooseRandomRegistered(int numUsableBrokers) throws Exception {
        MockTime time = new MockTime(0, 0, 0);
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(-1);
        MockRandom random = new MockRandom();
        ClusterControlManager clusterControl = new ClusterControlManager(
            new LogContext(), time, snapshotRegistry, 1000,
            new SimpleReplicaPlacementPolicy(random));
        clusterControl.activate();
        for (int i = 0; i < numUsableBrokers; i++) {
            RegisterBrokerRecord brokerRecord =
                new RegisterBrokerRecord().setBrokerEpoch(100).setBrokerId(i);
            brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                setPort((short) 9092).
                setName("PLAINTEXT").
                setHost("example.com"));
            clusterControl.replay(brokerRecord);
            ControllerResult<BrokerHeartbeatReply> result = clusterControl.
                processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                    setBrokerId(i).setBrokerEpoch(100).setCurrentMetadataOffset(1).
                    setShouldFence(false).setShouldShutdown(false), 0);
            assertEquals(new BrokerHeartbeatReply(true, false, false), result.response());
            ControllerTestUtils.replayAll(clusterControl, result.records());
        }
        for (int i = 0; i < numUsableBrokers; i++) {
            assertTrue(clusterControl.unfenced(i),
                String.format("broker %d was not unfenced.", i));
        }
        for (int i = 0; i < 100; i++) {
            List<List<Integer>> results = clusterControl.placeReplicas(1, (short) 3);
            HashSet<Integer> seen = new HashSet<>();
            for (Integer result : results.get(0)) {
                assertTrue(result >= 0);
                assertTrue(result < numUsableBrokers);
                assertTrue(seen.add(result));
            }
        }
    }
}

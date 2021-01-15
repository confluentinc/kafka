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

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(40)
public class ReplicationControlManagerTest {
    private static ReplicationControlManager newReplicationControlManager() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(-1);
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        ClusterControlManager clusterControl =
            new ClusterControlManager(logContext, time, snapshotRegistry, 1000, 100);
        ConfigurationControlManager configurationControl =
            new ConfigurationControlManager(snapshotRegistry, Collections.emptyMap());
        return new ReplicationControlManager(snapshotRegistry,
            new LogContext(),
            new MockRandomSource(),
            3,
            1,
            configurationControl,
            clusterControl);
    }

    private static void registerBroker(int brokerId, ClusterControlManager clusterControl) {
        RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
            setBrokerEpoch(100).setBrokerId(brokerId);
        brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
            setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
            setPort((short) 9092 + brokerId).
            setName("PLAINTEXT").
            setHost("localhost"));
        clusterControl.replay(brokerRecord);
    }

    private static void unfenceBroker(int brokerId, ClusterControlManager clusterControl) {
        UnfenceBrokerRecord unfenceBrokerRecord =
            new UnfenceBrokerRecord().setId(brokerId).setEpoch(100);
        clusterControl.replay(unfenceBrokerRecord);
    }

    @Test
    public void testCreateTopics() throws Exception {
        ReplicationControlManager replicationControl = newReplicationControlManager();
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        request.topics().add(new CreateTopicsRequestData.CreatableTopic().setName("foo").
            setNumPartitions(-1).setReplicationFactor((short) -1));
        ControllerResult<CreateTopicsResponseData> result =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse = new CreateTopicsResponseData();
        expectedResponse.topics().add(new CreatableTopicResult().setName("foo").
            setErrorCode(Errors.INVALID_REPLICATION_FACTOR.code()).
                setErrorMessage("Unable to replicate the partition 3 times: there are only 0 usable brokers"));
        assertEquals(expectedResponse, result.response());

        registerBroker(0, replicationControl.clusterControl);
        unfenceBroker(0, replicationControl.clusterControl);
        registerBroker(1, replicationControl.clusterControl);
        unfenceBroker(1, replicationControl.clusterControl);
        registerBroker(2, replicationControl.clusterControl);
        unfenceBroker(2, replicationControl.clusterControl);
        ControllerResult<CreateTopicsResponseData> result2 =
            replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse2 = new CreateTopicsResponseData();
        expectedResponse2.topics().add(new CreatableTopicResult().setName("foo").
            setNumPartitions(1).setReplicationFactor((short) 3));
        assertEquals(expectedResponse2, result2.response());
    }
}

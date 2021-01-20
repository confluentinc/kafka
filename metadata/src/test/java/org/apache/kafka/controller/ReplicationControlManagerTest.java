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
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.controller.ReplicationControlManager.PartitionControlInfo;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.protocol.Errors.INVALID_TOPIC_EXCEPTION;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(40)
public class ReplicationControlManagerTest {
    private static ReplicationControlManager newReplicationControlManager() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(-1);
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        MockRandom random = new MockRandom();
        ClusterControlManager clusterControl = new ClusterControlManager(
            logContext, time, snapshotRegistry, 1000, 100,
            new SimpleReplicaPlacementPolicy(random));
        ConfigurationControlManager configurationControl =
            new ConfigurationControlManager(snapshotRegistry, Collections.emptyMap());
        return new ReplicationControlManager(snapshotRegistry,
            new LogContext(),
            random,
            (short) 3,
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
        request.topics().add(new CreatableTopic().setName("foo").
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
        ControllerTestUtils.replayAll(replicationControl, result2.records());
        assertEquals(new PartitionControlInfo(new int[] {1, 2, 0},
            new int[] {1, 2, 0}, null, null, 1, 0),
            replicationControl.getPartition(
                ((TopicRecord) result2.records().get(0).message()).topicId(), 0));
        ControllerResult<CreateTopicsResponseData> result3 =
                replicationControl.createTopics(request);
        CreateTopicsResponseData expectedResponse3 = new CreateTopicsResponseData();
        expectedResponse3.topics().add(new CreatableTopicResult().setName("foo").
                setErrorCode(Errors.TOPIC_ALREADY_EXISTS.code()).
                setErrorMessage(Errors.TOPIC_ALREADY_EXISTS.exception().getMessage()));
        assertEquals(expectedResponse3, result3.response());
    }

    @Test
    public void testValidateNewTopicNames() throws Exception {
        Map<String, ApiError> topicErrors = new HashMap<>();
        CreatableTopicCollection topics = new CreatableTopicCollection();
        topics.add(new CreatableTopic().setName(""));
        topics.add(new CreatableTopic().setName("woo"));
        topics.add(new CreatableTopic().setName("."));
        ReplicationControlManager.validateNewTopicNames(topicErrors, topics);
        Map<String, ApiError> expectedTopicErrors = new HashMap<>();
        expectedTopicErrors.put("", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name is illegal, it can't be empty"));
        expectedTopicErrors.put(".", new ApiError(INVALID_TOPIC_EXCEPTION,
            "Topic name cannot be \".\" or \"..\""));
        assertEquals(expectedTopicErrors, topicErrors);
    }
}

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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class BeginQuorumEpochRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<BeginQuorumEpochRequest> {
        private final BeginQuorumEpochRequestData data;

        public Builder(BeginQuorumEpochRequestData data) {
            super(ApiKeys.BEGIN_QUORUM_EPOCH);
            this.data = data;
        }

        @Override
        public BeginQuorumEpochRequest build(short version) {
            return new BeginQuorumEpochRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final BeginQuorumEpochRequestData data;

    private BeginQuorumEpochRequest(BeginQuorumEpochRequestData data, short version) {
        super(ApiKeys.BEGIN_QUORUM_EPOCH, version);
        this.data = data;
    }

    public BeginQuorumEpochRequest(Struct struct, short version) {
        super(ApiKeys.BEGIN_QUORUM_EPOCH, version);
        this.data = new BeginQuorumEpochRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public BeginQuorumEpochResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        BeginQuorumEpochResponseData data = new BeginQuorumEpochResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new BeginQuorumEpochResponse(data);
    }

    public static BeginQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                               int leaderEpoch,
                                                               int leaderId) {
        return singletonRequest(topicPartition, null, leaderEpoch, leaderId);
    }

    public static BeginQuorumEpochRequestData singletonRequest(TopicPartition topicPartition,
                                                               String clusterId,
                                                               int leaderEpoch,
                                                               int leaderId) {
        return new BeginQuorumEpochRequestData()
                   .setClusterId(clusterId)
                   .setTopics(Collections.singletonList(
                       new BeginQuorumEpochRequestData.BeginQuorumTopicRequest()
                           .setTopicName(topicPartition.topic())
                           .setPartitions(Collections.singletonList(
                               new BeginQuorumEpochRequestData.BeginQuorumPartitionRequest()
                                   .setPartitionIndex(topicPartition.partition())
                                   .setLeaderEpoch(leaderEpoch)
                                   .setLeaderId(leaderId))))
                   );
    }


    public static BeginQuorumEpochResponseData getPartitionLevelErrorResponse(BeginQuorumEpochRequestData data, Errors error) {
        short errorCode = error.code();
        List<BeginQuorumEpochResponseData.BeginQuorumTopicResponse> topicResponses = new ArrayList<>();
        for (BeginQuorumEpochRequestData.BeginQuorumTopicRequest topic : data.topics()) {
            topicResponses.add(
                new BeginQuorumEpochResponseData.BeginQuorumTopicResponse()
                    .setTopicName(topic.topicName())
                    .setPartitions(topic.partitions().stream().map(
                        requestPartition -> new BeginQuorumEpochResponseData.BeginQuorumPartitionResponse()
                                                .setPartitionIndex(requestPartition.partitionIndex())
                                                .setErrorCode(errorCode)
                    ).collect(Collectors.toList())));
        }

        return new BeginQuorumEpochResponseData().setTopics(topicResponses);
    }


    public static BeginQuorumEpochResponseData getTopLevelErrorResponse(Errors error) {
        return new BeginQuorumEpochResponseData().setErrorCode(error.code());
    }
}

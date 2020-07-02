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
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VoteResponse extends AbstractResponse {
    public final VoteResponseData data;

    public VoteResponse(VoteResponseData data) {
        this.data = data;
    }

    public VoteResponse(Struct struct, short version) {
        this.data = new VoteResponseData(struct, version);
    }

    public VoteResponse(Struct struct) {
        short latestVersion = (short) (VoteResponseData.SCHEMAS.length - 1);
        this.data = new VoteResponseData(struct, latestVersion);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static VoteResponseData singletonResponse(TopicPartition topicPartition,
                                                     int leaderId,
                                                               int leaderEpoch,
                                                               long highWatermark,
                                                               List<DescribeQuorumResponseData.ReplicaState> voterStates,
                                                               List<DescribeQuorumResponseData.ReplicaState> observerStates) {
        return new VoteResponseData()
                   .setTopics(Collections.singletonList(new VoteResponseData.VoteTopicResponse()
                                                            .setTopicName(topicPartition.topic())
                                                            .setPartitions(Collections.singletonList(new VoteResponseData.VotePartitionResponse()
                                                                                                         .setErrorCode(Errors.NONE.code())
                                                                                                         .setLeaderId(leaderId)
                                                                                                         .setLeaderEpoch(leaderEpoch)))));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errors = new HashMap<>();
        for (VoteResponseData.VoteTopicResponse topicResponse : data.topics()) {
            for (VoteResponseData.VotePartitionResponse partitionResponse : topicResponse.partitions()) {
                errors.compute(Errors.forCode(partitionResponse.errorCode()),
                    (error, count) -> count == null ? 1 : count + 1);
            }
        }
        return errors;
    }

    public static VoteResponse parse(ByteBuffer buffer, short version) {
        return new VoteResponse(ApiKeys.VOTE.responseSchema(version).read(buffer), version);
    }
}

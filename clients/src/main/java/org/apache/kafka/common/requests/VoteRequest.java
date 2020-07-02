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

import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VoteRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<VoteRequest> {
        private final VoteRequestData data;

        public Builder(VoteRequestData data) {
            super(ApiKeys.VOTE);
            this.data = data;
        }

        @Override
        public VoteRequest build(short version) {
            return new VoteRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final VoteRequestData data;

    private VoteRequest(VoteRequestData data, short version) {
        super(ApiKeys.VOTE, version);
        this.data = data;
    }

    public VoteRequest(Struct struct, short version) {
        super(ApiKeys.VOTE, version);
        this.data = new VoteRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return getErrorResponse(this.data, Errors.forException(e));
    }

    public static VoteResponse getErrorResponse(VoteRequestData data, Errors error) {
        short errorCode = error.code();

        List<VoteResponseData.VoteTopicResponse> topicResponses = new ArrayList<>();
        for (VoteRequestData.VoteTopicRequest topic : data.topics()) {
            topicResponses.add(
                new VoteResponseData.VoteTopicResponse()
                    .setTopicName(topic.topicName())
                    .setPartitions(topic.partitions().stream().map(
                        requestPartition -> new VoteResponseData.VotePartitionResponse()
                                                .setPartitionIndex(requestPartition.partitionIndex())
                                                .setErrorCode(errorCode)
                    ).collect(Collectors.toList())));
        }

        return new VoteResponse(
            new VoteResponseData().setTopics(topicResponses));
    }
}

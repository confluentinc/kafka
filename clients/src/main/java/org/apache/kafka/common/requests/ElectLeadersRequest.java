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

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersRequestData.TopicPartitions;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.Readable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ElectLeadersRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ElectLeadersRequest> {
        private final ElectionType electionType;
        private final Collection<TopicPartition> topicPartitions;
        private final int timeoutMs;

        public Builder(ElectionType electionType, Collection<TopicPartition> topicPartitions, int timeoutMs) {
            super(ApiKeys.ELECT_LEADERS);
            this.electionType = electionType;
            this.topicPartitions = topicPartitions;
            this.timeoutMs = timeoutMs;
        }

        @Override
        public ElectLeadersRequest build(short version) {
            return new ElectLeadersRequest(toRequestData(version), version);
        }

        @Override
        public String toString() {
            return "ElectLeadersRequest("
                + "electionType=" + electionType
                + ", topicPartitions=" + ((topicPartitions == null) ? "null" : MessageUtil.deepToString(topicPartitions.iterator()))
                + ", timeoutMs=" + timeoutMs
                + ")";
        }

        private ElectLeadersRequestData toRequestData(short version) {
            if (electionType != ElectionType.PREFERRED && version == 0) {
                throw new UnsupportedVersionException("API Version 0 only supports PREFERRED election type");
            }

            ElectLeadersRequestData data = new ElectLeadersRequestData()
                .setTimeoutMs(timeoutMs);

            if (topicPartitions != null) {
                topicPartitions.forEach(tp -> {
                    ElectLeadersRequestData.TopicPartitions tps = data.topicPartitions().find(tp.topic());
                    if (tps == null) {
                        tps = new ElectLeadersRequestData.TopicPartitions().setTopic(tp.topic());
                        data.topicPartitions().add(tps);
                    }
                    tps.partitions().add(tp.partition());
                });
            } else {
                data.setTopicPartitions(null);
            }

            data.setElectionType(electionType.value);

            return data;
        }
    }


    public Set<TopicPartition> topicPartitions() {
        if (this.data.topicPartitions() == null) {
            return Collections.emptySet();
        }
        return this.data.topicPartitions().stream()
            .flatMap(topicPartition -> topicPartition.partitions().stream()
                    .map(partitionId -> new TopicPartition(topicPartition.topic(), partitionId))
            )
            .collect(Collectors.toSet());
    }

    public ElectionType electionType() {
        if (this.version() == 0) {
            return ElectionType.PREFERRED;
        }
        return ElectionType.valueOf(this.data.electionType());
    }

    private final ElectLeadersRequestData data;

    private ElectLeadersRequest(ElectLeadersRequestData data, short version) {
        super(ApiKeys.ELECT_LEADERS, version);
        this.data = data;
    }

    @Override
    public ElectLeadersRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        List<ReplicaElectionResult> electionResults = new ArrayList<>();

        if (data.topicPartitions() != null) {
            for (TopicPartitions topic : data.topicPartitions()) {
                ReplicaElectionResult electionResult = new ReplicaElectionResult();

                electionResult.setTopic(topic.topic());
                for (Integer partitionId : topic.partitions()) {
                    PartitionResult partitionResult = new PartitionResult();
                    partitionResult.setPartitionId(partitionId);
                    partitionResult.setErrorCode(apiError.error().code());
                    partitionResult.setErrorMessage(apiError.message());

                    electionResult.partitionResult().add(partitionResult);
                }

                electionResults.add(electionResult);
            }
        }

        return new ElectLeadersResponse(throttleTimeMs, apiError.error().code(), electionResults, version());
    }

    public static ElectLeadersRequest parse(Readable readable, short version) {
        return new ElectLeadersRequest(new ElectLeadersRequestData(readable, version), version);
    }
}

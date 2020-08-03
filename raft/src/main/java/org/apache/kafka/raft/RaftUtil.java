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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.BeginQuorumEpochRequestData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.EndQuorumEpochRequestData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchQuorumRecordsResponseData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.BeginQuorumEpochRequest;
import org.apache.kafka.common.requests.EndQuorumEpochRequest;
import org.apache.kafka.common.requests.VoteRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.OptionalInt;

public class RaftUtil {

    public static ByteBuffer serializeRecords(Records records) throws IOException {
        if (records instanceof MemoryRecords) {
            MemoryRecords memoryRecords = (MemoryRecords) records;
            return memoryRecords.buffer();
        } else if (records instanceof FileRecords) {
            FileRecords fileRecords = (FileRecords) records;
            ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());
            fileRecords.readInto(buffer, 0);
            return buffer;
        } else {
            throw new UnsupportedOperationException("Serialization not yet supported for " + records.getClass());
        }
    }

    public static ApiMessage errorResponse(ApiKeys apiKey,
                                           Errors error) {
        return errorResponse(apiKey, error, 0, OptionalInt.empty());
    }

    public static ApiMessage errorResponse(
        ApiKeys apiKey,
        Errors error,
        int epoch,
        OptionalInt leaderIdOpt
    ) {
        int leaderId = leaderIdOpt.orElse(-1);
        switch (apiKey) {
            case VOTE:
                return VoteRequest.getTopLevelErrorResponse(error);
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochRequest.getTopLevelErrorResponse(error);
            case END_QUORUM_EPOCH:
                return EndQuorumEpochRequest.getTopLevelErrorResponse(error);
            case FETCH_QUORUM_RECORDS:
                return new FetchQuorumRecordsResponseData()
                    .setErrorCode(error.code())
                    .setLeaderEpoch(epoch)
                    .setLeaderId(leaderId)
                    .setHighWatermark(-1)
                    .setRecords(ByteBuffer.wrap(new byte[0]));
            case FIND_QUORUM:
                return new FindQuorumResponseData()
                    .setErrorCode(error.code())
                    .setLeaderEpoch(leaderId)
                    .setLeaderId(epoch);
            default:
                throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        }
    }

    static boolean hasValidTopicPartition(VoteResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(VoteRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(BeginQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(EndQuorumEpochResponseData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }

    static boolean hasValidTopicPartition(DescribeQuorumRequestData data, TopicPartition topicPartition) {
        return data.topics().size() == 1 &&
                   data.topics().get(0).topicName().equals(topicPartition.topic()) &&
                   data.topics().get(0).partitions().size() == 1 &&
                   data.topics().get(0).partitions().get(0).partitionIndex() == topicPartition.partition();
    }
}

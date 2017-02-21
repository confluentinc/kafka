/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ProduceRequest extends AbstractRequest {
    private static final String TRANSACTIONAL_ID_KEY_NAME = "transactional_id";
    private static final String ACKS_KEY_NAME = "acks";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String TOPIC_DATA_KEY_NAME = "topic_data";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_DATA_KEY_NAME = "data";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";
    private static final String RECORD_SET_KEY_NAME = "record_set";

    public static class Builder extends AbstractRequest.Builder<ProduceRequest> {
        private final short acks;
        private final int timeout;
        private final Map<TopicPartition, MemoryRecords> partitionRecords;

        public Builder(short acks, int timeout, Map<TopicPartition, MemoryRecords> partitionRecords) {
            super(ApiKeys.PRODUCE);
            this.acks = acks;
            this.timeout = timeout;
            this.partitionRecords = partitionRecords;
        }

        @Override
        public ProduceRequest build() {
            short version = version();
            if (version < 2) {
                throw new UnsupportedVersionException("ProduceRequest versions older than 2 are not supported.");
            }
            return new ProduceRequest(version, acks, timeout, partitionRecords);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ProduceRequest")
                    .append(", acks=").append(acks)
                    .append(", timeout=").append(timeout)
                    .append(", partitionRecords=(").append(Utils.mkString(partitionRecords))
                    .append("))");
            return bld.toString();
        }
    }

    private final short acks;
    private final int timeout;
    private final String transactionalId;
    private final Map<TopicPartition, MemoryRecords> partitionRecords;

    private ProduceRequest(short version, short acks, int timeout, Map<TopicPartition, MemoryRecords> partitionRecords) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.PRODUCE.id, version)), version);
        Map<String, Map<Integer, MemoryRecords>> recordsByTopic = CollectionUtils.groupDataByTopic(partitionRecords);
        struct.set(ACKS_KEY_NAME, acks);
        struct.set(TIMEOUT_KEY_NAME, timeout);


        // TODO: Include transactional id once transactions are supported
        String transactionalId = null;
        if (version >= 3)
            struct.set(TRANSACTIONAL_ID_KEY_NAME, transactionalId);

        List<Struct> topicDatas = new ArrayList<>(recordsByTopic.size());
        for (Map.Entry<String, Map<Integer, MemoryRecords>> topicEntry : recordsByTopic.entrySet()) {
            Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.getKey());
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, MemoryRecords> partitionEntry : topicEntry.getValue().entrySet()) {
                MemoryRecords records = partitionEntry.getValue();
                Struct part = topicData.instance(PARTITION_DATA_KEY_NAME)
                                       .set(PARTITION_KEY_NAME, partitionEntry.getKey());

                if (version >= 3)
                    validateRecords(version, records);

                part.set(RECORD_SET_KEY_NAME, records);
                partitionArray.add(part);
            }
            topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
            topicDatas.add(topicData);
        }
        struct.set(TOPIC_DATA_KEY_NAME, topicDatas.toArray());
        this.acks = acks;
        this.timeout = timeout;
        this.transactionalId = transactionalId;
        this.partitionRecords = partitionRecords;
    }

    public ProduceRequest(Struct struct, short version) {
        super(struct, version);
        partitionRecords = new HashMap<>();
        for (Object topicDataObj : struct.getArray(TOPIC_DATA_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicData.getArray(PARTITION_DATA_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                MemoryRecords records = (MemoryRecords) partitionResponse.getRecords(RECORD_SET_KEY_NAME);

                if (version >= 3)
                    validateRecords(version, records);

                partitionRecords.put(new TopicPartition(topic, partition), records);
            }
        }
        acks = struct.getShort(ACKS_KEY_NAME);
        timeout = struct.getInt(TIMEOUT_KEY_NAME);
        transactionalId = version >= 3 ? struct.getString(TRANSACTIONAL_ID_KEY_NAME) : null;
    }

    private void validateRecords(short version, MemoryRecords records) {
        if (version >= 3) {
            Iterator<LogEntry.MutableLogEntry> iterator = records.entries().iterator();
            if (!iterator.hasNext())
                throw new InvalidRecordException("Version 3 and above of the produce request contained no log entries");

            LogEntry.MutableLogEntry entry = iterator.next();
            if (entry.magic() != LogEntry.MAGIC_VALUE_V2)
                throw new InvalidRecordException("Version 3 and above of the produce request is only allowed to " +
                        "contain log entries with magic version 2");

            if (iterator.hasNext())
                throw new InvalidRecordException("Version 3 and above of the produce request is only allowed to " +
                        "contain more than one log entry");
        }
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        /* In case the producer doesn't actually want any response */
        if (acks == 0)
            return null;

        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new HashMap<>();
        ProduceResponse.PartitionResponse partitionResponse = new ProduceResponse.PartitionResponse(Errors.forException(e));

        for (Map.Entry<TopicPartition, MemoryRecords> entry : partitionRecords.entrySet())
            responseMap.put(entry.getKey(), partitionResponse);

        short versionId = version();
        switch (versionId) {
            case 0:
                return new ProduceResponse(responseMap);
            case 1:
            case 2:
            case 3:
                return new ProduceResponse(responseMap, ProduceResponse.DEFAULT_THROTTLE_TIME, versionId);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.PRODUCE.id)));
        }
    }

    public short acks() {
        return acks;
    }

    public int timeout() {
        return timeout;
    }

    public String transactionalId() {
        return transactionalId;
    }

    public Map<TopicPartition, MemoryRecords> partitionRecords() {
        return partitionRecords;
    }

    public void clearPartitionRecords() {
        struct.clear();
        partitionRecords.clear();
    }

    public static ProduceRequest parse(ByteBuffer buffer, int versionId) {
        return new ProduceRequest(ProtoUtils.parseRequest(ApiKeys.PRODUCE.id, versionId, buffer), (short) versionId);
    }

    public static ProduceRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.PRODUCE.id));
    }
}

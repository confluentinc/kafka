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

package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.node.TopicImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents a topic in the metadata image.
 *
 * This class is thread-safe.
 */
public record TopicImage(String name, Uuid id, Map<Integer, PartitionRegistration> partitions) {
    public TopicImage {
        partitions = Collections.unmodifiableMap(partitions);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        writer.write(0, new TopicRecord().
            setName(name).
            setTopicId(id));
        for (Entry<Integer, PartitionRegistration> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            PartitionRegistration partition = entry.getValue();
            writer.write(partition.toRecord(id, partitionId, options));
        }
    }

    @Override
    public String toString() {
        return new TopicImageNode(this).stringify();
    }
}

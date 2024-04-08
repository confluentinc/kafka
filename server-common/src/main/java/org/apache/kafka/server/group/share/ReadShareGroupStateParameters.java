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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.message.ReadShareGroupStateRequestData;

import java.util.stream.Collectors;

public class ReadShareGroupStateParameters implements PersisterParameters {

  private final GroupTopicPartitionData groupTopicPartitionData;

  private ReadShareGroupStateParameters(GroupTopicPartitionData groupTopicPartitionData) {
    this.groupTopicPartitionData = groupTopicPartitionData;
  }

  public GroupTopicPartitionData groupTopicPartitionData() {
    return groupTopicPartitionData;
  }

  public static ReadShareGroupStateParameters from(ReadShareGroupStateRequestData data) {
//    {
//      groupId,
//      topics[
//          topicId,
//          partitions [
//              partitionId
//          ]
//      ]
//    }
    return new Builder()
        .setGroupTopicPartitionData(new GroupTopicPartitionData(data.groupId(), data.topics().stream()
            .map(readStateData -> new TopicData(readStateData.topicId(),
                readStateData.partitions().stream()
                    .map(partitionData -> new PartitionData(partitionData.partition(), -1, -1, (short) 0, null))
                    .collect(Collectors.toList())))
            .collect(Collectors.toList())))
        .build();
  }

  public static class Builder {
    private GroupTopicPartitionData groupTopicPartitionData;

    public Builder setGroupTopicPartitionData(GroupTopicPartitionData groupTopicPartitionData) {
      this.groupTopicPartitionData = groupTopicPartitionData;
      return this;
    }

    public ReadShareGroupStateParameters build() {
      return new ReadShareGroupStateParameters(groupTopicPartitionData);
    }
  }
}

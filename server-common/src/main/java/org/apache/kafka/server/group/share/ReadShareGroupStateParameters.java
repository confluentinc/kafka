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

public class ReadShareGroupStateParameters implements PersisterResult {

  private final GroupTopicPartitionData groupTopicPartitionData;

  private ReadShareGroupStateParameters(GroupTopicPartitionData groupTopicPartitionData) {
    this.groupTopicPartitionData = groupTopicPartitionData;
  }

  public GroupTopicPartitionData groupTopicPartitionData() {
    return groupTopicPartitionData;
  }

  public static ReadShareGroupStateParameters from(ReadShareGroupStateRequestData data) {
    return new Builder()
        .setGroupTopicPartitionData(new GroupTopicPartitionData(data.groupId(), data.topicId(), data.partition()))
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

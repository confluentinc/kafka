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

import java.util.List;
import java.util.Objects;

public class GroupTopicPartitionData {
  private final String groupId;
  private final List<TopicData> topicsData;

  public GroupTopicPartitionData(String groupId, List<TopicData> topicsData) {
    this.groupId = groupId;
    this.topicsData = topicsData;
  }

  public String groupId() {
    return groupId;
  }

  public List<TopicData> topicsData() {
    return topicsData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    GroupTopicPartitionData that = (GroupTopicPartitionData) o;
    return Objects.equals(groupId, that.groupId) && Objects.equals(topicsData, that.topicsData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, topicsData);
  }

  public static class Builder {
    private String groupId;
    private List<TopicData> topicsData;

    public Builder setGroupId(String groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setTopicsData(List<TopicData> topicsData) {
      this.topicsData = topicsData;
      return this;
    }

    public Builder setGroupTopicPartition(GroupTopicPartitionData groupTopicPartitionData) {
      this.groupId = groupTopicPartitionData.groupId();
      this.topicsData = groupTopicPartitionData.topicsData();
      return this;
    }

    public GroupTopicPartitionData build() {
      return new GroupTopicPartitionData(this.groupId, this.topicsData);
    }
  }
}

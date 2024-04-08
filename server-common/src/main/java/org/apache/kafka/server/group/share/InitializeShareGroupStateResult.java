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

import org.apache.kafka.common.message.InitializeShareGroupStateResponseData;

import java.util.List;
import java.util.stream.Collectors;

public class InitializeShareGroupStateResult implements PersisterResult {
  private final List<TopicData> topicsData;

  private InitializeShareGroupStateResult(List<TopicData> topicsData) {
    this.topicsData = topicsData;
  }

  public List<TopicData> topicsData() {
    return topicsData;
  }

  public static InitializeShareGroupStateResult from(InitializeShareGroupStateResponseData data) {
    return new Builder()
        .setTopicsData(data.results().stream()
            .map(initializeStateResult -> new TopicData(initializeStateResult.topicId(),
                initializeStateResult.partitions().stream()
                    .map(partitionResult -> new PartitionData(partitionResult.partition(), -1, -1, partitionResult.errorCode(), null))
                    .collect(Collectors.toList())))
            .collect(Collectors.toList()))
        .build();
  }

  public static class Builder {
    private List<TopicData> topicsData;

    public Builder setTopicsData(List<TopicData> topicsData) {
      this.topicsData = topicsData;
      return this;
    }

    public InitializeShareGroupStateResult build() {
      return new InitializeShareGroupStateResult(topicsData);
    }
  }
}

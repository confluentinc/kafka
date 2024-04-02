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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.WriteShareGroupStateRequestData;

import java.util.List;
import java.util.stream.Collectors;

public class WriteShareGroupStateParameters implements PersisterParamResult {
  private final String groupId;
  private final Uuid topicId;
  private final int partition;
  private final int stateEpoch;
  private final long startOffset;
  private final List<PersisterStateBatch> stateBatches;

  private WriteShareGroupStateParameters(String groupId, Uuid topicId, int partition, int stateEpoch, long startOffset, List<PersisterStateBatch> stateBatches) {
    this.groupId = groupId;
    this.topicId = topicId;
    this.partition = partition;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
    this.stateBatches = stateBatches;
  }

  public String getGroupId() {
    return groupId;
  }

  public Uuid getTopicId() {
    return topicId;
  }

  public int getPartition() {
    return partition;
  }

  public int getStateEpoch() {
    return stateEpoch;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public List<PersisterStateBatch> getStateBatches() {
    return stateBatches;
  }

  public static WriteShareGroupStateParameters from(WriteShareGroupStateRequestData data) {
    return new Builder()
        .setGroupId(data.groupId())
        .setTopicId(data.topicId())
        .setPartition(data.partition())
        .setStateEpoch(data.stateEpoch())
        .setStartOffset(data.startOffset())
        .setStateBatches(data.stateBatches().stream()
            .map(PersisterStateBatch::from)
            .collect(Collectors.toList()))
        .build();
  }

  public static class Builder {
    private String groupId;
    private Uuid topicId;
    private int partition;
    private int stateEpoch;
    private long startOffset;
    private List<PersisterStateBatch> stateBatches;

    public Builder setGroupId(String groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder setTopicId(Uuid topicId) {
      this.topicId = topicId;
      return this;
    }

    public Builder setPartition(int partition) {
      this.partition = partition;
      return this;
    }

    public Builder setStateEpoch(int stateEpoch) {
      this.stateEpoch = stateEpoch;
      return this;
    }

    public Builder setStartOffset(long startOffset) {
      this.startOffset = startOffset;
      return this;
    }

    public Builder setStateBatches(List<PersisterStateBatch> stateBatches) {
      this.stateBatches = stateBatches;
      return this;
    }

    public WriteShareGroupStateParameters build() {
      return new WriteShareGroupStateParameters(groupId, topicId, partition, stateEpoch, startOffset, stateBatches);
    }
  }
}

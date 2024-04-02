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

public class InitializeShareGroupStateRequestDTO implements PersisterDTO {

  private final String groupId;
  private final Uuid topicId;
  private final int partition;
  private final int stateEpoch;
  private final long startOffset;

  private InitializeShareGroupStateRequestDTO(String groupId, Uuid topicId, int partition, int stateEpoch, int startOffset) {
    this.groupId = groupId;
    this.topicId = topicId;
    this.partition = partition;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
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

  public static class Builder {
    private String groupId;
    private Uuid topicId;
    private int partition;
    private int stateEpoch;
    private int startOffset;
    public void setGroupId(String groupId) {
      this.groupId = groupId;
    }

    public void setTopicId(Uuid topicId) {
      this.topicId = topicId;
    }

    public void setPartition(int partition) {
      this.partition = partition;
    }

    public void setStateEpoch(int stateEpoch) {
      this.stateEpoch = stateEpoch;
    }

    public void setStartOffset(int startOffset) {
      this.startOffset = startOffset;
    }

    public InitializeShareGroupStateRequestDTO build() {
      return new InitializeShareGroupStateRequestDTO(this.groupId, this.topicId, this.partition, this.stateEpoch, this.startOffset);
    }
  }
}

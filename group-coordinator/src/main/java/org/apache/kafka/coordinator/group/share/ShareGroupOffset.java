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

package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.message.WriteShareGroupStateRequestData;
import org.apache.kafka.server.group.share.PersisterStateBatch;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ShareGroupOffset {
  public final int snapshotEpoch;
  public final int stateEpoch;
  public final int leaderEpoch;
  public final long startOffset;
  public final List<PersisterStateBatch> stateBatches;

  public ShareGroupOffset(int snapshotEpoch,
                          int stateEpoch,
                          int leaderEpoch,
                          long startOffset,
                          List<PersisterStateBatch> stateBatches) {
    this.snapshotEpoch = snapshotEpoch;
    this.stateEpoch = stateEpoch;
    this.leaderEpoch = leaderEpoch;
    this.startOffset = startOffset;
    this.stateBatches = stateBatches;
  }

  public static ShareGroupOffset fromRecord() {
    return null;
  }

  public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data) {
    return fromRequest(data, 0);
  }

  public static ShareGroupOffset fromRequest(WriteShareGroupStateRequestData.PartitionData data, int snapshotEpoch) {
    return new ShareGroupOffset(snapshotEpoch,
        data.stateEpoch(),
        data.leaderEpoch(),
        data.startOffset(),
        data.stateBatches().stream()
            .map(PersisterStateBatch::from)
            .collect(Collectors.toList()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShareGroupOffset that = (ShareGroupOffset) o;
    return snapshotEpoch == that.snapshotEpoch && stateEpoch == that.stateEpoch && leaderEpoch == that.leaderEpoch && startOffset == that.startOffset && Objects.equals(stateBatches, that.stateBatches);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshotEpoch, stateEpoch, leaderEpoch, startOffset, stateBatches);
  }
}

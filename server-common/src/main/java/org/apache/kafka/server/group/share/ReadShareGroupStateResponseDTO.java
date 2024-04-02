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

public class ReadShareGroupStateResponseDTO implements PersisterDTO {
  private final short errorCode;
  private final int stateEpoch;
  private final long startOffset;
  private final List<PersisterStateBatch> stateBatches;

  private ReadShareGroupStateResponseDTO(short errorCode, int stateEpoch, long startOffset, List<PersisterStateBatch> stateBatches) {
    this.errorCode = errorCode;
    this.stateEpoch = stateEpoch;
    this.startOffset = startOffset;
    this.stateBatches = stateBatches;
  }

  public short getErrorCode() {
    return errorCode;
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

  public static class Builder {

    private short errorCode;
    private int stateEpoch;
    private long startOffset;
    private List<PersisterStateBatch> stateBatches;

    public void setErrorCode(short errorCode) {
      this.errorCode = errorCode;
    }

    public void setStateEpoch(int stateEpoch) {
      this.stateEpoch = stateEpoch;
    }

    public void setStartOffset(long startOffset) {
      this.startOffset = startOffset;
    }

    public void setStateBatches(List<PersisterStateBatch> stateBatches) {
      this.stateBatches = stateBatches;
    }

    public ReadShareGroupStateResponseDTO build() {
      return new ReadShareGroupStateResponseDTO(errorCode, stateEpoch, startOffset, stateBatches);
    }
  }
}

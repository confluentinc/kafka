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

import org.apache.kafka.common.annotation.InterfaceStability;

@InterfaceStability.Evolving
public class DefaultStatePersister {
  /**
   * The InitializeShareGroupState API is used by the group coordinator to initialize the share-partition state.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request InitializeShareGroupStateRequestDTO
   * @return InitializeShareGroupStateResponseDTO
   */
  public InitializeShareGroupStateResponseDTO initializeShareGroupState(InitializeShareGroupStateRequestDTO request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The ReadShareGroupState API is used by share-partition leaders to read share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupStateRequestDTO
   * @return ReadShareGroupStateResponseDTO
   */
  public ReadShareGroupStateResponseDTO readShareGroupState(ReadShareGroupStateRequestDTO request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The WriteShareGroupState API is used by share-partition leaders to write share-partition state to a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param requestDTO WriteShareGroupStateRequestDTO
   * @return WriteShareGroupStateResponseDTO
   */
  public WriteShareGroupStateResponseDTO writeShareGroupState(WriteShareGroupStateRequestDTO requestDTO) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The DeleteShareGroupState API is used by the group coordinator to delete share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request DeleteShareGroupStateRequestDTO
   * @return DeleteShareGroupStateResponseDTO
   */
  public DeleteShareGroupStateResponseDTO deleteShareGroupState(DeleteShareGroupStateRequestDTO request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The ReadShareGroupOffsetsState API is used by the group coordinator to read the offset information from share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupOffsetsStateRequestDTO
   * @return ReadShareGroupOffsetsStateRequestDTO
   */
  public ReadShareGroupOffsetsStateResponseDTO readShareGroupOffsets(ReadShareGroupOffsetsStateRequestDTO request) {
    throw new RuntimeException("not implemented");
  }
}

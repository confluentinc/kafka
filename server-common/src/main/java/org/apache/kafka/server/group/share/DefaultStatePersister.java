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

import java.util.concurrent.CompletableFuture;

/**
 * The default implementation of the {@link Persister} interface which is used by the
 * group coordinator and share-partition leaders to manage the durable share-partition state.
 * This implementation uses inter-broker RPCs to make requests with the share coordinator
 * which is responsible for persisting the share-partition state.
 */
@InterfaceStability.Evolving
public class DefaultStatePersister implements Persister {
  /**
   * The InitializeShareGroupState API is used by the group coordinator to initialize the share-partition state.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request InitializeShareGroupStateParameters
   * @return InitializeShareGroupStateResult
   */
  public CompletableFuture<InitializeShareGroupStateResult> initializeShareGroupState(InitializeShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The ReadShareGroupState API is used by share-partition leaders to read share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupStateParameters
   * @return ReadShareGroupStateResult
   */
  public CompletableFuture<ReadShareGroupStateResult> readShareGroupState(ReadShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The WriteShareGroupState API is used by share-partition leaders to write share-partition state to a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request WriteShareGroupStateParameters
   * @return WriteShareGroupStateResult
   */
  public CompletableFuture<WriteShareGroupStateResult> writeShareGroupState(WriteShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The DeleteShareGroupState API is used by the group coordinator to delete share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request DeleteShareGroupStateParameters
   * @return DeleteShareGroupStateResult
   */
  public CompletableFuture<DeleteShareGroupStateResult> deleteShareGroupState(DeleteShareGroupStateParameters request) {
    throw new RuntimeException("not implemented");
  }

  /**
   * The ReadShareGroupOffsetsState API is used by the group coordinator to read the offset information from share-partition state from a share coordinator.
   * This is an inter-broker RPC authorized as a cluster action.
   *
   * @param request ReadShareGroupOffsetsStateParameters
   * @return ReadShareGroupOffsetsStateParameters
   */
  public CompletableFuture<ReadShareGroupOffsetsStateResult> readShareGroupOffsets(ReadShareGroupOffsetsStateParameters request) {
    throw new RuntimeException("not implemented");
  }
}
